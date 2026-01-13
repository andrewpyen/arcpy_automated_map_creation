import re
from typing import List, Dict, Tuple, Optional, Union
import os
import glob
from datetime import datetime
import logging
from pathlib import Path as FSPath
from logging.handlers import RotatingFileHandler

from app.models.response_models import LogEntry, LogLevelFilter, LogsByLevel

# Regex that matches text like: `2025-08-18 10:24:39,480 | INFO | custom_tool.job.tmpyca4elw6 | Job started`
# (?P<ts>.+?)           ==> Captures the timestamp at the beginning (2025-08-18 10:24:39,480). .+? means "match one or more characters, but as few as possible" (lazy).
# \s*\|\s*              ==> Matches spaces, then a pipe |, then spaces. This is the delimiter.
# (?P<level>DEBUG|INFO|WARNING|ERROR|CRITICAL)  ==> Captures the log level word (one of these 5).
# (?P<logger>[^|]+)     ==> Captures the logger name (custom_tool.job.tmpyca4elw6).
# [^|]+                 ==> Means "one or more characters that are not a pipe".
# (?P<msg>.*)           ==> Captures the message (Job started), which is everything after the last pipe.
RE_PIPE = re.compile(
    r"^(?P<ts>.+?)\s*\|\s*(?P<level>DEBUG|INFO|WARNING|ERROR|CRITICAL)\s*\|\s*(?P<logger>[^|]+)\s*\|\s*(?P<msg>.*)$"
)

# Regex that matches text like: `2025-08-18 10:24:39,480 - INFO - Job started`
# (?P<ts>.+?)           ==> Matches timestamp again (2025-08-18 10:24:39,480).
# \s*-\s*               ==> Matches spaces, dash, spaces.
# (?P<level>DEBUG|INFO|WARNING|ERROR|CRITICAL) ==> Log level word.
# (?P<msg>.*)           ==> The message text (e.g. "Job started").
RE_DASH = re.compile(
    r"^(?P<ts>.+?)\s*-\s*(?P<level>DEBUG|INFO|WARNING|ERROR|CRITICAL)\s*-\s*(?P<msg>.*)$"
)

def _parse_structured(line: str) -> Optional[LogEntry]:
    """ Parses each log line to search for matches using Regex. """
    matched = RE_PIPE.match(line) or RE_DASH.match(line)
    if not matched:
        return None
    ts_str = matched.group("ts")
    try:
        # Try parsing as datetime
        ts = datetime.fromisoformat(ts_str.replace(",", "."))
    except Exception:
        ts = ts_str  # fallback: keep raw string if parsing fails
    return LogEntry(
        ts=ts, 
        level=matched.group("level"), 
        logger=matched.groupdict().get("logger") or "", 
        msg=matched.group("msg")
    )

def collect_logs_grouped_all(output_dir: str, max_files: int = 50) -> LogsByLevel:
    """
    Parse ALL lines from log files in <output_dir>/logs and group by level.
    * Structured lines (pipe/dash) are parsed normally.
    * Raw lines that contain 'error'/'warning' (case-insensitive) are promoted to ERROR/WARNING entries.
    * Bare lines that don't match anything are appended to the previous entry (multi-line GP messages).
    """
    # logs_dir = os.path.join(output_dir, "logs")
    logs_dir = FSPath(output_dir) / "logs"
    output_logslevel_list = LogsByLevel(info=[], warning=[], error=[])

    if not logs_dir.is_dir():
        return output_logslevel_list

    # Build candidate list as Path objects
    candidates: List[FSPath] = []
    candidates += list(logs_dir.glob("*.txt"))
    candidates += list(logs_dir.glob("*.log"))

    if not candidates:
        return LogsByLevel(info=[], warning=[], error=[], note="No log files found")

    # Sort by mtime (oldest -> newest)
    candidates.sort(key=lambda p: p.stat().st_mtime)

    for path in candidates[:max_files]:
        last_entry: Optional[LogEntry] = None
        file_hint_ts = datetime.fromtimestamp(path.stat().st_mtime).isoformat()

        try:
            with path.open("r", encoding="utf-8", errors="replace") as f:
                for raw in f:
                    # Dict olding all entries for meesages
                    parsed = _parse_structured(raw)
                    if parsed:
                        entry: LogEntry = LogEntry(
                            ts=parsed.ts,
                            level=parsed.level, 
                            logger=parsed.logger,
                            msg=parsed.msg,
                            file=path.name
                        )
                        if parsed.level in ("ERROR", "CRITICAL"):
                            output_logslevel_list["error"].append(entry)
                        elif parsed.level == "WARNING":
                            output_logslevel_list["warning"].append(entry)
                        else:
                            output_logslevel_list["info"].append(entry)
                        last_entry = entry
                        continue

                    line = raw.rstrip("\r\n")
                    if not line.strip():
                        # blank separator
                        continue

                    lower = line.lower()
                    if "error" in lower:
                        entry: LogEntry = LogEntry(
                            ts=file_hint_ts,
                            level="ERROR", 
                            logger="raw",
                            msg=line,
                            file=path.name
                        )
                        output_logslevel_list["error"].append(entry)
                        last_entry = entry
                    elif "warning" in lower:
                        entry: LogEntry = LogEntry(
                            ts=file_hint_ts,
                            level="WARNING", 
                            logger="raw",
                            msg=line,
                            file=path.name
                        )
                        output_logslevel_list["warning"].append(entry)
                        last_entry = entry
                    else:
                        # continuation of previous message (e.g., GP dumps after WARNING/ERROR)
                        if last_entry is not None:
                            last_entry["msg"] = f"{last_entry['msg']}\n{line}"
                        else:
                            entry: LogEntry = LogEntry(
                                ts=file_hint_ts,
                                level="INFO", 
                                logger="raw",
                                msg=line,
                                file=path.name
                            )
                            output_logslevel_list["info"].append(entry)
                            last_entry = entry
        except Exception as e:
            entry: LogEntry = LogEntry(
                ts=datetime.now().isoformat(),
                level="ERROR", 
                logger="log.reader",
                msg=f"Failed reading {path.name}: {e}",
                file=path.name
            )
            output_logslevel_list["error"].append(entry)

    # return LogsByLevel(info=by_level["INFO"], warning=by_level["WARNING"], error=by_level["ERROR"], note="Logs found.")
    return output_logslevel_list



def build_job_logger(job_id: str, output_dir: str, debug: bool=False) -> logging.Logger:
    logs_dir = FSPath(output_dir) / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    logger_name = f"survey_mapper.job.{job_id}"
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    # Avoid duplicate handlers if called again
    if not logger.handlers:
        log_path = logs_dir / f"log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        fh = RotatingFileHandler(log_path, maxBytes=10_000_000, backupCount=5, encoding="utf-8")
        fh.setLevel(logging.INFO)
        fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s"))
        logger.addHandler(fh)

        if debug:
            # Also echo to console for debugging
            sh = logging.StreamHandler()
            sh.setLevel(logging.INFO)
            sh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s"))
            logger.addHandler(sh)

    return logger


def filter_logs_by_level(all_logs: LogsByLevel, level: LogLevelFilter) -> LogsByLevel:
    if level == LogLevelFilter.all:
        return all_logs
    if level == LogLevelFilter.info:
        return LogsByLevel(info=all_logs.info, warning=[], error=[], note=all_logs.note)
    if level == LogLevelFilter.warning:
        return LogsByLevel(info=[], warning=all_logs.warning, error=[], note=all_logs.note)
    if level == LogLevelFilter.error:
        return LogsByLevel(info=[], warning=[], error=all_logs.error, note=all_logs.note)
    return all_logs

