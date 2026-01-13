import logging
from pathlib import Path
import re


RE_ARCGIS_ERR = re.compile(
    r"""^\s*
        (?:  (?P<ts>\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:[.,]\d{3,6})?)\s* )?
        ERROR\s+(?P<code>\d{3,6})\s*:\s*(?P<msg>.+?)\s*$
    """,
    re.IGNORECASE | re.VERBOSE,
)

# Will find 'Failed to execute (Tool).' line (often follows an ERROR 000### in Esri tools)
RE_ARCGIS_FAILED = re.compile(r"^\s*Failed to execute\s*\((?P<tool>[^)]+)\)\.\s*$", re.IGNORECASE)

class FailFastLogWatcher(logging.Handler):
    """
    Watches logger records for errors AND (optionally) scans the active log file
    for any occurrence of 'error' (case-insensitive).
    """
    ERROR_WORD = re.compile(r"\berror\b", re.IGNORECASE)

    def __init__(self, log_file: Path | None, poll_file: bool = True):
        super().__init__(level=logging.INFO)
        self.log_file = Path(log_file) if log_file else None
        self.poll_file = poll_file and self.log_file is not None
        self._error_msg: str | None = None

    @property
    def error_message(self) -> str | None:
        return self._error_msg

    def emit(self, record):
            msg = record.getMessage()
            if record.levelno >= logging.ERROR or RE_ARCGIS_ERR.search(msg) or self.ERROR_WORD.search(msg):
                self._error_msg = self._error_msg or msg

    def scan_file_once(self) -> None:
        """Catch raw lines written outside the logger that contain 'error'."""
        if not self.poll_file or not self.log_file:
            return
        try:
            if self.log_file:
                txt = self.log_file.read_text(encoding="utf-8", errors="ignore")
                if RE_ARCGIS_ERR.search(txt) or self.ERROR_WORD.search(txt):
                    self._error_msg = self._error_msg or "Error detected in log file"
        except Exception:
            pass