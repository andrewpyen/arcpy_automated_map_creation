import os
import time
import mimetypes
import zipfile
import tempfile
import sqlite3
import logging
import pandas as pd
import uuid
import re
from enum import Enum
from logging.handlers import RotatingFileHandler
from datetime import datetime
from io import BytesIO
from pathlib import Path as FSPath
from typing import Annotated, List, Dict, Union, Optional
from dotenv import load_dotenv
from fastapi import APIRouter, FastAPI, HTTPException, UploadFile, File, Query, Body, BackgroundTasks
from fastapi.responses import JSONResponse, FileResponse
from threading import Event, Lock

# Local imports
from app.utils import helpers
from app.api.file_access.file_access import save_upload_to_temp_excel, zip_directory
from app.custom_logging.custom_logger import build_job_logger, collect_logs_grouped_all, filter_logs_by_level
from app.dbconnector.database_connector import DatabaseConnector
from app.api.survey_audit.survey_mapper_class import SurveyMapper
from app.models.response_models import (
    JobStatus,
    ErrorResponse,
    HealthResponse,
    JobQueuedResponse,
    LogLevelFilter,
    LogsByLevel
)
from app.custom_logging.fail_fast_logger import FailFastLogWatcher
from app.config_loading.settings import get_settings, refresh_settings
from app.config_loading.config_loader import get_config
from app.config_loading.zip_registry_single import (
    list_zip_files_single,
    latest_zip_name_single,
    zip_path_single,
    build_zip_enum,
    refresh_zip_enum,
)
from app.api.config_routes import config_router, wire_dynamic_enums_and_links 

# Build the Enum once on startup so OpenAPI has the choices
ZipNameEnum = build_zip_enum()

# Shows a dropdown in Swagger, but stays a plain str at runtime
SurveyTypeParam = Annotated[
    str | None,
    Query(
        description="Survey type - pick from the dropdown.",
        json_schema_extra={"x-dynamic-enum": "survey_types"}
    ),
]

ZipNameParam = Annotated[
    Optional[str | None],
    Query(
        description="Zip file - leave blank to auto-pick newest.",
        json_schema_extra={"x-dynamic-enum": "zip_files"}
    ),
]

app = FastAPI(
    title="GeoInfo Processor API (Async)",
    description="Asynchronous job execution for geoprocessing tasks.",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    openapi_tags=[
        {"name": "Survey Loaders", "description": "Zip listing, Refresh zip list, Process GDBs."},
        {"name": "Survey Status Checks", "description": "Health check, Status checks, Cancel processes."},
        {"name": "Survey Results", "description": "Can download zipped files."},
    ],
)

# ----------------------------------------------------------------------------
# Adding Router Headers
# Use these to group endpoints. If you add more, add in include_router() in main.py
loaders_router = APIRouter(prefix="", tags=["Survey Loaders"])
results_router  = APIRouter(prefix="", tags=["Survey Results"])
status_router  = APIRouter(prefix="", tags=["Survey Status Checks"])
# ----------------------------------------------------------------------------

load_dotenv()

# Choose an output base dir. Prefer env var else OS temp dir.
OUTPUT_BASE_DIR = FSPath(os.getenv("OUTPUT_DIR", '../../../output'))
OUTPUT_BASE_DIR.mkdir(parents=True, exist_ok=True)
RESULTS_ZIP_FOLDER = "results"
RESULTS_ZIP_FILENAME = "results.zip"

# Global state for running and cancelling jobs
RUNNING_JOBS: dict[str, Event] = {}
JOBS_LOCK = Lock()

# Strict application logger for API layer
APP_LOGS_DIR = OUTPUT_BASE_DIR / "_api_logs"
APP_LOGS_DIR.mkdir(parents=True, exist_ok=True)

# Set up application logger
# This logger is used for API-level logging, not job-specific logs
app_logger = logging.getLogger("geoinfo.api")
app_logger.setLevel(logging.INFO)
if not app_logger.handlers:
    app_fh = RotatingFileHandler(APP_LOGS_DIR / "api.log", maxBytes=5_000_000, backupCount=3, encoding="utf-8")
    app_fh.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    app_fh.setFormatter(fmt)
    app_logger.addHandler(app_fh)

# SQLite database for job tracking
# This is a simple file-based database to track job statuses
# Near the top
APP_ROOT = FSPath(os.getenv("APP_ROOT") or os.getcwd())
DB_PATH = str((APP_ROOT / "job_status.db").resolve())


def init_db():
    """Initializes the SQLite job tracking database if it does not already exist."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS jobs (
            job_id TEXT PRIMARY KEY,
            status TEXT,
            created_at TEXT,
            updated_at TEXT,
            error TEXT,
            output_dir TEXT,
            result_zip_path TEXT
        )
        """
    )
    conn.commit()
    conn.close()


def update_status_safe(job_id: str, status: str, error: Optional[str] = None, retries: int = 6, backoff: float = 0.25) -> None:
    """
    Update job status with retries and never raise. Uses its own short-lived connection.
    Retries on 'database is locked' and other transient errors.
    """
    now_ = datetime.now().isoformat()
    attempt = 0
    while True:
        try:
            conn = sqlite3.connect(DB_PATH, timeout=30)
            try:
                conn.execute(
                    "UPDATE jobs SET status = ?, updated_at = ?, error = ? WHERE job_id = ?",
                    (status, now_, error, job_id),
                )
                conn.commit()
            finally:
                conn.close()
            return
        except Exception as e:
            attempt += 1
            # Log locally to app logger so we can see failures of status updates
            try:
                app_logger.warning("update_status_safe attempt %d failed for %s -> %s: %s", attempt, job_id, status, e)
            except Exception:
                pass
            if attempt >= retries:
                # Final attempt failed; swallow to avoid masking the original error path
                return
            time.sleep(backoff * attempt)


def drop_jobs_table():
    """Drops SQLite job tracking jobs table if it exists."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS jobs;")
    conn.commit()
    conn.close()


# Uncomment to reset DB during development
# drop_jobs_table()

# Initialize the database
init_db()


# --------------------------------------------------------------
#-------------------- Admin Docs Refresh -----------------------
@app.post("/admin/docs/refresh", status_code=204, tags=["Configuration"])
def refresh_docs():
    app.openapi_schema = None
    return

@app.post("/admin/settings/reload", status_code=204, tags=["Configuration"])
def reload_settings():
    """ Re-read .env, rebuild Settings, and refresh Swagger dropdowns"""
    refresh_settings()
    app.openapi_schema = None
    return

import json
import re

def _write_env_value(key: str, value: str, path: str = ".env") -> None:
    lines = []
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            lines = f.read().splitlines()
    pat = re.compile(rf"^{re.escape(key)}\s*=")
    replaced = False
    for i, line in enumerate(lines):
        if pat.match(line):
            lines[i] = f"{key}={value}"
            replaced = True
            break
    if not replaced:
        lines.append(f"{key}={value}")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

@app.post("/admin/survey-types/add", tags=["Configuration"])
def add_survey_type(new_item: str) -> Dict[str, object]:
    """ Dynamically update and save the new survey type in the .env file. """
    cur = list(get_settings().SURVEY_TYPES or [])
    if new_item in cur:
        return {"status": "ok", "survey_types": cur, "note": "already present"}

    cur.append(new_item)
    # Persist as JSON list so Pydantic parses it back
    _write_env_value("SURVEY_TYPES", json.dumps(cur))
    # Reload settings from disk and refresh Swagger dropdowns
    refresh_settings()
    app.openapi_schema = None
    return {"status": "ok", "survey_types": cur}

# --------------------------------------------------------------
# -------------------- Survey Loaders --------------------------
@loaders_router.get("/zip-files")
def get_zip_files_single():
    """List current .zip files in the single configured directory."""
    return {"zip_files": list_zip_files_single()}

@loaders_router.post("/zip-files/refresh")
def refresh_zip_files_single():
    """Rescan the directory, clear cached list, and refresh the cached list used for runtime validation."""
    # You do not need to reassign the type annotation here; This will refresh and clear cached list without a restart.
    refresh_zip_enum()
    app.openapi_schema = None
    return {"status": "ok", "zip_files": list_zip_files_single()}

@loaders_router.get("/surveytypes")
def get_survey_types():
    """Rescan and list the surveys and zipped files loaded. No API refresh required."""
    return {
        "survey_types": list(get_settings().SURVEY_TYPES or []),
        "zip_files": list_zip_files_single()
    }

@loaders_router.post("/process-async/", response_model=Union[JobQueuedResponse, ErrorResponse])
async def process_data_async(
    background_tasks: BackgroundTasks,
    survey_type: SurveyTypeParam = None,
    zip_name: ZipNameParam = None,
    alternate_name_excel_file: Optional[UploadFile] = File(None, description="Excel file with alternate names"),
    gridzone_excel_file: UploadFile = File(..., description="Excel file with Gridzones contained")
) -> Union[Dict[str, str], JSONResponse]:
    """
    Accept files, stage them to temp folder, and queue background processing tasks.
    Returns a job_id immediately. All exceptions are returned as JSON.
    Add an optional zip_name to select a .zip file from the server's zip directory for the given survey_type.

    Enhancement:
    - If zip_name is omitted, automatically select the newest '*_YYYYMMDD.gdb.zip'
      from the configured directory for the survey_type.
    """
    try:
        # Convert survey_type string to SurveyType Enum instance
        survey_type_str = survey_type
        if survey_type_str not in get_settings().SURVEY_TYPES:
            return JSONResponse(
                status_code=422,
                content={"status": "error", "message": f"Invalid survey_type: {survey_type_str}"}
            )
        
        # Resolve chosen zip (explicit or newest)
        chosen_zip_name: Optional[ZipNameParam] = zip_name
        if not chosen_zip_name:
            chosen_zip_name = latest_zip_name_single()
            if not chosen_zip_name:
                return JSONResponse(status_code=400, content={"status": "error", "message": "No .zip files found in SINGLE_ZIP_DIR"})

        # Validate existence in the single directory
        names = set(list_zip_files_single())
        if chosen_zip_name not in names:
            return JSONResponse(
                status_code=422,
                content={"status": "error", "message": f"zip_name '{chosen_zip_name}' not found in SINGLE_ZIP_DIR"}
            )
        
        # Extract the Division 3-letter code
        division_code = extract_division_code_from_zip(chosen_zip_name) or None


        chosen_zip_path = str(zip_path_single(chosen_zip_name))

        # Create job id and tmp folder
        job_id = str(uuid.uuid4())
        tmpdir = os.path.join(tempfile.gettempdir(), job_id)
        os.makedirs(tmpdir, exist_ok=True)
        now = datetime.now().isoformat()

        # Optional LUT
        alternate_name_df = None
        if alternate_name_excel_file is not None:
            contents = await alternate_name_excel_file.read()
            alternate_name_df = pd.read_excel(BytesIO(contents), sheet_name=0, engine='openpyxl')

        # Gridzone Excel persisted
        gridzone_excel_path: FSPath = await save_upload_to_temp_excel(gridzone_excel_file)

        # Extract GDB from selected zip
        gdb_extract_path = os.path.join(tmpdir, "gdb")
        with zipfile.ZipFile(chosen_zip_path, "r") as zf:
            zf.extractall(gdb_extract_path)
        gdb_dirs = [d for d in os.listdir(gdb_extract_path) if d.lower().endswith(".gdb")]
        if not gdb_dirs:
            return JSONResponse(status_code=400, content={"status": "error", "message": f"No .gdb found inside {chosen_zip_name}."})
        gdb_path = os.path.join(gdb_extract_path, gdb_dirs[0])

        # Output folder
        output_dir = os.path.join("output", job_id)
        os.makedirs(output_dir, exist_ok=True)

        # Record job
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                "INSERT INTO jobs (job_id, status, created_at, updated_at, error, output_dir) VALUES (?, ?, ?, ?, ?, ?)",
                (job_id, "queued", now, now, None, output_dir),
            )
            conn.commit()

        # Track cancel
        cancel_event = Event()
        with JOBS_LOCK:
            RUNNING_JOBS[job_id] = cancel_event

        # Queue worker
        background_tasks.add_task(
            run_survey_mapper,
            job_id,
            alternate_name_df,
            str(gridzone_excel_path),
            gdb_path,
            output_dir,
            survey_type,   # keep if config resolution still needs it
            cancel_event,
            division_code
        )

        return {"status": "queued", "job_id": job_id}

    except Exception as e:
        app_logger.exception("Error queueing job")
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e)})


# -------------------- Survey Loaders --------------------------
# --------------------------------------------------------------


# --------------------------------------------------------------
# -------------------- Survey Results --------------------------

@results_router.get("/download/{job_id}")
def download_zip(job_id: str):
    """
    Download the zipped output for a completed job.
    Looks for <job_id>_output.zip under OUTPUT_BASE_DIR, and also inside a per-job subfolder.
    """
    if "/" in job_id or "\\" in job_id or ".." in job_id:
        raise HTTPException(status_code=400, detail="Invalid job id")

    zip_path = OUTPUT_BASE_DIR / job_id / RESULTS_ZIP_FILENAME
    if not zip_path:
        raise HTTPException(status_code=404, detail="ZIP file not found")

    media_type = mimetypes.guess_type(str(zip_path))[0] or "application/octet-stream"
    return FileResponse(
        path=str(zip_path),
        media_type=media_type,
        filename=zip_path.name,
        headers={"Cache-Control": "no-store"}
    )
# -------------------- Survey Results --------------------------
# --------------------------------------------------------------



# --------------------------------------------------------------
# -------------------- Survey Status Checks --------------------
@status_router.get("/status-all", response_model=List[JobStatus])
def get_all_jobs(
    level: LogLevelFilter = Query(LogLevelFilter.all, description="Filter logs by level"),
) -> Union[List[JobStatus], JSONResponse]:
    """ Return all jobs and statuses. """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT job_id, status, created_at, updated_at, error, output_dir FROM jobs ORDER BY created_at DESC")
    rows = cursor.fetchall()
    conn.close()

    logs_summary_filtered: Optional[LogsByLevel] = None
    # filename = None
    results_download_filepath = None
    results: List[JobStatus] = []
    for job_id, status, created_at, updated_at, error, output_dir in rows:
        if output_dir:
            try:
                logs_all: LogsByLevel = collect_logs_grouped_all(output_dir)
                logs_summary_filtered = filter_logs_by_level(logs_all, level)
                results_download_filepath = f"/{output_dir}/{RESULTS_ZIP_FILENAME}"
            except Exception as e:
                # Keep endpoint resilient - return a note rather than failing the request
                try:
                    app_logger.exception("Failed to collect or filter logs for job_id=%s: %s", job_id, e)
                except Exception:
                    pass
                logs_summary_filtered = LogsByLevel(info=[], warning=[], error=[], note=f"Log collection error: {e}")


        # results_download_file = None
        # if status == "complete" and results_download_filepath is not None and FSPath(results_download_filepath).exists():
        #     results_download_file = FileResponse(path=results_download_filepath, filename=filename, media_type="application/octet-stream")

        item: JobStatus = JobStatus(
            job_id=job_id,
            status=status,
            created_at=created_at,
            updated_at=updated_at,
            error=error or None,
            download_url=results_download_filepath if status == "complete" else None,
            # download_url=results_download_file,
            # Preserve original behavior of hiding logs when there is no output_dir
            logs_summary=logs_summary_filtered if output_dir else None,            
        )

        results.append(item)

    return results


@status_router.get("/status/{job_id}", response_model=Union[JobStatus, ErrorResponse])
def get_job_status(
    job_id: str,
    level: LogLevelFilter = Query(LogLevelFilter.all, description="Filter logs by level"),
) -> Union[JobStatus, JSONResponse]:
    """Return status and details for a specific job_id, with logs grouped and filtered by level."""

    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT status, created_at, updated_at, error, output_dir FROM jobs WHERE job_id = ?",
            (job_id,)
        ).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail={"status": "error", "message": "Job ID not found"})

    status, created_at, updated_at, error_text, output_dir = row

    logs_summary_filtered: Optional[LogsByLevel] = None
    # filename = None
    results_download_filepath = None
    if output_dir:
        try:
            logs_all: LogsByLevel = collect_logs_grouped_all(output_dir)
            logs_summary_filtered = filter_logs_by_level(logs_all, level)
            results_download_filepath = f"/{output_dir}/{RESULTS_ZIP_FILENAME}"
        except Exception as e:
            # Keep endpoint resilient - return a note rather than failing the request
            try:
                app_logger.exception("Failed to collect or filter logs for job_id=%s: %s", job_id, e)
            except Exception:
                pass
            logs_summary_filtered = LogsByLevel(info=[], warning=[], error=[], note=f"Log collection error: {e}")

    # results_download_file = None
    # if status == "complete" and results_download_filepath is not None and FSPath(results_download_filepath).exists():
    #     results_download_file = FileResponse(path=results_download_filepath, filename=filename, media_type="application/octet-stream")

    resp: JobStatus = JobStatus(
        job_id=job_id,
        status=status,
        created_at=created_at,
        updated_at=updated_at,
        error=error_text or None,
        #download_url=f"/{output_dir}/{job_id}" if status == "complete" and results_download_filepath else None,
        download_url=results_download_filepath if status == "complete" and results_download_filepath else None,
        # Preserve original behavior of hiding logs when there is no output_dir
        logs_summary=logs_summary_filtered if output_dir else None,
    )
    return resp


@status_router.post("/cancel/{job_id}")
def cancel_job(job_id: str) -> Dict[str, str]:
    """Signal a running or queued job to cancel. Returns 404 if the job is unknown."""
    with JOBS_LOCK:
        evnt = RUNNING_JOBS.get(job_id)

    if evnt is None:
        # Job may be finished or unknown; reflect current DB state if present
        with sqlite3.connect(DB_PATH) as conn:
            row = conn.execute("SELECT status FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Job not found")
        return {"job_id": job_id, "status": row[0], "message": "Job not running; no cancel needed"}

    now_ = datetime.now().isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "UPDATE jobs SET status = ?, updated_at = ? WHERE job_id = ? AND status IN ('queued','processing')",
            ("cancelling", now_, job_id),
        )
        conn.commit()

    evnt.set()
    return {"job_id": job_id, "status": "cancelling", "message": "Cancellation requested"}

def save_final_zip_location(job_id: str, zip_location: str) -> Dict[str, str]:
    """
    Updates the job's final zip file location into its resil;t_zip_path.
    Returns 404 if the job is unknown or a 409 if the zip file is missing.
    Expects the jobs table to include at least:
      - output_dir TEXT
      - result_zip_path TEXT (nullable)
      - work_dir TEXT (build area used during the run)
      - status TEXT
    Adjust column names if yours differ.
    """
    # 1) Check whether the job is known in DB
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT job_id, status, output_dir FROM jobs WHERE job_id = ?",
            (job_id,),
        ).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Job not found")

    status = row["status"]
    output_dir = row["output_dir"]
    # result_zip_path = row["result_zip_path"]

    # 2) Do not ship while job is still running
    with JOBS_LOCK:
        running_ev = RUNNING_JOBS.get(job_id)

    if running_ev is not None and status in ("queued", "processing", "cancelling"):
        raise HTTPException(status_code=409, detail="Job is not finished yet")

    # 3) Resolve the source ZIP
    src_zip = None
    if zip_location and FSPath(zip_location).exists():
        src_zip = FSPath(zip_location)
    else:
        raise

    if not src_zip or not src_zip.exists():
        raise HTTPException(status_code=409, detail="Final ZIP not found for this job")

    # 5) Build destination path inside output_dir
    if not output_dir:
        raise HTTPException(status_code=409, detail="Job has no output_dir recorded")
    dest_dir = FSPath(output_dir)
    dest_zip = FSPath(src_zip)

    # 7) Update DB
    now_ = datetime.now().isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        # If you track delivered_at or an output_zip_path, update them here
        conn.execute(
            """
            UPDATE jobs
               SET result_zip_path = ?,
                   updated_at = ?
             WHERE job_id = ?
            """,
            (str(dest_zip), now_, job_id),
        )
        conn.commit()

    return {
        "job_id": job_id,
        "status": "complete",
        "source_zip": str(src_zip),
        "output_dir": str(dest_dir),
        "output_zip_path": str(dest_zip),
        "message": "Final ZIP in output_dir",
    }

@status_router.post("/cancel-all")
def cancel_all_jobs() -> Dict[str, Union[str, int, List[str]]]:
    """Signal all running or queued jobs to cancel."""
    cancelled: List[str] = []
    with JOBS_LOCK:
        ids = list(RUNNING_JOBS.keys())
        for jid in ids:
            RUNNING_JOBS[jid].set()
            cancelled.append(jid)

    now_ = datetime.now().isoformat()
    if cancelled:
        with sqlite3.connect(DB_PATH) as conn:
            for jid in cancelled:
                conn.execute(
                    "UPDATE jobs SET status = ?, updated_at = ? WHERE job_id = ? AND status IN ('queued','processing')",
                    ("cancelling", now_, jid),
                )
            conn.commit()

    return {"status": "ok", "count": len(cancelled), "job_ids": cancelled}


@status_router.get("/health", response_model=HealthResponse)
def health_check() -> Dict[str, str]:
    return {"status": "ok", "message": "GeoInfo Processor Async API is healthy."}

# -------------------- Survey Status Checks --------------------
# --------------------------------------------------------------

def run_survey_mapper(
    job_id: str,
    alternate_name_df: Union[pd.DataFrame, None],
    gridzone_excel_path: str,
    gdb_path: str,
    output_dir: str,
    survey_type: SurveyTypeParam,
    cancel_event: Event,
    division_code: Union[str,None] = None
) -> None:
    """
    Background task. Calls GeoInfo Processor and custom tool methods which return result dicts.
    Job is marked failed if any step returns failed, False, or throws.
    """
    job_logger = build_job_logger(job_id, output_dir)

    # Discover the current log file path used by build_job_logger
    logs_dir = FSPath(output_dir) / "logs"
    latest_log = max(logs_dir.glob("log_*.txt"), key=lambda p: p.stat().st_mtime, default=None)

    # Initialize the fail fast logger to abort processing if any errors.
    watcher = FailFastLogWatcher(latest_log, poll_file=True)
    job_logger.addHandler(watcher)

    def _abort_if_error(prefix: str = "") -> bool:
        """ Check if any error are in the watched log files and abort processing. """
        watcher.scan_file_once()
        if watcher.error_message:
            msg = f"{prefix}Fail-fast due to error in logs: {watcher.error_message}"
            update_status_safe(job_id=job_id, status="failed", error=msg)
            job_logger.error(msg)
            return True
        return False

    try:
        update_status_safe(job_id=job_id, status="processing", error=None)
        job_logger.info("Job started")

        if _abort_if_error("pre-start: "): return

        # Early cancel check
        if cancel_event.is_set():
            update_status_safe(job_id=job_id, status="canceled", error="Canceled before start")
            job_logger.warning("Cancellation flag set before start")
            return

        # Optionally fetch LUTAssetTypes from a database
        if alternate_name_df is None and os.getenv("USE_DATABASE", "false").lower() == "true":
            try:
                job_logger.info("Attempting database fetch for LUTAssetTypes")
                db = DatabaseConnector(
                    db_type=os.getenv("DB_TYPE"),  # type: ignore
                    username=os.getenv("DB_USER"),
                    password=os.getenv("DB_PASS"),
                    host=os.getenv("DB_HOST"),
                    port=os.getenv("DB_PORT"),
                    database=os.getenv("DB_NAME")
                ).connect()
                alternate_name_df = pd.read_sql("SELECT * FROM LUTAssetTypes", db)
                job_logger.info("Database fetch succeeded")
            except Exception as exc:
                msg = f"Warning: could not load LUTAssetTypes from DB - {exc}"
                update_status_safe(job_id=job_id, status="processing", error=msg)
                job_logger.warning(msg)

        # Another cancel check before heavy work
        if cancel_event.is_set():
            update_status_safe(job_id=job_id, status="canceled", error="Canceled before processing")
            job_logger.warning("Cancellation before processing")
            return

        job_logger = build_job_logger(job_id, output_dir)

        try:

            # 1. Validate against the .env list
            if survey_type not in get_settings().SURVEY_TYPES:
                raise HTTPException(
                    status_code=400,
                    detail=f"survey_type must be one of {get_settings().SURVEY_TYPES}",
                )
            cfg = get_config(survey_type)
        except (FileNotFoundError, ValueError) as e:
            raise HTTPException(status_code=400, detail=str(e))

        # Convert Pydantic model to plain dict
        cfg_dict = cfg.model_dump()

        processor = SurveyMapper(
            gdb_path=gdb_path,
            parent_dir=output_dir,
            gridzone_excel_path=gridzone_excel_path,
            logger=job_logger,
            alternate_name_df=alternate_name_df,
            config_dict=cfg_dict
        )

        # Step 1 - grid and clipping
        if cancel_event.is_set():
            update_status_safe(job_id=job_id, status="canceled", error="Canceled before grid processing")
            job_logger.warning("Cancellation before grid processing")
            return

        job_logger.info("Step 1: process grid and clipping - start")
        step1 = processor._process_grid_sheet()

        # if _abort_if_error("after step1: "): return

        # Check step1 result for status of success bool value
        if not isinstance(step1, dict) or not step1.get("success", False):
            update_status_safe(job_id=job_id, status="failed", error="Grid processing failed")
            job_logger.error("Step 1 failed")
            
            if _abort_if_error("after step1: "): return

            return

        job_logger.info("Step 1: completed successfully")

        export_input_folder = step1.get("data")

        # Step 2 - feature collections
        if cancel_event.is_set():
            update_status_safe(job_id=job_id, status="canceled", error="Canceled before export")
            job_logger.warning("Cancellation before export")
            return

        job_logger.info("Step 2: export feature collections - start")
        step2 = processor.export_feature_collections(input_folder=export_input_folder)
        if not isinstance(step2, dict):
            update_status_safe(job_id=job_id, status="failed", error="Internal error - export_feature_collections did not return a dict")
            job_logger.error("Step 2 failed: invalid return type")
            return
        
        # Check step1 result for status of success bool value
        if not step2.get("success", False):
            errors = step2.get("errors", ["Export failed"])
            if isinstance(errors, str):
                errors = [errors]
            elif not isinstance(errors, list):
                errors = [str(errors)]
            msg = "; ".join(errors)
            update_status_safe(job_id=job_id, status="failed", error=msg)
            job_logger.error("Step 2 failed: %s", msg)
            return
        job_logger.info("Step 2: completed successfully")

        # Step 3 - zip output for download
        if cancel_event.is_set():
            update_status_safe(job_id=job_id, status="canceled", error="Canceled before zip")
            job_logger.warning("Cancellation before zip")
            return

        output_base = FSPath(f"{output_dir}/{RESULTS_ZIP_FOLDER}")
        zip_dest = FSPath(f"{output_dir}/{RESULTS_ZIP_FILENAME}")
        job_logger.info("Step 3: zipping output from '%s' to '%s'", output_base, str(zip_dest))
        
        if _abort_if_error("before zip: "): return
        try:
            # Clear cache
            job_logger.info("Clearing caches before zipping")
            helpers.clear_locks()

            zip_directory(str(output_base), zip_dest)
            job_logger.info("Zipping output completed: %s", str(zip_dest))

        except Exception as xc:
            msg = f"Zipping failed: {xc}"
            update_status_safe(job_id=job_id, status="failed", error=msg)
            job_logger.exception(msg)
            return

        job_logger.info("Zipping completed")
        update_status_safe(job_id=job_id, status="complete", error=None)
        save_final_zip_location(job_id=job_id, zip_location=str(zip_dest))
        job_logger.info("Job completed successfully")

    except Exception as exc:
        update_status_safe(job_id=job_id, status="failed", error=str(exc))
        job_logger.exception("Unhandled error in job")
    finally:
        with JOBS_LOCK:
            RUNNING_JOBS.pop(job_id, None)
        job_logger.info("Job finalizer finished")



def extract_division_code_from_zip(zip_name: ZipNameParam = None) -> str | None:
    """
    Return a 3-letter uppercase division code from the given zip filename,
    e.g. 'MyProject_SAZ_20250109.gdb.zip' -> 'SAZ'.
    Strategy:
      - Look for an underscore/dash boundary followed by exactly 3 A-Z letters.
      - Fall back to any standalone 3 A-Z letters chunk if needed.
    """
    # common pattern: *_SAZ_*.zip or *_SAZ.zip
    m = re.search(r'[_\-]([A-Z]{3})(?=[_\.\-])', zip_name)
    if m:
        return m.group(1)

    # fallback: any chunk of exactly 3 caps
    m2 = re.search(r'\b([A-Z]{3})\b', zip_name)
    return m2.group(1) if m2 else None

# register routers on the sub-app
app.include_router(config_router)
app.include_router(loaders_router)
app.include_router(results_router)
app.include_router(status_router)

# Include wired enums and Config Editor link
wire_dynamic_enums_and_links(app)

__all__ = ["app"]  # export only the sub-app