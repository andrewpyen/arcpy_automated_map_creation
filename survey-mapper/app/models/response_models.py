from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Any, Union, List, Optional
from datetime import datetime
from enum import Enum

class LogEntry(BaseModel):
    ts: Union[datetime, str]
    level: str
    logger: str
    msg: str
    file: Optional[str] = None  # which log file it came from

    def __getitem__(self, key: str) -> Any:
        if not hasattr(self, key):
            raise KeyError(f"Invalid log entry: {key}")
        return getattr(self, key)

    def __setitem__(self, key: str, value: Any) -> None:
        if not hasattr(self, key):
            raise KeyError(f"Invalid log entry: {key}")
        setattr(self, key, value)


class LogsByLevel(BaseModel):
    info: List[LogEntry] = []
    warning: List[LogEntry] = []
    error: List[LogEntry] = []
    note: Optional[str] = None

    def __getitem__(self, key: str) -> Any:
        if not hasattr(self, key):
            raise KeyError(f"Invalid log level: {key}")
        return getattr(self, key)

    def __setitem__(self, key: str, value: Any) -> None:
        if not hasattr(self, key):
            raise KeyError(f"Invalid log level: {key}")
        setattr(self, key, value)


class LogLevelFilter(str, Enum):
    all = "all"
    info = "info"
    warning = "warning"
    error = "error"


class JobStatus(BaseModel):
    job_id: str
    status: str
    created_at: str
    updated_at: str
    error: Optional[Union[str, None]] = None
    download_url: Union[str, None] = None
    logs_summary: Optional[Union[LogsByLevel, None]] = None


class JobListResponse(BaseModel):
    jobs: List[JobStatus]


class HealthResponse(BaseModel):
    status: str
    message: str


class JobQueuedResponse(BaseModel):
    status: str
    job_id: str


class ErrorResponse(BaseModel):
    status: str
    message: str
