# app/config_loader.py
import json
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union
from pydantic import BaseModel, Field, ValidationError
from app.config_loading.settings import get_settings

class AppConfig(BaseModel):
    projectName: str = Field(
        ...,
        title="Project name",
        description="Human readable name shown in logs and outputs."
    )
    surveyType: str = Field(
        ...,
        title="Survey type",
        description="Must match one of your configured survey types. This is auto-enforced by the UI when saving."
    )
    outputDirectory: str = Field(
        ...,
        title="Output directory",
        description="Absolute or relative path where results will be written. The service will create it if missing."
    )
    lutassettypes: Dict[str, str] = Field(
        default_factory=dict,
        title="LUT Asset Types",
        description="Lookup of asset type code to display name. Example: {\"MH\": \"Manhole\"}."
    )
    gridzones: Dict[str, Union[str, List[object]]] = Field(
        default_factory=dict,
        title="Grid zones",
        description="Per-grid configuration. Keys are grid identifiers; values can be a string or a list depending on your process."
    )

_config_cache: Dict[Path, Tuple[float, AppConfig]] = {}

def _resolve_config_path(survey_type: Optional[str]) -> Path:
    root = Path(get_settings().CONFIG_ROOT).expanduser().resolve()
    if not root.exists():
        raise FileNotFoundError(f"CONFIG_ROOT not found: {root}")

    if get_settings().CONFIG_PER_SURVEY_TYPE_SUBFOLDER:
        if not survey_type:
            raise FileNotFoundError(
                "Survey type is required because CONFIG_PER_SURVEY_TYPE_SUBFOLDER=True."
            )
        path = (root / survey_type / get_settings().CONFIG_FILENAME).resolve()
    else:
        path = (
            (root / f"{survey_type}.json").resolve()
            if survey_type
            else (root / get_settings().CONFIG_FILENAME).resolve()
        )
    return path

def _load_from_disk(path: Path) -> AppConfig:
    if not path.exists():
        raise FileNotFoundError(f"Config not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        raw = json.load(f)
    try:
        return AppConfig.model_validate(raw)
    except ValidationError as ve:
        raise ValueError(f"Config validation failed for {path}: {ve}") from ve

def get_config(survey_type: Optional[str]) -> AppConfig:
    path = _resolve_config_path(survey_type)
    now = time.monotonic()
    ttl = int(get_settings().CONFIG_TTL_SECONDS)

    cached = _config_cache.get(path)
    if cached:
        ts, cfg = cached
        if now - ts < ttl:
            return cfg

    cfg = _load_from_disk(path)
    _config_cache[path] = (now, cfg)
    return cfg

def clear_config_cache() -> None:
    _config_cache.clear()

# --- New: persist edits and keep cache fresh ---

def save_config(survey_type: Optional[str], cfg: AppConfig) -> Path:
    """
    Persist the given AppConfig to its resolved JSON file.
    - Writes atomically via a temp file then replace
    - Ensures parent directory exists
    - Updates the TTL cache
    """
    path = _resolve_config_path(survey_type)
    path.parent.mkdir(parents=True, exist_ok=True)

    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(cfg.model_dump_json(indent=2), encoding="utf-8")
    tmp.replace(path)

    # refresh cache
    _config_cache[path] = (time.monotonic(), cfg)
    return path

# --- Optional: discover survey types from disk if you prefer not to rely only on get_get_settings()().SURVEY_TYPES ---

def list_available_survey_types() -> List[str]:
    root = Path(get_settings().CONFIG_ROOT).expanduser().resolve()
    if get_settings().CONFIG_PER_SURVEY_TYPE_SUBFOLDER:
        return sorted([p.name for p in root.iterdir() if p.is_dir()])
    else:
        return sorted([p.stem for p in root.glob("*.json") if p.is_file()])
