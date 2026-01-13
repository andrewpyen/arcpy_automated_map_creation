# app/zip_registry_single.py
from pathlib import Path
from functools import lru_cache
from enum import Enum
import re
from typing import List, Type
from .settings import get_settings

ZIP_EXTS = (".zip",)

def _sanitize_enum_name(name: str) -> str:
    # Make a safe Enum member name that is stable across runs
    n = re.sub(r"[^A-Za-z0-9_]", "_", name)
    if not re.match(r"^[A-Za-z_]", n):
        n = f"Z_{n}"
    return n

def _zip_dir() -> Path:
    p = Path(get_settings().SINGLE_ZIP_DIR)
    p.mkdir(parents=True, exist_ok=True)
    return p

def list_zip_files_single(refresh: bool = False) -> List[str]:
    # no in-proc cache yet, simple read each time
    return sorted([p.name for p in _zip_dir().iterdir() if p.is_file() and p.suffix.lower() in ZIP_EXTS])

def latest_zip_name_single() -> str | None:
    zips = [_zip_dir() / n for n in list_zip_files_single()]
    if not zips:
        return None
    zips.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return zips[0].name

def zip_path_single(zip_name: str) -> Path:
    return _zip_dir() / zip_name

@lru_cache(maxsize=1)
def build_zip_enum() -> Type[Enum]:
    """
    Build a string Enum from current zip names so FastAPI renders a dropdown. Caches list in memory.
    Call refresh_zip_enum() to rebuild after files change.
    """
    names = list_zip_files_single()
    if not names:
        # at least one placeholder so the schema remains valid
        names = ["__NO_ZIPS_FOUND__"]
    members = { _sanitize_enum_name(n): n for n in names }
    return Enum("ZipNameEnum", members)  # values are the actual filenames

def refresh_zip_enum() -> Type[Enum]:
    build_zip_enum.cache_clear()         # type: ignore[attr-defined]
    return build_zip_enum()
