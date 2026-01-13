import os, json
import arcpy
import gc
import time
import zipfile
from shutil import copy2, rmtree
from pathlib import Path
import shutil
import tempfile

def _load_json_env(name: str) -> dict:
    """Load and parse a JSON object from an environment variable."""
    raw = os.getenv(name, "").strip()
    if not raw:
        raise ValueError(f"{name} is not set in the environment.")
    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(f"{name} is not valid JSON: {e}")

def clear_locks():
    """Clear any locks held by arcpy in this process, and run garbage collection."""
    try:
        arcpy.management.ClearWorkspaceCache()
        print("Cleared workspace cache.")
    except Exception as exc:
        print(f"Could not clear workspace cache: {exc}")
        pass
    gc.collect()

def _wait_for_no_schema_lock(dataset, timeout_sec=30, poll_sec=0.5):
    """Return True when lock is gone, False on timeout."""
    t0 = time.time()
    while time.time() - t0 < timeout_sec:
        if arcpy.TestSchemaLock(dataset):
            return True
        time.sleep(poll_sec)
    return False

def _close_arcpy_handles(*objs):
    """Delete arcpy objects, clear caches, and run garbage collection."""
    for o in objs:
        try:
            del o
        except Exception:
            pass
    # Clear ArcGIS workspace caches and prompt GC
    try:
        arcpy.management.ClearWorkspaceCache()
    except Exception:
        pass
    gc.collect()
    time.sleep(0.2)  # tiny grace period

def _zip_directory_skip_locks(src_dir, zip_path):
    """Zip contents of a directory, skipping any .lock files."""
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, allowZip64=True) as zf:
        for root, dirs, files in os.walk(src_dir):
            for name in files:
                if name.endswith(".lock") or name.endswith(".sr.lock") or name == "Thumbs.db":
                    continue
                fp = os.path.join(root, name)
                # Some antivirus tools can race; protect with retry
                for attempt in range(5):
                    try:
                        arcname = os.path.relpath(fp, src_dir)
                        zf.write(fp, arcname)
                        break
                    except PermissionError:
                        time.sleep(0.5)
                else:
                    raise

def _stage_gdb_for_zip(gdb_path, staging_root):
    """Copy a file geodatabase to a staging folder, skipping any .lock files."""
    staging_root = Path(staging_root)
    if staging_root.exists():
        rmtree(staging_root, ignore_errors=True)
    staging_root.mkdir(parents=True, exist_ok=True)

    # Recreate folder tree and copy files except locks
    for p in Path(gdb_path).rglob("*"):
        rel = p.relative_to(gdb_path)
        dest = staging_root / rel
        if p.is_dir():
            dest.mkdir(parents=True, exist_ok=True)
        else:
            if p.suffix == ".lock" or p.name.endswith(".sr.lock"):
                continue
            dest.parent.mkdir(parents=True, exist_ok=True)
            copy2(p, dest)
    return str(staging_root)

def package_gdb(gdb_path, zip_path):
    """Package a file geodatabase into a zip file, handling locks."""
    _close_arcpy_handles()
    ok = _wait_for_no_schema_lock(gdb_path, timeout_sec=45)
    # If still locked, fall back to staging copy
    if not ok:
        stage_dir = Path(tempfile.mkdtemp(prefix="gdb_stage_")) / Path(gdb_path).name
        staged = _stage_gdb_for_zip(gdb_path, stage_dir)
        _zip_directory_skip_locks(staged, zip_path)
        shutil.rmtree(stage_dir.parent, ignore_errors=True)
    else:
        _zip_directory_skip_locks(gdb_path, zip_path)