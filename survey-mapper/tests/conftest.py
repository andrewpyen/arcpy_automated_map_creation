# backend/tests/conftest.py
# File needed to configure how modules are imported for test scripts

import sys
from pathlib import Path
import os
import types

# <repo_root>/backend
BACKEND = Path(__file__).resolve().parents[1]

if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

# --- Keep tests hermetic ---
os.environ.setdefault("OUTPUT_DIR", str(BACKEND / "_test_outputs"))
os.environ.setdefault("USE_DATABASE", "false")   # prevents DB lookups in worker

# --- Stub heavy/optional deps BEFORE app modules are imported ---

# ArcPy is huge; stub it so imports don't load ArcGIS runtime.
if "arcpy" not in sys.modules:
    sys.modules["arcpy"] = types.ModuleType("arcpy")

# If your CustomToolClass (or others) import arcpy submodules, stub those too:
for name in [
    "arcpy.management", "arcpy.analysis", "arcpy.env", "arcpy.conversion"
]:
    mod = types.ModuleType(name)
    sys.modules[name] = mod

# (Optional) If any test path triggers this import, keep it lightweight.
# This helper is imported only inside a route, but stubbing is harmless.
# utils_mod = types.ModuleType("app.utils.agol_downloader")
# def _noop_download(urls, gdb_path):  # pragma: no cover
#     return None
# utils_mod.download_features_to_gdb = _noop_download
# sys.modules["app.utils.agol_downloader"] = utils_mod