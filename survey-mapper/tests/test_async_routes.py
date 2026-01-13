# tests/test_async_routes.py
import io
import os
import json
import zipfile
import sqlite3
import tempfile
from pathlib import Path
from datetime import datetime

import pandas as pd
import pytest
from fastapi.testclient import TestClient

#
# IMPORTANT: change this import if your app file has a different module name
#
import app.api.async_routes as script_under_test  # <-- your file from the prompt

# ---------- helpers ----------

@pytest.fixture(autouse=True)
def _isolate_tmp_env(tmp_path, monkeypatch):
    """
    Isolate filesystem + DB for every test:
      - new OUTPUT_DIR
      - new job_status.db
    Re-initialize DB after patching.
    """
    outdir = tmp_path / "outputs"
    outdir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("OUTPUT_DIR", str(outdir))

    # swap DB file and re-init table
    monkeypatch.setattr(script_under_test, "DB_PATH", str(tmp_path / "job_status.db"), raising=True)
    script_under_test.drop_jobs_table()
    script_under_test.init_db()

    # clear global running-jobs registry
    script_under_test.RUNNING_JOBS.clear()

    yield

@pytest.fixture()
def client():
    return TestClient(script_under_test.async_app)

def _fake_excel_bytes():
    # Any bytes work—tool doesn’t parse; we just need a filename with .xlsx
    return io.BytesIO(b"PK\x03\x04dummy-excel")

def _make_gdb_zip_bytes():
    import io, zipfile
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("dummy.gdb/placeholder.txt", "ok")
    buf.seek(0)
    return buf

def test_process_async_queues_and_completes(client, tmp_path, monkeypatch):
    import pandas as pd
    import async_routes as m  # adjust if your module path differs

    # 1) Stub pandas.read_excel so fake bytes don't blow up
    monkeypatch.setattr(m.pd, "read_excel",
        lambda *a, **k: pd.DataFrame({"name":[1], "alternativename":["x"]}),
        raising=True)

# ---------- monkeypatches for heavy helpers ----------

@pytest.fixture(autouse=True)
def _patch_helpers(monkeypatch, tmp_path):
    """
    - save_upload_to_temp_excel: write to disk and return a real path
    - zip_directory: create a tiny zip
    - build_job_logger/collect_logs/filter: we keep defaults (they're light)
    - run_custom_tool: replaced with a tiny worker that:
        * logs a few lines in the expected "pipe" format
        * marks job COMPLETE in the DB
        * creates an output zip so /download works (optional)
    """
    # save_upload_to_temp_excel -> write file and return path
    def fake_save(uploadfile):
        suffix = Path(uploadfile.filename).suffix or ".xlsx"
        dest_dir = tmp_path / "uploaded"
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = dest_dir / f"grid{suffix}"
        content = uploadfile.file.read()
        dest.write_bytes(content)
        return dest
    monkeypatch.setattr(script_under_test, "save_upload_to_temp_excel", fake_save, raising=True)

    # zip_directory -> minimal no-op zip creator
    def fake_zip(src_dir, dest_zip):
        dest_zip = Path(dest_zip)
        dest_zip.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(dest_zip, "w", zipfile.ZIP_DEFLATED) as z:
            z.writestr("ok.txt", "ok")
        return dest_zip
    monkeypatch.setattr(script_under_test, "zip_directory", fake_zip, raising=True)

    # run_custom_tool -> short fake worker that sets COMPLETE + writes logs
    def fake_worker(job_id, alternate_name_df, gridzone_excel_path, gdb_path, output_dir, config_path, cancel_event):
        # mark processing
        now_ = datetime.now().isoformat()
        with sqlite3.connect(script_under_test.DB_PATH) as c:
            c.execute("UPDATE jobs SET status=?, updated_at=? WHERE job_id=?",
                      ("processing", now_, job_id))
            c.commit()

        # per-job logs
        logs_dir = Path(output_dir) / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)
        logf = logs_dir / f"log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        # strict "pipe" format so collectors pick it up
        lines = [
            "2025-08-18T06:54:33.575000 | INFO | custom_tool_class.job.%s | job started\n" % job_id,
            "2025-08-18T06:54:33.592000 | INFO | custom_tool_class.job.%s | step 1: process grid and clipping - start\n" % job_id,
            "2025-08-18T06:54:35.000000 | WARNING | custom_tool_class.job.%s | some warning here\n" % job_id,
            "2025-08-18T06:54:36.000000 | ERROR | custom_tool_class.job.%s | some error here\n" % job_id,
            "2025-08-18T06:54:37.000000 | INFO | custom_tool_class.job.%s | step 1: completed successfully\n" % job_id,
            "2025-08-18T06:54:38.000000 | INFO | custom_tool_class.job.%s | step 2: export feature collections - start\n" % job_id,
            "2025-08-18T06:54:39.000000 | INFO | custom_tool_class.job.%s | step 2: completed successfully\n" % job_id,
            "2025-08-18T06:54:40.000000 | INFO | custom_tool_class.job.%s | step 3: zipping output\n" % job_id,
            "2025-08-18T06:54:41.000000 | INFO | custom_tool_class.job.%s | job completed successfully\n" % job_id,
        ]
        logf.write_text("".join(lines), encoding="utf-8")

        # create a tiny zip in OUTPUT_BASE_DIR/<job>/job.zip so /download works
        out_base = Path(os.getenv("OUTPUT_DIR", "")) or Path(tempfile.gettempdir()) / "outputs"
        dest_zip = Path(out_base) / job_id / f"{job_id}_output.zip"
        dest_zip.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(dest_zip, "w", zipfile.ZIP_DEFLATED) as z:
            z.writestr("ok.txt", "ok")

        with sqlite3.connect(script_under_test.DB_PATH) as c:
            c.execute("UPDATE jobs SET status=?, updated_at=? WHERE job_id=?",
                      ("complete", datetime.now().isoformat(), job_id))
            c.commit()

    monkeypatch.setattr(script_under_test, "run_custom_tool", fake_worker, raising=True)

# ---------- tests ----------

def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"

def test_status_all_starts_empty(client):
    r = client.get("/status-all")
    assert r.status_code == 200
    assert r.json() == []

@pytest.fixture(autouse=True)
def test_process_async_queues_and_completes(client, tmp_path, monkeypatch):
    # build request files
    config = ("config.json", io.BytesIO(b'{"ok": true}'), "application/json")
    lut_assettypes = ("lut.xlsx", _fake_excel_bytes(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    grid = ("grid.xlsx", _fake_excel_bytes(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    gdbz = ("data.gdb.zip", _make_gdb_zip_bytes(), "application/zip")

    # (1) Using a monkeypath to pass the pandas DataFrame instead of the excel file bytess
    # Stub pandas.read_excel so fake bytes don't blow up
    monkeypatch.setattr(script_under_test.pd, "read_excel", lambda *a, **k: pd.DataFrame({"Name":[1], "AlternativeName":["x"]}),
                        raising=True)
    
    # (2) Stub save_upload_to_temp_excel to write a real file and return its path
    def fake_save(upload):
        dest = tmp_path / "grid.xlsx"
        dest.write_bytes(upload.file.read())
        return dest
    monkeypatch.setattr(script_under_test, "save_upload_to_temp_excel", fake_save, raising=True)

    r = client.post(
        "/process-async/",
        files={
            "config_file": config,
            "alternate_name_excel_file": lut_assettypes,
            "gridzone_excel_file": grid,
            "gdb_zip": gdbz,
        },
    )
    assert r.status_code == 200
    job_id = r.json()["job_id"]

    # background task runs after the response lifecycle in TestClient
    # check status
    # Status will be 'processing' or 'complete' depending on how fast the background task runs.
    sr = client.get(f"/status/{job_id}?level=all&max_lines_per_level=5000")
    assert sr.status_code == 200
    body = sr.json()
    assert body["job_id"] == job_id
    assert body["status"] in ("processing", "complete")  # allow a brief race
    # logs exist and include more than 2 lines
    logs = body["logs_summary"]
    assert isinstance(logs, dict)
    assert len(logs["info"]) >= 3  # we wrote many INFO lines
    # warning and error present
    assert len(logs["warning"]) >= 1
    assert len(logs["error"]) >= 1

def test_log_level_filter(client):
    # Reuse a quick job
    r = client.post(
        "/process-async/",
        files={
            "config_file": ("c.json", io.BytesIO(b"{}"), "application/json"),
            "gridzone_excel_file": ("g.xlsx", _fake_excel_bytes(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
            "gdb_zip": ("d.gdb.zip", _make_gdb_zip_bytes(), "application/zip"),
        },
    )
    job_id = r.json()["job_id"]

    # Only errors
    er = client.get(f"/status/{job_id}?level=error")
    assert er.status_code == 200
    ebody = er.json()
    assert ebody["logs_summary"]["info"] == []
    assert ebody["logs_summary"]["warning"] == []
    assert len(ebody["logs_summary"]["error"]) >= 1

    # Only warnings
    wr = client.get(f"/status/{job_id}?level=warning")
    wbody = wr.json()
    assert len(wbody["logs_summary"]["warning"]) >= 1
    assert wbody["logs_summary"]["error"] == []
    assert wbody["logs_summary"]["info"] == []

def test_status_all_includes_logs(client):
    # Ensure at least one job exists
    r = client.post(
        "/process-async/",
        files={
            "config_file": ("c.json", io.BytesIO(b"{}"), "application/json"),
            "gridzone_excel_file": ("g.xlsx", _fake_excel_bytes(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
            "gdb_zip": ("d.gdb.zip", _make_gdb_zip_bytes(), "application/zip"),
        },
    )
    # list
    lr = client.get("/status-all")
    assert lr.status_code == 200
    jobs = lr.json()
    assert isinstance(jobs, list) and len(jobs) >= 1
    assert "logs_summary" in jobs[0]

def test_cancel_endpoints(client):
    # Start a job (the fake worker completes fast, but RUNNING_JOBS gets set)
    r = client.post(
        "/process-async/",
        files={
            "config_file": ("c.json", io.BytesIO(b"{}"), "application/json"),
            "gridzone_excel_file": ("g.xlsx", _fake_excel_bytes(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
            "gdb_zip": ("d.gdb.zip", _make_gdb_zip_bytes(), "application/zip"),
        },
    )
    job_id = r.json()["job_id"]

    # Cancel specific (if it's already finished, API should say "no cancel needed")
    cr = client.post(f"/cancel/{job_id}")
    assert cr.status_code in (200, 404)
    if cr.status_code == 200:
        assert "status" in cr.json()

    # Cancel all
    allr = client.post("/cancel-all")
    assert allr.status_code == 200
    assert "status" in allr.json() and "count" in allr.json()

def test_download_404(client):
    r = client.get("/download/does-not-exist")
    assert r.status_code == 404
