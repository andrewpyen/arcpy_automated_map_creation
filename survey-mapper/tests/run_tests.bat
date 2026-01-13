@echo off

set PYTHONPATH="C:\Program Files\ArcGIS\Pro\bin\Python\envs\geoinfo-processor"

REM Activate conda environment
CALL "C:\Program Files\ArcGIS\Pro\bin\Python\condabin\conda.bat" activate geoinfo-processor

set PYTHONPATH=%CD%
pytest -q --maxfail=1 --disable-warnings
if %errorlevel% neq 0 (
  echo Tests FAILED
  exit /b 1
) else (
  echo Tests PASSED
)
