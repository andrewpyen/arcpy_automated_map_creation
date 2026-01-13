REM Activate ArcGIS Pro Conda Environment and run FastAPI
set PYTHONPATH="C:\Program Files\ArcGIS\Pro\bin\Python\envs\survey-mapper"

CALL "C:\Program Files\ArcGIS\Pro\bin\Python\condabin\conda.bat" activate survey-mapper

REM Debugging - disable the frozen module validation in python
@REM set PYDEVD_DISABLE_FILE_VALIDATION=1
python -m debugpy --listen 5678 --wait-for-client -m uvicorn app.api.main:app --host 0.0.0.0 --port 8000 --reload

pause
