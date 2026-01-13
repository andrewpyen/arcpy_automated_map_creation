REM Activate ArcGIS Pro Conda Environment and run FastAPI
set PYTHONPATH="C:\Program Files\ArcGIS\Pro\bin\Python\envs\survey-mapper"

CALL "C:\Program Files\ArcGIS\Pro\bin\Python\condabin\conda.bat" activate survey-mapper

python -m uvicorn app.api.main:app --host 0.0.0.0 --port 8000 --reload

pause
