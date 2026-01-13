@REM SET "CONDA_BAT=C:\Program Files\ArcGIS\Pro\bin\Python\condabin\conda.bat"
@REM SET "ENV_NAME=survey-mapper"

@REM Remove an environment
"C:\Program Files\ArcGIS\Pro\bin\Python\condabin\conda.bat" remove --name survey-mapper
@REM conda" remove --name ENV_NAME
@REM CALL "%CONDA_BAT%" remove --name ENV_NAME

@REM Check conda environment list
"C:\Program Files\ArcGIS\Pro\bin\Python\condabin\conda.bat" env list

@REM Check that all environments are updated
"C:\Program Files\ArcGIS\Pro\bin\Python\condabin\conda.bat" info --envs

@REM Create a new environment
@REM CALL "%CONDA_BAT%" create -n "%ENV_NAME%" --clone arcgispro-py3