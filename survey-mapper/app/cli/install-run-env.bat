SETLOCAL
echo off

REM Path to ArcGIS Pro's conda
SET "CONDA_BAT=C:\Program Files\ArcGIS\Pro\bin\Python\condabin\conda.bat"
SET "ENV_NAME=survey-mapper"
SET "ENV_FILE=environment.yml"

REM New variable: set to <YES> or <NO> to force re-create the environment
SET "FORCE_RECREATE=YES"
@REM SET "FORCE_RECREATE=NO"

REM Check if environment exists
CALL "%CONDA_BAT%" env list | findstr /B /C:"%ENV_NAME%" >nul
IF %ERRORLEVEL%==0 (
    REM Env exists
    IF "%FORCE_RECREATE%"=="YES" (
        echo Forcing removal of %ENV_NAME%...
        call "%CONDA_BAT%" remove --yes -n %ENV_NAME% --all
        SET "ENV_EXISTS=0"
    ) ELSE (
        SET "ENV_EXISTS=1"
    )
) ELSE (
    SET "ENV_EXISTS=0"
)

IF "%ENV_EXISTS%"=="0" (
    echo
    echo Creating environment %ENV_NAME%...
    REM Example: "C:\Program Files\ArcGIS\Pro\bin\Python\condabin\conda.bat" create --yes -n survey-mapper --clone arcgispro-py
    CALL "%CONDA_BAT%" create -n "%ENV_NAME%" --clone arcgispro-py3
    
    echo Activating survey-mapper environment...
    CALL "%CONDA_BAT%" activate %ENV_NAME%
    
    REM --------------------------------------------------------------------------------------
    REM Installing latest packages for environment and letting conda resolve dependencies
    REM If you want to install specific versions, there is an environment.yml file that can be used instead. 
    REM But it's recommended to clone the arcgispro-py3 environment first and then install the latest 
    REM versions of the packages you need. The environment.yml file is for reviewing the current state.
    REM --------------------------------------------------------------------------------------

    REM Example: "C:\Program Files\ArcGIS\Pro\bin\Python\condabin\conda.bat" install fastapi uvicorn pydantic_settings python-dotenv python-multipart pandas openpyxl sqlalchemy asyncpg psycopg2-binary -c conda-forge -y
    echo Y | CALL "%CONDA_BAT%" install fastapi uvicorn pydantic-settings python-dotenv python-multipart pandas openpyxl sqlalchemy asyncpg psycopg2-binary -c conda-forge -y
    
    echo Exporting environment to environment.yml...
    CALL "%CONDA_BAT%" env export --no-builds | findstr /B /V "prefix:" > environment.yml
    echo "Wrote environment.yml"
) ELSE (
    echo Environment %ENV_NAME% already exists. Skipping creation.
    CALL "%CONDA_BAT%" env update -f environment.yml --prune
)

REM Activate environment
CALL "%CONDA_BAT%" activate %ENV_NAME%

ENDLOCAL
