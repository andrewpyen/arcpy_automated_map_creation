# Survey Mapper

## 1. Background

The **Survey Mapper** tool is a Python/FastAPI-based utility that automates:

* Extracting GIS layers from a file geodatabase (or shapefiles)
* Joining those layers with Excel sheet attributes
* Clipping them by zones (e.g., GridZones or Mapsheets)
* Exporting them as ESRI Shapefiles by survey type or map sheet
* (TODO) Creating and exporting maps

It supports organizations conducting field surveys who need to rapidly subset and export survey maps by zone.


---

## 2. Setting Up the Environment with ArcGIS Pro's Conda

ArcGIS Pro includes its own pre-configured conda environment that contains `arcpy` and geospatial libraries. This project builds on that environment.

### Run Bat File to Create or Update Conda Environment
Run the batch file to create/update the environment:
```cmd
.\install-run-env.bat
```

>### If you need to troubleshoot the conda environment, use these steps to activate the conda environment and manually install, remove, or update an environment.
>```cmd
>># List all conda commands
>"C:\Program Files\ArcGIS\Pro\bin\Python\condabin\conda.bat" --help
>
># Activate the virtual environment to install/remove/list packages
>"C:\Program Files\ArcGIS\Pro\bin\Python\condabin\conda.bat" activate survey-mapper
>
># Install new package
>"C:\Program Files\ArcGIS\Pro\bin\Python\condabin\conda.bat" install uvicorn
>
># List packages installed
>"C:\Program Files\ArcGIS\Pro\bin\Python\condabin\conda.bat" list
>
># Check on virtual environments currently installed on system and where they are:
>"C:\Program Files\ArcGIS\Pro\bin\Python\condabin\conda.bat" info --envs

### Run the VS Code Task
There are VS Code tasks pre-configured to call the bat file to start either a debugging session or non-debugging session. See file `survey-mapper\app\cli\run_survey_mapper.bat`
1. Under `Terminal` in VS Code --> Run Task
2. Select the task "1 - Call test.bat - to debug" (this is found in `GIS-MAPCREATION\.vscode\tasks.json`)
3. If debugging, then attach a debugger (hist the "play" debugger)
4. Visit the app at http://localhost:8000/docs 

### To Deactivate the Environment
In the terminal run this one command:
```cmd
"C:\Program Files\ArcGIS\Pro\bin\Python\condabin\conda.bat" deactivate
```
---

## 3. Using the CLI

### Configuration

Prepare a config file like `config/survey_mapper_config.json`:

```json
{
  "projectName": "AZCA",
  "surveyType": "DOT",
  "outputDirectory": "/output",
  "lutAssetTypesNewNameField": "AlternativeName",
  "gridzones": {
    "feature_class_name_source": "Gridzones",
    "shapefile_name_target": "Gridzones",
    "GridZoneId_field": "GridZoneId",
    "fields_to_map": [
      {"name": "FNAME", "alias": "FNAME", "type": "Text", "length": 50},
      {"name": "LNAME", "alias": "LNAME", "type": "Text", "length": 50}
    ]
  },
  "feature_classes_to_clip": [
    {
      "feature_class_name_source": "Mains",
      "shapefile_name_target": "Mains",
      "fields_to_map": [
        {"name": "FNAME", "alias": "FNAME", "type": "Text", "length": 50}
      ],
      "attribute_query": "MAINTYPE IN ('Feeder','Transmission','Distribution')"
    },
          {
        "feature_class_name_source_DETAILS": "Required. Name of feature class contained in file geodatabase.",
        "feature_class_name_source": "LONG_SERVICES",
        "shapefile_name_target": "NILines",
        "fields_to_map_DETAILS": "Optional. List of field mapping configurations with name, alias, type, and length for ExportFeatures.",
        "fields_to_map": [
          {
            "name": "Type",
            "alias": "Type",
            "type": "Text",
            "length": 255
          }
        ],
        "merge_clip_result_with_these_clip_layers": ["NILines", "MegaServices"]
      },
      {
        "feature_class_name_source_DETAILS": "Required. Name of feature class contained in file geodatabase.",
        "isAnnotationLayer": true,
        "feature_class_name_source": "LandbaseTextDimension",
        "shapefile_name_target": "LandbaseTextDimension",
        "merge_clip_result_with_these_clip_layers": [
          "LandbaseTextDimension", 
          "MainAnnotation", 
          "ServiceAnnotation", 
          "LandbaseTextProposed", 
          "LandbaseTextLotNum", 
          "LandbaseTextAddr"
        ]
      },
  ]
}
```

### Run the CLI Tool

Run the `run_survey_mapper.bat` file in the Windows machine to start the fast api application.

```cmd
cli\run_survey_mapper.bat
```

---

## 4. FastAPI Service
### Features

- Accepts Excel (.xlsx) and zipped File Geodatabase (.gdb.zip)
- Uses a JSON configuration file
- Runs clipping and merging in background jobs
- Job status available via /jobs/{id}
- `/health` endpoint for monitoring

### Run the API
1. Activate the environment:
```cmd
conda activate survey-mapper
```
2. Install required packages if missing:
```cmd
conda install fastapi uvicorn python-dotenv python-multipart pandas openpyxl sqlalchemy asyncpg psycopg2-binary -c conda-forge
```
3. Start the server:
```cmd
uvicorn app.main:main_app --host 0.0.0.0 --port 8000 --reload
```
4. Open API docs:
- http://localhost:8000/docs

5. Open the Config Editor
- http://localhost:8000/config/edit


## 5. Deployment (Shared Drive Configuration)
Survey Mapper can be deployed without passing configs through routes. Instead, configurations can be stored in a central Microsoft Shared drive (UNC path or a synced SharePoint/OneDrive library).

### Deployment notes for Microsoft Shared drive paths on Windows

**Prerequisite:**: The GIS-MAPCREATION repo should be synced and pulled from the Microsoft Azure DevOps repository to store the files locally on the machine. Each time there is an update to the configs you will need to run a `git pull` onto this web server machine to have the latest code changes. Alternatively, you could can choose to copy the config.json files manually outside of source control.

- OPTION 1: If you use a UNC path like `\\fileserver\GIS-MAPCREATION\survey-mapper\configs`, run your FastAPI service under a Windows account that has read permission to that share. Services running as LocalSystem will not see domain shares. `!!IMPORTANT!!` **Use a domain service account or gMSA and grant it read permission.**

- OPTION 2: If you use OneDrive/SharePoint "Shared Library" synced locally, point CONFIG_ROOT to the synced folder path, for example: `C:\Users\<user>\SharePoint\<SiteName>\GIS-MAPCREATION\survey-mapper\configs`

Avoid mapped drive letters in services. Stick to UNC or absolute local paths.
### Setup
1. Place a `config.json` file for each client inside the shared drive:
```txt
\\fileserver\GIS\SurveyMapper\configs\<client>\config.json
```
2. Set environment variables (in .env or system service) as follows. Create the `.env` file within the `\survey-mapper` folder:
```ini
# .env file for Survey Mapper deployment

# Root folder where all client configs live.
# UNC path for Shared Drive OR local SharePoint/OneDrive sync path.
#CONFIG_ROOT=\\fileserver\GIS\SurveyMapper\configs
CONFIG_ROOT=C:\<SharePoint Synced Location>\repos\GIS-MAPCREATION\survey-mapper\configs

# List of survey types supported, comma-separated, no spaces, no quotes
SURVEY_TYPES=dot, mobile-patrol, CAZ_Mobile_Patrol

# Name of the config file expected in each survey type folder.
CONFIG_FILENAME=config.json

# If true, looks for config in a subfolder for each survey type:
#   \\fileserver\GIS\SurveyMapper\configs\<survey_type>\config.json
# If false, uses CONFIG_ROOT\<survey type>.json
CONFIG_PER_SURVEY_TYPE_SUBFOLDER=true

# Cache time-to-live (seconds) for loaded configs before reload from disk.
CONFIG_TTL_SECONDS=60

# (Optional) Environment for ArcGIS Pro conda setup
CONDA_DEFAULT_ENV=survey-mapper

# Options are true or false
# If true, enables database usage for storing survey results.
USE_DATABASE=false

# Types of databases supported: postgres, mssql, mysql, sqlite
DB_TYPE=""
DB_USER=""
DB_PASS=""
DB_HOST=""
DB_PORT=""
DB_NAME=""
```

3. Ensure the FastAPI service account has read permissions on the share.
- Avoid mapped drives (e.g. Z:). Use UNC paths or a local synced folder.

### Using in Routes
Requests specify the Survey Type either as a query parameter or header:

```bash
POST /jobs?surveyType=dot
```

Survey Mapper will automatically:
- Resolve the path `\\fileserver\GIS\SurveyMapper\configs\dot\config.json`
- Load and validate the config
- Run the requested job

This design supports multi-tenant deployments without uploading configs per request.

---

## 6. Testing
### Enable Unit Test Discovery
1. VS Code → Ctrl+Shift+P
2. Choose Python: Configure Tests
3. Select:
- Framework: unittest
- Folder: .

### Run Tests
```bash
python -m unittest discover -s tests
```

---

For questions or extensions (e.g., zip exports, GDB input, ArcGIS Online support), open an issue or contact the author.
