from functools import lru_cache
from typing import List
from pydantic_settings import BaseSettings, SettingsConfigDict
from dotenv import load_dotenv

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
    )

    # Where all zipped files with be stored
    SINGLE_ZIP_DIR: str = ""

    # TTL for directory scans so we do not rescan on every request
    ZIP_SCAN_TTL_SECONDS: int = 10

    SURVEY_TYPES: List[str] = []
    OUTPUT_DIR: str = ""
    CONFIG_ROOT: str = ""
    CONFIG_FILENAME: str = "config.json"
    CONFIG_PER_SURVEY_TYPE_SUBFOLDER: bool = True
    CONFIG_TTL_SECONDS: int = 60
    CONDA_DEFAULT_ENV: str = "survey-mapper"
    USE_DATABASE: bool = False

@lru_cache
def get_settings() -> Settings:
    return Settings()

def refresh_settings() -> Settings:
    # Re-read .env on disk and rebuild Settings
    load_dotenv(override=True)
    get_settings.cache_clear()  # type: ignore[attr-defined]
    return get_settings()
