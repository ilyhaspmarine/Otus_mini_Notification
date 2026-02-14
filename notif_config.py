from pathlib import Path
from pydantic import BaseModel
from pydantic_settings import BaseSettings
import os

BASE_DIR = Path(__file__).parent

class ProfileURL(BaseModel):
    host: str = os.getenv("PROFILE_HOST", "arch.homework")
    path: str = os.getenv("PROFILE_PATH")
    get_endpoint: str = os.getenv("PROFILE_GET_ENDPOINT", "users")
    port: str = os.getenv("PROFILE_PORT")

class DbSettings(BaseModel):
    driver: str = "postgresql+" + os.getenv("DB_DRIVER_ASYNC", "asyncpg")
    username: str = os.getenv("DB_USER", "username")
    password: str = os.getenv("DB_PASSWORD", "password")
    host: str = os.getenv("DB_HOST", "host.docker.internal")
    port: str = os.getenv("DB_PORT", "5432")
    database: str = os.getenv("DB_NAME", "notifications")

class Settings(BaseSettings):
    db: DbSettings = DbSettings()
    prof_url: ProfileURL = ProfileURL()

settings = Settings()