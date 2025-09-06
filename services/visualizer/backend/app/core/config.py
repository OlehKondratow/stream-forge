import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    ARANGO_URL: str = os.getenv("ARANGO_URL", "http://localhost:8529")
    ARANGO_USER: str = os.getenv("ARANGO_USER", "root")
    ARANGO_PASSWORD: str = os.getenv("ARANGO_PASSWORD", "root")
    ARANGO_DB: str = os.getenv("ARANGO_DB", "stream_forge")
    ARANGO_COLLECTION: str = os.getenv("ARANGO_COLLECTION", "technical_indicators_stream")

    class Config:
        env_file = ".env"

settings = Settings()
