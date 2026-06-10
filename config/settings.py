"""
config/settings.py
==================
Centralised application settings loaded from environment variables.
All pipeline components import from here — never read os.environ directly.
"""
import os
from dotenv import load_dotenv

load_dotenv()


class DatabaseSettings:
    HOST: str = os.getenv("DB_HOST", "localhost")
    PORT: int = int(os.getenv("DB_PORT", 5432))
    NAME: str = os.getenv("DB_NAME", "scraping_pipeline")
    USER: str = os.getenv("DB_USER", "postgres")
    PASSWORD: str = os.getenv("DB_PASSWORD", "")

    @classmethod
    def dsn(cls) -> str:
        return (
            f"postgresql://{cls.USER}:{cls.PASSWORD}"
            f"@{cls.HOST}:{cls.PORT}/{cls.NAME}"
        )

    @classmethod
    def psycopg2_params(cls) -> dict:
        return {
            "host": cls.HOST,
            "port": cls.PORT,
            "dbname": cls.NAME,
            "user": cls.USER,
            "password": cls.PASSWORD,
        }


class PipelineSettings:
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", 100))
    REQUEST_DELAY: float = float(os.getenv("REQUEST_DELAY", 1.5))
    REQUEST_TIMEOUT: int = int(os.getenv("REQUEST_TIMEOUT", 30))
    MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", 3))
    CONCURRENT_REQUESTS: int = int(os.getenv("CONCURRENT_REQUESTS", 8))
    SOURCES_CONFIG: str = os.path.join(
        os.path.dirname(__file__), "sources.yaml"
    )


class ScrapySettings:
    USER_AGENT: str = os.getenv(
        "SCRAPY_USER_AGENT",
        "Mozilla/5.0 (compatible; ResearchBot/1.0)",
    )
    ROBOTSTXT_OBEY: bool = True
    DOWNLOAD_DELAY: float = PipelineSettings.REQUEST_DELAY
    CONCURRENT_REQUESTS_PER_DOMAIN: int = 2
    AUTOTHROTTLE_ENABLED: bool = True
    AUTOTHROTTLE_START_DELAY: float = 1.0
    AUTOTHROTTLE_MAX_DELAY: float = 10.0
    HTTPCACHE_ENABLED: bool = False
    LOG_LEVEL: str = "WARNING"
