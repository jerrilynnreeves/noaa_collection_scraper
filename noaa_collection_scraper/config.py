from __future__ import annotations
from pathlib import Path
import os
import logging
import sys


class Config:
    """
    Centralized configuration for the NOAA Collection Scraper pipeline.
    Defines directory structure, file paths, API endpoints, and runtime parameters.
    """

    # --- Base Directories ---
    BASE_DIR = Path(__file__).resolve().parent.parent.parent
    DATA_DIR = BASE_DIR / "data"
    LOG_DIR = BASE_DIR / "logs"

    # --- Data Files ---
    DB_PATH: Path = DATA_DIR / "etag_store.db"
    PARQUET_FILE: Path = DATA_DIR / "metadata_extracted.parquet"
    JSON_FILE: Path = DATA_DIR / "metadata_extracted.json" # Only used when user uncomment for debug purposes

    # --- API Endpoints ---
    OSIM_SEARCH_API: str = "https://data.noaa.gov/onestop/api/search"
    OSIM_COLLECTION_SEARCH: str = f"{OSIM_SEARCH_API}/collection/{{uuid}}"

    WAF_BASE_URL: str = "https://data.noaa.gov/waf/"

    # --- Network / Scraper Parameters ---
    MAX_CONCURRENT_REQUESTS: int = 15 # Overridden to 10 in metadata_scraper.py for gentler load
    REQUEST_TIMEOUT: int = 30  # seconds
    RETRIES: int = 4
    BACKOFF_BASE: float = 2.0
    BATCH_SIZE: int = 5000

    HEADERS: dict[str, str] = {"User-Agent": "TMF-Collection-Tool/2.0"}

    # --- Runtime Stat Bar Behavior Flags ---
    TQDM_ENABLED: bool = True  # Display progress bars by default

    # --- Utility Methods ---

    @staticmethod
    def ensure_dirs() -> None:
        """Create required directories if they don't already exist."""
        Config.DATA_DIR.mkdir(parents=True, exist_ok=True)
        Config.LOG_DIR.mkdir(parents=True, exist_ok=True)

    @classmethod
    def from_env(cls) -> None:
        """
        Load configuration overrides from environment variables.
        Allows orchestration or external tools to adjust behavior without editing code.
        """
        cls.TQDM_ENABLED = os.getenv("TQDM_ENABLED", "true").lower() in (
            "1", "true", "yes", "on"
        )

    @classmethod
    def set_tqdm(cls, enabled: bool) -> None:
        """
        Manually enable/disable tqdm progress bars.
        Primarily used by orchestrator before launching subprocesses.
        """
        cls.TQDM_ENABLED = enabled

    @staticmethod
    def setup_logger(name: str, log_file: Path, level=logging.INFO) -> logging.Logger:
        """
        Configures and returns a named logger with a file and console handler.
        """
        Config.LOG_DIR.mkdir(parents=True, exist_ok=True)
        logger = logging.getLogger(name)
        logger.setLevel(level)

        # Prevent duplicate handlers if already configured
        if logger.handlers:
            return logger

        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )

        # File Handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Console Handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        return logger
