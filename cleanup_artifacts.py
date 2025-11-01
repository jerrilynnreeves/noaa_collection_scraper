#!/usr/bin/env python3
"""
Cleanup Artifacts Utility
------------------------------------------
Deletes old artifacts in the data directory
according to retention policy.

Can be run independently or as part of the orchestration.
"""

import logging
from pathlib import Path
from datetime import datetime
from typing import List
from noaa_collection_scraper.config import Config


# -------------------- CONFIGURATION --------------------

# Ensure directories exist before defining log file path
Config.ensure_dirs()

# Define the log file path using the current date
LOG_FILE: Path = Config.LOG_DIR / f"cleanup_{datetime.now():%Y-%m-%d}.log"

# Initialize the logger using the centralized utility
logger = Config.setup_logger(__name__, LOG_FILE)


KEEP_COUNTS: dict[str, int] = {
    "waf_list_*.json": 7,
    "*.log": 14,
}

# When automatic db dumps are implemented also clean out x number of old dumps

# -------------------- UTILITIES --------------------

def _glob_sorted(pattern: str) -> List[Path]:
    """Return a list of files matching pattern, sorted by modification time (newest first)."""
    files: List[Path] = list(Config.DATA_DIR.glob(pattern))
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return files


def cleanup_old_files() -> None:
    """Remove old data and log artifacts based on KEEP_COUNTS."""
    logger.info("Cleaning up old artifacts in %s", Config.DATA_DIR)
    for pattern, keep in KEEP_COUNTS.items():
        files: List[Path] = _glob_sorted(pattern)
        to_delete: List[Path] = files[keep:]
        for f in to_delete:
            try:
                f.unlink()
                logger.info("Deleted: %s", f.name)
            except Exception as e:
                logger.warning("Failed to delete %s: %s", f.name, e)
        logger.info(
            "Kept %d of %s (had %d)", min(len(files), keep), pattern, len(files)
        )


def main() -> None:
    """Entry point for cleanup utility."""
    # The logger is already initialized globally above the main function
    logger.info("==== Cleanup started ====")
    cleanup_old_files()
    logger.info("Cleanup completed successfully.")


if __name__ == "__main__":
    main()