#!/usr/bin/env python3
"""
NOAA Metadata Pipeline Orchestrator
------------------------------------------
Sequentially runs the NOAA WAF → ETag → Metadata → OSIM pipeline,
then calls a separate cleanup script to manage retention of old files.

Responsibilities:
  • Execute each stage as a subprocess
  • Capture logs in /logs/orchestration_<timestamp>.log
  • Print a timing summary for each step
"""

import sys
import os
import time
import subprocess
import logging
from datetime import datetime
from pathlib import Path
from noaa_collection_scraper.config import Config


# -------------------- CONFIG --------------------

Config.ensure_dirs()
LOG_FILE = Config.LOG_DIR / f"pipeline_runner__{datetime.now():%Y-%m-%d_%H%M%S}.log"

# Disable tqdm progress bars in orchestration logs
Config.set_tqdm(False)
os.environ["TQDM_ENABLED"] = "false"

# Define pipeline steps as module names (NOT file paths)
STEPS = [
    "url_scraper",
    "metadata_scraper",
    "osim_meta",
    "cleanup_artifacts",
]


# -------------------- LOGGING SETUP --------------------

# Configure the logger using the centralized Config utility
logger = Config.setup_logger(__name__, LOG_FILE)

# Add a StreamHandler to ensure logs also output to the console (sys.stdout)
# We must do this manually because Config.setup_logger only adds the FileHandler.
console_handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
    "%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
console_handler.setFormatter(formatter)

# Only add the console handler if it's not already present
if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
    logger.addHandler(console_handler)

# Initial log messages using the configured logger
logger.info("==== Pipeline started ====")
logger.info("Log file: %s", LOG_FILE)


# -------------------- SUBPROCESS EXECUTION --------------------

def run_step(module_name: str) -> tuple[int, float]:
    """
    Run a pipeline step as a Python module in a subprocess.
    Returns (return_code, elapsed_seconds).
    """
    start = time.perf_counter()
    # The command should assume the module is runnable within the package structure
    cmd = [sys.executable, "-m", f"noaa_collection_scraper.{module_name}"] 
    logger.info("-> Running module: %s", module_name) # Use logger

    try:
        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
        elapsed = time.perf_counter() - start

        # Stream subprocess output into orchestrator log
        if proc.stdout:
            for line in proc.stdout.splitlines():
                logger.info("[%-24s] %s", module_name, line) # Use logger

        if proc.returncode == 0:
            logger.info("<- %s completed OK in %.2fs", module_name, elapsed) # Use logger
        else:
            logger.error("<- %s FAILED (code %s) in %.2fs", module_name, proc.returncode, elapsed) # Use logger

        return proc.returncode, elapsed

    except Exception as e:
        elapsed = time.perf_counter() - start
        logger.exception("<- %s raised exception after %.2fs: %s", module_name, elapsed, e) # Use logger
        return 1, elapsed


# -------------------- MAIN --------------------

def main() -> None:
    # Logging setup is now done globally above the main function
    
    overall_start = time.perf_counter()
    failures: list[str] = []
    timings: list[tuple[str, float]] = []

    for step in STEPS:
        rc, elapsed = run_step(step)
        timings.append((step, elapsed))
        if rc != 0:
            failures.append(step)

    total_elapsed = time.perf_counter() - overall_start
    mins, secs = divmod(total_elapsed, 60)

    # Summary (Using the configured 'logger')
    logger.info("\n----------------------------")
    logger.info("Pipeline Timing Summary")
    logger.info("----------------------------")
    for step, t in timings:
        step_mins, step_secs = divmod(t, 60)
        logger.info("%-25s : %6.1fs (%dm %ds)", step, t, int(step_mins), int(step_secs))

    logger.info("----------------------------")
    logger.info("Total: %6.1fs (%dm %ds)", total_elapsed, int(mins), int(secs))
    logger.info("----------------------------")

    if failures:
        logger.error("Pipeline completed with errors: %s", ", ".join(failures))
        sys.exit(1)
    else:
        logger.info("All pipeline steps completed successfully.")


if __name__ == "__main__":
    main()