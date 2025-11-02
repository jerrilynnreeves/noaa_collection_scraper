#!/usr/bin/env python3
"""
OSIM Status Updater (PyArrow Version, Refactored and Safe)
----------------------------------------------------------
Checks whether each NOAA collection (by UUID) exists in the OneStop (OSIM) API.
Updates both the 'in_osim' (bool) and 'granule_count' (int) columns
in the local metadata_extracted.parquet file.

Key improvements:
- Keeps all rows aligned (even with missing UUIDs)
- Uses asyncio.gather() for safe, ordered concurrency
- Includes detailed logging and sanity checks
- Writes updated Parquet atomically and efficiently
"""

from __future__ import annotations
import asyncio
import aiohttp
import time
from typing import Any, Tuple, List
from tqdm.asyncio import tqdm as atqdm
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
from noaa_collection_scraper.config import Config


# -------------------- CONFIG --------------------
Config.ensure_dirs()
LOG_FILE = Config.LOG_DIR / f"osim_meta_{datetime.now():%Y-%m-%d}.log"

MAX_CONCURRENT: int = 100
REQUEST_TIMEOUT: int = Config.REQUEST_TIMEOUT
HEADERS: dict[str, str] = {"User-Agent": "NOAA-OSIM-Checker/1.0"}
OSIM_URL: str = Config.OSIM_COLLECTION_SEARCH  # should include {uuid} placeholder

# Setup logger
logger = Config.setup_logger(__name__, LOG_FILE)


# -------------------- FETCH --------------------
async def check_in_osim(session: aiohttp.ClientSession, uuid: str | None) -> tuple[bool, int | None]:
    """
    Query the OSIM API for a collection UUID.
    Returns (in_osim: bool, granule_count: int or None).

    If UUID is None or blank, returns (False, None) without request.
    """
    if not uuid or uuid.lower() == "none":
        return False, None

    url: str = OSIM_URL.format(uuid=uuid)
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as resp:
            status = resp.status
            if status == 200:
                data: dict[str, Any] = await resp.json()
                granule_count: int | None = data.get("meta", {}).get("totalGranules")
                #logger.info(f"{uuid} | Found (granule_count: {granule_count})")
                return True, granule_count
            elif status in (404, 500):
                #logger.warning(f"{uuid} | Not Found HTTP {status}")
                return False, None
            else:
                logger.warning(f"{uuid} | Unexpected HTTP {status}")
                return False, None
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as e:
        logger.warning(f"{uuid} | Request failed: {e}")
        return False, None


# -------------------- MAIN --------------------
async def main() -> None:
    """Main entry point for OSIM status update process."""
    start_time = time.time()
    parquet_path = Config.PARQUET_FILE

    if not parquet_path.exists():
        raise FileNotFoundError(f"Missing parquet file: {parquet_path}")

    # --- Load data ---
    table: pa.Table = pq.read_table(parquet_path)
    data: dict[str, list[Any]] = table.to_pydict()

    if "uuid" not in data:
        raise ValueError("The parquet file must contain a 'uuid' column")

    uuid_column = data["uuid"]
    uuids: list[str | None] = [str(u) if u is not None else None for u in uuid_column]

    logger.info(f"Starting OSIM check for {len(uuids)} UUIDs")

    # --- Async HTTP session setup ---
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT, ttl_dns_cache=300)
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
    sem = asyncio.Semaphore(MAX_CONCURRENT)

    async with aiohttp.ClientSession(headers=HEADERS, connector=connector, timeout=timeout) as session:
        async def bounded(uuid: str | None) -> tuple[bool, int | None]:
            async with sem:
                return await check_in_osim(session, uuid)

        # Launch all tasks (gather preserves order)
        tasks = [bounded(u) for u in uuids]
        results: list[tuple[bool, int | None]] = await atqdm.gather(*tasks, desc="Checking OSIM", total=len(tasks))

    # --- Validate alignment ---
    if len(results) != len(uuids):
        raise ValueError(f"Result mismatch: got {len(results)}, expected {len(uuids)}")

    # --- Unpack results ---
    in_osim_vals, granule_counts = zip(*results)
    data["in_osim"] = list(in_osim_vals)
    data["granule_count"] = list(granule_counts)

    # --- Sanity check before writing ---
    expected_len = len(uuid_column)
    for k, v in data.items():
        if len(v) != expected_len:
            logger.error(f"Column '{k}' length mismatch ({len(v)} vs {expected_len})")
            raise ValueError(f"Column '{k}' length mismatch ({len(v)} vs {expected_len})")

    # --- Write updated parquet ---
    tmp_path = parquet_path.with_suffix(".osim_tmp.parquet")
    new_table = pa.table(data)
    pq.write_table(new_table, tmp_path, compression="snappy")

    # Replace original only if successful
    tmp_path.replace(parquet_path)

    # --- Summary ---
    found_count = sum(in_osim_vals)
    missing_count = len(uuids) - found_count
    elapsed = round(time.time() - start_time, 2)
    rps = len(uuids) / elapsed if elapsed else 0

    msg = (
        f"Found: {found_count:,} | Missing: {missing_count:,}\n"
        f"Completed OSIM update in {elapsed:.2f}s ({rps:.2f} UUIDs/sec)\n"
        f"Updated parquet saved to: {parquet_path}"
    )
    logger.info(msg)
    print(f"\n✅ {msg}")


# -------------------- ENTRY --------------------
if __name__ == "__main__":
    asyncio.run(main())
