#!/usr/bin/env python3
"""
OSIM Status Updater (PyArrow Version)
------------------------------------------
Checks whether each NOAA collection (by UUID) exists in the OneStop (OSIM) API.
Updates both the 'in_osim' (bool) and 'granule_count' (int) columns
in the local metadata_extracted.parquet file.

Now uses Apache Arrow for I/O (faster and memory-efficient).
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


# Configure the logger using the centralized Config utility
logger = Config.setup_logger(__name__, LOG_FILE)


# -------------------- FETCH --------------------
async def check_in_osim(session: aiohttp.ClientSession, uuid: str) -> tuple[bool, int | None]:
    """
    Query the OSIM API for a collection UUID.
    Returns (in_osim: bool, granule_count: int or None).

    Notes:
      • OSIM sometimes returns HTTP 500 when a record does not exist.
        We treat that as 'not in OSIM' (False, None).
      • Only HTTP 200 is considered a valid 'found' response.
    """
    url: str = OSIM_URL.format(uuid=uuid)
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as resp:
            status: int = resp.status

            if status == 200:
                data: dict[str, Any] = await resp.json()
                granule_count: int | None = data.get("meta", {}).get("totalGranules")
                return True, granule_count

            elif status in (404, 500):
                logger.info(f"{uuid} | Not found (HTTP {status})")
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
    start_time: float = time.time()
    parquet_path = Config.PARQUET_FILE

    if not parquet_path.exists():
        raise FileNotFoundError(f"Missing parquet file: {parquet_path}")

    # Read Parquet with PyArrow (fast and memory-efficient)
    table: pa.Table = pq.read_table(parquet_path)
    data: dict[str, list[Any]] = table.to_pydict()

    if "uuid" not in data:
        raise ValueError("The parquet file must contain a 'uuid' column")

    uuids: list[str] = [str(u) for u in data["uuid"] if u is not None]
    logger.info(f"Starting OSIM check for {len(uuids)} UUIDs")

    sem = asyncio.Semaphore(MAX_CONCURRENT)
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT, ttl_dns_cache=300)

    async with aiohttp.ClientSession(headers=HEADERS, connector=connector) as session:
        async def bounded(uuid: str) -> tuple[bool, int | None]:
            async with sem:
                return await check_in_osim(session, uuid)

        tasks: list[asyncio.Task[tuple[bool, int | None]]] = [asyncio.create_task(bounded(u)) for u in uuids]

        # Manual async progress bar
        results: list[tuple[bool, int | None]] = []
        for coro in atqdm.as_completed(tasks, total=len(tasks), desc="Checking OSIM"):
            try:
                r: tuple[bool, int | None] = await coro
                results.append(r)
            except Exception as e:
                logger.error(f"Task failed with exception: {e}")
                results.append((False, None))

    # Ensure alignment between results and uuids
    if len(results) != len(uuids):
        logger.error(f"Row mismatch: results={len(results)}, uuids={len(uuids)}")
        while len(results) < len(uuids):
            results.append((False, None))
        if len(results) > len(uuids):
            results = results[: len(uuids)]

    # Unpack results
    in_osim_vals, granule_counts = zip(*results)

    # Update or add new columns
    data["in_osim"] = list(in_osim_vals)
    data["granule_count"] = list(granule_counts)

    # Convert back to Arrow Table and write to Parquet
    new_table: pa.Table = pa.table(data)
    pq.write_table(new_table, parquet_path, compression="snappy")

    # Summary stats
    found_count: int = sum(in_osim_vals)
    missing_count: int = len(uuids) - found_count
    elapsed: float = round(time.time() - start_time, 2)
    rps: float = len(uuids) / elapsed if elapsed else 0

    logger.info(f"Found: {found_count:,} | Missing: {missing_count:,}")
    logger.info(f"Completed OSIM update in {elapsed:.2f}s ({rps:.2f} UUIDs/sec)")
    logger.info(f"Updated parquet saved to: {parquet_path}")

    print(f"Done: {found_count:,} found, {missing_count:,} missing in {elapsed:.2f}s ({rps:.2f} UUIDs/sec)")


# -------------------- ENTRY --------------------
if __name__ == "__main__":
    asyncio.run(main())
