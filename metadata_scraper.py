#!/usr/bin/env python3
"""
Unified Metadata Fetcher based on ETag Changes (PyArrow Version)
------------------------------------------------
Checks NOAA WAF XML URLs stored in the database for changes via ETag headers.

Behavior:
  • Loads all active (deleted=0) URLs from the etags table
  • Performs conditional GET using ETag headers
  • If unchanged (304) → updates last_checked in DB
  • If changed (200) → updates ETag, extracts metadata, and appends to Parquet
  • Uses Apache Arrow instead of Pandas for efficient I/O
"""

from __future__ import annotations
import asyncio
import aiohttp
import json  # kept for possible future output
import time
import random
from typing import Any, Dict, List, Tuple
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from lxml import etree
from tqdm.asyncio import tqdm
from noaa_collection_scraper.config import Config
from noaa_collection_scraper.db_utils import connect_db, init_schema


# -------------------- CONFIG --------------------
Config.ensure_dirs()
DATA_DIR = Config.DATA_DIR
PARQUET_PATH = Config.PARQUET_FILE
JSON_PATH = Config.JSON_FILE
LOG_FILE = Config.LOG_DIR / f"metadata_scracper_{datetime.now():%Y-%m-%d}.log"

MAX_CONCURRENT_REQUESTS: int = 10
REQUEST_TIMEOUT: int = Config.REQUEST_TIMEOUT
RETRIES: int = Config.RETRIES
BACKOFF_BASE: float = Config.BACKOFF_BASE
HEADERS: dict[str, str] = Config.HEADERS


# Configure the logger using the centralized Config utility
logger = Config.setup_logger(__name__, LOG_FILE)


# -------------------- NAMESPACES --------------------
NS: dict[str, str] = {
    "gmd": "http://www.isotc211.org/2005/gmd",
    "gco": "http://www.isotc211.org/2005/gco",
    "gmx": "http://www.isotc211.org/2005/gmx",
    "xlink": "http://www.w3.org/1999/xlink",
}


# -------------------- METADATA EXTRACTION --------------------
def extract_metadata(xml_bytes: bytes, url: str) -> dict[str, Any]:
    """Parse minimal metadata fields from an XML record."""
    try:
        tree = etree.fromstring(xml_bytes)
    except Exception as e:
        return {"source": url, "error": f"Invalid XML: {e}"}

    def text(xpath: str) -> str | None:
        res = tree.xpath(xpath, namespaces=NS)
        return res[0].strip() if res else None

    return {
        "source": url,
        "uuid": tree.attrib.get("uuid"),
        "fileIdentifier": text("//gmd:fileIdentifier/gco:CharacterString/text() | //gmd:fileIdentifier/gmx:Anchor/text()"),
        "title": text("//gmd:identificationInfo//gmd:citation//gmd:title/*/text()"),
        "edition": text("//gmd:identificationInfo//gmd:citation//gmd:edition/gco:CharacterString/text()"),
        "doi": text("//gmd:identifier//gmx:Anchor[contains(@xlink:href, 'doi.org')]/text()"),
        "dateStamp": text("//gmd:dateStamp/gco:DateTime/text() | //gmd:dateStamp/gco:Date/text()"),
    }


# -------------------- DATABASE HELPERS --------------------
def load_active_etags() -> dict[str, str | None]:
    """Return {url: etag} for all active (deleted=0) URLs."""
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("SELECT url, etag FROM etags WHERE deleted=0;")
    data = dict(cur.fetchall())
    conn.close()
    return data


def update_etag_record(url: str, etag: str | None) -> None:
    """Update the ETag and timestamp for a URL."""
    conn = connect_db()
    cur = conn.cursor()
    now = datetime.now().isoformat()
    cur.execute(
        """
        UPDATE etags
        SET etag=?, last_checked=?
        WHERE url=?;
        """,
        (etag, now, url),
    )
    conn.commit()
    conn.close()


# -------------------- CONDITIONAL FETCH --------------------
async def fetch_conditional(
    session: aiohttp.ClientSession, url: str, etag: str | None
) -> dict[str, Any]:
    """Perform conditional GET using the current ETag value."""
    headers = HEADERS.copy()
    first_check: bool = etag is None

    if etag:
        headers["If-None-Match"] = f'"{etag}"'

    for attempt in range(1, RETRIES + 1):
        try:
            async with session.get(
                url,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
            ) as resp:
                status = resp.status

                # Unchanged
                if status == 304:
                    return {
                        "url": url,
                        "etag": etag,
                        "changed": False,
                        "first_check": first_check,
                    }

                # Error
                if status >= 400:
                    logger.warning(f"[ERROR {status}] {url}")
                    return {
                        "url": url,
                        "etag": etag,
                        "changed": False,
                        "error": f"HTTP {status}",
                    }

                # Changed
                new_etag = resp.headers.get("ETag")
                if new_etag:
                    new_etag = new_etag.strip('"')
                xml_bytes: bytes = await resp.read()
                metadata = extract_metadata(xml_bytes, url)
                logger.info(f"[CHANGED] {url} | new ETag={new_etag}")
                return {
                    "url": url,
                    "etag": new_etag,
                    "changed": True,
                    "metadata": metadata,
                    "first_check": first_check,
                }

        except Exception as e:
            logger.warning(f"{url} | Attempt {attempt} failed: {e}")
            delay: float = (BACKOFF_BASE ** attempt) + random.uniform(0, 1)
            await asyncio.sleep(delay)

    return {"url": url, "etag": etag, "error": "Failed after retries", "first_check": first_check}


async def scrape_all(etag_map: dict[str, str | None]) -> list[dict[str, Any]]:
    """Run conditional requests for all active URLs."""
    sem = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    results: list[dict[str, Any]] = []
    urls: list[str] = list(etag_map.keys())

    for i in range(0, len(urls), Config.BATCH_SIZE):
        batch = urls[i : i + Config.BATCH_SIZE]
        connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=300)
        async with aiohttp.ClientSession(connector=connector) as session:
            async def bounded(u: str) -> dict[str, Any]:
                async with sem:
                    return await fetch_conditional(session, u, etag_map.get(u))
            batch_results: list[dict[str, Any]] = await tqdm.gather(
                *[bounded(u) for u in batch], total=len(batch)
            )
            results.extend(batch_results)
        await asyncio.sleep(1)

    return results


# -------------------- MAIN --------------------
async def main() -> None:
    """Main entry point for unified ETag + metadata fetcher."""
    start: float = time.time()
    logger.info("Starting unified ETag + metadata fetcher...")

    conn = connect_db(allow_create=True)
    init_schema(conn, close_after=True)

    etag_map: dict[str, str | None] = load_active_etags()
    logger.info(f"Loaded {len(etag_map)} active URLs for ETag check.")

    results: list[dict[str, Any]] = await scrape_all(etag_map)

    changed = [r for r in results if r.get("changed")]
    unchanged = [r for r in results if r.get("changed") is False]
    errors = [r for r in results if "error" in r]

    for r in changed + unchanged:
        update_etag_record(r["url"], r.get("etag"))

    # Summary
    print("\nSummary:")
    print(f"  • Total checked: {len(results)}")
    print(f"  • Changed: {len(changed)}")
    print(f"  • Unchanged: {len(unchanged)}")
    print(f"  • Errors: {len(errors)}")

    # --- Save metadata for changed URLs ---
    new_records: list[dict[str, Any]] = [r["metadata"] for r in changed if "metadata" in r]
    if new_records:
        new_table: pa.Table = pa.Table.from_pylist(new_records)

        if PARQUET_PATH.exists():
            existing_table: pa.Table = pq.read_table(PARQUET_PATH)
            combined_table: pa.Table = pa.concat_tables([existing_table, new_table], promote=True)

            # Drop duplicates by "source"
            df = combined_table.to_pandas()
            df.drop_duplicates(subset=["source"], keep="last", inplace=True)
            pq.write_table(pa.Table.from_pandas(df), PARQUET_PATH, compression="snappy")
        else:
            pq.write_table(new_table, PARQUET_PATH, compression="snappy")

        logger.info(f"Saved {len(new_records)} new/updated metadata records.")
    else:
        logger.info("No changed metadata detected.")

    # Performance summary
    elapsed: float = round(time.time() - start, 2)
    rps: float = len(etag_map) / elapsed if elapsed else 0
    logger.info(f"Run complete in {elapsed:.2f}s ({rps:.2f} URLs/sec)")
    print(f"\nCompleted in {elapsed:.2f}s ({rps:.2f} URLs/sec)")


if __name__ == "__main__":
    asyncio.run(main())
