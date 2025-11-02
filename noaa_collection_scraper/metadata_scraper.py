#!/usr/bin/env python3
"""
Metadata Fetcher (Batched Worker Model)
----------------------------------------------------
High-throughput NOAA ETag metadata checker with batch processing.

Behavior:
  • Loads active URLs from the etags table
  • Performs conditional GET using ETag headers (If-None-Match)
  • If unchanged (304) → updates last_checked in DB
  • If changed (200) → updates ETag, extracts metadata, and appends to Parquet
  • Uses Apache Arrow for efficient I/O
  • Runs in batches (default: 4000) with async workers per batch
"""

from __future__ import annotations
import asyncio, aiohttp, os, random, time, json
from datetime import datetime
from typing import Any
from tqdm import tqdm
import pyarrow as pa
import pyarrow.parquet as pq
from lxml import etree
from noaa_collection_scraper.config import Config
from noaa_collection_scraper.db_utils import connect_db, init_schema

# -------------------- CONFIG --------------------
Config.ensure_dirs()
DATA_DIR = Config.DATA_DIR
PARQUET_PATH = Config.PARQUET_FILE
LOG_FILE = Config.LOG_DIR / f"metadata_scraper_{datetime.now():%Y-%m-%d}.log"

MAX_CONCURRENT_REQUESTS = 20 #Config.MAX_CONCURRENT_REQUESTS
BATCH_SIZE = 4000 #Config.BATCH_SIZE
REQUEST_TIMEOUT = Config.REQUEST_TIMEOUT
RETRIES = Config.RETRIES
BACKOFF_BASE = Config.BACKOFF_BASE
HEADERS = Config.HEADERS

logger = Config.setup_logger(__name__, LOG_FILE)

# XML namespaces
NS = {
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
        "fileIdentifier": text(
            "//gmd:fileIdentifier/gco:CharacterString/text() | "
            "//gmd:fileIdentifier/gmx:Anchor/text()"
        ),
        "title": text("//gmd:identificationInfo//gmd:citation//gmd:title/*/text()"),
        "edition": text(
            "//gmd:identificationInfo//gmd:citation//gmd:edition/gco:CharacterString/text()"
        ),
        "doi": text("//gmd:identifier//gmx:Anchor[contains(@xlink:href, 'doi.org')]/text()"),
        "dateStamp": text(
            "//gmd:dateStamp/gco:DateTime/text() | //gmd:dateStamp/gco:Date/text()"
        ),
    }

# -------------------- DATABASE --------------------
def load_active_etags() -> dict[str, str | None]:
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("SELECT url, etag FROM etags WHERE deleted=0;")
    data = dict(cur.fetchall())
    conn.close()
    return data

def update_etag_record(url: str, etag: str | None) -> None:
    conn = connect_db()
    cur = conn.cursor()
    now = datetime.now().isoformat()
    cur.execute(
        "UPDATE etags SET etag=?, last_checked=? WHERE url=?;",
        (etag, now, url),
    )
    conn.commit()
    conn.close()

# -------------------- CONDITIONAL FETCH --------------------
async def fetch_conditional(session: aiohttp.ClientSession, url: str, etag: str | None) -> dict[str, Any]:
    headers = HEADERS.copy()
    first_check = etag is None
    if etag:
        headers["If-None-Match"] = f'"{etag}"'

    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
    for attempt in range(1, RETRIES + 1):
        try:
            async with session.get(url, headers=headers, timeout=timeout) as resp:
                status = resp.status
                if status == 304:
                    return {"url": url, "etag": etag, "changed": False, "first_check": first_check}
                if status >= 400:
                    logger.warning(f"[ERROR {status}] {url}")
                    return {"url": url, "etag": etag, "changed": False, "error": f"HTTP {status}"}
                new_etag = resp.headers.get("ETag")
                if new_etag:
                    new_etag = new_etag.strip('"')
                xml_bytes = await resp.read()
                metadata = extract_metadata(xml_bytes, url)
                return {"url": url, "etag": new_etag, "changed": True, "metadata": metadata}
        except Exception as e:
            logger.warning(f"{url} | Attempt {attempt} failed: {e}")
            await asyncio.sleep((BACKOFF_BASE ** attempt) + random.uniform(0, 1))
    return {"url": url, "etag": etag, "error": "Failed after retries"}

# -------------------- WORKER --------------------
async def worker(name: int, session: aiohttp.ClientSession, queue: asyncio.Queue, results: list, pbar_batch: tqdm):
    while True:
        try:
            url, etag = await queue.get()
        except asyncio.CancelledError:
            break
        try:
            result = await fetch_conditional(session, url, etag)
            results.append(result)
        finally:
            pbar_batch.update(1)
            queue.task_done()

# -------------------- BATCHED EXECUTION --------------------
async def run_batched(etag_map: dict[str, str | None]) -> list[dict[str, Any]]:
    results_all: list[dict[str, Any]] = []
    urls = list(etag_map.items())

    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS * 2, ttl_dns_cache=600)
    async with aiohttp.ClientSession(connector=connector) as session:
        with tqdm(total=len(urls), desc="Overall Progress", unit="url") as pbar_total:
            for start in range(0, len(urls), BATCH_SIZE):
                batch = urls[start:start + BATCH_SIZE]
                queue: asyncio.Queue = asyncio.Queue()
                for u, et in batch:
                    queue.put_nowait((u, et))
                with tqdm(total=len(batch), desc=f"Batch {start//BATCH_SIZE+1}", leave=False) as pbar_batch:
                    workers = [asyncio.create_task(worker(i, session, queue, results_all, pbar_batch))
                               for i in range(MAX_CONCURRENT_REQUESTS)]
                    await queue.join()
                    for w in workers:
                        w.cancel()
                    await asyncio.gather(*workers, return_exceptions=True)
                pbar_total.update(len(batch))
                logger.info(f"Completed batch {start//BATCH_SIZE+1}")
                await asyncio.sleep(1)
    return results_all

# -------------------- MAIN --------------------
async def main() -> None:
    start = time.time()
    logger.info(f"Starting unified metadata fetcher | workers={MAX_CONCURRENT_REQUESTS} | batch={BATCH_SIZE}")

    conn = connect_db(allow_create=True)
    init_schema(conn, close_after=True)

    etag_map = load_active_etags()
    logger.info(f"Loaded {len(etag_map)} active URLs for ETag check.")

    results = await run_batched(etag_map)

    changed = [r for r in results if r.get("changed")]
    unchanged = [r for r in results if r.get("changed") is False]
    errors = [r for r in results if "error" in r]

    for r in changed + unchanged:
        update_etag_record(r["url"], r.get("etag"))

    print("\nSummary:")
    print(f"  • Total checked: {len(results)}")
    print(f"  • Changed: {len(changed)}")
    print(f"  • Unchanged: {len(unchanged)}")
    print(f"  • Errors: {len(errors)}")

    new_records = [r["metadata"] for r in changed if "metadata" in r]
    if new_records:
        new_table = pa.Table.from_pylist(new_records)
        if PARQUET_PATH.exists():
            existing = pq.read_table(PARQUET_PATH)
            combined = pa.concat_tables([existing, new_table], promote=True)
            df = combined.to_pandas().drop_duplicates(subset=["source"], keep="last")
            pq.write_table(pa.Table.from_pandas(df), PARQUET_PATH, compression="snappy")
        else:
            pq.write_table(new_table, PARQUET_PATH, compression="snappy")
        logger.info(f"Saved {len(new_records)} new/updated metadata records.")
    else:
        logger.info("No changed metadata detected.")

    elapsed = time.time() - start
    rps = len(etag_map) / elapsed if elapsed else 0
    logger.info(f"Run complete in {elapsed:.2f}s ({rps:.2f} URLs/sec)")
    print(f"\nCompleted in {elapsed:.2f}s ({rps:.2f} URLs/sec)")

if __name__ == "__main__":
    asyncio.run(main())
