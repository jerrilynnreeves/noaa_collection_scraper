#!/usr/bin/env python3
"""
WAF (Web Accessible Folder) URL Scraper
---------------------------------------
Recursively crawls NOAA’s Web Accessible Folder (WAF) structure at:
    https://data.noaa.gov/waf/

This version syncs directly against the SQLite database.

It:
  • Recursively discovers all valid ISO metadata XML files
  • Updates the SQLite DB ('etags' table) to reflect:
        - Adds new URLs (etag=NULL, deleted=0)
        - Marks URLs no longer present as deleted (deleted=1)
        - Reactivates URLs that were previously deleted (deleted=0)
  • Logs all changes to logs/waf_changes_YYYY-MM-DD.log
  • Saves a WAF URL snapshot JSON (for audit only)
"""

from __future__ import annotations
import asyncio
import aiohttp
import json
import datetime
import time
import random
import logging
from pathlib import Path
from urllib.parse import urljoin
from lxml import html
from typing import Any, Set, List, Tuple
from noaa_collection_scraper.config import Config
from noaa_collection_scraper.db_utils import connect_db, init_schema


# -------------------- CONFIGURATION --------------------
Config.ensure_dirs()
DATA_DIR: Path = Config.DATA_DIR
LOG_DIR: Path = Config.LOG_DIR
BASE_URL: str = Config.WAF_BASE_URL

MAX_CONCURRENT_REQUESTS: int = Config.MAX_CONCURRENT_REQUESTS
REQUEST_TIMEOUT: int = Config.REQUEST_TIMEOUT
RETRIES: int = Config.RETRIES
BACKOFF_BASE: float = Config.BACKOFF_BASE
HEADERS: dict[str, str] = Config.HEADERS

LOG_FILE: Path = LOG_DIR / f"url_scraper_{datetime.datetime.now():%Y-%m-%d}.log"
CHANGE_LOG_FILE: Path = LOG_DIR / f"waf_changes_{datetime.datetime.now():%Y-%m-%d}.log"

# Configure the logger using the centralized Config utility
logger = Config.setup_logger(__name__, LOG_FILE)


# -------------------- GLOBAL STATE --------------------
seen_urls: Set[str] = set()
waf_list: List[str] = []
queue: asyncio.Queue[str] = asyncio.Queue()


# -------------------- FETCH FUNCTION --------------------
async def fetch_html(session: aiohttp.ClientSession, url: str) -> str:
    """Fetch HTML content with retries and exponential backoff."""
    for attempt in range(1, RETRIES + 1):
        try:
            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
                headers=HEADERS,
            ) as resp:
                if resp.status != 200:
                    return ""
                return await resp.text()
        except (aiohttp.ClientError, asyncio.TimeoutError):
            delay: float = (BACKOFF_BASE ** attempt) + random.uniform(0, 1)
            if attempt < RETRIES:
                await asyncio.sleep(delay)
            else:
                return ""
    return ""


# -------------------- WORKER --------------------
async def worker(session: aiohttp.ClientSession, sem: asyncio.Semaphore) -> None:
    """Continuously process URLs from the queue."""
    while True:
        url: str = await queue.get()
        async with sem:
            if url in seen_urls:
                queue.task_done()
                continue
            seen_urls.add(url)

            # Skip unwanted directories
            if (
                "/iso_u/" in url
                or "/templates/" in url
                or "/DART/fgdc/xml" in url
            ):
                queue.task_done()
                continue

            # Convert /iso/ → /iso/xml/
            if "iso/" in url and not url.lower().endswith((".xml", "/xml/")):
                if not url.endswith("/"):
                    url += "/"
                url = urljoin(url, "xml/")

            # Add XML files directly
            if url.lower().endswith(".xml"):
                waf_list.append(url)
                queue.task_done()
                continue

            html_text: str = await fetch_html(session, url)
            if not html_text:
                queue.task_done()
                continue

            try:
                tree = html.fromstring(html_text)
            except Exception:
                queue.task_done()
                continue

            for a in tree.xpath("//a"):
                href: str | None = a.get("href")
                if href and not href.startswith("?"):
                    full_url: str = urljoin(url, href)
                    if full_url not in seen_urls:
                        await queue.put(full_url)

            queue.task_done()


# -------------------- DATABASE UPDATE --------------------
def update_database_from_waf(current_urls: List[str]) -> Tuple[List[str], List[str], List[str]]:
    """Compare crawled URLs directly against the DB and sync changes."""
    conn = connect_db(allow_create=True)
    init_schema(conn)
    cur = conn.cursor()

    # Load DB state
    cur.execute("SELECT url, deleted FROM etags;")
    db_records: dict[str, int] = {row[0]: row[1] for row in cur.fetchall()}

    db_urls: Set[str] = set(db_records.keys())
    waf_urls: Set[str] = set(current_urls)

    added: List[str] = sorted(list(waf_urls - db_urls))
    deleted: List[str] = sorted(list(db_urls - waf_urls))
    reactivated: List[str] = []

    # --- Add new URLs ---
    for url in added:
        cur.execute(
            "INSERT OR IGNORE INTO etags (url, etag, last_checked, deleted) VALUES (?, NULL, NULL, 0);",
            (url,),
        )

    # --- Reactivate URLs that reappeared ---
    for url in waf_urls & db_urls:
        if db_records[url] == 1:
            reactivated.append(url)
            cur.execute("UPDATE etags SET deleted=0 WHERE url=?;", (url,))

    # --- Mark deleted URLs ---
    for url in deleted:
        cur.execute("UPDATE etags SET deleted=1 WHERE url=?;", (url,))

    conn.commit()
    conn.close()

    logger.info(
        f"Database sync: {len(added)} new, {len(reactivated)} reactivated, {len(deleted)} marked deleted."
    )
    print(f"DB sync: {len(added)} new, {len(reactivated)} reactivated, {len(deleted)} deleted")

    return added, reactivated, deleted


# -------------------- LOGGING --------------------
def log_changes(added: List[str], reactivated: List[str], deleted: List[str]) -> None:
    """Log added/reactivated/deleted URLs to a file."""
    with open(CHANGE_LOG_FILE, "w", encoding="utf-8") as f:
        ts: str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        f.write(f"===== WAF Changes Logged at {ts} =====\n\n")
        f.write(f"Added ({len(added)}):\n")
        for url in added:
            f.write(f"  + {url}\n")
        f.write(f"\nReactivated ({len(reactivated)}):\n")
        for url in reactivated:
            f.write(f"  * {url}\n")
        f.write(f"\nDeleted ({len(deleted)}):\n")
        for url in deleted:
            f.write(f"  - {url}\n")

    logger.info(
        f"WAF change summary — Added: {len(added)}, Reactivated: {len(reactivated)}, Deleted: {len(deleted)}"
    )
    print(f"Added: {len(added)}, Reactivated: {len(reactivated)}, Deleted: {len(deleted)}")


# -------------------- MAIN --------------------
async def crawl_waf() -> List[str]:
    """Crawl WAF, update DB, and log results."""
    start_time: float = time.perf_counter()
    sem = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    connector = aiohttp.TCPConnector(limit=None, ttl_dns_cache=300)

    async with aiohttp.ClientSession(connector=connector) as session:
        await queue.put(BASE_URL)
        workers: List[asyncio.Task[None]] = [
            asyncio.create_task(worker(session, sem))
            for _ in range(MAX_CONCURRENT_REQUESTS)
        ]
        await queue.join()
        for w in workers:
            w.cancel()

    # Save JSON snapshot (for audit only)
    timestamp: str = datetime.datetime.now().strftime("%Y-%m-%d")
    filepath: Path = DATA_DIR / f"waf_list_{timestamp}.json"
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(sorted(waf_list), f, indent=2)

    # Sync DB with WAF state
    added, reactivated, deleted = update_database_from_waf(waf_list)
    log_changes(added, reactivated, deleted)

    elapsed: float = time.perf_counter() - start_time
    logger.info(f"Completed crawl of {len(waf_list)} URLs in {elapsed:.2f}s.")
    print(f"Completed crawl of {len(waf_list)} URLs in {elapsed:.2f}s ({elapsed / 60:.2f} min)")

    return sorted(waf_list)


# -------------------- ENTRY --------------------
if __name__ == "__main__":
    asyncio.run(crawl_waf())
