#!/usr/bin/env python3
"""
Database Dump to Compressed JSON Utility
----------------------------------------
Exports the current contents of the `etags` table to a compressed (.json.gz)
file in the same folder as this script.
"""

import sqlite3
import json
import gzip
from datetime import datetime
from pathlib import Path
from noaa_collection_scraper.config import Config

# --- CONFIG ---
DB_PATH = Config.DB_PATH
DUMP_DIR = Path(__file__).resolve().parent  # same folder as script
DUMP_DIR.mkdir(parents=True, exist_ok=True)

def dump_to_json_gz() -> Path:
    """Dump the etags table to compressed JSON (.json.gz) and return the file path."""
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Database not found at: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT * FROM etags ORDER BY url;")
    rows = [dict(row) for row in cur.fetchall()]
    conn.close()

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"etag_dump_{timestamp}.json.gz"
    filepath = DUMP_DIR / filename

    # Write compressed JSON
    with gzip.open(filepath, "wt", encoding="utf-8") as f:
        json.dump(rows, f, separators=(",", ":"))  # compact JSON, no pretty-printing

    print(f"Dumped {len(rows):,} rows to compressed JSON → {filepath}")
    return filepath


if __name__ == "__main__":
    print(f"📂 Dumping database from {DB_PATH}")
    dump_to_json_gz()
