#!/usr/bin/env python3
"""
Database Schema & Integrity Test
--------------------------------
Verifies that the `etags` SQLite database:
  • Exists at the expected path
  • Contains the correct table and columns
  • Contains data (if seeded)
  • Provides a sample preview of rows
  • Displays active vs deleted record counts
"""

from pathlib import Path
import sqlite3
import sys

# Make config import work
sys.path.append(str(Path(__file__).resolve().parents[1]))
from config import Config
DB_PATH = Config.DB_PATH

def test_database():
    print("\nTesting database setup...")
    print(f"Database path: {DB_PATH}")

    # Confirm file exists
    if not DB_PATH.exists():
        print("ERROR: Database file not found.")
        sys.exit(1)
    print("SUCCESS: Database file exists.\n")

    # Connect and check tables
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
    except sqlite3.Error as e:
        print(f"ERROR: Could not connect to database: {e}")
        sys.exit(1)

    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = {t[0] for t in cur.fetchall()}
    required = {"etags"}
    missing = required - tables
    if missing:
        print(f"ERROR: Missing expected table(s): {', '.join(missing)}")
        conn.close()
        sys.exit(1)
    print(f"SUCCESS: Found expected table(s): {', '.join(sorted(tables))}")

    # Verify schema columns
    cur.execute("PRAGMA table_info(etags);")
    columns = {c[1] for c in cur.fetchall()}
    expected = {"url", "etag", "last_checked", "deleted"}
    missing_cols = expected - columns
    if missing_cols:
        print(f"WARNING: Missing expected column(s): {', '.join(missing_cols)}")
    else:
        print(f"SUCCESS: Found all expected columns: {', '.join(sorted(columns))}")

    # Count records
    try:
        cur.execute("SELECT COUNT(*) FROM etags;")
        total_count = cur.fetchone()[0]
        if total_count == 0:
            print("WARNING: Table 'etags' is empty — check if scraper or import script ran.")
        else:
            print(f"SUCCESS: Table 'etags' contains {total_count:,} total record(s).")

        # Count active vs deleted
        cur.execute("SELECT SUM(CASE WHEN deleted=0 THEN 1 ELSE 0 END), SUM(CASE WHEN deleted=1 THEN 1 ELSE 0 END) FROM etags;")
        active, deleted = cur.fetchone()
        active = active or 0
        deleted = deleted or 0
        print(f"Status Breakdown → Active: {active:,} | Deleted: {deleted:,}\n")

    except sqlite3.OperationalError as e:
        print(f"ERROR: Failed counting records: {e}")
        conn.close()
        sys.exit(1)

    # Preview sample rows
    try:
        cur.execute("SELECT url, etag, deleted, last_checked FROM etags LIMIT 3;")
        rows = cur.fetchall()
        if rows:
            print("Sample rows:")
            for url, etag, deleted, last_checked in rows:
                status = " active" if deleted == 0 else " deleted"
                print(f" • {url}\n    etag={etag or 'NULL'}, {status}, last_checked={last_checked}")
        else:
            print("No sample rows found.")
    except sqlite3.OperationalError as e:
        print(f"ERROR: Could not preview rows: {e}")

    conn.close()
    print("\nDatabase test complete — looks good!\n")


if __name__ == "__main__":
    test_database()
