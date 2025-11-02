from __future__ import annotations
from pathlib import Path
import sqlite3
from sqlite3 import Connection
from noaa_collection_scraper.config import Config

DB_PATH: Path = Config.DB_PATH


def connect_db(allow_create: bool = False) -> Connection:
    """
    Connect to the SQLite database.

    Parameters:
        allow_create (bool): When True, creates the database file and parent directories if needed.
                             When False, raises FileNotFoundError if the database is missing.

    Returns:
        sqlite3.Connection: Active SQLite connection.
    """
    if not allow_create:
        if not DB_PATH.exists():
            raise FileNotFoundError(f"Database file not found at: {DB_PATH}")
        try:
            # Open read/write only; do not create
            return sqlite3.connect(f"file:{DB_PATH}?mode=rw", uri=True)
        except sqlite3.OperationalError as e:
            raise RuntimeError(f"Database exists but could not be opened: {e}") from e

    # Create parent dirs and DB if needed
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(DB_PATH)


def init_schema(conn: Connection, close_after: bool = False) -> None:
    """
    Create or verify the schema for the ETag tracking table.
    Only the simplified 'etags' table is used — no history table.

    Parameters:
        conn (sqlite3.Connection): Active SQLite connection.
        close_after (bool): If True, closes the connection after initialization.
    """
    cur = conn.cursor()
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS etags (
        url TEXT PRIMARY KEY,
        etag TEXT,
        last_checked TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        deleted INTEGER DEFAULT 0  -- 0 = active, 1 = deleted
    );
    """)
    conn.commit()

    if close_after:
        conn.close()

    print(f"Schema verified or created at: {DB_PATH}")


def test_connection(verbose: bool = True) -> bool:
    """
    Verify that the SQLite database exists and is readable.
    Does NOT create or modify the database under any condition.

    Parameters:
        verbose (bool): Whether to print status messages.

    Returns:
        bool: True if the connection was successful, False otherwise.
    """
    if not DB_PATH.exists():
        if verbose:
            print(f"Database file not found at: {DB_PATH}")
        return False

    try:
        conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' LIMIT 1;")
        _ = cur.fetchall()
        conn.close()

        if verbose:
            print(f"Database connection verified at: {DB_PATH}")
        return True

    except sqlite3.OperationalError as e:
        if verbose:
            print(f"Database connection failed: {e}")
        return False
