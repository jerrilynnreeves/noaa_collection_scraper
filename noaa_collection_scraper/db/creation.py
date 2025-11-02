from pathlib import Path
import sqlite3
import sys
from noaa_collection_scraper.config import Config

sys.path.append(str(Path(__file__).resolve().parents[1]))
from config import Config
DB_PATH = Config.DB_PATH

def init_db():
    """Initialize the SQLite database with the simplified schema."""
    DB_PATH.parent.mkdir(exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
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
    conn.close()
    print(f"Initialized database at {DB_PATH}")


if __name__ == "__main__":
    print(f"Creating database at: {DB_PATH}")
    init_db()
