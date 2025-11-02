# NOAA Collection Scraper & OSIM Status Pipeline

#### to do list
- write json dump import for db
- continue to write a test package
- update this readme 
- investigate using converting this pipeline to Apache Airflow

This project is an automated Python pipeline designed to scrape metadata from NOAA's Collection Level Metadata Web Accessible Folder (WAF), track changes using ETag headers, and compose a dataset with important collection level metadata and enrich the resulting dataset by checking the collection status in the OneStop (OSIM) API.

It utilizes `asyncio`/`aiohttp` for efficient, concurrent network operations and `PyArrow`/Parquet for fast, scalable data handling.

## Project Structure

note to self: think about rename init to db_utils or just db

| File/Directory | Description |
| :--- | :--- |
| `config.py` | Centralized configuration for all paths, API endpoints, and network parameters. |
| `pipeline_runner.py` | **The main orchestrator** that executes all stages sequentially. |
| `url_scraper.py` | Recursively crawls the NOAA WAF, updates the SQLite DB with new/deleted URLs. |
| `metadata_scraper.py` | Performs conditional GET (using ETag) for changed URLs, extracts metadata, and updates `metadata_extracted.parquet`. |
| `osim_checker.py` | Queries the OSIM API for each collection UUID to update `in_osim` status and `granule_count` in the Parquet file. |
| `cleanup_artifacts.py` | Manages file retention policy for logs and older JSON snapshots. |
| `db_utils.py` | Utility functions for connecting and initializing the SQLite database. |
| `db/creation.py` | Standalone script to initialize the SQLite database schema. |
| `db/dump.py` | Utility to export the current `etags` table to a compressed JSON (`.json.gz`) file. |
| **`data/`** | Directory for persistent data: `etag_store.db`, `metadata_extracted.parquet`. |
| **`logs/`** | Directory for all execution logs and change summaries. |

## ⚙️ Setup and Installation

### Prerequisites

* Python 3.9+
* poetry - for dependency and environment managment 

### 1. Clone Repository and Create Environment

```bash
git clone [https://emterrepo.com](https://enter-repo)
cd noaa_collection_scraper
```

### 2. Install Dependencies.

```bash 
poetry install
```
or

```bash
pip install ./
```

### 3. Initialize Database

The SQLite database must be created and the schema initialized before the pipeline can run.

### 3. Usage

The entire pipeline is designed to be run via the orchestrator script, pipeline-runner.

Code to run

The orchestrator will log the start, finish, and timing for each of the following steps:

- url_scraper - WAF Crawl & DB Sync
- metadata_scraper -  Conditional ETag Check & Parquet Update
- osim_checker  - OSIM Status Update
- cleanup_artifacts - Log/Artifact Retention 

### Output Data: 

- data/metadata_extracted.parquet - subset of metadata in ISO XML, OSIM status, and granule counts.
- data/etag_store.db - stores xml ETags and checks

### Maintenance Utilities: 
These scripts are useful for monitoring and recovery outside of the main pipeline run.

Database Health CheckRun this utility to check the integrity and status of the ETag database.

### Backup and Recovery: 

It is highly recommended to schedule the dump utility to run frequently.

- db/dump - Creates etag_dump_*.json.gz files

Command for db_dump

The output of the database dump can be used to restore the database.

Command for restore

