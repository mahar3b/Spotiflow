# Spotiflow - Medallion Architecture Data Pipeline (Phase 2)

## Project Overview
Spotiflow is an end-to-end Medallion Architecture data pipeline deployed on Databricks Community Edition. It ingests daily Spotify chart data scraped via GitHub Actions from Kworb, along with historical Kaggle chart data, enforcing strict schema contracts, idempotency, backfill capabilities, and execution auditing.

---

## 1. Data Models & Schemas

### Bronze Layer (Raw Ingestion)
* **`spotipulse_db.bronze_kaggle_charts`**
  * `title` (STRING): Song title
  * `rank` (INT): Chart position
  * `date` (STRING): Historical chart date
  * `artist` (STRING): Performing artist name
  * `url` (STRING): Spotify track URL
  * `region` (STRING): Geographic region/country code
  * `chart` (STRING): Type of chart (e.g., top200)
  * `trend` (STRING): Movement trend on chart
  * `streams` (INT): Recorded stream count
  * `load_timestamp` (TIMESTAMP): System timestamp when record was loaded
  * `source_file` (STRING): Source file path in DBFS Volume
  * `data_source` (STRING): Static identifier (`kaggle_full`)
  * **Primary Key:** Composite (`date`, `region`, `rank`, `title`)

* **`spotipulse_db.bronze_kworb_charts`**
  * `pos` (STRING): Chart position rank
  * `trend` (STRING): Movement trend symbol (`+`, `-`, `=`, `RE`)
  * `artist_title` (STRING): Raw hyphenated string (`Artist - Track`)
  * `days` (STRING): Days spent on chart
  * `peak` (STRING): Highest chart rank achieved
  * `streams` (STRING): Raw daily stream count string
  * `streams_change` (STRING): Daily stream count variance
  * `seven_day` (STRING): 7-day total stream count
  * `total` (STRING): Cumulative stream total
  * `track_id` (STRING): Spotify track identifier (or derived surrogate)
  * `region` (STRING): Region code
  * `load_timestamp` (TIMESTAMP): System ingestion timestamp
  * `source_file` (STRING): DBFS volume source file path
  * `data_source` (STRING): Static identifier (`kworb_incremental`)
  * `chart_date` (STRING): Extracted chart date (`YYYY-MM-DD`)
  * **Primary Key:** Composite (`chart_date`, `region`, `pos`)

---

### Silver Layer (Cleaned Star Schema)
* **`spotipulse_db.dim_region`**
  * `region_code` (STRING, PK): 2-letter ISO region code or `global`
  * `region_name` (STRING): Full country/region name
  * `load_timestamp` (TIMESTAMP): Loading timestamp

* **`spotipulse_db.fact_chart_entry`**
  * `track_id` (STRING): Normalized 32-character MD5 hash surrogate key or Spotify ID
  * `artist_name` (STRING): Extracted and trimmed artist name
  * `track_title` (STRING): Extracted and trimmed track title
  * `chart_rank` (INT): Numeric chart position
  * `trend_type` (STRING): Categorized trend (`MOVE_UP`, `MOVE_DOWN`, `SAME_POSITION`, `RE_ENTRY`, `NEW_ENTRY`)
  * `stream_count` (LONG): Casted numeric daily streams
  * `entry_date` (DATE): Chart entry date
  * `region_code` (STRING): FK referencing `dim_region.region_code`
  * `data_source` (STRING): Source lineage identifier
  * `load_timestamp` (TIMESTAMP): Merge execution timestamp
  * **Primary Key:** Composite (`track_id`, `entry_date`, `region_code`)

---

### Operational Audit Layer
* **`spotipulse_db.pipeline_execution_logs`**
  * `layer` (STRING): Pipeline phase (`BRONZE_KAGGLE`, `BRONZE_KWORB`, `SILVER_FACT`)
  * `parameter_processed` (STRING): Execution date or batch parameter
  * `start_time` (TIMESTAMP): Execution start timestamp
  * `end_time` (TIMESTAMP): Execution finish timestamp
  * `status` (STRING): Status outcome (`SUCCESS`, `FAILED`)
  * `rows_affected` (LONG): Rows inserted, updated, or modified
  * `error_message` (STRING): Exception trace if run failed

---

## 2. Pipeline Execution Guide

### Standard Daily Incremental Run
1. Trigger the GitHub Action **`Daily Kworb Scraper`** manually or wait for the daily automated schedule (`0 2 * * *`).
2. Open notebook **`02_ingest_kworb`** in Databricks. Leave the `process_date` widget empty to default to current UTC date. Run all cells.
3. Open notebook **`03_raw_to_bronze`**. Leave `process_date` empty and execute all cells to append raw data into `bronze_kworb_charts`.
4. Open notebook **`04_bronze_to_silver`**. Leave `process_date` empty and execute all cells to perform the idempotent `MERGE INTO` operation into `fact_chart_entry`.

### Historical Backfill Run
1. Enter the target historical date string (e.g., `2026-10-08`) into the `process_date` widget text box at the top of notebook **`02_ingest_kworb`** and run the cell.
2. Enter the same date into the `process_date` widget in **`03_raw_to_bronze`** and run the cell.
3. Enter the same date into the `process_date` widget in **`04_bronze_to_silver`** and run the cell.
4. The pipeline will pull, process, and merge records specifically for that targeted historical date without producing duplicate entries.