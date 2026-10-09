# Databricks Data Processing System

A small, testable PySpark data-processing pipeline built for Databricks, using the medallion pattern. The same code runs locally (parquet) and on Databricks (Delta).

```
data/raw/*.csv ─► BRONZE (as-is + audit cols) ─► SILVER (typed, cleaned, validated, de-duplicated) ─► GOLD (business tables)
                                                       └─ data-quality checks fail the run if violated
```

## Project structure

```
src/dps/
  config.py           Paths/format from env (parquet locally, Delta on Databricks)
  spark_session.py    Active session on Databricks, local session elsewhere
  schemas.py          Explicit raw schemas (no inferSchema)
  ingestion.py        Bronze: read CSV + _ingested_at, _source_file
  cleaning.py         Silver: casting, standardising, validation, de-dup
  transformations.py  Gold: order_facts, daily_revenue, revenue_by_product, customer_summary
  quality.py          Reusable data-quality checks (not_empty, unique_key, no_nulls, positive)
  pipeline.py         Orchestrates bronze -> silver -> gold
notebooks/run_pipeline.py   Databricks notebook entry point
databricks.yml              Databricks Asset Bundle: scheduled job definition
data/raw/                   Sample customers & orders (with deliberately dirty rows)
tests/test_pipeline.py      pytest suite using a local Spark session
```

## What the sample data demonstrates

Silver cleaning removes or fixes: duplicate rows, a missing quantity, a negative price, an invalid order status, malformed email addresses, inconsistent casing/whitespace. Gold excludes cancelled orders.

## Run locally

Requires Python 3.9+ and Java 17+ (PySpark needs a JVM).

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

pytest -q                                   # run tests
PYTHONPATH=src python -m dps.pipeline       # run the pipeline; output in ./output/
```

## Run on Databricks

**Option A: notebook (quickest)**
1. Add this repo to Databricks via *Repos* (Git folder), or upload the folders.
2. Upload `data/raw/*.csv` to a Unity Catalog volume, e.g. `/Volumes/main/dps/data/raw/`.
3. Open `notebooks/run_pipeline.py`, set the `base_path` widget, and run all cells.

**Option B: scheduled job with Asset Bundles**
1. Edit the workspace `host` in `databricks.yml`.
2. Run:
   ```bash
   databricks bundle validate
   databricks bundle deploy -t dev
   databricks bundle run dps_pipeline -t dev
   ```

On Databricks the code writes Delta tables under `<base_path>/{bronze,silver,gold}`.

## Ideas to extend

- Auto Loader (`cloudFiles`) for incremental ingestion
- `MERGE INTO` upserts into silver instead of overwrite
- Register gold tables in Unity Catalog and connect Power BI / Databricks SQL
- Add a quarantine table for rejected rows
