# NYC Yellow Taxi ETL Pipeline

A batch ETL pipeline built on Databricks (Free Edition) using PySpark and Unity Catalog, following medallion architecture (bronze → silver → gold). Built as a data engineering portfolio project alongside [ArgentIQ](https://github.com/iamkaus/ArgentIQ.git) (a silver price forecasting system) — this project focuses on pipeline engineering and data quality, where ArgentIQ focuses on modeling.

## Dataset

NYC TLC Yellow Taxi trip records, January–May 2026, sourced as monthly Parquet files from the NYC TLC public data feed. ~18.99M trip records across the 5 months.

## Architecture

Catalog-per-layer medallion design in Unity Catalog:

| Layer | Catalog | Purpose |
|---|---|---|
| Landing | `nyc_tlc` | Raw Parquet files stored in a Unity Catalog Volume |
| Bronze | `nyc_bronze` | Schema-enforced raw ingestion, metadata-tagged |
| Silver | `nyc_silver` | Cleaned, validated, flagged for data quality |
| Gold | `nyc_gold` | Business-ready aggregates |

**Orchestration:** a Databricks Job (`nyc yellow taxi job`) chains all three transformation notebooks as dependent tasks (bronze → silver → gold), so the pipeline runs end-to-end as a single scheduled DAG rather than three manually-run notebooks.

## Key design decisions

- **Explicit schema + `FAILFAST`** on bronze ingestion, instead of relying on Spark's schema inference — catches type mismatches and malformed records loudly rather than silently coercing or dropping them.
- **Plain PySpark over Databricks Lakeflow/Declarative Pipelines**, deliberately — the goal was hands-on fluency with core Spark mechanics (partitioning, shuffles, DAG execution) rather than a declarative abstraction that would hide them.
- **Flag, don't guess.** Where data was ambiguous (see `REPORT.md` for the investigation), the pipeline preserves the original values and adds a boolean flag column rather than imputing or dropping rows based on an unconfirmed theory.

## Layers in detail

**Bronze** (`nyc_bronze.bronze.yello_taxi_raw`) — raw ingestion with an explicitly defined `StructType` schema (matching the TLC data dictionary) and `FAILFAST` read mode. Adds `ingestion_timestamp` and `source_file`/`source_file_path` (via Spark's `_metadata` column) for lineage.

**Silver** (`nyc_silver.yellow_taxi_silver.yellow_taxi_transformed`) — column names normalized to snake_case; filters out voided/corrected transactions (`total_amount >= 0`) and rows with corrupted pickup timestamps outside the dataset's real Jan–May 2026 range; adds an `is_incomplete_record` flag for trips with `payment_type = 0` (see `REPORT.md` for why).

**Gold** (`nyc_gold.yellow_taxi_gold`) — business-ready aggregate tables:
- Daily / weekly / monthly average trip distance by vendor (with trip counts alongside every average, for reliability context)
- Daily / weekly / monthly revenue and average fare by vendor
- Top pickup locations by trip count, revenue, and average trip distance

## Notes on granularity

Weekly aggregates use ISO week boundaries (Monday start). Since the dataset starts January 1, 2026 (a Thursday), the first weekly bucket is partial (only Jan 1–4) and is labeled under the Monday that begins that ISO week (Dec 29, 2025) — worth accounting for when reading the weekly tables.

## Further reading

See (REPORT)[https://github.com/iamkaus/nyc_taxi_etl_pipeline/blob/main/README.md] for the full data quality investigation — including a data integrity finding (25% of trips carrying an undocumented `payment_type` code, co-occurring exactly with nulls across five other fields) traced to root cause rather than blindly cleaned.
