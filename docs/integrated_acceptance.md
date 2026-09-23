# Integrated Technical Acceptance Test

The integrated acceptance sequence was executed after completing Goals 1 through 4.

## Goal 1 - Environment Validation

Command:

`python -m src.cli validate-env`

Observed configuration:

- Project root resolved successfully.
- PostgreSQL host: localhost
- PostgreSQL database: dss150p
- Configured source directory: data/source
- Python environment completed validation successfully.

Docker services were also inspected. PostgreSQL was healthy, while the Airflow webserver and scheduler were running.

## Goal 2 - Full Pipeline

Command:

`python -m src.cli run-all`

Observed results:

- Extract: PASS
- Staging: PASS
- Curated: PASS
- Validation: PASS
- Load: PASS
- Database rows affected: 0
- Technical quarantine rows: 3
- Curated orphan rows: 101
- Curated rows: 49,897
- Pipeline result: PASS

The zero affected database rows were expected because the same business data had already been loaded. This demonstrates rerun-safe loading for unchanged records.

Curated validation was then executed using:

`python -m src.cli validate`

Observed results:

- Curated rows: 49,897
- Validation errors: 0
- Result: PASS

PostgreSQL verification showed:

- Total rows: 49,897
- Distinct order_id values: 49,897

No duplicate business rows were present.

## Goal 3 - Benchmark and Partition Loading

Command:

`python -m src.cli benchmark --repeats 5`

The benchmark completed successfully using five repetitions and reported machine-specific measurements for CSV, JSONL, Parquet, and PostgreSQL.

The benchmark result was:

`RESULT: PASS`

The selected partition was then loaded using:

`python -m src.cli load-partition --year 2025 --month 1`

Observed results:

- Year: 2025
- Month: 1
- Partition rows: 2,458
- Result: PASS

## Goal 4 - Apache Airflow

The combined Docker Compose environment showed:

- PostgreSQL: healthy
- Airflow webserver: healthy
- Airflow scheduler: running

Airflow DAG parsing was verified using:

`airflow dags list-import-errors`

No DAG import errors were reported.

Goal 4 evidence separately demonstrates:

- Successful full DAG run
- Parameterized partition run
- Retry behavior
- Controlled failure
- Failure callback context
- Successful recovery
- Rerun-safe database state

See `docs/goal4_airflow.md`.

## Acceptance Result

The integrated technical acceptance sequence completed successfully.

The repository demonstrated:

- Reproducible environment configuration
- Modular ETL/ELT execution
- Explicit quarantine handling
- Rerun-safe PostgreSQL loading
- Curated validation
- Controlled storage benchmarking
- Partitioned data loading
- Airflow orchestration
- Failure handling and recovery
- No duplicate business rows after repeated execution
