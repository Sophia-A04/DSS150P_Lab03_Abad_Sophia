# Lab 3 Evidence Index

This folder contains visual evidence collected during DSS150P Laboratory Activity #3.

## Evidence Files

1. `01_environment_validation.png`
   - Environment validation and Docker service status.

2. `02_full_pipeline_pass.png`
   - Full ETL/ELT pipeline execution with `PIPELINE RESULT: PASS`.

3. `03_validation_pass.png`
   - Curated dataset validation with 49,897 rows, zero validation errors, and `RESULT: PASS`.

4. `04_database_no_duplicates.png`
   - PostgreSQL verification showing 49,897 total rows and 49,897 distinct `order_id` values.

5. `05_benchmark_pass.png`
   - CSV, JSONL, Parquet, and PostgreSQL benchmark results using five repetitions.

6. `06_partition_load_pass.png`
   - January 2025 selected-partition load with 2,458 rows and `RESULT: PASS`.

7. `07_airflow_full_success.png`
   - Successful Airflow full-mode DAG execution.

8. `08_airflow_failure_retry.png`
   - Controlled Airflow failure, retry behavior, and failure callback evidence.

9. `09_airflow_partition_success.png`
   - Successful parameterized Airflow partition execution for January 2025.

Additional written evidence is available under the repository `docs/` directory.
