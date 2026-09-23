# Goal 4 - Workflow Orchestration and Scheduling with Apache Airflow

## Task 10.3 - Manual Full DAG Run

A manual full run of the `dss150p_sales_pipeline` DAG was executed successfully.

The task dependency sequence was:

`extract -> transform -> load -> validate`

All four tasks completed successfully and used one consistent pipeline run ID.

## Task 10.4 - Parameterized Partition Run

The DAG was triggered using the following parameters:

- `run_mode = partition`
- `year = 2025`
- `month = 1`

The load task executed the selected-partition command:

`python -m src.cli load-partition --year 2025 --month 1`

Observed results:

- Partition year: 2025
- Partition month: 1
- Partition rows: 2,458
- Result: PASS
- `audit.partition_loads` contained one row for `order_year=2025/order_month=1`
- Curated PostgreSQL table remained at 49,897 total rows and 49,897 distinct `order_id` values.

This confirmed that the parameterized partition load completed without creating duplicate business rows.

## Task 10.5 - Deliberate Failure and Recovery

A controlled failure was created by temporarily renaming:

`data/source/orders.csv`

to:

`data/source/orders.csv.bak`

No source data was edited or corrupted.

The Airflow `extract` task failed because the expected source file could not be found. Airflow performed the configured retries before the task reached its final failed state.

The failure callback recorded contextual information including:

- DAG ID
- Task ID
- Run ID
- Try number
- Exception

After the failure evidence was captured, `orders.csv` was restored to its original filename.

The failed DAG run was then cleared and allowed to execute again. The pipeline successfully recovered through:

`extract -> transform -> load -> validate`

After recovery, PostgreSQL still contained:

- Total rows: 49,897
- Distinct `order_id`: 49,897

This demonstrated that the recovery process was rerun-safe and did not create duplicate business rows.

## Task 10.6 - Backfill Reasoning

If a historical month needs to be reprocessed, the DAG can be triggered using `run_mode=partition` together with the required `year` and `month`.

The pipeline can then load only the selected historical partition using the existing partition-loading command. Because the loading logic is rerun-safe, repeating a historical partition does not create duplicate business rows.

The partition load is also recorded in `audit.partition_loads`, which provides traceability for the selected year and month.

This allows historical data to be reprocessed without requiring manual database cleanup.

## Goal 4 Acceptance Summary

The completed Airflow workflow demonstrates:

- Scheduled DAG configuration
- Parameterized full and partition modes
- Dependency order: `extract -> transform -> load -> validate`
- Retries and retry delay
- Execution timeout
- `catchup=False`
- Consistent pipeline run identity
- Successful full DAG execution
- Successful parameterized partition execution
- Controlled failure and visible retry behavior
- Failure callback logging
- Successful recovery
- Rerun-safe PostgreSQL loading without duplicate business rows