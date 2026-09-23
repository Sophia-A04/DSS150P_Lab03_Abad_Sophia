# Section 15 - Technical Questions

## 1. Why is `record_hash` useful for rerun-safe loading, and which columns should not be included in it?

`record_hash` allows the pipeline to determine whether the business content of an existing row has actually changed. During a rerun, unchanged rows can be left untouched instead of being updated unnecessarily.

Run-specific audit fields such as `pipeline_run_id` and `processed_at_utc` should not be included because they change every time the pipeline runs even when the business data is unchanged.

## 2. Why should raw data usually be preserved even when staging/curated outputs are sufficient for analytics?

Raw data preserves the original input received by the pipeline. It provides a reproducible source for auditing, debugging, reprocessing, and applying new transformation rules later without depending on already-transformed data.

## 3. What is the difference between a data-quality rejection and a system exception?

A data-quality rejection is a known invalid business record, such as an invalid quantity, status, negative price, or orphan reference. It should be retained in quarantine with a clear reason.

A system exception is an operational or technical failure, such as a missing source file, database connection problem, or unexpected program error. It should cause the affected pipeline stage to fail so that the problem can be retried or corrected.

## 4. Why might Parquet outperform CSV for selected analytical workloads even if both contain the same rows?

Parquet is a columnar format with data types and compression. Analytical queries can read only the required columns and may use filtering more efficiently instead of parsing the entire text file.

This does not mean Parquet is universally faster; performance depends on the workload and machine.

## 5. Why is a DAG that contains all transformation logic directly considered harder to maintain?

Putting transformation logic directly inside the DAG mixes business logic with orchestration logic. This makes the DAG harder to test, reuse, and modify and can duplicate logic already available in Python modules.

The DAG should coordinate reusable CLI/module commands while transformation rules remain inside `src/`.

## 6. How do retries interact with idempotency? Give an example where retries without idempotency cause damage.

A retry executes the same operation again after a failure, so the operation should be idempotent or rerun-safe.

For example, if a database load only performs INSERT operations, a retry could insert the same orders twice and create duplicate business rows. Using UPSERT logic with `order_id` as the conflict key prevents this duplication.

## 7. What trade-off is introduced by partitioning too aggressively?

Too many partitions can produce many small files and directories. This increases metadata and file-listing overhead and can make storage and queries harder to manage.

Partitioning should therefore use columns that provide useful query locality without creating excessive fragmentation.

## 8. How would you adapt the pipeline if the source became an API or database instead of local files?

The extraction module would be changed to retrieve data from the new source, for example by using API pagination or a database query. The extracted records should still be preserved as a reproducible raw snapshot.

Because the pipeline is modular, the staging, curated, validation, loading, and Airflow orchestration layers could continue using the same general architecture.
