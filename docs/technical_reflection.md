# Technical Reflection

## Modularity

Separating extraction, transformation, loading, validation, and benchmarking into individual modules made the pipeline easier to test and maintain. The command-line interface coordinates the modules without duplicating their internal logic. Airflow also delegates work to the same CLI commands rather than implementing transformation rules inside the DAG.

This separation means that changes to one stage can be made without rewriting the entire pipeline.

## Idempotency and Rerun Safety

Rerun safety was important because scheduled pipelines may execute repeatedly or retry after failures.

The curated PostgreSQL load uses `order_id` as the conflict key and uses `record_hash` to determine whether business content has actually changed. As a result, rerunning the same data does not create duplicate rows or unnecessarily update unchanged records.

This behavior was demonstrated when repeated executions left the curated table at 49,897 total rows and 49,897 distinct `order_id` values.

## Storage Trade-offs

CSV, JSONL, Parquet, and PostgreSQL were compared using repeated measurements on the same curated dataset.

The results demonstrated that storage choices depend on the workload. Parquet provided compact storage and efficient analytical reads on this machine, while JSONL was more verbose. PostgreSQL provided database features such as indexed querying, persistence, concurrency, and controlled updates that file formats alone do not provide.

The benchmark results are machine-specific measurements rather than universal performance rankings.

## Error Handling and Business Logic

Data-quality problems and system failures were handled differently.

Invalid business records, such as invalid quantities, statuses, prices, or orphan references, were retained in quarantine with traceable reasons. In contrast, runtime problems such as a missing source file raised an exception instead of being silently treated as bad business data.

The Airflow failure experiment demonstrated this distinction. Temporarily removing `orders.csv` caused the extraction stage to fail, retry, and eventually invoke the failure callback. Restoring the source file allowed the pipeline to recover safely.

## Orchestration Versus Transformation Logic

The Airflow DAG is responsible for orchestration rather than transformation logic. It defines scheduling, parameters, dependency order, retries, timeout behavior, run identity, and failure handling.

The actual extraction, transformation, loading, and validation rules remain inside reusable Python modules and CLI commands.

This keeps the DAG easier to understand and prevents business logic from becoming tightly coupled to Airflow.
