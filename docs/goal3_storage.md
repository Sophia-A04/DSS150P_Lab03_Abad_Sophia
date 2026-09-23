# Goal 3 - Storage, Benchmarking, and Partitioning Evidence

## Overview

Goal 3 compares the same curated logical dataset across CSV, JSON Lines,
compressed Parquet, and PostgreSQL, then implements year/month partitioned
Parquet and rerun-safe selected-partition PostgreSQL loading with audit
tracking.

The curated logical dataset contains 49,897 rows.

## Task 9.1 - Storage Representations

The same curated dataset was represented as:

- CSV
- JSON Lines
- Snappy-compressed Parquet
- PostgreSQL table `curated.sales_order_lines`

Observed storage sizes:

| Representation | Size in bytes |
|---|---:|
| CSV | 14,947,456 |
| JSONL | 30,240,770 |
| Parquet (Snappy) | 5,471,032 |
| PostgreSQL total relation size | 15,974,400 |

PostgreSQL storage was measured as server-side relation size rather than as
a standalone file.

CSV, JSONL, and Parquet were read back successfully. Each contained:

- 49,897 rows
- 49,897 distinct `order_id` values
- the same `order_id` set

## Task 9.2 - Benchmark Methodology

The benchmark used the same curated logical dataset across all four
representations.

Filtered retrieval used:

`status = DELIVERED`

Each full-read and filtered-read/query measurement used 5 repetitions and
reported the median.

All four representations returned:

`8,355 DELIVERED rows`

### Latest Measured Benchmark Results

| Storage | Write (s) | Median Full Read (s) | Median Filtered Read/Query (s) | Filtered Rows |
|---|---:|---:|---:|---:|
| CSV | 1.002357 | 0.250896 | 0.260712 | 8,355 |
| JSONL | 1.089630 | 0.832807 | 0.934192 | 8,355 |
| Parquet | 0.189309 | 0.066550 | 0.030829 | 8,355 |
| PostgreSQL | Not measured in this benchmark | 0.490735 | 0.097478 | 8,355 |

The PostgreSQL persistent table had already been loaded during Goal 2, so a
new PostgreSQL write time was not mixed into the Goal 3 benchmark.

### Machine Context

- OS: Windows 11
- Architecture: AMD64
- Processor: AMD64 Family 25 Model 117 Stepping 2, AuthenticAMD
- Logical CPU count: 16
- Python: 3.12.10
- pandas: 2.2.3
- Repetitions: 5
- Filter: `status = DELIVERED`

### Observations

For this dataset and measured run:

- Parquet produced the smallest standalone file.
- JSON Lines produced the largest standalone file.
- Parquet had the lowest measured full-read median.
- Parquet had the lowest measured filtered-read median.
- PostgreSQL filtered querying was faster than CSV and JSONL in this run.
- Timing results are specific to this machine, dataset, and execution and
  should not be treated as universal performance rankings.

## Task 9.3 - Partitioned Parquet

The curated dataset was partitioned using derived:

- `order_year`
- `order_month`

Observed results:

- Total rows: 49,897
- Years represented: 2025 and 2026
- Distinct year/month partitions: 21

The partition hierarchy follows:

```text
order_year=2025/
    order_month=1/
    order_month=2/
    ...
order_year=2026/
    order_month=1/
    ...

    ```

This organization allows a selected year/month to be read without scanning the entire curated dataset.

## Task 9.4 - Selected Partition Loading

The January 2025 partition was loaded using:

```bash
python -m src.cli load-partition --year 2025 --month 1
```

Observed results:

- Year: 2025
- Month: 1
- Partition rows: 2,458
- Result: PASS

The load was recorded in `audit.partition_loads` using the partition key:

`order_year=2025/order_month=1`

The audit record reported:

- Row count: 2,458

After the selected-partition load, the curated PostgreSQL table remained at:

- Total rows: 49,897
- Distinct `order_id` values: 49,897

This demonstrated that selected-partition loading was rerun-safe and did not create duplicate business rows.

## Goal 3 Acceptance

Goal 3 demonstrated:

- equivalent CSV, JSONL, Parquet, and PostgreSQL representations
- controlled repeated benchmarking
- machine-context reporting
- compressed Parquet storage
- year/month partitioning
- filtered partition reads
- selected-partition PostgreSQL loading
- partition audit tracking
- rerun-safe loading without duplicate business rows
