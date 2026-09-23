import json
import os
import platform
import sys
from pathlib import Path
from statistics import median
from time import perf_counter

import pandas as pd
import psycopg

from src.config import DB


def materialize_storage_formats(
    df: pd.DataFrame,
    output_dir,
) -> dict:
    """Write the same curated dataset to CSV, JSONL, and compressed Parquet.

    PostgreSQL is verified against curated.sales_order_lines because the
    curated dataset was already loaded there during Goal 2.
    """

    output_dir = Path(output_dir)
    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    csv_path = output_dir / "sales_order_lines.csv"
    jsonl_path = output_dir / "sales_order_lines.jsonl"
    parquet_path = output_dir / "sales_order_lines.parquet"

    # ---------------------------------------------------------
    # CSV
    # ---------------------------------------------------------
    df.to_csv(
        csv_path,
        index=False,
    )

    # ---------------------------------------------------------
    # JSON LINES
    # ---------------------------------------------------------
    df.to_json(
        jsonl_path,
        orient="records",
        lines=True,
        date_format="iso",
    )

    # ---------------------------------------------------------
    # COMPRESSED PARQUET
    # ---------------------------------------------------------
    df.to_parquet(
        parquet_path,
        index=False,
        compression="snappy",
    )

    # ---------------------------------------------------------
    # POSTGRESQL VERIFICATION
    # ---------------------------------------------------------
    with psycopg.connect(**DB) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*) AS total_rows,
                    COUNT(DISTINCT order_id) AS distinct_orders
                FROM curated.sales_order_lines
            """)

            postgres_total, postgres_distinct = cur.fetchone()

    return {
        "csv": {
            "path": csv_path,
            "size_bytes": csv_path.stat().st_size,
            "rows": len(df),
        },
        "jsonl": {
            "path": jsonl_path,
            "size_bytes": jsonl_path.stat().st_size,
            "rows": len(df),
        },
        "parquet": {
            "path": parquet_path,
            "size_bytes": parquet_path.stat().st_size,
            "rows": len(df),
            "compression": "snappy",
        },
        "postgresql": {
            "table": "curated.sales_order_lines",
            "rows": postgres_total,
            "distinct_orders": postgres_distinct,
        },
    }


def run_benchmark(curated_path, output_dir, repeats: int = 5):
    """Benchmark CSV, JSONL, Parquet, and PostgreSQL storage behavior."""

    if repeats < 5:
        raise ValueError(
            "Goal 3 requires at least 5 read/query repetitions"
        )

    curated_path = Path(curated_path)
    output_dir = Path(output_dir)

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    df = pd.read_parquet(curated_path)

    filter_status = "DELIVERED"

    csv_path = output_dir / "benchmark_sales_order_lines.csv"
    jsonl_path = output_dir / "benchmark_sales_order_lines.jsonl"
    parquet_path = output_dir / "benchmark_sales_order_lines.parquet"

    results = []

    # ---------------------------------------------------------
    # CSV WRITE
    # ---------------------------------------------------------
    start = perf_counter()

    df.to_csv(
        csv_path,
        index=False,
    )

    csv_write_seconds = perf_counter() - start

    # ---------------------------------------------------------
    # JSONL WRITE
    # ---------------------------------------------------------
    start = perf_counter()

    df.to_json(
        jsonl_path,
        orient="records",
        lines=True,
        date_format="iso",
    )

    jsonl_write_seconds = perf_counter() - start

    # ---------------------------------------------------------
    # PARQUET WRITE
    # ---------------------------------------------------------
    start = perf_counter()

    df.to_parquet(
        parquet_path,
        index=False,
        compression="snappy",
    )

    parquet_write_seconds = perf_counter() - start

    # ---------------------------------------------------------
    # CSV READ BENCHMARK
    # ---------------------------------------------------------
    csv_full_times = []
    csv_filter_times = []
    csv_filtered_rows = None

    for _ in range(repeats):
        start = perf_counter()

        csv_full = pd.read_csv(csv_path)

        csv_full_times.append(
            perf_counter() - start
        )

        start = perf_counter()

        csv_filtered = pd.read_csv(csv_path)
        csv_filtered = csv_filtered.loc[
            csv_filtered["status"].eq(filter_status)
        ]

        csv_filter_times.append(
            perf_counter() - start
        )

        csv_filtered_rows = len(csv_filtered)

    results.append(
        {
            "storage": "CSV",
            "size_bytes": csv_path.stat().st_size,
            "server_table_size_bytes": None,
            "write_seconds": csv_write_seconds,
            "full_read_median_seconds": median(csv_full_times),
            "filtered_read_median_seconds": median(csv_filter_times),
            "filtered_rows": csv_filtered_rows,
            "repetitions": repeats,
        }
    )

    # ---------------------------------------------------------
    # JSONL READ BENCHMARK
    # ---------------------------------------------------------
    jsonl_full_times = []
    jsonl_filter_times = []
    jsonl_filtered_rows = None

    for _ in range(repeats):
        start = perf_counter()

        jsonl_full = pd.read_json(
            jsonl_path,
            lines=True,
        )

        jsonl_full_times.append(
            perf_counter() - start
        )

        start = perf_counter()

        jsonl_filtered = pd.read_json(
            jsonl_path,
            lines=True,
        )

        jsonl_filtered = jsonl_filtered.loc[
            jsonl_filtered["status"].eq(filter_status)
        ]

        jsonl_filter_times.append(
            perf_counter() - start
        )

        jsonl_filtered_rows = len(jsonl_filtered)

    results.append(
        {
            "storage": "JSONL",
            "size_bytes": jsonl_path.stat().st_size,
            "server_table_size_bytes": None,
            "write_seconds": jsonl_write_seconds,
            "full_read_median_seconds": median(jsonl_full_times),
            "filtered_read_median_seconds": median(jsonl_filter_times),
            "filtered_rows": jsonl_filtered_rows,
            "repetitions": repeats,
        }
    )

    # ---------------------------------------------------------
    # PARQUET READ BENCHMARK
    # ---------------------------------------------------------
    parquet_full_times = []
    parquet_filter_times = []
    parquet_filtered_rows = None

    for _ in range(repeats):
        start = perf_counter()

        parquet_full = pd.read_parquet(
            parquet_path
        )

        parquet_full_times.append(
            perf_counter() - start
        )

        start = perf_counter()

        parquet_filtered = pd.read_parquet(
            parquet_path,
            filters=[
                ("status", "==", filter_status)
            ],
        )

        parquet_filter_times.append(
            perf_counter() - start
        )

        parquet_filtered_rows = len(
            parquet_filtered
        )

    results.append(
        {
            "storage": "Parquet",
            "size_bytes": parquet_path.stat().st_size,
            "server_table_size_bytes": None,
            "write_seconds": parquet_write_seconds,
            "full_read_median_seconds": median(
                parquet_full_times
            ),
            "filtered_read_median_seconds": median(
                parquet_filter_times
            ),
            "filtered_rows": parquet_filtered_rows,
            "repetitions": repeats,
        }
    )

    # ---------------------------------------------------------
    # POSTGRESQL QUERY BENCHMARK
    # ---------------------------------------------------------
    postgres_full_times = []
    postgres_filter_times = []
    postgres_filtered_rows = None

    with psycopg.connect(**DB) as conn:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT COUNT(*)
                FROM curated.sales_order_lines
            """)

            postgres_total_rows = cur.fetchone()[0]

            cur.execute("""
                SELECT pg_total_relation_size(
                    'curated.sales_order_lines'
                )
            """)

            postgres_table_size = cur.fetchone()[0]

            for _ in range(repeats):
                start = perf_counter()

                cur.execute("""
                    SELECT *
                    FROM curated.sales_order_lines
                """)

                full_rows = cur.fetchall()

                postgres_full_times.append(
                    perf_counter() - start
                )

                start = perf_counter()

                cur.execute("""
                    SELECT *
                    FROM curated.sales_order_lines
                    WHERE status = %s
                """, (filter_status,))

                filtered_rows = cur.fetchall()

                postgres_filter_times.append(
                    perf_counter() - start
                )

                postgres_filtered_rows = len(
                    filtered_rows
                )

    results.append(
        {
            "storage": "PostgreSQL",
            "size_bytes": None,
            "server_table_size_bytes": postgres_table_size,
            "write_seconds": None,
            "full_read_median_seconds": median(
                postgres_full_times
            ),
            "filtered_read_median_seconds": median(
                postgres_filter_times
            ),
            "filtered_rows": postgres_filtered_rows,
            "repetitions": repeats,
        }
    )

    # ---------------------------------------------------------
    # VALIDATE COMPARABILITY
    # ---------------------------------------------------------
    expected_filtered_rows = int(
        df["status"].eq(filter_status).sum()
    )

    if postgres_total_rows != len(df):
        raise ValueError(
            "PostgreSQL row count does not match curated dataset"
        )

    for result in results:
        if result["filtered_rows"] != expected_filtered_rows:
            raise ValueError(
                f"{result['storage']} filtered row count mismatch"
            )

    # ---------------------------------------------------------
    # SAVE RESULTS
    # ---------------------------------------------------------
    results_df = pd.DataFrame(results)

    results_path = (
        output_dir / "benchmark_results.csv"
    )

    results_df.to_csv(
        results_path,
        index=False,
    )

    context = {
        "os": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "logical_cpu_count": os.cpu_count(),
        "python_version": sys.version.split()[0],
        "pandas_version": pd.__version__,
        "repetitions": repeats,
        "filter_status": filter_status,
        "curated_rows": len(df),
        "filtered_rows": expected_filtered_rows,
        "postgresql_write_time_note": (
            "Not measured here because the persistent curated table "
            "was already loaded during Goal 2."
        ),
    }

    context_path = (
        output_dir / "benchmark_context.json"
    )

    with context_path.open(
        "w",
        encoding="utf-8",
    ) as f:
        json.dump(
            context,
            f,
            indent=2,
        )

    return results_df, context


def write_partitioned_parquet(df, output_dir):
    """Write Parquet partitioned by order_year/order_month."""

    raise NotImplementedError(
        "Implement Task 9.3 partitioning"
    )