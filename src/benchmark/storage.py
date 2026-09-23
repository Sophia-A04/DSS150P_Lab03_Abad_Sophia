from pathlib import Path

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
    """Compare the same logical dataset in CSV, JSON Lines, Parquet, and PostgreSQL.

    Capture:
    - storage/file size where applicable
    - write time
    - full-read time
    - filtered-read/query time
    - row count

    Use multiple repetitions and report a median for read/query timing.
    """

    raise NotImplementedError(
        "Implement Task 9.2 storage benchmark methodology"
    )


def write_partitioned_parquet(df, output_dir):
    """Write Parquet partitioned by order_year/order_month."""

    raise NotImplementedError(
        "Implement Task 9.3 partitioning"
    )