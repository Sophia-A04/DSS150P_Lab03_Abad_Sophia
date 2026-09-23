import pandas as pd
import psycopg

from src.config import DB
from src.common.errors import stage_error


CURATED_COLUMNS = [
    "order_id",
    "customer_id",
    "product_id",
    "order_timestamp",
    "customer_city",
    "customer_tier",
    "product_name",
    "category",
    "brand",
    "quantity",
    "unit_price",
    "discount_pct",
    "gross_amount",
    "discount_amount",
    "net_amount",
    "status",
    "source_updated_at",
    "pipeline_run_id",
    "processed_at_utc",
    "record_hash",
]


def _to_python_value(value):
    """Convert pandas/numpy scalars into values psycopg can adapt."""

    if pd.isna(value):
        return None

    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()

    if hasattr(value, "item"):
        return value.item()

    return value


@stage_error("PostgreSQL curated load")
def upsert_curated(df, run_id: str) -> int:
    """Load curated.sales_order_lines using rerun-safe UPSERT semantics."""

    missing_columns = [
        column
        for column in CURATED_COLUMNS
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Curated dataframe is missing columns: {missing_columns}"
        )

    records = [
        tuple(
            _to_python_value(value)
            for value in row
        )
        for row in df[CURATED_COLUMNS].itertuples(
            index=False,
            name=None,
        )
    ]

    sql = """
        INSERT INTO curated.sales_order_lines AS target (
            order_id,
            customer_id,
            product_id,
            order_timestamp,
            customer_city,
            customer_tier,
            product_name,
            category,
            brand,
            quantity,
            unit_price,
            discount_pct,
            gross_amount,
            discount_amount,
            net_amount,
            status,
            source_updated_at,
            pipeline_run_id,
            processed_at_utc,
            record_hash
        )
        VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s
        )
        ON CONFLICT (order_id)
        DO UPDATE SET
            customer_id = EXCLUDED.customer_id,
            product_id = EXCLUDED.product_id,
            order_timestamp = EXCLUDED.order_timestamp,
            customer_city = EXCLUDED.customer_city,
            customer_tier = EXCLUDED.customer_tier,
            product_name = EXCLUDED.product_name,
            category = EXCLUDED.category,
            brand = EXCLUDED.brand,
            quantity = EXCLUDED.quantity,
            unit_price = EXCLUDED.unit_price,
            discount_pct = EXCLUDED.discount_pct,
            gross_amount = EXCLUDED.gross_amount,
            discount_amount = EXCLUDED.discount_amount,
            net_amount = EXCLUDED.net_amount,
            status = EXCLUDED.status,
            source_updated_at = EXCLUDED.source_updated_at,
            pipeline_run_id = EXCLUDED.pipeline_run_id,
            processed_at_utc = EXCLUDED.processed_at_utc,
            record_hash = EXCLUDED.record_hash
        WHERE target.record_hash
            IS DISTINCT FROM EXCLUDED.record_hash
    """

    with psycopg.connect(**DB) as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, records)

            affected_rows = cur.rowcount

    return affected_rows


@stage_error("PostgreSQL partition load")
def load_partition(
    df,
    year: int,
    month: int,
    run_id: str,
) -> int:
    """Load one year/month partition and record its audit state."""

    if month < 1 or month > 12:
        raise ValueError(
            f"Invalid month: {month}"
        )

    if df.empty:
        raise ValueError(
            f"Selected partition {year}-{month:02d} is empty"
        )

    timestamps = pd.to_datetime(
        df["order_timestamp"],
        utc=True,
        errors="coerce",
    )

    if timestamps.isna().any():
        raise ValueError(
            "Selected partition contains invalid order_timestamp values"
        )

    expected_partition = (
        timestamps.dt.year.eq(year)
        & timestamps.dt.month.eq(month)
    )

    if not expected_partition.all():
        invalid_rows = int(
            (~expected_partition).sum()
        )

        raise ValueError(
            f"Selected partition contains {invalid_rows} rows "
            f"outside {year}-{month:02d}"
        )

    # Reuse the existing Goal 2 rerun-safe UPSERT logic.
    upsert_curated(
        df,
        run_id,
    )

    partition_key = (
        f"order_year={year}/order_month={month}"
    )

    row_count = len(df)

    with psycopg.connect(**DB) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO audit.partition_loads (
                    partition_key,
                    loaded_at_utc,
                    row_count,
                    pipeline_run_id
                )
                VALUES (
                    %s,
                    NOW(),
                    %s,
                    %s
                )
                ON CONFLICT (partition_key)
                DO UPDATE SET
                    loaded_at_utc =
                        EXCLUDED.loaded_at_utc,
                    row_count =
                        EXCLUDED.row_count,
                    pipeline_run_id =
                        EXCLUDED.pipeline_run_id
                """,
                (
                    partition_key,
                    row_count,
                    run_id,
                ),
            )

    return row_count    