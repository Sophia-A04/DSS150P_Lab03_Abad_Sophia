import json
import pandas as pd

from src.config import path_for, SETTINGS
from src.common.errors import stage_error

def _add_reason(reason: pd.Series, mask: pd.Series, message: str) -> None:
    """Append a readable quarantine reason wherever mask is True."""
    first_reason = mask & reason.eq("")
    additional_reason = mask & reason.ne("")

    reason.loc[first_reason] = message
    reason.loc[additional_reason] = (
        reason.loc[additional_reason] + "; " + message
    )


def _latest_by_key(df: pd.DataFrame, key: str) -> pd.DataFrame:
    """Keep the most recent record for each business key."""
    return (
        df.sort_values("updated_at", kind="mergesort")
        .drop_duplicates(subset=[key], keep="last")
        .reset_index(drop=True)
    )


def _quarantine_rows(
    df: pd.DataFrame,
    reason: pd.Series,
    dataset: str,
    run_id: str,
    quarantined_at
) -> pd.DataFrame:
    """Return rows that have at least one quarantine reason."""
    mask = reason.ne("")
    quarantined = df.loc[mask].copy()

    if not quarantined.empty:
        quarantined["source_dataset"] = dataset
        quarantined["quarantine_reason"] = reason.loc[mask].values
        quarantined["pipeline_run_id"] = run_id
        quarantined["quarantined_at_utc"] = quarantined_at

    return quarantined

@stage_error("staging transformation")
def build_staging(raw_dir, run_id: str):
    """Create cleaned, typed staging datasets and quarantine invalid records."""

    staged_at = pd.Timestamp.now(tz="UTC")

    staging_dir = path_for("staging_dir") / f"run_id={run_id}"
    quarantine_dir = path_for("quarantine_dir") / f"run_id={run_id}"

    staging_dir.mkdir(parents=True, exist_ok=True)
    quarantine_dir.mkdir(parents=True, exist_ok=True)

    quarantine_frames = []

    # ---------------------------------------------------------
    # CUSTOMERS
    # ---------------------------------------------------------
    customers = pd.read_csv(raw_dir / "customers.csv")

    customers["created_at"] = pd.to_datetime(
        customers["created_at"], utc=True, errors="coerce"
    )
    customers["updated_at"] = pd.to_datetime(
        customers["updated_at"], utc=True, errors="coerce"
    )

    customer_reason = pd.Series("", index=customers.index, dtype="string")

    _add_reason(
        customer_reason,
        customers["customer_id"].isna(),
        "missing customer_id",
    )
    _add_reason(
        customer_reason,
        customers["created_at"].isna(),
        "invalid created_at",
    )
    _add_reason(
        customer_reason,
        customers["updated_at"].isna(),
        "invalid updated_at",
    )

    q = _quarantine_rows(
        customers,
        customer_reason,
        "customers",
        run_id,
        staged_at,
    )
    if not q.empty:
        quarantine_frames.append(q)

    customers = customers.loc[customer_reason.eq("")].copy()
    customers = _latest_by_key(customers, "customer_id")

    customers["email"] = (
        customers["email"]
        .astype("string")
        .str.strip()
        .str.lower()
    )
    customers["email"] = customers["email"].replace("", pd.NA)
    customers["email_missing"] = customers["email"].isna()

    customers["city"] = (
        customers["city"]
        .astype("string")
        .str.strip()
        .str.title()
    )

    customers["pipeline_run_id"] = run_id
    customers["staged_at_utc"] = staged_at

    # ---------------------------------------------------------
    # PRODUCTS
    # ---------------------------------------------------------
    with (raw_dir / "products.json").open(encoding="utf-8") as f:
        product_records = json.load(f)

    products = pd.json_normalize(product_records)

    products = products.rename(
        columns={
            "category.name": "category_name",
            "category.department": "category_department",
        }
    )

    products["updated_at"] = pd.to_datetime(
        products["updated_at"], utc=True, errors="coerce"
    )

    product_reason = pd.Series("", index=products.index, dtype="string")

    _add_reason(
        product_reason,
        products["product_id"].isna(),
        "missing product_id",
    )
    _add_reason(
        product_reason,
        products["updated_at"].isna(),
        "invalid updated_at",
    )

    q = _quarantine_rows(
        products,
        product_reason,
        "products",
        run_id,
        staged_at,
    )
    if not q.empty:
        quarantine_frames.append(q)

    products = products.loc[product_reason.eq("")].copy()
    products = _latest_by_key(products, "product_id")

    products["unit_price"] = pd.to_numeric(
        products["unit_price"], errors="coerce"
    )

    product_reason = pd.Series("", index=products.index, dtype="string")

    _add_reason(
        product_reason,
        products["unit_price"].isna(),
        "invalid product unit_price",
    )
    _add_reason(
        product_reason,
        products["unit_price"].lt(0),
        "negative product unit_price",
    )

    q = _quarantine_rows(
        products,
        product_reason,
        "products",
        run_id,
        staged_at,
    )
    if not q.empty:
        quarantine_frames.append(q)

    products = products.loc[product_reason.eq("")].copy()

    products["pipeline_run_id"] = run_id
    products["staged_at_utc"] = staged_at

    # ---------------------------------------------------------
    # ORDERS
    # ---------------------------------------------------------
    orders = pd.read_csv(raw_dir / "orders.csv")

    orders["order_timestamp"] = pd.to_datetime(
        orders["order_timestamp"], utc=True, errors="coerce"
    )
    orders["updated_at"] = pd.to_datetime(
        orders["updated_at"], utc=True, errors="coerce"
    )

    order_reason = pd.Series("", index=orders.index, dtype="string")

    _add_reason(
        order_reason,
        orders["order_id"].isna(),
        "missing order_id",
    )
    _add_reason(
        order_reason,
        orders["order_timestamp"].isna(),
        "invalid order_timestamp",
    )
    _add_reason(
        order_reason,
        orders["updated_at"].isna(),
        "invalid updated_at",
    )

    q = _quarantine_rows(
        orders,
        order_reason,
        "orders",
        run_id,
        staged_at,
    )
    if not q.empty:
        quarantine_frames.append(q)

    orders = orders.loc[order_reason.eq("")].copy()
    orders = _latest_by_key(orders, "order_id")

    orders["quantity"] = pd.to_numeric(
        orders["quantity"], errors="coerce"
    )
    orders["unit_price"] = pd.to_numeric(
        orders["unit_price"], errors="coerce"
    )
    orders["discount_pct"] = pd.to_numeric(
        orders["discount_pct"], errors="coerce"
    )

    orders["status"] = (
        orders["status"]
        .astype("string")
        .str.strip()
        .str.upper()
    )

    allowed_statuses = set(
        SETTINGS["quality"]["allowed_order_statuses"]
    )
    min_quantity = SETTINGS["quality"]["min_quantity"]
    max_quantity = SETTINGS["quality"]["max_quantity"]

    order_reason = pd.Series("", index=orders.index, dtype="string")

    invalid_quantity = (
        orders["quantity"].isna()
        | orders["quantity"].lt(min_quantity)
        | orders["quantity"].gt(max_quantity)
        | orders["quantity"].mod(1).ne(0)
    )

    _add_reason(
        order_reason,
        invalid_quantity,
        "invalid order quantity",
    )
    _add_reason(
        order_reason,
        ~orders["status"].isin(allowed_statuses),
        "invalid order status",
    )
    _add_reason(
        order_reason,
        orders["unit_price"].isna(),
        "invalid order unit_price",
    )
    _add_reason(
        order_reason,
        orders["discount_pct"].isna(),
        "invalid discount_pct",
    )

    q = _quarantine_rows(
        orders,
        order_reason,
        "orders",
        run_id,
        staged_at,
    )
    if not q.empty:
        quarantine_frames.append(q)

    orders = orders.loc[order_reason.eq("")].copy()

    orders["quantity"] = orders["quantity"].astype("Int64")
    orders["pipeline_run_id"] = run_id
    orders["staged_at_utc"] = staged_at

    # ---------------------------------------------------------
    # WRITE STAGING OUTPUTS
    # ---------------------------------------------------------
    staging = {
        "customers": customers.reset_index(drop=True),
        "products": products.reset_index(drop=True),
        "orders": orders.reset_index(drop=True),
    }

    for name, frame in staging.items():
        frame.to_parquet(
            staging_dir / f"{name}.parquet",
            index=False,
        )

    if quarantine_frames:
        quarantine = pd.concat(
            quarantine_frames,
            ignore_index=True,
            sort=False,
        )
    else:
        quarantine = pd.DataFrame(
            columns=[
                "source_dataset",
                "quarantine_reason",
                "pipeline_run_id",
                "quarantined_at_utc",
            ]
        )

    quarantine.to_parquet(
        quarantine_dir / "quarantine.parquet",
        index=False,
    )

    return staging, quarantine