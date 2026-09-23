from hashlib import sha256
import json

import pandas as pd

from src.config import path_for


HASH_COLUMNS = [
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
]

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


def _add_reason(
    reason: pd.Series,
    mask: pd.Series,
    message: str,
) -> None:
    """Append a readable quarantine reason wherever mask is True."""

    first_reason = mask & reason.eq("")
    additional_reason = mask & reason.ne("")

    reason.loc[first_reason] = message
    reason.loc[additional_reason] = (
        reason.loc[additional_reason] + "; " + message
    )


def _canonical_value(value):
    """Convert a scalar into a stable representation for hashing."""

    if pd.isna(value):
        return None

    if isinstance(value, pd.Timestamp):
        return value.isoformat()

    if hasattr(value, "item"):
        value = value.item()

    if isinstance(value, float):
        return format(value, ".12g")

    return value


def _record_hash(row: pd.Series) -> str:
    """Create a deterministic SHA-256 hash from business content only."""

    payload = {
        column: _canonical_value(row[column])
        for column in HASH_COLUMNS
    }

    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")

    return sha256(encoded).hexdigest()


def build_curated(staging: dict, run_id: str):
    """Join valid staging data and create analysis-ready sales rows."""

    processed_at = pd.Timestamp.now(tz="UTC")

    customers = staging["customers"].copy()
    products = staging["products"].copy()
    orders = staging["orders"].copy()

    # ---------------------------------------------------------
    # IDENTIFY CROSS-SOURCE ORPHANS
    # ---------------------------------------------------------
    orphan_reason = pd.Series(
        "",
        index=orders.index,
        dtype="string",
    )

    missing_customer = ~orders["customer_id"].isin(
        customers["customer_id"]
    )

    missing_product = ~orders["product_id"].isin(
        products["product_id"]
    )

    _add_reason(
        orphan_reason,
        missing_customer,
        "customer_id not found in valid customer staging",
    )

    _add_reason(
        orphan_reason,
        missing_product,
        "product_id not found in valid product staging",
    )

    orphan_mask = orphan_reason.ne("")

    orphan_quarantine = orders.loc[orphan_mask].copy()

    if not orphan_quarantine.empty:
        orphan_quarantine["source_dataset"] = "orders"
        orphan_quarantine["quarantine_reason"] = (
            orphan_reason.loc[orphan_mask].values
        )
        orphan_quarantine["pipeline_run_id"] = run_id
        orphan_quarantine["quarantined_at_utc"] = processed_at
        orphan_quarantine["quarantine_stage"] = "curated"

    valid_orders = orders.loc[~orphan_mask].copy()

    # ---------------------------------------------------------
    # PREPARE DIMENSION ATTRIBUTES
    # ---------------------------------------------------------
    customer_dim = customers[
        [
            "customer_id",
            "city",
            "customer_tier",
        ]
    ].rename(
        columns={
            "city": "customer_city",
        }
    )

    product_dim = products[
        [
            "product_id",
            "name",
            "category_name",
            "brand",
        ]
    ].rename(
        columns={
            "name": "product_name",
            "category_name": "category",
        }
    )

    # ---------------------------------------------------------
    # JOIN ORDERS TO VALID CUSTOMERS AND PRODUCTS
    # ---------------------------------------------------------
    curated = valid_orders.merge(
        customer_dim,
        on="customer_id",
        how="left",
        validate="many_to_one",
    )

    curated = curated.merge(
        product_dim,
        on="product_id",
        how="left",
        validate="many_to_one",
    )

    curated = curated.rename(
        columns={
            "updated_at": "source_updated_at",
        }
    )

    # ---------------------------------------------------------
    # BUSINESS CALCULATIONS
    # ---------------------------------------------------------
    curated["gross_amount"] = (
        curated["quantity"].astype("float64")
        * curated["unit_price"]
    ).round(2)

    curated["discount_amount"] = (
        curated["gross_amount"]
        * curated["discount_pct"]
    ).round(2)

    curated["net_amount"] = (
        curated["gross_amount"]
        - curated["discount_amount"]
    ).round(2)

    # ---------------------------------------------------------
    # AUDIT METADATA + DETERMINISTIC HASH
    # ---------------------------------------------------------
    curated["pipeline_run_id"] = run_id
    curated["processed_at_utc"] = processed_at

    curated["record_hash"] = curated.apply(
        _record_hash,
        axis=1,
    )

    curated = (
        curated[CURATED_COLUMNS]
        .sort_values("order_id", kind="mergesort")
        .reset_index(drop=True)
    )

    # ---------------------------------------------------------
    # WRITE CURATED OUTPUT
    # ---------------------------------------------------------
    curated_dir = (
        path_for("curated_dir")
        / f"run_id={run_id}"
    )

    curated_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    curated.to_parquet(
        curated_dir / "sales_order_lines.parquet",
        index=False,
    )

    # ---------------------------------------------------------
    # ADD CURATED ORPHANS TO QUARANTINE
    # ---------------------------------------------------------
    quarantine_dir = (
        path_for("quarantine_dir")
        / f"run_id={run_id}"
    )

    quarantine_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    quarantine_path = (
        quarantine_dir / "quarantine.parquet"
    )

    if quarantine_path.exists():
        existing_quarantine = pd.read_parquet(
            quarantine_path
        )

        # If this curated step is rerun using the same run_id,
        # remove its previous orphan output before rebuilding it.
        if "quarantine_stage" in existing_quarantine.columns:
            existing_quarantine = existing_quarantine.loc[
                existing_quarantine[
                    "quarantine_stage"
                ].fillna("").ne("curated")
            ].copy()

    else:
        existing_quarantine = pd.DataFrame()

    combined_quarantine = pd.concat(
        [
            existing_quarantine,
            orphan_quarantine,
        ],
        ignore_index=True,
        sort=False,
    )

    combined_quarantine.to_parquet(
        quarantine_path,
        index=False,
    )

    return (
        curated,
        orphan_quarantine.reset_index(drop=True),
    )