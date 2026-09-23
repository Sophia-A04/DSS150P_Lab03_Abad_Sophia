import pandas as pd

from src.config import SETTINGS


REQUIRED_COLUMNS = [
    "order_id",
    "customer_id",
    "product_id",
    "order_timestamp",
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


def validate_curated(df) -> list[str]:
    """Return human-readable data-quality errors for curated sales rows."""

    errors = []

    # ---------------------------------------------------------
    # REQUIRED COLUMNS
    # ---------------------------------------------------------
    missing_columns = [
        column
        for column in REQUIRED_COLUMNS
        if column not in df.columns
    ]

    if missing_columns:
        errors.append(
            f"missing required columns: {missing_columns}"
        )
        return errors

    # ---------------------------------------------------------
    # BUSINESS KEYS
    # ---------------------------------------------------------
    null_order_id = df["order_id"].isna().sum()

    if null_order_id:
        errors.append(
            f"null order_id values: {null_order_id}"
        )

    duplicate_order_id = df["order_id"].duplicated().sum()

    if duplicate_order_id:
        errors.append(
            f"duplicate order_id values: {duplicate_order_id}"
        )

    null_customer_id = df["customer_id"].isna().sum()

    if null_customer_id:
        errors.append(
            f"null customer_id values: {null_customer_id}"
        )

    null_product_id = df["product_id"].isna().sum()

    if null_product_id:
        errors.append(
            f"null product_id values: {null_product_id}"
        )

    # ---------------------------------------------------------
    # QUANTITY
    # ---------------------------------------------------------
    min_quantity = SETTINGS["quality"]["min_quantity"]
    max_quantity = SETTINGS["quality"]["max_quantity"]

    quantity_numeric = pd.to_numeric(
        df["quantity"],
        errors="coerce",
    )

    invalid_quantity = (
        quantity_numeric.isna()
        | quantity_numeric.lt(min_quantity)
        | quantity_numeric.gt(max_quantity)
        | quantity_numeric.mod(1).ne(0)
    )

    invalid_quantity_count = invalid_quantity.sum()

    if invalid_quantity_count:
        errors.append(
            f"invalid quantity values: {invalid_quantity_count}"
        )

    # ---------------------------------------------------------
    # STATUS
    # ---------------------------------------------------------
    allowed_statuses = set(
        SETTINGS["quality"]["allowed_order_statuses"]
    )

    invalid_status = ~df["status"].isin(allowed_statuses)
    invalid_status_count = invalid_status.sum()

    if invalid_status_count:
        errors.append(
            f"invalid status values: {invalid_status_count}"
        )

    # ---------------------------------------------------------
    # AMOUNTS
    # ---------------------------------------------------------
    amount_columns = [
        "unit_price",
        "gross_amount",
        "discount_amount",
        "net_amount",
    ]

    for column in amount_columns:
        values = pd.to_numeric(
            df[column],
            errors="coerce",
        )

        invalid = values.isna() | values.lt(0)
        invalid_count = invalid.sum()

        if invalid_count:
            errors.append(
                f"invalid {column} values: {invalid_count}"
            )

    discount_pct = pd.to_numeric(
        df["discount_pct"],
        errors="coerce",
    )

    invalid_discount_pct = (
        discount_pct.isna()
        | discount_pct.lt(0)
        | discount_pct.gt(1)
    )

    if invalid_discount_pct.sum():
        errors.append(
            "invalid discount_pct values: "
            f"{invalid_discount_pct.sum()}"
        )

    # ---------------------------------------------------------
    # BUSINESS CALCULATION CONSISTENCY
    # ---------------------------------------------------------
    expected_gross = (
        quantity_numeric
        * pd.to_numeric(df["unit_price"], errors="coerce")
    ).round(2)

    gross_mismatch = (
        expected_gross
        != pd.to_numeric(
            df["gross_amount"],
            errors="coerce",
        ).round(2)
    )

    if gross_mismatch.sum():
        errors.append(
            f"gross_amount mismatches: {gross_mismatch.sum()}"
        )

    expected_discount = (
        pd.to_numeric(
            df["gross_amount"],
            errors="coerce",
        )
        * discount_pct
    ).round(2)

    discount_mismatch = (
        expected_discount
        != pd.to_numeric(
            df["discount_amount"],
            errors="coerce",
        ).round(2)
    )

    if discount_mismatch.sum():
        errors.append(
            "discount_amount mismatches: "
            f"{discount_mismatch.sum()}"
        )

    expected_net = (
        pd.to_numeric(
            df["gross_amount"],
            errors="coerce",
        )
        - pd.to_numeric(
            df["discount_amount"],
            errors="coerce",
        )
    ).round(2)

    net_mismatch = (
        expected_net
        != pd.to_numeric(
            df["net_amount"],
            errors="coerce",
        ).round(2)
    )

    if net_mismatch.sum():
        errors.append(
            f"net_amount mismatches: {net_mismatch.sum()}"
        )

    # ---------------------------------------------------------
    # TIMESTAMPS / AUDIT FIELDS
    # ---------------------------------------------------------
    for column in [
        "order_timestamp",
        "source_updated_at",
        "pipeline_run_id",
        "processed_at_utc",
        "record_hash",
    ]:
        null_count = df[column].isna().sum()

        if null_count:
            errors.append(
                f"null {column} values: {null_count}"
            )

    invalid_hash = (
        df["record_hash"]
        .astype("string")
        .str.len()
        .ne(64)
    )

    if invalid_hash.sum():
        errors.append(
            f"invalid record_hash values: {invalid_hash.sum()}"
        )

    return errors