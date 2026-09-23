# Goal 2 - ETL/ELT and Transformation Pipeline Evidence

## Overview

Goal 2 implements a modular raw -> staging -> curated -> PostgreSQL pipeline.

The pipeline preserves immutable raw source snapshots, performs technical
cleanup and deduplication in staging, quarantines invalid records with
traceable reasons, applies cross-source business logic in curated, validates
the resulting dataset, and loads curated rows to PostgreSQL using rerun-safe
UPSERT logic.

## Raw Extraction

A run-specific raw directory is created for each pipeline run.

Example run:

`run_20260923T031123Z_b89bd814`

SHA-256 comparison between the instructor source files and raw snapshots:

| Source file | Result |
|---|---|
| customers.csv | MATCH |
| products.json | MATCH |
| orders.csv | MATCH |

This confirms raw extraction does not alter source content.

## Staging Results

Observed staging output:

| Dataset | Valid staged rows |
|---|---:|
| customers | 3,000 |
| products | 599 |
| orders | 49,998 |

Duplicate business keys after staging:

| Business key | Duplicate count |
|---|---:|
| customer_id | 0 |
| product_id | 0 |
| order_id | 0 |

Staging keeps the most recent duplicate version according to `updated_at`.

Customer emails are normalized by trimming and lowercasing. Missing customer
emails are retained and flagged rather than silently removed.

Products are normalized from nested JSON, typed, and checked for invalid
prices.

Orders are typed and checked for valid quantity and status values.

All staging outputs contain:

- `pipeline_run_id`
- `staged_at_utc`

## Quarantine Results

Total quarantined records after staging and curated processing: **104**

Observed reasons:

| Quarantine reason | Rows |
|---|---:|
| product_id not found in valid product staging | 100 |
| customer_id not found in valid customer staging | 1 |
| negative product unit_price | 1 |
| invalid order quantity | 1 |
| invalid order status | 1 |

The 100 unmatched product references include orders referencing product
`P0078`, whose latest product record was invalid and therefore correctly
excluded from valid product staging, plus the true product orphan.

No rejected rows are silently discarded.

## Curated Transformation

Curated output rows: **49,897**

The curated dataset joins valid orders with customer and product descriptive
attributes.

Business measures are calculated as:

```text
gross_amount = quantity * unit_price
discount_amount = gross_amount * discount_pct
net_amount = gross_amount - discount_amount
```

The curated dataset retains lineage through `pipeline_run_id` and contains the valid business rows after quarantine handling.

## Validation Results

Curated validation completed successfully.

Observed results:

- Curated rows: 49,897
- Validation errors: 0
- Result: PASS

## PostgreSQL Loading and Rerun Safety

The curated dataset was persisted to:

`curated.sales_order_lines`

A repeated full pipeline execution reported:

- Database rows affected: 0
- Total PostgreSQL rows: 49,897
- Distinct `order_id` values: 49,897

Because an unchanged rerun did not insert duplicate business rows, the PostgreSQL loading process demonstrated rerun-safe behavior.

## Goal 2 Acceptance

Goal 2 demonstrated:

- immutable raw extraction
- staging cleanup and deduplication
- explicit technical quarantine
- orphan quarantine during curated processing
- cross-source curated transformation
- data-quality validation
- PostgreSQL persistence
- rerun-safe repeated execution
- no duplicate `order_id` values after rerunning the pipeline