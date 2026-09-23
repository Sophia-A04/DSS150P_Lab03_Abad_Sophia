import os
import unittest

import pandas as pd


# Safe test-only defaults so importing src.config does not require
# a developer's private .env values.
os.environ.setdefault("POSTGRES_HOST", "localhost")
os.environ.setdefault("POSTGRES_PORT", "5432")
os.environ.setdefault("POSTGRES_DB", "dss150p")
os.environ.setdefault("POSTGRES_USER", "dss150p")
os.environ.setdefault("POSTGRES_PASSWORD", "test_only")

from src.transform.staging import (
    _add_reason,
    _latest_by_key,
    _quarantine_rows,
)
from src.transform.curated import _record_hash


class TransformRuleTests(unittest.TestCase):

    def test_latest_by_key_keeps_most_recent_record(self):
        df = pd.DataFrame(
            {
                "customer_id": ["C1", "C1"],
                "updated_at": pd.to_datetime(
                    [
                        "2026-01-01T00:00:00Z",
                        "2026-02-01T00:00:00Z",
                    ],
                    utc=True,
                ),
                "name": ["Old", "New"],
            }
        )

        result = _latest_by_key(df, "customer_id")

        self.assertEqual(len(result), 1)
        self.assertEqual(result.loc[0, "name"], "New")

    def test_add_reason_appends_multiple_reasons(self):
        reason = pd.Series(["", ""], dtype="string")

        _add_reason(
            reason,
            pd.Series([True, False]),
            "first reason",
        )
        _add_reason(
            reason,
            pd.Series([True, True]),
            "second reason",
        )

        self.assertEqual(
            reason.iloc[0],
            "first reason; second reason",
        )
        self.assertEqual(
            reason.iloc[1],
            "second reason",
        )

    def test_quarantine_rows_adds_audit_metadata(self):
        df = pd.DataFrame(
            {
                "order_id": ["O1", "O2"],
            }
        )
        reason = pd.Series(
            ["", "invalid order quantity"],
            dtype="string",
        )
        timestamp = pd.Timestamp(
            "2026-09-23T00:00:00Z"
        )

        result = _quarantine_rows(
            df,
            reason,
            "orders",
            "test_run",
            timestamp,
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(
            result.iloc[0]["order_id"],
            "O2",
        )
        self.assertEqual(
            result.iloc[0]["source_dataset"],
            "orders",
        )
        self.assertEqual(
            result.iloc[0]["pipeline_run_id"],
            "test_run",
        )

    def _business_row(self):
        return pd.Series(
            {
                "order_id": "O1",
                "customer_id": "C1",
                "product_id": "P1",
                "order_timestamp": pd.Timestamp(
                    "2026-01-01T00:00:00Z"
                ),
                "customer_city": "Manila",
                "customer_tier": "Gold",
                "product_name": "Example Product",
                "category": "Example",
                "brand": "Brand",
                "quantity": 2,
                "unit_price": 100.0,
                "discount_pct": 0.10,
                "gross_amount": 200.0,
                "discount_amount": 20.0,
                "net_amount": 180.0,
                "status": "DELIVERED",
                "source_updated_at": pd.Timestamp(
                    "2026-01-02T00:00:00Z"
                ),
                "pipeline_run_id": "run_1",
                "processed_at_utc": pd.Timestamp(
                    "2026-01-03T00:00:00Z"
                ),
            }
        )

    def test_record_hash_ignores_run_specific_audit_fields(self):
        first = self._business_row()
        second = self._business_row()

        second["pipeline_run_id"] = "run_2"
        second["processed_at_utc"] = pd.Timestamp(
            "2026-02-01T00:00:00Z"
        )

        self.assertEqual(
            _record_hash(first),
            _record_hash(second),
        )

    def test_record_hash_changes_when_business_content_changes(self):
        first = self._business_row()
        second = self._business_row()

        second["quantity"] = 3

        self.assertNotEqual(
            _record_hash(first),
            _record_hash(second),
        )


if __name__ == "__main__":
    unittest.main()