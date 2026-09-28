"""Unit and integration tests for the Petpooja sales pipeline."""

import sqlite3
import unittest
from unittest.mock import MagicMock, patch

import requests

from main import (
    extract_field,
    fetch_sales_data,
    insert_records,
    parse_records,
    safe_float,
)


class TestPetpoojaPipeline(unittest.TestCase):
    def test_safe_float(self):
        self.assertEqual(safe_float(100), 100.0)
        self.assertEqual(safe_float("1,250.50"), 1250.50)
        self.assertEqual(safe_float("45.2"), 45.2)
        self.assertIsNone(safe_float(None))
        self.assertIsNone(safe_float(""))
        self.assertEqual(safe_float("invalid"), 0.0)

    def test_extract_field(self):
        record = {"Receipt number": "REC-001", "other": "val"}
        self.assertEqual(extract_field(record, ["Receipt number", "receipt_no"]), "REC-001")
        self.assertEqual(extract_field(record, ["nonexistent", "other"]), "val")
        self.assertIsNone(extract_field(record, ["nonexistent"]))

    def test_parse_records(self):
        raw_api_data = {
            "success": "1",
            "message": "Success",
            "data": [
                {
                    "Receipt number": "REC-1001",
                    "Receipt Date": "2024-01-01",
                    "Transaction time": "14:35:10",
                    "Invoice amount": "550.00",
                    "Tax amount": "27.50",
                    "Discount amount": "50.00",
                    "Round off": "0.50",
                    "Net sale": "528.00",
                    "Payment mode": "UPI",
                    "Order type": "Dine In",
                    "Transaction status": "SALE",
                },
                {
                    # Record with missing optional fields
                    "Receipt number": "REC-1002",
                    "Receipt Date": "2024-01-01",
                    "Invoice amount": 200,
                    "Tax amount": 10,
                    "Discount amount": 0,
                    "Net sale": 210,
                    "Transaction status": "RETURN",
                },
            ],
        }

        parsed = parse_records(raw_api_data)
        self.assertEqual(len(parsed), 2)

        # First record checks
        r1 = parsed[0]
        self.assertEqual(r1["receipt_number"], "REC-1001")
        self.assertEqual(r1["sale_date"], "2024-01-01")
        self.assertEqual(r1["transaction_time"], "14:35:10")
        self.assertEqual(r1["sale_amount"], 550.0)
        self.assertEqual(r1["tax_amount"], 27.50)
        self.assertEqual(r1["discount_amount"], 50.0)
        self.assertEqual(r1["round_off"], 0.50)
        self.assertEqual(r1["net_sale"], 528.0)
        self.assertEqual(r1["payment_mode"], "UPI")
        self.assertEqual(r1["order_type"], "Dine In")
        self.assertEqual(r1["transaction_status"], "SALE")

        # Second record checks optional fields are None (NULL)
        r2 = parsed[1]
        self.assertEqual(r2["receipt_number"], "REC-1002")
        self.assertIsNone(r2["transaction_time"])
        self.assertIsNone(r2["round_off"])
        self.assertIsNone(r2["payment_mode"])
        self.assertIsNone(r2["order_type"])
        self.assertEqual(r2["transaction_status"], "RETURN")

    def test_database_insert_and_idempotency(self):
        # In-memory SQLite for test
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row

        # Setup schema
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE sales_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                receipt_number TEXT,
                sale_date TEXT,
                transaction_time TEXT,
                sale_amount REAL,
                tax_amount REAL,
                discount_amount REAL,
                round_off REAL,
                net_sale REAL,
                payment_mode TEXT,
                order_type TEXT,
                transaction_status TEXT
            );
        """)
        cursor.execute("""
            CREATE UNIQUE INDEX idx_sales_receipt_date
            ON sales_data(receipt_number, sale_date);
        """)
        conn.commit()

        records = [
            {
                "receipt_number": "REC-01",
                "sale_date": "2024-01-01",
                "transaction_time": "12:00:00",
                "sale_amount": 100.0,
                "tax_amount": 5.0,
                "discount_amount": 0.0,
                "round_off": None,
                "net_sale": 105.0,
                "payment_mode": "Cash",
                "order_type": "Takeaway",
                "transaction_status": "SALE",
            }
        ]

        # First insert
        count = insert_records(conn, records)
        self.assertEqual(count, 1)

        # Check row count
        cursor.execute("SELECT COUNT(*) FROM sales_data;")
        self.assertEqual(cursor.fetchone()[0], 1)

        # Re-run same record with updated amount to verify duplicate prevention / replacement
        records[0]["sale_amount"] = 120.0
        insert_records(conn, records)

        cursor.execute("SELECT COUNT(*), sale_amount FROM sales_data WHERE receipt_number='REC-01';")
        row = cursor.fetchone()
        self.assertEqual(row[0], 1)  # No duplicate row inserted
        self.assertEqual(row[1], 120.0)  # Value updated
        conn.close()

    @patch("time.sleep", return_value=None)
    @patch("requests.post")
    def test_fetch_sales_data_retry(self, mock_post, mock_sleep):
        # Simulate 2 timeouts then success
        resp_mock = MagicMock()
        resp_mock.status_code = 200
        resp_mock.json.return_value = {"success": "1", "data": []}

        mock_post.side_effect = [
            requests.exceptions.Timeout("Connection timed out"),
            requests.exceptions.HTTPError("Server busy 503"),
            resp_mock,
        ]

        result = fetch_sales_data(
            base_url="http://mock.petpooja.com/api",
            app_key="k",
            app_secret="s",
            access_token="t",
            rest_id="r",
            from_date="2024-01-01",
            to_date="2024-01-02",
            max_retries=3,
            backoff_factor=0.01,
        )

        self.assertEqual(result["success"], "1")
        self.assertEqual(mock_post.call_count, 3)
        self.assertEqual(mock_sleep.call_count, 2)


if __name__ == "__main__":
    unittest.main()
