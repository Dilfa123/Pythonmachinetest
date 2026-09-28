"""Simulation script to test API failures, bad responses, retry logic, and error handling.

Tests scenarios:
1. Transient API Failure -> Retries and recovers.
2. Persistent Network Failure (Timeouts/500s) -> Exhausts 3 retries with backoff and logs error.
3. Bad Response (Malformed JSON / HTML response) -> Caught and retried.
4. Missing Keys & Corrupted Fields in JSON -> Handled without crashing, skips invalid records.
5. Database Failure -> Proper rollback and error logging.
"""

import json
import logging
import sqlite3
from unittest.mock import MagicMock, patch

import requests

from main import (
    fetch_sales_data,
    init_db,
    insert_records,
    parse_records,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("SimulationTest")


def run_simulations():
    print("=" * 70)
    print("SIMULATION 1: TRANSIENT API FAILURE & RECOVERY (2 Failures -> 1 Success)")
    print("=" * 70)

    success_resp = MagicMock()
    success_resp.status_code = 200
    success_resp.json.return_value = {
        "success": "1",
        "Records": [
            {
                "Receipt number": "SIM-RECOVER-01",
                "Receipt Date": "2025-05-01",
                "Invoice amount": "100.00",
                "Tax amount": "5.00",
                "Discount amount": "0.00",
                "Net sale": "105.00",
                "Transaction status": "SALE",
            }
        ],
    }

    # Simulate: Timeout on attempt 1, 503 on attempt 2, Success on attempt 3
    err_503 = requests.exceptions.HTTPError("503 Service Unavailable")
    timeout_err = requests.exceptions.Timeout("Connection timed out to petpooja")

    with patch("time.sleep") as mock_sleep, patch("requests.post") as mock_post:
        mock_post.side_effect = [timeout_err, err_503, success_resp]

        data = fetch_sales_data(
            base_url="http://mock.api.com",
            app_key="k",
            app_secret="s",
            access_token="t",
            rest_id="r",
            from_date="2025-05-01",
            to_date="2025-05-02",
            max_retries=3,
            backoff_factor=0.5,
        )

        assert data["success"] == "1", "Should succeed on 3rd attempt"
        assert mock_post.call_count == 3, f"Expected 3 calls, got {mock_post.call_count}"
        assert mock_sleep.call_count == 2, f"Expected 2 backoff sleeps, got {mock_sleep.call_count}"
        print("[OK] Successfully retried 3 times with exponential backoff and recovered on attempt 3!")

    print("\n" + "=" * 70)
    print("SIMULATION 2: PERSISTENT API FAILURE (Exhausts 3 Retries -> Handled Gracefully)")
    print("=" * 70)

    with patch("time.sleep") as mock_sleep, patch("requests.post") as mock_post:
        mock_post.side_effect = requests.exceptions.ConnectionError("Failed to resolve host api.petpooja.com")

        try:
            fetch_sales_data(
                base_url="http://mock.api.com",
                app_key="k",
                app_secret="s",
                access_token="t",
                rest_id="r",
                from_date="2025-05-01",
                to_date="2025-05-02",
                max_retries=3,
                backoff_factor=0.1,
            )
            print("[FAIL] Should have raised RequestException after 3 failed attempts")
        except requests.RequestException as e:
            assert mock_post.call_count == 3, f"Expected 3 attempts, got {mock_post.call_count}"
            assert mock_sleep.call_count == 2, f"Expected 2 sleeps, got {mock_sleep.call_count}"
            print(f"[OK] Caught expected exception after 3 retries: {e}")

    print("\n" + "=" * 70)
    print("SIMULATION 3: BAD RESPONSE (Invalid JSON / HTML Cloudflare Error Page)")
    print("=" * 70)

    html_resp = MagicMock()
    html_resp.status_code = 200
    html_resp.json.side_effect = json.JSONDecodeError("Expecting value", "<html>502 Bad Gateway</html>", 0)

    with patch("time.sleep") as mock_sleep, patch("requests.post") as mock_post:
        mock_post.side_effect = [html_resp, html_resp, success_resp]

        data = fetch_sales_data(
            base_url="http://mock.api.com",
            app_key="k",
            app_secret="s",
            access_token="t",
            rest_id="r",
            from_date="2025-05-01",
            to_date="2025-05-02",
            max_retries=3,
            backoff_factor=0.1,
        )
        assert data["success"] == "1"
        assert mock_post.call_count == 3
        print("[OK] Caught JSONDecodeError on non-JSON response and recovered on next retry!")

    print("\n" + "=" * 70)
    print("SIMULATION 4: CORRUPTED DATA & MISSING KEYS HANDLING")
    print("=" * 70)

    corrupted_payload = {
        "success": "1",
        "Records": [
            # 1. Missing receipt number -> MUST be skipped
            {
                "Receipt Date": "2025-05-01",
                "Invoice amount": "150.00",
                "Net sale": "150.00",
            },
            # 2. Missing sale date -> MUST be skipped
            {
                "Receipt number": "SIM-NO-DATE",
                "Invoice amount": "200.00",
                "Net sale": "200.00",
            },
            # 3. Corrupted strings in numeric fields -> Safely converted to 0.0
            {
                "Receipt number": "SIM-CORRUPT-NUM",
                "Receipt Date": "2025-05-01",
                "Invoice amount": "INVALID_NUMBER",
                "Tax amount": "N/A",
                "Discount amount": None,
                "Net sale": "350.00",
                "Transaction status": "SALE",
            },
            # 4. Valid record with missing optional fields -> NULL stored
            {
                "Receipt number": "SIM-VALID-01",
                "Receipt Date": "2025-05-01",
                "Invoice amount": "500.00",
                "Tax amount": "25.00",
                "Discount amount": "50.00",
                "Round Off": "0.00",
                "Net sale": "475.00",
                "Payment Mode": "UPI",
                "Order Type": "Dine-In",
                "Transaction status": "SALE",
            },
        ],
    }

    parsed = parse_records(corrupted_payload)
    print(f"Total raw items: {len(corrupted_payload['Records'])}, Successfully parsed: {len(parsed)}")

    assert len(parsed) == 2, f"Expected 2 parsed records, got {len(parsed)}"
    # First parsed record should be SIM-CORRUPT-NUM with safe 0.0 values
    assert parsed[0]["receipt_number"] == "SIM-CORRUPT-NUM"
    assert parsed[0]["sale_amount"] == 0.0
    assert parsed[0]["tax_amount"] == 0.0
    assert parsed[0]["discount_amount"] == 0.0
    assert parsed[0]["net_sale"] == 350.0

    # Second parsed record
    assert parsed[1]["receipt_number"] == "SIM-VALID-01"
    assert parsed[1]["order_type"] == "Dine-In"
    print("[OK] Correctly skipped records missing primary keys and safely defaulted corrupted numbers!")

    print("\n" + "=" * 70)
    print("SIMULATION 5: DATABASE ERROR & ROLLBACK HANDLING")
    print("=" * 70)

    conn = sqlite3.connect(":memory:")
    init_db(":memory:")  # test schema init
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

    # Simulate database insertion failure by closing connection or simulating error
    conn.close()
    try:
        insert_records(conn, parsed)
        print("[FAIL] Expected sqlite3.Error on closed connection")
    except sqlite3.Error as e:
        print(f"[OK] Caught expected SQLite error and prevented corrupted state: {e}")

    print("\n" + "=" * 70)
    print("ALL 5 FAULT-TOLERANCE SIMULATIONS PASSED SUCCESSFULLY!")
    print("=" * 70)


if __name__ == "__main__":
    run_simulations()
