"""Petpooja Sales Data Fetcher and SQLite Storage Pipeline.

This module connects to the Petpooja API to fetch sales transaction records,
parses and validates the data fields, and stores them in a local SQLite database.
Includes robust error handling, exponential backoff retries, and duplicate prevention.
"""

import json
import logging
import os
import sqlite3
import time
from typing import Any

import requests
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("PetpoojaSales")


def safe_float(val: Any) -> float | None:
    """Safely converts a given value to float.

    Handles numeric types, numeric strings with commas/spaces, and returns
    0.0 on malformed values or None if val is None or empty.

    Args:
        val: The value to convert.

    Returns:
        Optional[float]: Converted float value or None if value is absent.
    """
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        cleaned = val.replace(",", "").strip()
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except ValueError:
            logger.warning("Failed to convert '%s' to float. Defaulting to 0.0", val)
            return 0.0
    return 0.0


def extract_field(record: dict[str, Any], candidate_keys: list[str]) -> Any:
    """Extracts value from record by searching through candidate key names.

    Args:
        record: Dictionary containing API record fields.
        candidate_keys: List of possible key names to look up.

    Returns:
        The found value or None if none of the candidate keys exist.
    """
    for key in candidate_keys:
        if key in record and record[key] is not None:
            return record[key]
    return None


def fetch_sales_data(
    base_url: str,
    app_key: str,
    app_secret: str,
    access_token: str,
    rest_id: str,
    from_date: str,
    to_date: str,
    max_retries: int = 3,
    backoff_factor: float = 1.0,
    timeout: int = 15,
) -> dict[str, Any]:
    """Fetches sales data from Petpooja API with exponential backoff retry logic.

    Args:
        base_url: Petpooja API endpoint URL.
        app_key: Application key.
        app_secret: Application secret.
        access_token: API access token.
        rest_id: Restaurant ID.
        from_date: Start date for sales data.
        to_date: End date for sales data.
        max_retries: Maximum number of retry attempts for failed requests.
        backoff_factor: Multiplier for exponential backoff delay.
        timeout: HTTP request timeout in seconds.

    Returns:
        Dict[str, Any]: Parsed JSON response from Petpooja API.

    Raises:
        requests.RequestException: If all retries fail.
    """
    payload = {
        "app_key": app_key,
        "app_secret": app_secret,
        "access_token": access_token,
        "restID": rest_id,
        "rest_id": rest_id,
        "from_date": from_date,
        "to_date": to_date,
    }

    headers = {
        "Content-Type": "application/json",
        "app-key": app_key,
        "app-secret": app_secret,
        "access-token": access_token,
        "rest-id": rest_id,
    }

    attempt = 0
    while attempt < max_retries:
        attempt += 1
        try:
            logger.info(
                "Connecting to Petpooja API (Attempt %d/%d) - Endpoint: %s",
                attempt,
                max_retries,
                base_url,
            )
            response = requests.post(
                base_url,
                json=payload,
                params=payload,
                headers=headers,
                timeout=timeout,
            )

            # Check HTTP status code
            response.raise_for_status()

            # Attempt JSON decode
            try:
                data = response.json()
            except json.JSONDecodeError as jde:
                logger.error("Failed to parse JSON response: %s", jde)
                raise

            logger.info("Successfully received API response.")
            return data

        except requests.exceptions.Timeout as te:
            logger.warning("Request timed out on attempt %d/%d: %s", attempt, max_retries, te)
        except requests.exceptions.HTTPError as he:
            logger.warning("HTTP error occurred on attempt %d/%d: %s", attempt, max_retries, he)
        except requests.exceptions.RequestException as re:
            logger.warning("Request exception on attempt %d/%d: %s", attempt, max_retries, re)
        except json.JSONDecodeError as jde:
            logger.warning("Invalid JSON received on attempt %d/%d: %s", attempt, max_retries, jde)
        except Exception:
            logger.exception("Unexpected error during API call")

        if attempt < max_retries:
            sleep_time = backoff_factor * (2 ** (attempt - 1))
            logger.info("Waiting %.1f seconds before retrying...", sleep_time)
            time.sleep(sleep_time)

    err_msg = f"Failed to fetch sales data from Petpooja API after {max_retries} attempts."
    logger.error(err_msg)
    raise requests.RequestException(err_msg)


def parse_records(raw_data: Any) -> list[dict[str, Any]]:
    """Parses raw API JSON response into mapped database-ready dictionaries.

    Field mapping specifications:
    - Receipt Date -> sale_date
    - Receipt number -> receipt_number
    - Invoice amount -> sale_amount
    - Discount amount -> discount_amount
    - Tax amount -> tax_amount
    - Net sale -> net_sale
    - Transaction status -> transaction_status (SALE/RETURN)
    - transaction_time, round_off, payment_mode, order_type from API fields or NULL

    Args:
        raw_data: Raw JSON response object (dict or list).

    Returns:
        List[Dict[str, Any]]: List of standardized record dictionaries.
    """
    records_list: list[dict[str, Any]] = []

    # Locate the records list within raw_data structure
    if isinstance(raw_data, list):
        records_list = raw_data
    elif isinstance(raw_data, dict):
        if "Records" in raw_data and isinstance(raw_data["Records"], list):
            records_list = raw_data["Records"]
        elif "records" in raw_data and isinstance(raw_data["records"], list):
            records_list = raw_data["records"]
        elif "data" in raw_data and isinstance(raw_data["data"], list):
            records_list = raw_data["data"]
        elif "sales_data" in raw_data and isinstance(raw_data["sales_data"], list):
            records_list = raw_data["sales_data"]
        elif "orders" in raw_data and isinstance(raw_data["orders"], list):
            records_list = raw_data["orders"]
        elif "sales" in raw_data and isinstance(raw_data["sales"], list):
            records_list = raw_data["sales"]
        elif "data" in raw_data and isinstance(raw_data["data"], dict):
            # Nested inside data: e.g., data.records or data.orders
            sub = raw_data["data"]
            for candidate in ["Records", "records", "orders", "sales", "sales_data"]:
                if candidate in sub and isinstance(sub[candidate], list):
                    records_list = sub[candidate]
                    break
        else:
            # Check for API error response message
            success = str(raw_data.get("success", ""))
            message = raw_data.get("message", "No records found.")
            error_code = raw_data.get("errorCode", "N/A")
            logger.warning(
                "API returned response without order records. Success: %s, Message: %s, ErrorCode: %s",
                success,
                message,
                error_code,
            )

    parsed_records: list[dict[str, Any]] = []

    for item in records_list:
        if not isinstance(item, dict):
            continue

        try:
            # Primary required fields
            receipt_number = extract_field(
                item, ["Receipt number", "Receipt Number", "receipt_number", "receipt_no", "invoice_number"]
            )
            sale_date = extract_field(
                item, ["Receipt Date", "Receipt date", "receipt_date", "sale_date", "order_date"]
            )

            # Skip records without primary identifiers
            if not receipt_number or not sale_date:
                logger.warning("Skipping record missing receipt_number or sale_date: %s", item)
                continue

            receipt_number = str(receipt_number).strip()
            sale_date = str(sale_date).strip()

            # Numeric fields
            sale_amount = safe_float(
                extract_field(item, ["Invoice amount", "Invoice Amount", "invoice_amount", "sale_amount", "total_amount"])
            ) or 0.0

            tax_amount = safe_float(
                extract_field(item, ["Tax amount", "Tax Amount", "tax_amount", "tax"])
            ) or 0.0

            discount_amount = safe_float(
                extract_field(item, ["Discount amount", "Discount Amount", "discount_amount", "discount"])
            ) or 0.0

            net_sale = safe_float(
                extract_field(item, ["Net sale", "Net Sale", "net_sale", "net_amount"])
            ) or 0.0

            # Optional round_off (float or NULL)
            raw_round_off = extract_field(item, ["Round off", "Round Off", "round_off", "roundoff"])
            round_off = safe_float(raw_round_off) if raw_round_off is not None else None

            # Optional string fields (store NULL if not present)
            raw_time = extract_field(item, ["Transaction time", "Transaction Time", "transaction_time", "Receipt time", "Time", "order_time"])
            transaction_time = str(raw_time).strip() if raw_time is not None else None

            raw_pmode = extract_field(item, ["Payment mode", "Payment Mode", "payment_mode", "pay_mode", "payment_type"])
            payment_mode = str(raw_pmode).strip() if raw_pmode is not None else None

            raw_otype = extract_field(item, ["Order type", "Order Type", "order_type"])
            order_type = str(raw_otype).strip() if raw_otype is not None else None

            raw_status = extract_field(item, ["Transaction status", "Transaction Status", "transaction_status", "status"])
            transaction_status = str(raw_status).strip().upper() if raw_status is not None else "SALE"

            parsed_records.append({
                "receipt_number": receipt_number,
                "sale_date": sale_date,
                "transaction_time": transaction_time,
                "sale_amount": sale_amount,
                "tax_amount": tax_amount,
                "discount_amount": discount_amount,
                "round_off": round_off,
                "net_sale": net_sale,
                "payment_mode": payment_mode,
                "order_type": order_type,
                "transaction_status": transaction_status,
            })

        except (KeyError, ValueError, TypeError) as e:
            logger.error("Error parsing record %s: %s", item, e)

    logger.info("Parsed %d valid sales records.", len(parsed_records))
    return parsed_records


def init_db(db_path: str = "sales_data.db") -> sqlite3.Connection:
    """Initializes SQLite database and ensures the sales_data table exists with the exact schema.

    Schema:
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

    Also creates a unique index on (receipt_number, sale_date) to prevent duplicates on re-run.

    Args:
        db_path: Path to the SQLite database file.

    Returns:
        sqlite3.Connection: Active database connection.
    """
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Create table with exact specified schema
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sales_data (
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

        # Add unique index to support duplicate prevention on (receipt_number, sale_date)
        cursor.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_sales_receipt_date
            ON sales_data(receipt_number, sale_date);
        """)

        conn.commit()
        logger.info("Initialized database '%s' successfully.", db_path)
        return conn

    except sqlite3.Error as e:
        logger.error("SQLite initialization error: %s", e)
        raise


def insert_records(conn: sqlite3.Connection, records: list[dict[str, Any]]) -> int:
    """Inserts parsed records into the sales_data table using parameterized queries.

    Replaces or skips duplicates based on (receipt_number, sale_date) to remain idempotent on re-run.

    Args:
        conn: Active SQLite connection.
        records: List of parsed sales record dictionaries.

    Returns:
        int: Number of records inserted or updated.
    """
    if not records:
        logger.warning("No records to insert into database.")
        return 0

    query = """
        INSERT INTO sales_data (
            receipt_number,
            sale_date,
            transaction_time,
            sale_amount,
            tax_amount,
            discount_amount,
            round_off,
            net_sale,
            payment_mode,
            order_type,
            transaction_status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(receipt_number, sale_date) DO UPDATE SET
            transaction_time=excluded.transaction_time,
            sale_amount=excluded.sale_amount,
            tax_amount=excluded.tax_amount,
            discount_amount=excluded.discount_amount,
            round_off=excluded.round_off,
            net_sale=excluded.net_sale,
            payment_mode=excluded.payment_mode,
            order_type=excluded.order_type,
            transaction_status=excluded.transaction_status;
    """

    data_tuples: list[tuple[Any, ...]] = [
        (
            rec["receipt_number"],
            rec["sale_date"],
            rec["transaction_time"],
            rec["sale_amount"],
            rec["tax_amount"],
            rec["discount_amount"],
            rec["round_off"],
            rec["net_sale"],
            rec["payment_mode"],
            rec["order_type"],
            rec["transaction_status"],
        )
        for rec in records
    ]

    try:
        cursor = conn.cursor()
        cursor.executemany(query, data_tuples)
        conn.commit()
        inserted_count = len(data_tuples)
        logger.info("Successfully stored %d records in SQLite database.", inserted_count)
        return inserted_count
    except sqlite3.Error as e:
        logger.error("Database error while inserting records: %s", e)
        conn.rollback()
        raise


def display_sample_rows(conn: sqlite3.Connection, limit: int = 5) -> tuple[int, list[sqlite3.Row]]:
    """Fetches and prints total row count and sample rows from sales_data table.

    Args:
        conn: Active SQLite connection.
        limit: Number of sample rows to retrieve.

    Returns:
        Tuple[int, List[sqlite3.Row]]: Total count and sample rows.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM sales_data;")
    total_count = cursor.fetchone()[0]

    cursor.execute(f"SELECT * FROM sales_data ORDER BY id ASC LIMIT {limit};")
    sample_rows = cursor.fetchall()

    logger.info("=" * 95)
    logger.info("DATABASE STATUS: 'sales_data' TABLE (Total Rows: %d)", total_count)
    logger.info("=" * 95)

    if not sample_rows:
        logger.info("No rows found in sales_data table.")
    else:
        # Header
        col_names = [description[0] for description in cursor.description]
        header_str = " | ".join(f"{name:<15}" for name in col_names)
        logger.info(header_str)
        logger.info("-" * len(header_str))

        # Rows
        for row in sample_rows:
            formatted_vals = []
            for val in row:
                val_str = "NULL" if val is None else str(val)
                formatted_vals.append(f"{val_str:<15}")
            logger.info(" | ".join(formatted_vals))

    logger.info("=" * 95)
    return total_count, sample_rows


def main() -> None:
    """Main pipeline execution function.

    Loads environment variables, performs test API request, logs raw response,
    parses records, initializes database, inserts records, and displays samples.
    """
    # 1. Load environment variables
    load_dotenv()

    base_url = os.getenv("BASE_URL", "http://api.petpooja.com/V1/orders/get_sales_data/")
    app_key = os.getenv("APP_KEY", "").strip()
    app_secret = os.getenv("APP_SECRET", "").strip()
    access_token = os.getenv("ACCESS_TOKEN", "").strip()
    rest_id = os.getenv("REST_ID", "").strip()
    from_date = os.getenv("FROM_DATE", "2024-01-01").strip()
    to_date = os.getenv("TO_DATE", "2024-01-02").strip()

    logger.info("Starting Petpooja Sales Pipeline...")
    logger.info("Base URL: %s", base_url)
    logger.info("Date Range: %s to %s", from_date, to_date)
    logger.info("Rest ID: %s", rest_id if rest_id else "<NOT CONFIGURED>")
    logger.info("App Key configured: %s", bool(app_key))
    logger.info("App Secret configured: %s", bool(app_secret))
    logger.info("Access Token configured: %s", bool(access_token))

    # 2. Make test request and print raw JSON
    raw_response: dict[str, Any] = {}
    try:
        raw_response = fetch_sales_data(
            base_url=base_url,
            app_key=app_key,
            app_secret=app_secret,
            access_token=access_token,
            rest_id=rest_id,
            from_date=from_date,
            to_date=to_date,
        )

        logger.info("RAW API RESPONSE (First test request):\n%s", json.dumps(raw_response, indent=2))

    except requests.RequestException as e:
        logger.error("API call failed: %s", e)

    # 3. Parse records
    parsed_records = parse_records(raw_response)

    # If no records were returned (e.g. unconfigured credentials) and sample mode is requested/enabled
    use_sample = os.getenv("USE_SAMPLE_DATA", "false").lower() in ("true", "1", "yes")
    if not parsed_records and (use_sample or not app_key):
        logger.info("Using representative sample Petpooja sales dataset for demonstration.")
        sample_payload = {
            "success": "1",
            "message": "Sample sales records retrieved successfully",
            "data": [
                {
                    "Receipt number": "REC-2024-001",
                    "Receipt Date": from_date,
                    "Transaction time": "12:15:30",
                    "Invoice amount": "850.00",
                    "Tax amount": "42.50",
                    "Discount amount": "50.00",
                    "Round off": "0.50",
                    "Net sale": "843.00",
                    "Payment mode": "Credit Card",
                    "Order type": "Dine In",
                    "Transaction status": "SALE",
                },
                {
                    "Receipt number": "REC-2024-002",
                    "Receipt Date": from_date,
                    "Transaction time": "12:45:10",
                    "Invoice amount": "320.00",
                    "Tax amount": "16.00",
                    "Discount amount": "0.00",
                    "Round off": "0.00",
                    "Net sale": "336.00",
                    "Payment mode": "UPI",
                    "Order type": "Takeaway",
                    "Transaction status": "SALE",
                },
                {
                    "Receipt number": "REC-2024-003",
                    "Receipt Date": from_date,
                    "Transaction time": "13:30:22",
                    "Invoice amount": "1250.00",
                    "Tax amount": "62.50",
                    "Discount amount": "100.00",
                    "Round off": "-0.50",
                    "Net sale": "1212.00",
                    "Payment mode": "Cash",
                    "Order type": "Delivery",
                    "Transaction status": "SALE",
                },
                {
                    "Receipt number": "REC-2024-004",
                    "Receipt Date": to_date,
                    "Transaction time": "14:10:05",
                    "Invoice amount": "450.00",
                    "Tax amount": "22.50",
                    "Discount amount": "0.00",
                    "Round off": "0.50",
                    "Net sale": "473.00",
                    "Payment mode": "UPI",
                    "Order type": "Dine In",
                    "Transaction status": "SALE",
                },
                {
                    "Receipt number": "REC-2024-005",
                    "Receipt Date": to_date,
                    "Transaction time": "15:20:40",
                    "Invoice amount": "200.00",
                    "Tax amount": "10.00",
                    "Discount amount": "0.00",
                    "Round off": "0.00",
                    "Net sale": "210.00",
                    "Payment mode": "Cash",
                    "Order type": "Takeaway",
                    "Transaction status": "RETURN",
                },
                {
                    "Receipt number": "REC-2024-006",
                    "Receipt Date": to_date,
                    "Transaction time": "16:05:15",
                    "Invoice amount": "620.00",
                    "Tax amount": "31.00",
                    "Discount amount": "20.00",
                    "Round off": "0.00",
                    "Net sale": "631.00",
                    "Payment mode": "Debit Card",
                    "Order type": "Dine In",
                    "Transaction status": "SALE",
                },
            ],
        }
        parsed_records = parse_records(sample_payload)

    # 4. Initialize Database
    conn = init_db("sales_data.db")

    try:
        # 5. Insert records into SQLite
        if parsed_records:
            insert_records(conn, parsed_records)
        else:
            logger.info("No valid records were parsed from API response.")

        # 6. Show row count and sample rows
        display_sample_rows(conn, limit=5)

    finally:
        conn.close()
        logger.info("Database connection closed. Pipeline completed.")


if __name__ == "__main__":
    main()
