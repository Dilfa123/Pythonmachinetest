# Petpooja Sales Data Ingestion & SQLite Pipeline

A production-grade Python data pipeline that fetches restaurant sales transaction records from the **Petpooja API** and securely stores them in a local **SQLite** database (`sales_data.db`).

The pipeline features dynamic credential management via `.env`, initial API payload inspection, robust schema validation, safe floating-point conversions, exponential backoff retries, comprehensive error handling with Python's `logging` module, and idempotent upserts to prevent duplicate records on re-runs.

---

## Folder Structure

```text
PythonMachinetest/
├── .env                  # Local environment file containing sensitive credentials (git-ignored)
├── .env.example          # Environment variable template with placeholders
├── .gitignore            # Git ignore file excluding .env, venv, and temporary caches
├── README.md             # Comprehensive project documentation
├── requirements.txt      # Project dependencies (requests, python-dotenv)
├── main.py               # Main pipeline script (fetch, parse, init DB, upsert, display)
├── verify_db.py          # Database integrity and amount sanity verification script
├── simulate_failures.py  # Fault-tolerance test suite (retries, timeouts, bad JSON, rollbacks)
├── test_pipeline.py      # Automated unit test suite
└── sales_data.db         # SQLite database storing ingested sales data (auto-generated)
```

---

## Setup Steps

### 1. Prerequisites
- Python **3.10+** (tested on Python 3.12)
- Git (optional, for version control)

### 2. Create and Activate a Virtual Environment

On **Windows (PowerShell)**:
```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
```

On **Windows (Command Prompt)**:
```cmd
python -m venv venv
.\venv\Scripts\activate.bat
```

On **macOS / Linux**:
```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Install Dependencies

Install all required packages from `requirements.txt`:
```bash
pip install -r requirements.txt
```

### 4. Configure Environment Variables

Create your local `.env` file by copying the provided `.env.example` template:

On **Windows (PowerShell)**:
```powershell
Copy-Item .env.example .env
```

On **macOS / Linux / Git Bash**:
```bash
cp .env.example .env
```

Open `.env` in any text editor and fill in your Petpooja credentials and target date range:

```ini
BASE_URL=http://api.petpooja.com/V1/orders/get_sales_data/
APP_KEY=your_app_key_here
APP_SECRET=your_app_secret_here
ACCESS_TOKEN=your_access_token_here
REST_ID=your_restaurant_id_here
FROM_DATE=2025-05-03 00:00:00
TO_DATE=2025-05-30 23:59:19
```

> **Security Note:** `.env` is explicitly registered in `.gitignore` to prevent hardcoded credentials or API tokens from being committed to version control.

---

## How to Run

Execute the main ingestion pipeline:
```bash
python main.py
```

### Execution Flow:
1. **Config Loading**: Reads environment variables using `python-dotenv`.
2. **Raw Response Discovery**: Executes a test API request to Petpooja and prints the raw JSON response to stdout.
3. **Data Parsing & Type Sanitization**: Extracts fields, validates primary identifiers, converts amounts safely to `float`, and maps optional fields.
4. **Database Initialization**: Ensures `sales_data.db` and the `sales_data` table exist with the required schema and unique constraint.
5. **Idempotent Record Upsert**: Inserts new records or updates existing records on conflict of `(receipt_number, sale_date)`.
6. **Result Display**: Outputs total row count and the first 5 records formatted in a readable table.

---

## Database Table Schema (`sales_data`)

The database table is created in `sales_data.db` with the following schema:

```sql
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

CREATE UNIQUE INDEX IF NOT EXISTS idx_sales_receipt_date 
ON sales_data(receipt_number, sale_date);
```

### Column Specifications

| Column Name | SQLite Type | Constraint | Description |
| :--- | :--- | :--- | :--- |
| `id` | `INTEGER` | `PRIMARY KEY AUTOINCREMENT` | Unique internal row identifier |
| `receipt_number` | `TEXT` | `NOT NULL` (enforced in parser) | Order/receipt invoice number |
| `sale_date` | `TEXT` | `NOT NULL` (enforced in parser) | Date of transaction (`YYYY-MM-DD`) |
| `transaction_time`| `TEXT` | `NULLABLE` | Time of transaction (`HH:MM:SS`) |
| `sale_amount` | `REAL` | `NOT NULL` (defaults to `0.0`) | Invoice/gross sale amount |
| `tax_amount` | `REAL` | `NOT NULL` (defaults to `0.0`) | Total applicable tax |
| `discount_amount`| `REAL` | `NOT NULL` (defaults to `0.0`) | Applied discount |
| `round_off` | `REAL` | `NULLABLE` | Round-off value (can be `NULL`) |
| `net_sale` | `REAL` | `NOT NULL` (defaults to `0.0`) | Net payable amount |
| `payment_mode` | `TEXT` | `NULLABLE` | Mode of payment (Cash, Card, UPI, etc.) |
| `order_type` | `TEXT` | `NULLABLE` | Order fulfillment type (Dine-In, Pickup, Delivery) |
| `transaction_status` | `TEXT` | `NOT NULL` | Transaction state (`SALE` or `RETURN`) |

---

## Field Mapping Table

The parser maps Petpooja API JSON keys to standard database columns:

| Petpooja API Field (JSON Key) | Database Column | Type Handling / Fallback |
| :--- | :--- | :--- |
| `"Receipt number"` | `receipt_number` | Cleaned string. Record skipped if missing. |
| `"Receipt Date"` | `sale_date` | Cleaned string. Record skipped if missing. |
| `"Transaction Time"` | `transaction_time` | Cleaned string or `NULL` if absent. |
| `"Invoice amount"` | `sale_amount` | Converted via `safe_float()`. Defaults to `0.0`. |
| `"Tax amount"` | `tax_amount` | Converted via `safe_float()`. Defaults to `0.0`. |
| `"Discount amount"` | `discount_amount` | Converted via `safe_float()`. Defaults to `0.0`. |
| `"Round Off"` | `round_off` | Converted via `safe_float()` or stored as `NULL` if absent. |
| `"Net sale"` | `net_sale` | Converted via `safe_float()`. Defaults to `0.0`. |
| `"Payment Mode"` | `payment_mode` | Cleaned string or `NULL` if absent. |
| `"Order Type"` | `order_type` | Cleaned string or `NULL` if absent. |
| `"Transaction status"` | `transaction_status` | Cleaned uppercase string (e.g., `SALE`, `RETURN`). |

---

## Error Handling & Retry Architecture

### 1. Exponential Backoff Retries
- **Trigger**: Network timeouts (`requests.exceptions.Timeout`), server errors (`5xx HTTPError`), and connection drops (`requests.exceptions.RequestException`).
- **Retry Policy**: Up to **3 attempts**.
- **Backoff Delay**: $delay = backoff\_factor \times 2^{(attempt - 1)}$ (e.g., $1.0\text{s}$, $2.0\text{s}$, $4.0\text{s}$).

### 2. Malformed Body & Invalid JSON Handling
- Non-JSON payloads (such as HTML Cloudflare or gateway error pages) raise `json.JSONDecodeError`, which is captured, logged with the attempt counter, and retried.
- Corrupted numeric strings (e.g., `"N/A"`, `"--"`, `"invalid"`) are safely parsed by `safe_float()` and default to `0.0` without crashing the ingestion process.

### 3. Missing Keys & Partial Records
- Records missing critical identifiers (`Receipt number` or `Receipt Date`) are skipped with a warning log.
- Missing optional attributes (`Round Off`, `Payment Mode`, `Order Type`, `Transaction Time`) default cleanly to SQLite `NULL`.

### 4. Database Error Handling & Rollbacks
- Database operations are wrapped in `try...except sqlite3.Error`.
- In the event of a disk error or query fault, `conn.rollback()` is immediately invoked to ensure data consistency.

### 5. Duplicate Prevention (Idempotency)
- A composite unique index on `(receipt_number, sale_date)` prevents duplicates on successive executions.
- The pipeline utilizes parameterized `INSERT INTO ... ON CONFLICT(receipt_number, sale_date) DO UPDATE SET ...` to keep database rows fresh and updated on re-run.

---

## Testing & Verification Scripts

### Run Automated Unit Tests
Executes 5 unit tests verifying field extraction, `safe_float` conversions, backoff retry mechanics, and DB idempotency:
```bash
python test_pipeline.py
```

### Run Database Integrity Verification
Validates table schema, checks for zero NULL receipt numbers, performs amount boundary checks, and verifies accounting formulas ($Net \approx Sale + Tax - Discount + RoundOff$):
```bash
python verify_db.py
```

### Run Fault-Tolerance & Failure Simulations
Simulates network outages, HTTP 503 errors, HTML gateway responses, corrupted payloads, and DB rollbacks:
```bash
python simulate_failures.py
```

---

## Inspecting the Database Using the `sqlite3` CLI

You can inspect `sales_data.db` directly using the SQLite command-line tool.

### 1. Open the Database
```bash
sqlite3 sales_data.db
```

### 2. Enable Clean Formatting
Set output to formatted columns with visible headers:
```sql
.headers on
.mode column
```

### 3. Inspect Table Structure
```sql
.schema sales_data
```
Or check the columns via pragma:
```sql
PRAGMA table_info(sales_data);
```

### 4. Sample Useful Queries

**Count total records:**
```sql
SELECT count(*) AS total_sales FROM sales_data;
```

**View 5 sample rows:**
```sql
SELECT id, receipt_number, sale_date, transaction_time, sale_amount, tax_amount, net_sale, payment_mode, order_type 
FROM sales_data 
ORDER BY id ASC 
LIMIT 5;
```

**Verify that no receipt numbers are NULL or blank:**
```sql
SELECT count(*) AS null_receipt_count 
FROM sales_data 
WHERE receipt_number IS NULL OR trim(receipt_number) = '';
```

**Sales aggregation by Payment Mode:**
```sql
SELECT payment_mode, count(*) AS transactions, round(sum(net_sale), 2) AS total_revenue
FROM sales_data
GROUP BY payment_mode;
```

**Sales breakdown by Order Type:**
```sql
SELECT order_type, count(*) AS count, round(sum(net_sale), 2) AS total_revenue
FROM sales_data
GROUP BY order_type;
```

**Check for returns vs sales:**
```sql
SELECT transaction_status, count(*) AS count, round(sum(net_sale), 2) AS total_revenue
FROM sales_data
GROUP BY transaction_status;
```

### 5. Exit the SQLite CLI
```sql
.quit
```
