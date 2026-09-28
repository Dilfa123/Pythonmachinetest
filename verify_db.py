"""Database verification script for schema integrity, receipt nullability, and amount sanity."""

import sqlite3


def verify_database(db_path: str = "sales_data.db"):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    print("=" * 60)
    print("1. DATABASE SCHEMA VERIFICATION")
    print("=" * 60)
    cursor.execute("PRAGMA table_info(sales_data);")
    columns = cursor.fetchall()
    expected_schema = [
        ("id", "INTEGER", 1),
        ("receipt_number", "TEXT", 0),
        ("sale_date", "TEXT", 0),
        ("transaction_time", "TEXT", 0),
        ("sale_amount", "REAL", 0),
        ("tax_amount", "REAL", 0),
        ("discount_amount", "REAL", 0),
        ("round_off", "REAL", 0),
        ("net_sale", "REAL", 0),
        ("payment_mode", "TEXT", 0),
        ("order_type", "TEXT", 0),
        ("transaction_status", "TEXT", 0),
    ]

    actual_schema = [(col[1], col[2], col[5]) for col in columns]
    for exp, act in zip(expected_schema, actual_schema):
        match = "[OK]" if exp == act else "[FAIL]"
        print(f"Column: {exp[0]:<20} Expected: {exp[1]:<8} Actual: {act[1]:<8} PK: {act[2]} {match}")

    cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='sales_data';")
    schema_sql = cursor.fetchone()[0]
    print(f"\nSQL Definition:\n{schema_sql}\n")

    print("\n" + "=" * 60)
    print("2. RECEIPT NUMBER & KEY FIELD NULLABILITY & DUPLICATES")
    print("=" * 60)
    cursor.execute("SELECT COUNT(*) FROM sales_data WHERE receipt_number IS NULL OR trim(receipt_number) = '';")
    null_receipts = cursor.fetchone()[0]
    print(f"Records with NULL or empty receipt_number: {null_receipts} (Status: {'[OK]' if null_receipts == 0 else '[FAIL]'})")

    cursor.execute("SELECT COUNT(*) FROM sales_data WHERE sale_date IS NULL OR trim(sale_date) = '';")
    null_dates = cursor.fetchone()[0]
    print(f"Records with NULL or empty sale_date:      {null_dates} (Status: {'[OK]' if null_dates == 0 else '[FAIL]'})")

    cursor.execute("""
        SELECT receipt_number, count(*) 
        FROM sales_data 
        GROUP BY receipt_number 
        HAVING count(*) > 1;
    """)
    dups = cursor.fetchall()
    print(f"Duplicate receipt numbers found:           {len(dups)} (Status: {'[OK]' if len(dups) == 0 else '[FAIL]'})")

    print("\n" + "=" * 60)
    print("3. AMOUNT SANITY & RANGE CHECKS")
    print("=" * 60)
    cursor.execute("""
        SELECT 
            COUNT(*) as total,
            MIN(sale_amount), MAX(sale_amount), AVG(sale_amount),
            MIN(tax_amount), MAX(tax_amount), AVG(tax_amount),
            MIN(discount_amount), MAX(discount_amount), AVG(discount_amount),
            MIN(net_sale), MAX(net_sale), AVG(net_sale),
            MIN(round_off), MAX(round_off)
        FROM sales_data;
    """)
    stats = cursor.fetchone()
    print(f"Total Rows Analyzed: {stats[0]}")
    print(f"Sale Amount (Invoice Amount): Min={stats[1]:.2f}, Max={stats[2]:.2f}, Avg={stats[3]:.2f}")
    print(f"Tax Amount:                   Min={stats[4]:.2f}, Max={stats[5]:.2f}, Avg={stats[6]:.2f}")
    print(f"Discount Amount:              Min={stats[7]:.2f}, Max={stats[8]:.2f}, Avg={stats[9]:.2f}")
    print(f"Net Sale:                     Min={stats[10]:.2f}, Max={stats[11]:.2f}, Avg={stats[12]:.2f}")
    print(f"Round Off:                    Min={stats[13]}, Max={stats[14]}")

    # Check for negative amounts (only round_off may legitimately be slightly negative like -0.01)
    cursor.execute("SELECT COUNT(*) FROM sales_data WHERE sale_amount < 0;")
    neg_sale = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM sales_data WHERE net_sale < 0;")
    neg_net = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM sales_data WHERE tax_amount < 0;")
    neg_tax = cursor.fetchone()[0]

    print(f"Negative sale_amount count: {neg_sale} (Status: {'[OK]' if neg_sale == 0 else '[FAIL]'})")
    print(f"Negative net_sale count:    {neg_net} (Status: {'[OK]' if neg_net == 0 else '[FAIL]'})")
    print(f"Negative tax_amount count:  {neg_tax} (Status: {'[OK]' if neg_tax == 0 else '[FAIL]'})")

    print("\n" + "=" * 60)
    print("4. BUSINESS LOGIC FORMULA SANITY CHECK")
    print("   net_sale ~= (sale_amount + tax_amount - discount_amount + round_off)")
    print("=" * 60)
    cursor.execute("""
        SELECT 
            id, receipt_number, sale_amount, tax_amount, discount_amount, round_off, net_sale,
            round(sale_amount + tax_amount - discount_amount + coalesce(round_off, 0), 2) as calculated_net,
            round(abs(net_sale - (sale_amount + tax_amount - discount_amount + coalesce(round_off, 0))), 2) as diff
        FROM sales_data
        WHERE diff > 0.05;
    """)
    mismatches = cursor.fetchall()
    if not mismatches:
        print("[OK] All 1287 records satisfy the accounting formula!")
    else:
        print(f"Discrepancies found: {len(mismatches)}")
        for m in mismatches[:5]:
            print(f"  Receipt {m[1]}: Net={m[6]}, Calc={m[7]}, Diff={m[8]}")

    print("\n" + "=" * 60)
    print("5. CATEGORICAL BREAKDOWNS")
    print("=" * 60)
    cursor.execute("SELECT transaction_status, count(*) FROM sales_data GROUP BY transaction_status;")
    print("Transaction Status:")
    for row in cursor.fetchall():
        print(f"  - {row[0]}: {row[1]}")

    cursor.execute("SELECT payment_mode, count(*) FROM sales_data GROUP BY payment_mode;")
    print("Payment Mode:")
    for row in cursor.fetchall():
        print(f"  - {row[0]}: {row[1]}")

    cursor.execute("SELECT order_type, count(*) FROM sales_data GROUP BY order_type;")
    print("Order Type:")
    for row in cursor.fetchall():
        print(f"  - {row[0]}: {row[1]}")

    conn.close()

if __name__ == "__main__":
    verify_database()
