#!/usr/bin/env python3
"""
CDC Pipeline Test Script
MySQL → Debezium → Kafka → Tabular Iceberg Sink → Iceberg (MinIO + Hive)

Tests: INSERT, UPDATE, DELETE operations and verifies in Trino.
"""

import time
import mysql.connector
import trino
import json

# ── Config ──────────────────────────────────────────────────
MYSQL_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "root",
    "database": "mydb",
}

TRINO_CONFIG = {
    "host": "localhost",
    "port": 8080,
    "user": "admin",
    "catalog": "iceberg",
    "schema": "mydb",
}

COMMIT_WAIT_SEC = 15   # Iceberg commit.interval-ms is 10s → wait 15s


# ── Helpers ──────────────────────────────────────────────────
def mysql_exec(cursor, sql, params=None):
    cursor.execute(sql, params or ())
    try:
        return cursor.fetchall()
    except Exception:
        return []


def trino_query(cursor, sql):
    cursor.execute(sql)
    return cursor.fetchall()


def wait_for_commit(label):
    print(f"  ⏳ Waiting {COMMIT_WAIT_SEC}s for Iceberg commit... ({label})")
    time.sleep(COMMIT_WAIT_SEC)


def print_rows(rows, header="Trino result"):
    print(f"\n  📊 {header}:")
    if not rows:
        print("    (empty)")
    for r in rows:
        print(f"    {r}")


# ── Main Test ────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("  CDC Pipeline Test: MySQL → Iceberg via Debezium")
    print("=" * 60)

    # Connect
    mysql_conn = mysql.connector.connect(**MYSQL_CONFIG)
    mysql_conn.autocommit = True
    mc = mysql_conn.cursor()

    trino_conn = trino.dbapi.connect(**TRINO_CONFIG)
    tc = trino_conn.cursor()

    # ── Clean up ──────────────────────────────────────────────
    print("\n🧹 Cleanup: truncate MySQL table")
    mysql_exec(mc, "TRUNCATE TABLE orders")

    # ── Test 1: INSERT ────────────────────────────────────────
    print("\n✅ Test 1: INSERT")
    mysql_exec(mc,
        "INSERT INTO orders (id, customer_name, product, amount, status) "
        "VALUES (1, 'Alice', 'Laptop', 999.99, 'pending')"
    )
    print("  → Inserted id=1 (Alice, Laptop, 999.99)")

    wait_for_commit("INSERT")

    rows = trino_query(tc,
        "SELECT id, customer_name, product, amount, status, __op "
        "FROM iceberg.mydb.orders_cdc WHERE id = 1"
    )
    print_rows(rows, "After INSERT")
    assert len(rows) == 1, f"Expected 1 row, got {len(rows)}"
    assert rows[0][5] == "I", f"Expected __op=I, got {rows[0][5]}"
    print("  ✓ INSERT passed")

    # ── Test 2: UPDATE ────────────────────────────────────────
    print("\n✅ Test 2: UPDATE")
    mysql_exec(mc,
        "UPDATE orders SET amount = 1199.99, status = 'shipped' WHERE id = 1"
    )
    print("  → Updated id=1 → amount=1199.99, status=shipped")

    wait_for_commit("UPDATE")

    rows = trino_query(tc,
        "SELECT id, customer_name, product, amount, status, __op "
        "FROM iceberg.mydb.orders_cdc WHERE id = 1"
    )
    print_rows(rows, "After UPDATE")
    assert len(rows) == 1, f"Expected 1 row after upsert, got {len(rows)}"
    assert float(rows[0][3]) == 1199.99, f"Expected amount=1199.99, got {rows[0][3]}"
    assert rows[0][5] == "U", f"Expected __op=U, got {rows[0][5]}"
    print("  ✓ UPDATE passed")

    # ── Test 3: INSERT a second row ───────────────────────────
    print("\n✅ Test 3: INSERT second row")
    mysql_exec(mc,
        "INSERT INTO orders (id, customer_name, product, amount, status) "
        "VALUES (2, 'Bob', 'Phone', 499.99, 'pending')"
    )
    print("  → Inserted id=2 (Bob, Phone, 499.99)")

    wait_for_commit("INSERT id=2")

    rows = trino_query(tc,
        "SELECT id, customer_name, __op FROM iceberg.mydb.orders_cdc ORDER BY id"
    )
    print_rows(rows, "After second INSERT")
    assert len(rows) == 2, f"Expected 2 rows, got {len(rows)}"
    print("  ✓ Second INSERT passed")

    # ── Test 4: DELETE ────────────────────────────────────────
    print("\n✅ Test 4: DELETE")
    mysql_exec(mc, "DELETE FROM orders WHERE id = 1")
    print("  → Deleted id=1")

    wait_for_commit("DELETE")

    rows = trino_query(tc,
        "SELECT id, customer_name, __op FROM iceberg.mydb.orders_cdc ORDER BY id"
    )
    print_rows(rows, "After DELETE id=1")
    ids = [r[0] for r in rows]
    assert 1 not in ids, f"id=1 should be deleted, but found in {ids}"
    assert 2 in ids, f"id=2 should still exist"
    print("  ✓ DELETE passed")

    # ── Summary ───────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  🎉 All tests passed!")
    print("=" * 60)

    mc.close()
    mysql_conn.close()
    tc.close()
    trino_conn.close()


if __name__ == "__main__":
    main()
