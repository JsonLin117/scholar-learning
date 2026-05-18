"""
Mini dbt materialization runner for an ODM SAP inventory pipeline.

Demonstrates five dbt materializations without requiring dbt or a warehouse:
- ephemeral: inline CTE only; no persisted object
- view: staging layer, computed at read time
- table: full drop/create pre-computation
- incremental: MERGE-like upsert with lookback window and target-scan predicate
- materialized_view: simulated DB-managed refresh table

Run:
  JAVA_HOME=/opt/homebrew/opt/openjdk@17 .venv/bin/python3 dbt/practice/dbt-materialization/solution.py
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, Iterable, List, Sequence

NOW = datetime(2026, 5, 19, 7, 30)
TODAY = date(2026, 5, 19)


@dataclass(frozen=True)
class Model:
    name: str
    materialized: str
    purpose: str


MODELS: Sequence[Model] = (
    Model("_ephem_valid_movements", "ephemeral", "filter cancelled/test movements + dedupe latest SAP extract"),
    Model("stg_sap__material_documents", "view", "rename SAP MATDOC fields into business-friendly columns"),
    Model("int_inventory_daily", "table", "pre-compute heavy daily inventory aggregation"),
    Model("fct_inventory_movements", "incremental", "MERGE-like fact table for high-volume MATDOC movement events"),
    Model("mv_inventory_by_plant", "materialized_view", "BI-facing plant/day summary refreshed by the database"),
)


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    return conn


def execute_many(conn: sqlite3.Connection, sql: str, rows: Iterable[tuple]) -> None:
    conn.executemany(sql, list(rows))
    conn.commit()


def relation_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE name = ? AND type IN ('table', 'view')",
        (name,),
    ).fetchone()
    return row is not None


def count_rows(conn: sqlite3.Connection, relation: str) -> int:
    return int(conn.execute(f"SELECT COUNT(*) AS n FROM {relation}").fetchone()["n"])


def seed_raw_sap_matdoc(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE raw_sap_matdoc (
            movement_id TEXT,
            material_id TEXT,
            plant_code TEXT,
            storage_location TEXT,
            movement_type TEXT,
            quantity INTEGER,
            movement_date TEXT,
            _etl_loaded_at TEXT,
            source_update_ts TEXT,
            is_cancelled INTEGER,
            source_system TEXT
        )
        """
    )
    rows = [
        ("M001", "CPU-900", "TW01", "RM01", "101", 120, "2026-05-16", "2026-05-16 01:00:00", "2026-05-16 00:50:00", 0, "SAP"),
        ("M002", "CPU-900", "TW01", "LINE", "261", -35, "2026-05-16", "2026-05-16 02:00:00", "2026-05-16 01:55:00", 0, "SAP"),
        ("M003", "SSD-2TB", "TW01", "RM01", "101", 80, "2026-05-17", "2026-05-17 01:00:00", "2026-05-17 00:58:00", 0, "SAP"),
        ("M004", "SSD-2TB", "TW01", "LINE", "261", -20, "2026-05-17", "2026-05-17 03:00:00", "2026-05-17 02:59:00", 0, "SAP"),
        ("M005", "GPU-A1", "VN01", "RM01", "101", 20, "2026-05-18", "2026-05-18 01:00:00", "2026-05-18 00:52:00", 0, "SAP"),
        ("M006", "GPU-A1", "VN01", "LINE", "261", -5, "2026-05-18", "2026-05-18 02:00:00", "2026-05-18 01:50:00", 0, "SAP"),
        # Should be removed by ephemeral model.
        ("TEST-1", "GPU-A1", "VN01", "TEST", "999", 999, "2026-05-18", "2026-05-18 03:00:00", "2026-05-18 02:50:00", 0, "TEST"),
        ("M007", "NIC-100G", "TW01", "RM01", "102", -10, "2026-05-18", "2026-05-18 04:00:00", "2026-05-18 03:55:00", 1, "SAP"),
        # Duplicate extract for M003; latest source_update_ts should win.
        ("M003", "SSD-2TB", "TW01", "RM01", "101", 82, "2026-05-17", "2026-05-18 05:00:00", "2026-05-18 04:59:00", 0, "SAP"),
    ]
    execute_many(
        conn,
        "INSERT INTO raw_sap_matdoc VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )


def ephemeral_valid_movements_sql() -> str:
    """This SQL is inline-only: dbt would never create a physical relation for it."""
    return """
    SELECT
        movement_id,
        material_id,
        plant_code,
        storage_location,
        movement_type,
        quantity,
        movement_date,
        _etl_loaded_at,
        source_update_ts
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY movement_id
                ORDER BY source_update_ts DESC, _etl_loaded_at DESC
            ) AS rn
        FROM raw_sap_matdoc
        WHERE is_cancelled = 0
          AND source_system = 'SAP'
          AND movement_id NOT LIKE 'TEST-%'
    )
    WHERE rn = 1
    """


def build_staging_view(conn: sqlite3.Connection) -> None:
    conn.execute("DROP VIEW IF EXISTS stg_sap__material_documents")
    conn.execute(
        f"""
        CREATE VIEW stg_sap__material_documents AS
        WITH __dbt__cte__ephem_valid_movements AS ({ephemeral_valid_movements_sql()})
        SELECT
            movement_id,
            material_id,
            plant_code,
            storage_location AS sloc,
            CASE movement_type
                WHEN '101' THEN 'goods_receipt'
                WHEN '261' THEN 'issue_to_production'
                WHEN '102' THEN 'receipt_reversal'
                ELSE 'other'
            END AS movement_type_name,
            quantity,
            movement_date,
            _etl_loaded_at,
            source_update_ts
        FROM __dbt__cte__ephem_valid_movements
        """
    )
    conn.commit()


def rebuild_inventory_daily_table(conn: sqlite3.Connection) -> None:
    conn.execute("DROP TABLE IF EXISTS int_inventory_daily")
    conn.execute(
        """
        CREATE TABLE int_inventory_daily AS
        SELECT
            plant_code,
            material_id,
            movement_date,
            SUM(CASE WHEN quantity > 0 THEN quantity ELSE 0 END) AS receipt_qty,
            SUM(CASE WHEN quantity < 0 THEN -quantity ELSE 0 END) AS issue_qty,
            SUM(quantity) AS net_qty,
            MAX(_etl_loaded_at) AS max_loaded_at
        FROM stg_sap__material_documents
        GROUP BY plant_code, material_id, movement_date
        """
    )
    conn.commit()


def create_incremental_target(conn: sqlite3.Connection, rows_sql: str) -> Dict[str, int | str]:
    conn.execute("DROP TABLE IF EXISTS fct_inventory_movements")
    conn.execute(
        f"""
        CREATE TABLE fct_inventory_movements AS
        {rows_sql}
        """
    )
    conn.execute("CREATE UNIQUE INDEX idx_fct_inventory_movements_id ON fct_inventory_movements(movement_id)")
    conn.commit()
    return {"mode": "full_build", "source_rows": count_rows(conn, "fct_inventory_movements"), "target_rows_scanned": 0, "upserts": count_rows(conn, "fct_inventory_movements")}


def incremental_rows_sql(where_clause: str = "1 = 1") -> str:
    return f"""
    SELECT
        movement_id,
        material_id,
        plant_code,
        sloc,
        movement_type_name,
        quantity,
        movement_date,
        _etl_loaded_at,
        source_update_ts,
        CURRENT_TIMESTAMP AS dbt_updated_at
    FROM stg_sap__material_documents
    WHERE {where_clause}
    """


def upsert_incremental_movements(conn: sqlite3.Connection, lookback_days: int = 3, target_predicate_days: int = 7) -> Dict[str, int | str]:
    if not relation_exists(conn, "fct_inventory_movements"):
        return create_incremental_target(conn, incremental_rows_sql())

    max_loaded_at = conn.execute("SELECT MAX(_etl_loaded_at) AS max_loaded FROM fct_inventory_movements").fetchone()["max_loaded"]
    watermark = (datetime.fromisoformat(max_loaded_at) - timedelta(days=lookback_days)).strftime("%Y-%m-%d %H:%M:%S")
    target_scan_cutoff = (TODAY - timedelta(days=target_predicate_days)).isoformat()

    source_rows = conn.execute(incremental_rows_sql("_etl_loaded_at >= ?"), (watermark,)).fetchall()
    target_rows_scanned = int(
        conn.execute(
            "SELECT COUNT(*) AS n FROM fct_inventory_movements WHERE movement_date >= ?",
            (target_scan_cutoff,),
        ).fetchone()["n"]
    )

    upserts = 0
    for row in source_rows:
        conn.execute(
            """
            INSERT INTO fct_inventory_movements (
                movement_id, material_id, plant_code, sloc, movement_type_name, quantity,
                movement_date, _etl_loaded_at, source_update_ts, dbt_updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(movement_id) DO UPDATE SET
                material_id = excluded.material_id,
                plant_code = excluded.plant_code,
                sloc = excluded.sloc,
                movement_type_name = excluded.movement_type_name,
                quantity = excluded.quantity,
                movement_date = excluded.movement_date,
                _etl_loaded_at = excluded._etl_loaded_at,
                source_update_ts = excluded.source_update_ts,
                dbt_updated_at = CURRENT_TIMESTAMP
            WHERE excluded.source_update_ts >= fct_inventory_movements.source_update_ts
            """,
            (
                row["movement_id"],
                row["material_id"],
                row["plant_code"],
                row["sloc"],
                row["movement_type_name"],
                row["quantity"],
                row["movement_date"],
                row["_etl_loaded_at"],
                row["source_update_ts"],
            ),
        )
        upserts += 1
    conn.commit()
    return {"mode": "incremental_merge", "source_rows": len(source_rows), "target_rows_scanned": target_rows_scanned, "upserts": upserts}


def refresh_materialized_view(conn: sqlite3.Connection) -> None:
    conn.execute("DROP TABLE IF EXISTS mv_inventory_by_plant")
    conn.execute(
        """
        CREATE TABLE mv_inventory_by_plant AS
        SELECT
            plant_code,
            movement_date,
            SUM(CASE WHEN movement_type_name = 'goods_receipt' THEN quantity ELSE 0 END) AS goods_receipt_qty,
            SUM(CASE WHEN movement_type_name = 'issue_to_production' THEN -quantity ELSE 0 END) AS issue_to_production_qty,
            SUM(quantity) AS net_qty,
            COUNT(*) AS movement_count,
            CURRENT_TIMESTAMP AS refreshed_at
        FROM fct_inventory_movements
        GROUP BY plant_code, movement_date
        """
    )
    conn.commit()


def append_second_batch(conn: sqlite3.Connection) -> None:
    """Add an update, a late-arriving movement, and a brand-new movement."""
    rows = [
        # Corrects M006 from -5 to -7 with a newer source timestamp.
        ("M006", "GPU-A1", "VN01", "LINE", "261", -7, "2026-05-18", "2026-05-19 01:00:00", "2026-05-19 00:55:00", 0, "SAP"),
        # Late-arriving receipt from two days ago; lookback should catch it.
        ("M008", "SSD-2TB", "TW01", "RM01", "101", 10, "2026-05-17", "2026-05-19 01:30:00", "2026-05-19 01:25:00", 0, "SAP"),
        # New same-day issue.
        ("M009", "CPU-900", "TW01", "LINE", "261", -12, "2026-05-19", "2026-05-19 02:00:00", "2026-05-19 01:58:00", 0, "SAP"),
    ]
    execute_many(conn, "INSERT INTO raw_sap_matdoc VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)


def print_rows(conn: sqlite3.Connection, title: str, sql: str) -> None:
    print(f"\n{title}")
    for row in conn.execute(sql):
        print("  " + " | ".join(f"{k}={row[k]}" for k in row.keys()))


def run_dbt_like_pipeline(conn: sqlite3.Connection) -> Dict[str, Dict[str, int | str]]:
    build_staging_view(conn)
    rebuild_inventory_daily_table(conn)
    metrics = {"incremental": upsert_incremental_movements(conn)}
    refresh_materialized_view(conn)
    return metrics


def main() -> None:
    conn = connect()
    seed_raw_sap_matdoc(conn)

    print("dbt materialization decision map")
    for model in MODELS:
        print(f"- {model.name:<32} {model.materialized:<17} {model.purpose}")

    first = run_dbt_like_pipeline(conn)
    print(f"\nFirst run metrics: {first['incremental']}")

    assert not relation_exists(conn, "_ephem_valid_movements"), "ephemeral model should not be persisted"
    assert relation_exists(conn, "stg_sap__material_documents"), "view should exist"
    assert count_rows(conn, "stg_sap__material_documents") == 6, "dedupe + filters should leave 6 valid movements"
    assert count_rows(conn, "fct_inventory_movements") == 6, "first incremental build should load all valid rows"

    append_second_batch(conn)
    second = run_dbt_like_pipeline(conn)
    print(f"Second run metrics: {second['incremental']}")

    m006 = conn.execute("SELECT quantity FROM fct_inventory_movements WHERE movement_id = 'M006'").fetchone()["quantity"]
    assert m006 == -7, "incremental merge should update corrected SAP movement M006"
    assert count_rows(conn, "fct_inventory_movements") == 8, "late + new movement should be inserted; duplicate update should not add row"

    # If incremental had no lookback, the late 2026-05-17 M008 would be missed.
    m008 = conn.execute("SELECT movement_date, quantity FROM fct_inventory_movements WHERE movement_id = 'M008'").fetchone()
    assert m008["movement_date"] == "2026-05-17" and m008["quantity"] == 10

    print_rows(
        conn,
        "BI materialized view: mv_inventory_by_plant",
        """
        SELECT plant_code, movement_date, goods_receipt_qty, issue_to_production_qty, net_qty, movement_count
        FROM mv_inventory_by_plant
        ORDER BY movement_date, plant_code
        """,
    )

    target_rows = count_rows(conn, "fct_inventory_movements")
    scanned = int(second["incremental"]["target_rows_scanned"])
    print(
        "\nSA takeaway: incremental_predicates limited MERGE target scan "
        f"to {scanned}/{target_rows} rows in this small demo; on Delta tables this is where Liquid Clustering pays off."
    )
    print("\n✅ dbt materialization practice completed: view/table/incremental/ephemeral/materialized_view all validated.")


if __name__ == "__main__":
    main()
