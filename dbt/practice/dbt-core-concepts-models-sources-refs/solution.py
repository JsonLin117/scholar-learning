"""
Mini dbt runner for ODM SAP P2P analytics.

Goal:
- Demonstrate dbt core concepts without requiring a warehouse:
  source(), ref(), models, DAG ordering, freshness, and target-specific schemas.
- Use SQLite tables to simulate raw SAP data -> staging -> intermediate -> mart.

Run:
  JAVA_HOME=/opt/homebrew/opt/openjdk@17 .venv/bin/python3 dbt/practice/dbt-core-concepts-models-sources-refs/solution.py
"""

from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Set, Tuple

TODAY = datetime(2026, 5, 12, 7, 30)


@dataclass(frozen=True)
class SourceTable:
    source_name: str
    table_name: str
    physical_name: str
    loaded_at_field: str = "_etl_loaded_at"
    warn_after_hours: int = 6
    error_after_hours: int = 24


@dataclass(frozen=True)
class Model:
    name: str
    layer: str
    materialized: str  # view or table
    sql: str


SOURCES: Dict[Tuple[str, str], SourceTable] = {
    ("sap", "ekko"): SourceTable("sap", "ekko", "raw_sap_ekko"),
    ("sap", "ekpo"): SourceTable("sap", "ekpo", "raw_sap_ekpo"),
    ("sap", "lfa1"): SourceTable("sap", "lfa1", "raw_sap_lfa1"),
    ("sap", "mara"): SourceTable("sap", "mara", "raw_sap_mara"),
}


MODELS: Dict[str, Model] = {
    "stg_sap__purchase_orders": Model(
        name="stg_sap__purchase_orders",
        layer="staging",
        materialized="view",
        sql="""
        SELECT
            ebeln AS po_number,
            bstyp AS doc_category,
            bsart AS doc_type,
            lifnr AS vendor_id,
            ekorg AS purchasing_org,
            ekgrp AS purchasing_group,
            waers AS currency,
            bedat AS po_date,
            _etl_loaded_at AS loaded_at
        FROM {{ source('sap', 'ekko') }}
        """,
    ),
    "stg_sap__po_items": Model(
        name="stg_sap__po_items",
        layer="staging",
        materialized="view",
        sql="""
        SELECT
            ebeln AS po_number,
            ebelp AS item_number,
            matnr AS material_id,
            menge AS quantity,
            netpr AS unit_price,
            werks AS plant,
            _etl_loaded_at AS loaded_at
        FROM {{ source('sap', 'ekpo') }}
        """,
    ),
    "stg_sap__vendors": Model(
        name="stg_sap__vendors",
        layer="staging",
        materialized="view",
        sql="""
        SELECT
            lifnr AS vendor_id,
            name1 AS vendor_name,
            land1 AS vendor_country,
            zterm AS payment_terms,
            _etl_loaded_at AS loaded_at
        FROM {{ source('sap', 'lfa1') }}
        """,
    ),
    "stg_sap__materials": Model(
        name="stg_sap__materials",
        layer="staging",
        materialized="view",
        sql="""
        SELECT
            matnr AS material_id,
            maktx AS material_desc,
            mtart AS material_type,
            matkl AS material_group,
            _etl_loaded_at AS loaded_at
        FROM {{ source('sap', 'mara') }}
        """,
    ),
    "int_po_items_enriched": Model(
        name="int_po_items_enriched",
        layer="intermediate",
        materialized="table",
        sql="""
        SELECT
            h.po_number,
            h.po_date,
            h.vendor_id,
            v.vendor_name,
            v.vendor_country,
            v.payment_terms,
            i.item_number,
            i.material_id,
            m.material_desc,
            m.material_group,
            i.plant,
            i.quantity,
            i.unit_price,
            ROUND(i.quantity * i.unit_price, 2) AS line_amount
        FROM {{ ref('stg_sap__purchase_orders') }} h
        JOIN {{ ref('stg_sap__po_items') }} i
          ON h.po_number = i.po_number
        LEFT JOIN {{ ref('stg_sap__vendors') }} v
          ON h.vendor_id = v.vendor_id
        LEFT JOIN {{ ref('stg_sap__materials') }} m
          ON i.material_id = m.material_id
        """,
    ),
    "mart_procurement_spend_by_vendor": Model(
        name="mart_procurement_spend_by_vendor",
        layer="marts",
        materialized="table",
        sql="""
        SELECT
            vendor_id,
            vendor_name,
            vendor_country,
            SUBSTR(po_date, 1, 7) AS order_month,
            COUNT(DISTINCT po_number) AS total_pos,
            COUNT(*) AS total_lines,
            COUNT(DISTINCT material_id) AS unique_materials,
            ROUND(SUM(line_amount), 2) AS total_spend,
            ROUND(AVG(line_amount), 2) AS avg_line_amount
        FROM {{ ref('int_po_items_enriched') }}
        GROUP BY vendor_id, vendor_name, vendor_country, SUBSTR(po_date, 1, 7)
        ORDER BY total_spend DESC
        """,
    ),
}


REF_RE = re.compile(r"\{\{\s*ref\(['\"]([^'\"]+)['\"]\)\s*\}\}")
SOURCE_RE = re.compile(
    r"\{\{\s*source\(['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\)\s*\}\}"
)


def target_relation(target: str, model_name: str) -> str:
    """SQLite has no schemas, so use target prefix to simulate dbt schema interpolation."""
    return f"analytics_{target}__{model_name}"


def model_dependencies(model: Model) -> Set[str]:
    return set(REF_RE.findall(model.sql))


def topological_order(models: Dict[str, Model]) -> List[str]:
    deps = {name: model_dependencies(model) for name, model in models.items()}
    unknown = sorted({dep for dep_set in deps.values() for dep in dep_set} - set(models))
    if unknown:
        raise ValueError(f"Unknown ref(s): {unknown}")

    order: List[str] = []
    ready = sorted([name for name, dep_set in deps.items() if not dep_set])
    remaining = {name: set(dep_set) for name, dep_set in deps.items()}

    while ready:
        current = ready.pop(0)
        order.append(current)
        remaining.pop(current)
        for name in sorted(remaining):
            remaining[name].discard(current)
        newly_ready = sorted([name for name, dep_set in remaining.items() if not dep_set and name not in ready])
        ready.extend(newly_ready)
        ready = sorted(set(ready))

    if remaining:
        raise ValueError(f"Cyclic ref() dependency detected: {remaining}")
    return order


def compile_sql(sql: str, target: str) -> str:
    def replace_source(match: re.Match[str]) -> str:
        key = (match.group(1), match.group(2))
        if key not in SOURCES:
            raise ValueError(f"Unknown source(): {key}")
        return SOURCES[key].physical_name

    def replace_ref(match: re.Match[str]) -> str:
        model_name = match.group(1)
        if model_name not in MODELS:
            raise ValueError(f"Unknown ref(): {model_name}")
        return target_relation(target, model_name)

    return REF_RE.sub(replace_ref, SOURCE_RE.sub(replace_source, sql)).strip()


def setup_raw_sap(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        DROP TABLE IF EXISTS raw_sap_ekko;
        DROP TABLE IF EXISTS raw_sap_ekpo;
        DROP TABLE IF EXISTS raw_sap_lfa1;
        DROP TABLE IF EXISTS raw_sap_mara;

        CREATE TABLE raw_sap_ekko (
            ebeln TEXT PRIMARY KEY, bstyp TEXT, bsart TEXT, lifnr TEXT,
            ekorg TEXT, ekgrp TEXT, waers TEXT, bedat TEXT, _etl_loaded_at TEXT
        );
        CREATE TABLE raw_sap_ekpo (
            ebeln TEXT, ebelp TEXT, matnr TEXT, menge REAL, netpr REAL,
            werks TEXT, _etl_loaded_at TEXT,
            PRIMARY KEY (ebeln, ebelp)
        );
        CREATE TABLE raw_sap_lfa1 (
            lifnr TEXT PRIMARY KEY, name1 TEXT, land1 TEXT, zterm TEXT, _etl_loaded_at TEXT
        );
        CREATE TABLE raw_sap_mara (
            matnr TEXT PRIMARY KEY, maktx TEXT, mtart TEXT, matkl TEXT, _etl_loaded_at TEXT
        );
        """
    )

    fresh = (TODAY - timedelta(hours=2)).isoformat(timespec="seconds")
    stale = (TODAY - timedelta(hours=10)).isoformat(timespec="seconds")

    conn.executemany(
        "INSERT INTO raw_sap_ekko VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("4500001001", "F", "NB", "V100", "TW01", "P01", "USD", "2026-05-01", fresh),
            ("4500001002", "F", "NB", "V200", "TW01", "P02", "USD", "2026-05-02", fresh),
            ("4500001003", "F", "NB", "V100", "TW02", "P01", "USD", "2026-05-03", fresh),
        ],
    )
    conn.executemany(
        "INSERT INTO raw_sap_ekpo VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            ("4500001001", "00010", "M-CPU-001", 120, 430.0, "F16", fresh),
            ("4500001001", "00020", "M-SSD-004", 500, 75.0, "F16", fresh),
            ("4500001002", "00010", "M-NIC-010", 240, 88.5, "F18", fresh),
            ("4500001003", "00010", "M-SSD-004", 300, 72.0, "F16", fresh),
            ("4500001003", "00020", "M-RAIL-002", 1000, 12.5, "F16", fresh),
        ],
    )
    conn.executemany(
        "INSERT INTO raw_sap_lfa1 VALUES (?, ?, ?, ?, ?)",
        [
            ("V100", "Alpha Components", "TW", "NET30", fresh),
            ("V200", "Blue Ocean Electronics", "SG", "NET45", fresh),
        ],
    )
    conn.executemany(
        "INSERT INTO raw_sap_mara VALUES (?, ?, ?, ?, ?)",
        [
            ("M-CPU-001", "Server CPU", "HALB", "Compute", stale),
            ("M-SSD-004", "Enterprise SSD", "ROH", "Storage", stale),
            ("M-NIC-010", "25G Network Card", "ROH", "Network", stale),
            ("M-RAIL-002", "Rack Rail Kit", "ROH", "Mechanical", stale),
        ],
    )
    conn.commit()


def check_source_freshness(conn: sqlite3.Connection) -> List[Tuple[str, str, float, str]]:
    results = []
    for source in SOURCES.values():
        loaded_at = conn.execute(
            f"SELECT MAX({source.loaded_at_field}) FROM {source.physical_name}"
        ).fetchone()[0]
        age_hours = (TODAY - datetime.fromisoformat(loaded_at)).total_seconds() / 3600
        status = "ok"
        if age_hours >= source.error_after_hours:
            status = "error"
        elif age_hours >= source.warn_after_hours:
            status = "warn"
        results.append((source.source_name, source.table_name, age_hours, status))
    return results


def build_models(conn: sqlite3.Connection, target: str) -> List[str]:
    order = topological_order(MODELS)
    for model_name in order:
        model = MODELS[model_name]
        relation = target_relation(target, model.name)
        compiled = compile_sql(model.sql, target)
        conn.execute(f"DROP VIEW IF EXISTS {relation}")
        conn.execute(f"DROP TABLE IF EXISTS {relation}")
        ddl = "CREATE VIEW" if model.materialized == "view" else "CREATE TABLE"
        conn.execute(f"{ddl} {relation} AS {compiled}")
    conn.commit()
    return order


def print_table(conn: sqlite3.Connection, sql: str, headers: Iterable[str]) -> None:
    rows = conn.execute(sql).fetchall()
    print(" | ".join(headers))
    print("-" * 88)
    for row in rows:
        print(" | ".join(str(value) for value in row))


def lineage_report(order: List[str]) -> None:
    print("\n=== Lineage / DAG ===")
    for name in order:
        deps = sorted(model_dependencies(MODELS[name]))
        parent = ", ".join(deps) if deps else "source('sap', ... )"
        print(f"{name:36s} <- {parent}")


def run_demo(target: str = "dev") -> None:
    conn = sqlite3.connect(":memory:")
    setup_raw_sap(conn)

    print("=== Source freshness ===")
    for source_name, table_name, age_hours, status in check_source_freshness(conn):
        icon = {"ok": "✅", "warn": "⚠️", "error": "❌"}[status]
        print(f"{icon} source('{source_name}', '{table_name}') age={age_hours:.1f}h status={status}")

    order = build_models(conn, target=target)
    lineage_report(order)

    mart = target_relation(target, "mart_procurement_spend_by_vendor")
    print(f"\n=== Mart output: {mart} ===")
    print_table(
        conn,
        f"""
        SELECT vendor_name, vendor_country, order_month, total_pos, total_lines,
               unique_materials, total_spend, avg_line_amount
        FROM {mart}
        """,
        [
            "vendor_name",
            "country",
            "month",
            "POs",
            "lines",
            "materials",
            "total_spend",
            "avg_line_amount",
        ],
    )

    compiled_example = compile_sql(MODELS["int_po_items_enriched"].sql, target)
    print("\n=== Compiled SQL sample: int_po_items_enriched ===")
    print(compiled_example)

    prod_relation = target_relation("prod", "stg_sap__purchase_orders")
    dev_relation = target_relation("dev", "stg_sap__purchase_orders")
    print("\n=== Target schema interpolation ===")
    print(f"dev ref('stg_sap__purchase_orders')  -> {dev_relation}")
    print(f"prod ref('stg_sap__purchase_orders') -> {prod_relation}")


if __name__ == "__main__":
    run_demo(target="dev")
