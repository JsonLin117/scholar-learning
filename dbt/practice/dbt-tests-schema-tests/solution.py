#!/usr/bin/env python3
"""
Mini dbt test runner for ODM supply-chain data quality.

This is dependency-free on purpose: the goal is to make dbt tests' execution
model visible. In real dbt, every data test compiles to SQL that returns the
bad rows. Here every Python test returns the bad records directly.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Callable, Iterable

Row = dict[str, Any]
Predicate = Callable[[Row], bool]


@dataclass(frozen=True)
class TestConfig:
    severity: str = "error"  # error | warn
    error_if: str = ">0"
    warn_if: str = ">0"
    where: Predicate | None = None
    store_failures: bool = False


@dataclass
class TestResult:
    name: str
    model: str
    status: str
    failure_count: int
    severity: str
    failures: list[Row] = field(default_factory=list)
    note: str = ""


def threshold_matches(count: int, expression: str) -> bool:
    op = expression[:2] if expression[:2] in {">=", "<=", "==", "!="} else expression[:1]
    rhs = expression[len(op) :].strip()
    if not rhs.isdigit():
        raise ValueError(f"Unsupported threshold expression: {expression!r}")
    value = int(rhs)
    return {
        ">": count > value,
        ">=": count >= value,
        "<": count < value,
        "<=": count <= value,
        "==": count == value,
        "!=": count != value,
    }[op]


def evaluate(name: str, model: str, bad_rows: list[Row], config: TestConfig, note: str = "") -> TestResult:
    count = len(bad_rows)
    status = "pass"

    # Mirrors dbt's severity behavior: severity=warn skips error_if entirely.
    if config.severity == "error" and threshold_matches(count, config.error_if):
        status = "error"
    elif threshold_matches(count, config.warn_if):
        status = "warn"

    return TestResult(
        name=name,
        model=model,
        status=status,
        failure_count=count,
        severity=config.severity,
        failures=bad_rows if config.store_failures else [],
        note=note,
    )


def scoped(rows: Iterable[Row], config: TestConfig) -> list[Row]:
    if config.where is None:
        return list(rows)
    return [row for row in rows if config.where(row)]


def test_not_null(model: str, rows: list[Row], column: str, config: TestConfig) -> TestResult:
    bad = [row for row in scoped(rows, config) if row.get(column) is None]
    return evaluate(f"not_null_{column}", model, bad, config)


def test_unique(model: str, rows: list[Row], column: str, config: TestConfig) -> TestResult:
    seen: dict[Any, list[Row]] = {}
    for row in scoped(rows, config):
        seen.setdefault(row.get(column), []).append(row)
    bad = [row for group in seen.values() if len(group) > 1 for row in group]
    return evaluate(f"unique_{column}", model, bad, config, "duplicate keys returned")


def test_accepted_values(model: str, rows: list[Row], column: str, values: set[Any], config: TestConfig) -> TestResult:
    # Same semantics as dbt: accepted_values ignores NULL. Add not_null if NULL is invalid.
    bad = [row for row in scoped(rows, config) if row.get(column) is not None and row.get(column) not in values]
    return evaluate(f"accepted_values_{column}", model, bad, config)


def test_relationships(
    model: str,
    rows: list[Row],
    column: str,
    parent_model: str,
    parent_rows: list[Row],
    parent_column: str,
    config: TestConfig,
) -> TestResult:
    # Same semantics as dbt foreign-key style tests: NULL child keys are skipped.
    parent_values = {row[parent_column] for row in parent_rows if row.get(parent_column) is not None}
    bad = [row for row in scoped(rows, config) if row.get(column) is not None and row.get(column) not in parent_values]
    return evaluate(f"relationships_{column}_to_{parent_model}_{parent_column}", model, bad, config)


def singular_high_value_po_requires_active_vendor(
    po_rows: list[Row], vendor_rows: list[Row], threshold: int, config: TestConfig
) -> TestResult:
    status_by_vendor = {row["vendor_id"]: row["avl_status"] for row in vendor_rows}
    bad = [
        row
        for row in scoped(po_rows, config)
        if row["net_amount"] >= threshold and status_by_vendor.get(row.get("vendor_id")) != "active"
    ]
    return evaluate(
        "assert_high_value_po_uses_active_vendor",
        "fct_purchase_orders",
        bad,
        config,
        f"PO >= {threshold:,} must use active AVL vendors",
    )


def calculate_vendor_score(deliveries: list[Row]) -> list[Row]:
    grouped: dict[str, list[Row]] = {}
    for row in deliveries:
        grouped.setdefault(row["vendor_id"], []).append(row)

    output = []
    for vendor_id, rows in grouped.items():
        total = len(rows)
        on_time = sum(1 for r in rows if r["actual_date"] <= r["promised_date"])
        good_quality = sum(1 for r in rows if r["defect_ppm"] <= 500)
        output.append(
            {
                "vendor_id": vendor_id,
                "otif_rate": round(on_time / total, 2),
                "quality_pass_rate": round(good_quality / total, 2),
                "score": round((on_time / total) * 70 + (good_quality / total) * 30, 1),
            }
        )
    return sorted(output, key=lambda r: r["vendor_id"])


def unit_test_vendor_scorecard() -> TestResult:
    given = [
        {"vendor_id": "V001", "promised_date": date(2026, 5, 1), "actual_date": date(2026, 5, 1), "defect_ppm": 100},
        {"vendor_id": "V001", "promised_date": date(2026, 5, 2), "actual_date": date(2026, 5, 5), "defect_ppm": 1200},
    ]
    expected = [{"vendor_id": "V001", "otif_rate": 0.5, "quality_pass_rate": 0.5, "score": 50.0}]
    actual = calculate_vendor_score(given)
    bad = [] if actual == expected else [{"expected": expected, "actual": actual}]
    return evaluate("unit_vendor_score_calculation", "int_vendor_scorecard", bad, TestConfig(severity="error"))


def print_results(results: list[TestResult], audit_tables: dict[str, list[Row]]) -> None:
    print("Mini dbt test runner — ODM P2P + AVL quality gate")
    print("=" * 66)
    for result in results:
        icon = {"pass": "✅", "warn": "⚠️ ", "error": "❌"}[result.status]
        print(f"{icon} {result.status.upper():5} {result.model}.{result.name} failures={result.failure_count}")
        if result.note:
            print(f"      note: {result.note}")
        if result.failures:
            audit_name = f"_dbt_test__audit.{result.model}__{result.name}"
            audit_tables[audit_name] = result.failures
            preview = result.failures[:2]
            print(f"      store_failures: {audit_name} preview={preview}")
    print("-" * 66)
    errors = sum(1 for r in results if r.status == "error")
    warnings = sum(1 for r in results if r.status == "warn")
    print(f"Summary: {len(results)} tests | errors={errors} warnings={warnings} audit_tables={len(audit_tables)}")
    if errors:
        print("Pipeline decision: BLOCK Gold build until error tests are fixed.")
    elif warnings:
        print("Pipeline decision: CONTINUE, but publish warnings to data quality dashboard.")
    else:
        print("Pipeline decision: CONTINUE.")


def main() -> int:
    # Sample staging data intentionally includes a few bad rows to show dbt-style behavior.
    stg_sap_vendor_master = [
        {"vendor_id": "V001", "vendor_name": "Alpha Electronics", "avl_status": "active"},
        {"vendor_id": "V002", "vendor_name": "Beta Metal", "avl_status": "probation"},
        {"vendor_id": "V003", "vendor_name": "Gamma Cable", "avl_status": "suspended"},
        {"vendor_id": "V004", "vendor_name": "Delta Packaging", "avl_status": "blacklisted"},  # invalid
    ]
    stg_sap_po_header = [
        {"po_number": "4500001", "vendor_id": "V001", "po_date": date(2026, 5, 27), "net_amount": 120_000},
        {"po_number": "4500002", "vendor_id": "V002", "po_date": date(2026, 5, 27), "net_amount": 75_000},
        {"po_number": "4500002", "vendor_id": "V002", "po_date": date(2026, 5, 27), "net_amount": 75_000},  # duplicate
        {"po_number": "4500003", "vendor_id": "V999", "po_date": date(2026, 5, 28), "net_amount": 30_000},  # orphan FK
        {"po_number": "4500004", "vendor_id": None, "po_date": date(2026, 5, 28), "net_amount": 5_000},  # relationships skips this
        {"po_number": "4500005", "vendor_id": "V003", "po_date": date(2026, 5, 28), "net_amount": 2_000_000},
    ]

    audit_tables: dict[str, list[Row]] = {}
    results = [
        # Bronze/Silver: warn, store for triage, avoid blocking exploration.
        test_not_null("stg_sap_po_header", stg_sap_po_header, "vendor_id", TestConfig(severity="warn", store_failures=True)),
        test_unique(
            "stg_sap_po_header",
            stg_sap_po_header,
            "po_number",
            TestConfig(severity="error", warn_if=">0", error_if=">1", store_failures=True),
        ),
        test_relationships(
            "stg_sap_po_header",
            stg_sap_po_header,
            "vendor_id",
            "stg_sap_vendor_master",
            stg_sap_vendor_master,
            "vendor_id",
            TestConfig(severity="error", store_failures=True),
        ),
        test_accepted_values(
            "stg_sap_vendor_master",
            stg_sap_vendor_master,
            "avl_status",
            {"active", "probation", "suspended", "pending"},
            TestConfig(severity="error", store_failures=True),
        ),
        singular_high_value_po_requires_active_vendor(
            stg_sap_po_header,
            stg_sap_vendor_master,
            threshold=1_000_000,
            config=TestConfig(severity="error", store_failures=True),
        ),
        unit_test_vendor_scorecard(),
    ]

    print_results(results, audit_tables)

    # This exercise intentionally contains data quality failures. The script itself
    # exits 0 after proving the runner detects them; assertions below validate the
    # expected gate behavior for repeatable practice runs.
    expected_status = {
        "not_null_vendor_id": "warn",
        "unique_po_number": "error",
        "relationships_vendor_id_to_stg_sap_vendor_master_vendor_id": "error",
        "accepted_values_avl_status": "error",
        "assert_high_value_po_uses_active_vendor": "error",
        "unit_vendor_score_calculation": "pass",
    }
    actual_status = {r.name: r.status for r in results}
    assert actual_status == expected_status, actual_status
    assert len(audit_tables) == 5
    print("\nSelf-check: expected warnings/errors detected exactly. ✅")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
