"""
Great Expectations core concepts practice — ODM BOM data quality.

This is a dependency-free mini GX simulator. The real Great Expectations package is
not installed in the practice venv, so the exercise implements the same mental model:
Data Asset -> Expectation Suite -> Validation Definition -> Checkpoint -> Data Docs.

Run:
  JAVA_HOME=/opt/homebrew/opt/openjdk@17 .venv/bin/python3 Great-Expectations/practice/great-expectations-core-concepts/solution.py
"""

from __future__ import annotations

from dataclasses import dataclass, field
from html import escape
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Sequence

Row = Dict[str, Any]


@dataclass(frozen=True)
class DataAsset:
    """A named batch of records that should be validated."""

    name: str
    rows: Sequence[Row]
    source: str = "erp_daily_extract"


@dataclass(frozen=True)
class ExpectationResult:
    expectation: str
    column: str | None
    success: bool
    severity: str
    observed_value: Any
    unexpected_count: int
    unexpected_examples: List[Any] = field(default_factory=list)
    note: str = ""


@dataclass(frozen=True)
class Expectation:
    """A single declarative assertion about data."""

    name: str
    severity: str
    validator: Callable[[DataAsset], ExpectationResult]

    def validate(self, asset: DataAsset) -> ExpectationResult:
        return self.validator(asset)


@dataclass
class ExpectationSuite:
    """A reusable group of expectations."""

    name: str
    expectations: List[Expectation] = field(default_factory=list)

    def add(self, expectation: Expectation) -> None:
        self.expectations.append(expectation)


@dataclass(frozen=True)
class ValidationDefinition:
    """Bind a data asset to an expectation suite."""

    name: str
    asset: DataAsset
    suite: ExpectationSuite


@dataclass(frozen=True)
class CheckpointResult:
    checkpoint_name: str
    validation_name: str
    asset_name: str
    success: bool
    critical_failures: int
    warning_failures: int
    results: List[ExpectationResult]
    docs_path: Path


class Checkpoint:
    """Production entry point: run validations and build Data Docs."""

    def __init__(self, name: str, validations: Sequence[ValidationDefinition], docs_path: Path) -> None:
        self.name = name
        self.validations = list(validations)
        self.docs_path = docs_path

    def run(self) -> List[CheckpointResult]:
        checkpoint_results: List[CheckpointResult] = []
        for validation in self.validations:
            results = [exp.validate(validation.asset) for exp in validation.suite.expectations]
            critical_failures = sum(1 for r in results if not r.success and r.severity == "critical")
            warning_failures = sum(1 for r in results if not r.success and r.severity == "warning")
            checkpoint_results.append(
                CheckpointResult(
                    checkpoint_name=self.name,
                    validation_name=validation.name,
                    asset_name=validation.asset.name,
                    success=critical_failures == 0,
                    critical_failures=critical_failures,
                    warning_failures=warning_failures,
                    results=results,
                    docs_path=self.docs_path,
                )
            )
        write_data_docs(checkpoint_results, self.docs_path)
        return checkpoint_results


# ---------------------------------------------------------------------------
# Expectation builders: similar names to common GX expectations
# ---------------------------------------------------------------------------


def expect_table_row_count_to_be_between(min_value: int, max_value: int, severity: str = "critical") -> Expectation:
    def validate(asset: DataAsset) -> ExpectationResult:
        observed = len(asset.rows)
        success = min_value <= observed <= max_value
        return ExpectationResult(
            expectation="expect_table_row_count_to_be_between",
            column=None,
            success=success,
            severity=severity,
            observed_value=observed,
            unexpected_count=0 if success else 1,
            note=f"expected {min_value:,} <= rows <= {max_value:,}",
        )

    return Expectation("expect_table_row_count_to_be_between", severity, validate)


def expect_column_values_to_not_be_null(column: str, severity: str = "critical") -> Expectation:
    def validate(asset: DataAsset) -> ExpectationResult:
        bad = [row for row in asset.rows if row.get(column) in (None, "")]
        return ExpectationResult(
            expectation="expect_column_values_to_not_be_null",
            column=column,
            success=not bad,
            severity=severity,
            observed_value=f"{len(asset.rows) - len(bad)}/{len(asset.rows)} non-null",
            unexpected_count=len(bad),
            unexpected_examples=[row.get("bom_line_key") for row in bad[:5]],
            note="JOIN key or mandatory master-data field must not be blank",
        )

    return Expectation("expect_column_values_to_not_be_null", severity, validate)


def expect_column_values_to_be_unique(column: str, severity: str = "critical") -> Expectation:
    def validate(asset: DataAsset) -> ExpectationResult:
        seen: set[Any] = set()
        duplicates: List[Any] = []
        for row in asset.rows:
            value = row.get(column)
            if value in seen:
                duplicates.append(value)
            seen.add(value)
        return ExpectationResult(
            expectation="expect_column_values_to_be_unique",
            column=column,
            success=not duplicates,
            severity=severity,
            observed_value=f"{len(seen)} distinct / {len(asset.rows)} rows",
            unexpected_count=len(duplicates),
            unexpected_examples=duplicates[:5],
            note="duplicate BOM line keys can double-count demand",
        )

    return Expectation("expect_column_values_to_be_unique", severity, validate)


def expect_column_values_to_be_in_set(column: str, value_set: Iterable[Any], severity: str = "warning") -> Expectation:
    allowed = set(value_set)

    def validate(asset: DataAsset) -> ExpectationResult:
        bad = [row.get(column) for row in asset.rows if row.get(column) not in allowed]
        return ExpectationResult(
            expectation="expect_column_values_to_be_in_set",
            column=column,
            success=not bad,
            severity=severity,
            observed_value=f"allowed={sorted(allowed)}",
            unexpected_count=len(bad),
            unexpected_examples=bad[:5],
            note="invalid code values break downstream business logic",
        )

    return Expectation("expect_column_values_to_be_in_set", severity, validate)


def expect_column_values_to_be_between(
    column: str,
    min_value: float,
    max_value: float,
    severity: str = "critical",
) -> Expectation:
    def validate(asset: DataAsset) -> ExpectationResult:
        bad = [row for row in asset.rows if not (min_value <= float(row.get(column, 0)) <= max_value)]
        return ExpectationResult(
            expectation="expect_column_values_to_be_between",
            column=column,
            success=not bad,
            severity=severity,
            observed_value=f"expected {min_value:g} <= {column} <= {max_value:g}",
            unexpected_count=len(bad),
            unexpected_examples=[{column: row.get(column), "bom_line_key": row.get("bom_line_key")} for row in bad[:5]],
            note="quantity range prevents impossible MRP demand signals",
        )

    return Expectation("expect_column_values_to_be_between", severity, validate)


# ---------------------------------------------------------------------------
# ODM sample data and reporting
# ---------------------------------------------------------------------------


def build_bom_snapshot() -> List[Row]:
    """Build a small ERP BOM extract with intentional quality defects."""

    rows: List[Row] = []
    plants = ["TW01", "MX01", "CZ01"]
    uoms = ["PCS", "KG", "M"]
    vendors = ["SUP-0001", "SUP-0002", "SUP-0003", "SUP-0004"]

    for i in range(1, 31):
        rows.append(
            {
                "bom_line_key": f"BOM-2026-05-14-{i:03d}",
                "finished_good": f"SERVER-X{i % 4 + 1}",
                "material_id": f"MAT-{1000 + i:05d}",
                "vendor_id": vendors[i % len(vendors)],
                "plant": plants[i % len(plants)],
                "quantity_per_unit": round(1 + (i % 7) * 0.25, 2),
                "uom": uoms[i % len(uoms)],
                "status": "ACTIVE" if i % 9 else "PHASE_OUT",
            }
        )

    # Intentional defects that mirror real ODM data quality failure modes.
    rows[4]["vendor_id"] = None  # critical: supplier dimension JOIN fails
    rows[11]["quantity_per_unit"] = -3  # critical: impossible demand
    rows[17]["uom"] = "EA"  # warning: non-standard UOM code
    rows[22]["bom_line_key"] = rows[21]["bom_line_key"]  # critical: duplicate line key
    rows[27]["status"] = "UNKNOWN"  # warning: invalid lifecycle state
    return rows


def status_icon(success: bool) -> str:
    return "✅" if success else "❌"


def write_data_docs(checkpoint_results: Sequence[CheckpointResult], docs_path: Path) -> None:
    rows_html: List[str] = []
    for checkpoint in checkpoint_results:
        for result in checkpoint.results:
            rows_html.append(
                "<tr>"
                f"<td>{status_icon(result.success)}</td>"
                f"<td>{escape(result.severity)}</td>"
                f"<td>{escape(result.expectation)}</td>"
                f"<td>{escape(result.column or 'table')}</td>"
                f"<td>{escape(str(result.observed_value))}</td>"
                f"<td>{result.unexpected_count}</td>"
                f"<td><code>{escape(str(result.unexpected_examples))}</code></td>"
                f"<td>{escape(result.note)}</td>"
                "</tr>"
            )

    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>Mini GX Data Docs — ODM BOM</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, sans-serif; margin: 32px; line-height: 1.45; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border: 1px solid #ddd; padding: 8px; vertical-align: top; }}
    th {{ background: #f5f5f5; text-align: left; }}
    code {{ white-space: pre-wrap; }}
    .fail {{ color: #b00020; font-weight: 700; }}
    .pass {{ color: #176b2c; font-weight: 700; }}
  </style>
</head>
<body>
  <h1>Mini GX Data Docs — ODM BOM Quality Check</h1>
  <p>Generated by <strong>Checkpoint</strong> after validating <code>erp_bom_snapshot</code>.</p>
  <table>
    <thead>
      <tr>
        <th>Status</th><th>Severity</th><th>Expectation</th><th>Column</th>
        <th>Observed</th><th>Unexpected</th><th>Examples</th><th>Why it matters</th>
      </tr>
    </thead>
    <tbody>
      {''.join(rows_html)}
    </tbody>
  </table>
</body>
</html>
"""
    docs_path.write_text(html, encoding="utf-8")


def print_checkpoint_summary(result: CheckpointResult) -> None:
    print("=" * 78)
    print("🏭 ODM BOM Data Quality — Mini Great Expectations Workflow")
    print("=" * 78)
    print(f"Data Asset          : {result.asset_name}")
    print(f"Validation Definition: {result.validation_name}")
    print(f"Checkpoint          : {result.checkpoint_name}")
    print(f"Overall Success     : {status_icon(result.success)} {result.success}")
    print(f"Critical Failures   : {result.critical_failures}")
    print(f"Warning Failures    : {result.warning_failures}")
    print()

    for item in result.results:
        column = item.column or "table"
        print(f"{status_icon(item.success)} [{item.severity.upper()}] {item.expectation}({column})")
        print(f"   observed   : {item.observed_value}")
        print(f"   unexpected : {item.unexpected_count}")
        if item.unexpected_examples:
            print(f"   examples   : {item.unexpected_examples}")
        print(f"   note       : {item.note}")
        print()

    print("🏗️ SA interpretation:")
    if result.critical_failures:
        print("  - Do NOT promote this ERP BOM batch from Bronze to Silver.")
        print("  - Open a data-quality incident for MDM/ERP owner: vendor_id, duplicate keys, and negative quantity are pipeline-breaking defects.")
    if result.warning_failures:
        print("  - Warnings can be routed to a weekly data steward queue to avoid alert fatigue.")
    print(f"\n📄 Data Docs written: {result.docs_path}")


def main() -> None:
    asset = DataAsset(name="erp_bom_snapshot", rows=build_bom_snapshot())

    suite = ExpectationSuite(name="bom_master_data_quality_suite")
    suite.add(expect_table_row_count_to_be_between(25, 40, severity="critical"))
    suite.add(expect_column_values_to_be_unique("bom_line_key", severity="critical"))
    suite.add(expect_column_values_to_not_be_null("material_id", severity="critical"))
    suite.add(expect_column_values_to_not_be_null("vendor_id", severity="critical"))
    suite.add(expect_column_values_to_be_in_set("uom", ["PCS", "KG", "M"], severity="warning"))
    suite.add(expect_column_values_to_be_in_set("status", ["ACTIVE", "PHASE_OUT", "EOL"], severity="warning"))
    suite.add(expect_column_values_to_be_between("quantity_per_unit", 0.001, 10_000, severity="critical"))

    validation = ValidationDefinition(
        name="validate_daily_erp_bom_before_silver_load",
        asset=asset,
        suite=suite,
    )

    docs_path = Path(__file__).with_name("data_docs.html")
    checkpoint = Checkpoint(
        name="daily_bom_quality_checkpoint",
        validations=[validation],
        docs_path=docs_path,
    )

    [result] = checkpoint.run()
    print_checkpoint_summary(result)

    # The exercise intentionally contains critical failures to demonstrate GX's gatekeeping role.
    # Exit code remains 0 so the practice script can be validated by cron without failing the run.


if __name__ == "__main__":
    main()
