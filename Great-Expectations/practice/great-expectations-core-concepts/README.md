# Great Expectations Core Concepts — ODM BOM Data Quality Practice

2026-05-14 | Subject: Great-Expectations | Topic: `great-expectations-core-concepts`

## Scenario

An ODM factory receives daily BOM master-data extracts from ERP. If `vendor_id`, `material_id`, or quantity fields are wrong, downstream MRP and procurement planning silently produce bad shortage signals.

This exercise implements a dependency-free **mini Great Expectations workflow** to make the four GX concepts concrete:

1. **Data Asset**: `erp_bom_snapshot`
2. **Expectation Suite**: a reusable group of data quality rules
3. **Validation Definition**: bind the asset to the suite
4. **Checkpoint**: run validation and generate human-readable Data Docs

No external package is installed; this is a small local simulator focused on the architecture mental model.

## Expectations Implemented

- `expect_table_row_count_to_be_between`
- `expect_column_values_to_not_be_null`
- `expect_column_values_to_be_unique`
- `expect_column_values_to_be_in_set`
- `expect_column_values_to_be_between`

## Why this matters in ODM supply chain

- `vendor_id` NULL → BOM joins to supplier dimension fail → MRP underestimates shortage risk.
- invalid UOM → quantity math becomes nonsense across PCS/KG/M.
- duplicated BOM line keys → procurement demand can be double-counted.
- negative quantity → physically impossible demand signal.

## Run

```bash
cd /Users/json/Projects/scholar-learning
JAVA_HOME=/opt/homebrew/opt/openjdk@17 .venv/bin/python3 Great-Expectations/practice/great-expectations-core-concepts/solution.py
```

The script writes a small HTML Data Docs file to:

```text
Great-Expectations/practice/great-expectations-core-concepts/data_docs.html
```
