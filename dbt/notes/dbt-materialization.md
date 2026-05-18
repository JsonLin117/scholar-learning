# dbt Materialization Notes — 2026-05-19

## 五種 Materialization 選型心法
- **view**: staging 層 rename/cast（預設，輕量起步）
- **table**: 複雜 JOIN、BOM 展開（預計算寫入磁碟）
- **incremental**: 海量事件/交易表（merge strategy 在 Delta Lake 最佳）
- **ephemeral**: 極簡 CTE 清洗（不建議多處引用）
- **materialized_view**: DB 自動增量刷新（不用手寫 is_incremental）

## Incremental 四策略（Databricks 推薦 merge）
- merge: MERGE INTO upsert, ACID, 最推薦
- append: 只 INSERT, 事件日誌
- insert_overwrite: 覆寫分區, 按天重算
- delete+insert: 先刪再插

## 關鍵配置
- `is_incremental()`: 表存在 + 非 full_refresh + materialized=incremental
- `on_schema_change`: ignore/fail/append_new_columns/sync_all_columns
- `incremental_predicates`: 限制 MERGE 掃描範圍
- `--full-refresh`: SQL 大改/PK 變/歷史 bug 時用
