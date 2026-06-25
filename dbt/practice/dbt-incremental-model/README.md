# dbt Incremental Model 練習

## 場景
ODM 工廠 SAP MATDOC（庫存異動表）的 incremental ETL pipeline。

## 練習目標
1. **Mini dbt Incremental Runner**：模擬 `is_incremental()` 分支邏輯（首跑全量 vs 增量）
2. **5 種 Strategy 對比**：append / merge / delete+insert / insert_overwrite / microbatch
3. **Lookback Window 模擬**：展示 3 天 lookback 如何捕捉 late-arriving data
4. **incremental_predicates 效果**：量化目標表掃描縮減比例
5. **on_schema_change 行為**：模擬 ECO 新增欄位時的 4 種處理模式
6. **Incremental vs Snapshot 比較**：同場景下 SCD1 vs SCD2 的行為差異

## 執行
```bash
cd /Users/json/Projects/scholar-learning
JAVA_HOME=/opt/homebrew/opt/openjdk@17 .venv/bin/python3 dbt/practice/dbt-incremental-model/solution.py
```
