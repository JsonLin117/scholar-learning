# dbt Materialization 實作練習

**日期：** 2026-05-19  
**科目：** dbt  
**場景：** ODM 供應鏈 / SAP 庫存異動 ELT

## 🏭 情境描述

你是 ODM 公司資料平台的 Data Engineer，要把 SAP MATDOC 庫存異動資料整理成可供 PMC、倉庫與採購查詢的分析層。不同模型有不同成本：staging 只做欄位標準化、daily summary 需要預計算、海量異動明細要增量 upsert，BI 查詢希望有自動刷新彙總。

## 💡 解題思路

用一個 dependency-free 的 mini dbt runner 模擬五種 materialization：

- `ephemeral`：只作為 CTE inline，清洗測試/取消資料，不落地
- `view`：staging model，查詢時即時計算
- `table`：每日庫存彙總，全量 drop/create 預計算
- `incremental`：庫存異動 fact，以 `movement_id` 做 merge upsert，含 lookback window 處理 late-arriving data
- `materialized_view`：部門/工廠 BI summary，用 table 模擬 DB-managed refresh

## 🔧 實作重點

1. `is_incremental()` 的核心行為：第一次 full build，後續只處理 `max(_etl_loaded_at) - lookback` 之後的資料。
2. `incremental_predicates` 的效能意義：MERGE 時只掃描近期 target window，而不是整張 fact 表。
3. ephemeral 不建立任何 DB 物件，只在下游 SQL 裡以 CTE 展開，避免 staging table 爆炸。

## 📊 SA 延伸思考

如果是 production Databricks + dbt：

- MATDOC / order events：用 `incremental_strategy='merge'`，搭配 `unique_key` 與 3~7 天 lookback。
- 大型 Delta table：加 `incremental_predicates` + Liquid Clustering，降低 MERGE 掃描範圍。
- BOM 展開、跨表重 JOIN：用 `table` 預計算，避免 BI 查詢反覆重算。
- 輕量 KPI：可用 Databricks Materialized View，但 schema 變更與刷新黑盒化要納入治理。
