# dbt Models / Sources / Refs 實作練習

**日期：** 2026-05-12  
**科目：** dbt  
**場景：** ODM 供應鏈 / SAP P2P analytics layer

## 🏭 情境描述

你是某 ODM 伺服器製造商的 Data Engineer。採購、PMC、財務每天都需要看 SAP P2P（Procure-to-Pay）資料：採購單、採購單項目、供應商、物料。raw SAP 表欄位是德文縮寫（EBELN、LIFNR、MATNR），而且不同環境 schema 不同。今天的練習用一個「迷你 dbt runner」模擬 dbt 的三個核心概念：`source()` 宣告原始資料、model 作為 SQL transformation unit、`ref()` 建立依賴 DAG。

## 💡 解題思路

這份練習不用真正安裝 dbt，而是用 Python + SQLite 實作一個最小可行版本：

1. 用 source registry 宣告 SAP raw tables 與 freshness SLA。
2. 用 model registry 定義 staging / intermediate / mart 三層 SQL。
3. 解析 SQL 裡的 `{{ source('sap', 'ekko') }}` 與 `{{ ref('stg_sap__purchase_orders') }}`。
4. 根據 ref 關係做 topological sort，確保 models 按正確順序執行。
5. 在 `dev` 與 `prod` target 下編譯出不同 schema 前綴，模擬 dbt schema interpolation。

## 🔧 實作重點

- **Sources**：raw table 不硬編碼，透過 `source()` 編譯成 target 對應的實體表名，並檢查 `_etl_loaded_at` freshness。
- **Refs / DAG**：`ref()` 不只是字串替換，它會建立 model dependency graph，runner 用拓撲排序避免下游先跑。
- **Layering**：staging 只 rename/cast；intermediate 做 join/enrichment；mart 做 BI-ready aggregation。

## 📊 SA 延伸思考

如果我是 SA，dbt 在 ODM 的價值不是「幫你跑 SQL」，而是把 transformation layer 變成可治理的產品：lineage 可追、source freshness 可告警、dev/prod 環境可切換、團隊命名規範可落地。trade-off 是 dbt 偏 batch 與 SQL-centric；若要處理秒級 Shop Floor streaming 或複雜 Python ML pipeline，應交給 Flink/Spark Streaming 或 Airflow/Dagster orchestrate。