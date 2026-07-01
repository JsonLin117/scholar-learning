# dbt Sources 實作練習

**日期：** 2026-07-02
**科目：** dbt
**場景：** ODM 供應鏈

## 🏭 情境描述

你是某 ODM 公司的 Data Engineer，正在建立 dbt 專案管理 SAP、MES、WMS 三套系統的 raw data。
需要設計 sources.yml 結構、source() 引用、freshness 監控、和 staging model，
並用 `dbt source freshness` 偵測 ERP pipeline 延遲。

## 💡 解題思路

1. 設計三套系統的 source 宣告（SAP MM/PP、MES SFCS、WMS）
2. 實作 source() 編譯器（解析 YAML → 產出 full table path）
3. 實作 freshness checker（loaded_at_field → warn/error 判斷）
4. 展示 source 與 staging model 的 1:1 對應最佳實踐
5. 模擬 ERP 延遲場景（ECO 導致 SAP 資料延遲 18 小時）

## 🔧 實作重點

1. **sources.yml 解析器**：支援 database/schema override、table identifier、freshness 繼承與覆寫
2. **source() 編譯**：`source('sap', 'matdoc')` → `raw.sap_mm.MATDOC_CDC`
3. **Freshness SLA**：warn_after/error_after 層級繼承、loaded_at_field 查詢生成
4. **DAG lineage**：source → staging → intermediate → mart 的完整血緣追蹤

## 📊 SA 延伸思考

Source freshness 是「事後偵測」不是「事前阻擋」——Airflow sensor 負責 gatekeeping，
dbt source freshness 負責 SLA 監控。兩者互補，不是替代。
在 ODM 場景下，MES 資料需要 near-realtime freshness（1h warn），
而 SAP 主檔只需日批次就夠（freshness: null）。
