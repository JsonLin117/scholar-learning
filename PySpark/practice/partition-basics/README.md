# Partition 基礎實作練習

**日期：** 2026-04-15
**科目：** PySpark（主修，第 4 課）
**場景：** ODM 供應鏈 — 庫存異動資料的 Partition 設計

---

## 🏭 情境描述

你是緯穎（Wistron）的 Data Engineer。
採購部門要你建立一個每日庫存異動分析 pipeline：
- 資料來源：從 SAP MM 系統匯出的 MATDOC（物料憑證）資料，每天約 500 萬筆
- 客戶包含：Microsoft（60% 訂單量）、Meta（15%）、AWS（10%）、其他小客戶（15%）
- 分析需求：每月庫存消耗報表（按料號、工廠、月份聚合）

任務：
1. 模擬不同 Partition 策略的效能差異
2. 觀察 Data Skew（超級大客戶）對效能的影響
3. 展示 AQE 如何自動處理 Data Skew

---

## 💡 解題思路

核心問題是 **Data Skew**：Microsoft 佔 60% 的資料 → 按 customer_id 分區會有一個 Executor 處理 60% 資料。

解法：
1. 展示 `partitionBy("customer_id")` 的 Data Skew 問題
2. 展示 `partitionBy("year_month")` 的正確做法（低基數，均勻分布）
3. 展示 AQE `skewJoin.enabled` 如何自動分裂傾斜的 partition
4. 用 `getNumPartitions()` 和 `explain()` 驗證結果

---

## 🔧 實作重點

1. **Data Skew 可視化**：用 `df.rdd.glom().map(len).collect()` 看各 partition 的資料量分布
2. **partitionBy 基數控制**：`year_month` 約 12 種值（低基數），不像 customer_id 有數千種
3. **AQE 參數設定**：`spark.sql.adaptive.skewJoin.enabled` + `advisoryPartitionSizeInBytes`

---

## 📊 SA 延伸思考

**如果我是 SA，這個方案還能怎麼改進？**

1. **Delta Lake + Liquid Clustering**：不用手動 `partitionBy`，讓 Delta Lake 自動根據查詢模式優化儲存佈局
2. **加鹽（Salting）處理 Data Skew**：`customer_id` 拼接隨機數（"MSFT_0" ~ "MSFT_9"），打散超級大客戶
3. **Databricks AQE + DBR 18.2**：Databricks Runtime 已整合更激進的 AQE 優化，不需要手動調 shuffle.partitions

**Trade-off：**
- `partitionBy` 物理分區：寫入快（每次只讀相關 partition），但基數選錯是災難
- 不分區 + AQE：靈活，但每次查詢都要掃全表（filter pushdown 無法跳過分區）
- 大型 ODM 廠建議：物理分區用 `year_month`，查詢過濾用 Z-ORDER/Liquid Clustering
