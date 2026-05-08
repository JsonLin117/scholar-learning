# Z-ORDER 多維度聚簇實作練習

**日期：** 2026-05-09
**科目：** Delta-Lake
**場景：** ODM 供應鏈

## 🏭 情境描述

你是某 ODM 公司（年出貨量 5000 萬台伺服器/儲存設備）的 Data Engineer。公司的庫存異動表（`inventory_movements`）每天有 500 萬筆異動紀錄，涵蓋 15 個廠區、30,000 種料號。供應鏈分析師最常下的查詢是「某廠區某天某料號的庫存消耗量」，但目前查詢需要掃描整個 partition，非常慢。

**任務：** 用 Z-ORDER 優化這張表的查詢效能，並量化 data skipping 的效果。

## 💡 解題思路

1. 用 PySpark 產生模擬的庫存異動資料（含 date、plant_id、part_number 等欄位）
2. 先不做 Z-ORDER，查詢並記錄掃描的檔案數
3. 執行 `OPTIMIZE ... ZORDER BY (plant_id, part_number)`
4. 再次查詢，比較 data skipping 的效果
5. 驗證 Z-ORDER 欄位數量對效果的影響（2 欄 vs 4 欄）

## 🔧 實作重點

1. **data skipping 效果量化**：透過 `_delta_log` JSON 的 `numFilesAdded`/`numFilesRemoved` 和查詢的 `filesRead` 指標
2. **Z-ORDER 欄位數量實驗**：2 欄 vs 4 欄的 data skipping 效率對比
3. **Partition + Z-ORDER 混合策略**：date partition + Z-ORDER by (plant_id, part_number)

## 📊 SA 延伸思考

- 如果分析師的查詢模式改變（從 plant_id+part_number 變成 customer_id+product_sku），Z-ORDER 需要重新跑嗎？→ 這就是 Liquid Clustering 的價值
- 在 Databricks 上可以用 Predictive Optimization 自動排程 OPTIMIZE ZORDER，減少手動維護成本
- 對於 TB 級的表，OPTIMIZE ZORDER 的運算成本本身也是要考量的
