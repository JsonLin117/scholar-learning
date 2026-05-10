# Snowflake Schema vs Star Schema 實作練習

**日期：** 2026-05-10
**科目：** Data-Warehouse
**場景：** ODM 供應鏈 — 零件採購 Fact Table 搭配 Star vs Snowflake 維度設計

## 🏭 情境描述

緯穎（ODM 伺服器製造商）的 DE 團隊要建立一個「零件採購分析」數倉。
需要設計 Fact Table + Dimension Tables，支援業務查詢：
- 按供應商/地區統計採購金額
- 按零件類別分析缺料趨勢
- 按時間維度看採購趨勢

分別用 Star Schema 和 Snowflake Schema 實作，對比查詢效能和 SQL 複雜度。

## 💡 解題思路

1. 用 PySpark 建立模擬資料（50 萬筆採購記錄）
2. 分別建立 Star Schema 和 Snowflake Schema 的表結構
3. 對同一個業務查詢，比較 JOIN 數量和邏輯計劃
4. 用 Spark `explain()` 對比執行計劃

## 🔧 實作重點

1. **Star Schema**：dim_part 攤平所有屬性，1 JOIN 完成查詢
2. **Snowflake Schema**：dim_part → dim_supplier → dim_region，3 JOINs
3. **Spark explain()** 比較兩者的邏輯計劃（JOIN 數量、Shuffle 次數）

## 📊 SA 延伸思考

在 Delta Lake 上，Star Schema 搭配 Z-ORDER BY (supplier_name, category) 可以進一步加速。
Snowflake Schema 因為需要多張表 JOIN，Z-ORDER 的效益反而不明顯。
