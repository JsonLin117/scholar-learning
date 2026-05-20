# cache() vs persist() 實作練習

**日期：** 2026-05-21  
**科目：** PySpark  
**場景：** ODM 供應鏈 / PySpark 效能練習

## 🏭 情境描述

你是 ODM 伺服器製造商的 Data Engineer。每天 Shop Floor 會產生大量工站事件，這些事件需要先 JOIN BOM 與供應商資料，再被三個下游報表重複使用：OEE 產能看板、缺料/報廢預警、供應商品質分析。如果不快取，昂貴的 enrichment DAG 會被每個下游 action 重跑一次。

## 💡 解題思路

用一個帶 accumulator 的「昂貴欄位計算」模擬重計算成本：

1. **No cache**：三個下游 action 各自觸發 lineage，昂貴計算會重跑三次。
2. **cache()**：先 `.cache()` 並用 `.count()` materialize，三個下游直接讀 cached partitions。
3. **persist(DISK_ONLY)**：示範資料太大或記憶體緊張時，可以改用磁碟型 storage level。

## 🔧 實作重點

- `cache()` 是 lazy：呼叫後不會立刻執行，必須 action 才 materialize。
- 同一個 enriched DataFrame 被多個分支使用時，cache 才有價值。
- 用完要 `unpersist()`，避免佔住 executor memory。

## 📊 SA 延伸思考

作為 SA，不應該把 cache 當成萬用加速器。推薦規則：

- Branching DAG + 昂貴 JOIN/UDF + 多次使用 → 可以 cache。
- DataFrame 只用一次、資料量接近 executor memory、或 Databricks 已有 Delta Cache → 不要習慣性 cache。
- 生產環境要用 Spark UI Storage Tab 驗證 cached partitions、memory/disk size，並在 pipeline 結束釋放。
