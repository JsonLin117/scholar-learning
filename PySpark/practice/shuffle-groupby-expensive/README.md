# PySpark Shuffle 實作練習

**日期：** 2026-04-26
**科目：** PySpark（main）
**場景：** ODM 供應鏈 — 採購訂單分析與 Shuffle 優化

---

## 🏭 情境描述

你是緯穎科技的 Data Engineer。採購部門需要一份**供應商月度採購分析報告**：

- 來源：ERP 的 PO 記錄表（每月 ~5000 萬筆，含 PO header + line items）
- 需要：按供應商 × 月份 × 物料分類（CPU/Memory/PCB/Other）做金額彙總
- 問題：現行 Spark job 跑了 2.5 小時，老闆要求縮短到 30 分鐘以內

你的任務：找出 shuffle 瓶頸並優化。

---

## 💡 解題思路

1. **問題根源**：原始程式碼用了 `groupByKey()`（RDD 舊寫法），全量 shuffle，map 端沒有做任何壓縮
2. **第一步優化**：改用 DataFrame API + `groupBy().agg()`，Catalyst 自動做 partial aggregation
3. **第二步優化**：供應商 master（幾百筆）broadcast 給所有 executor，join 時不 shuffle 大表
4. **第三步**：開啟 AQE，讓 Spark 動態合併 shuffle 後的空 partition（200 個預設 partition，實際可能只有 50 個供應商有資料）

---

## 🔧 實作重點

1. **`groupByKey` vs `reduceByKey`**：groupByKey 不做 map-side combine，OOM 風險；reduceByKey 先在本地壓縮，shuffle 資料量大幅下降
2. **`broadcast(small_df)`**：強制 Spark 廣播小表，大表完全不動，Shuffle Write = 0
3. **`spark.sql.adaptive.enabled = true`**：AQE 在 shuffle 後動態合併小 partition，自動處理 200 → 50 的 partition 壓縮

---

## 📊 SA 延伸思考

如果資料量繼續增長到 10 億筆/月：

1. **考慮 Streaming**：不用等到月底才跑 batch，每天用 Spark Structured Streaming 做增量 aggregation（存到 Delta Lake），月底直接讀預先計算的結果
2. **Data Skew 問題**：如果某個供應商（如鴻準/台積電）的訂單量是其他的 100 倍，AQE skew 優化可能不夠 → 需要 salting（手動 key 加後綴）
3. **Delta Lake 分區設計**：PO 表按 year_month 分區，每次只讀當月資料，避免全表 scan + shuffle
