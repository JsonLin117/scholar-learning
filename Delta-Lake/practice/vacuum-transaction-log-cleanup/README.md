# Delta Lake VACUUM 實作練習

**日期：** 2026-05-07
**科目：** Delta-Lake（main track，進度第 2 課）
**場景：** ODM 供應鏈 — 庫存管理 Lakehouse

---

## 🏭 情境描述

你是某 ODM 公司（年出貨量 5000 萬台伺服器）的 Data Engineer。
公司的 Silver Layer 有一張「全球零件庫存表」（`silver.inventory`），
每天來自 100+ 工廠的 WMS 系統進行幾百萬次 UPDATE（庫存進出庫）。

**問題：**
- 3 個月後 Delta table 大小從 50GB 暴漲到 600GB（12 倍！）
- BI 報表查詢時間從 2 秒變成 30 秒
- 工程師發現從未設定 VACUUM 排程
- 同時有一個 Spark Structured Streaming job 在實時消費這張表

你需要：
1. 診斷問題（用 DESCRIBE HISTORY）
2. 設計 VACUUM 策略（考慮 Streaming 的限制）
3. 設定定期排程
4. 驗證清理效果

---

## 💡 解題思路

用 Delta Lake 的 `VACUUM` 指令清理過期的舊版本 Parquet 檔。
關鍵考量：Streaming job 最多可能停機 3 天後重啟，
所以 VACUUM retention 至少要設 7 天（3天 + buffer）。

設計原則：OPTIMIZE（每天）→ VACUUM（每週，retention=14天）

---

## 🔧 實作重點

1. **DRY RUN 先確認**：用 `VACUUM ... DRY RUN` 看會刪哪些檔案，確認不影響 Streaming
2. **保留期 > Streaming 最長停機時間**：防止 Streaming 回補時找不到舊檔案
3. **OPTIMIZE 在 VACUUM 之前**：先整理小檔案，再清理垃圾，一次到位

---

## 📊 SA 延伸思考

如果這張表有 Databricks Predictive Optimization（Auto OPTIMIZE + Auto VACUUM），
應該要關掉手動排程還是保留？
→ Predictive Optimization 會自動判斷時機，但 retention 設定還是要手動確認 ≥ stream 停機時間。
→ 混合策略：Auto OPTIMIZE + 手動 VACUUM 排程（確保 retention 正確）
