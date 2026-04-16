# repartition vs coalesce 實作練習

**日期：** 2026-04-16
**科目：** PySpark（main track，第 5 課）
**場景：** ODM 供應鏈 — 良率日誌分析

## 🏭 情境描述

你是緯穎（Wistron）的 Data Engineer。工廠每天產出 **5 億筆**伺服器零件測試日誌（HDD / RAM / CPU / NIC 等），每筆約 200 bytes，存在 Delta Lake 的 Bronze Layer（2000 個 partition）。

品質部門想要做每日良率分析，需要你：
1. 過濾出**今日失效（FAIL）紀錄**（預計不到 0.1%，約 50 萬筆）
2. 計算各零件類型的**首次通過率（FPY）**
3. 找出**失效次數最多的供應商**（BOM 展開後 join 供應商資料）
4. 將結果寫入 Silver Layer（`/delta/yield_report_silver`），按 `test_date` 分區

**挑戰**：過濾後 50 萬筆仍散在 2000 個空分區裡，直接寫入會產生 2000 個微小 Parquet 檔案。供應商資料只有 3,000 家，是個小表。

## 💡 解題思路

1. **讀 Bronze → Filter FAIL** → 50 萬筆散在 2000 partitions
2. **Broadcast Join** 供應商小表（不用 shuffle join）
3. **Aggregation** 計算 FPY、統計供應商失效數
4. **coalesce(10)** 最後寫入前合併分區（只寫 10 個乾淨檔案）
5. **repartition(col) + partitionBy** 分區表寫入

關鍵：用 `coalesce` 不用 `repartition` 的原因 → aggregation 完後資料已是彙整結果，不存在嚴重傾斜，就近合併就夠了。

## 🔧 實作重點

1. **coalesce 永遠放最後一步**：aggregation → coalesce → write，順序不能亂
2. **Broadcast Join 小表**：供應商 3000 筆，遠低於 `spark.sql.autoBroadcastJoinThreshold`（預設 10MB），Spark 會自動 broadcast，但顯式 `broadcast()` hint 更保險
3. **AQE 自動調整**：開啟後 Spark 會在 shuffle 後自動合併空分區，但最後 write 前仍需手動 coalesce 確保檔案數符合預期

## 📊 SA 延伸思考

- **如果失效率忽然暴增到 10%（500 萬筆）？** → coalesce 數量從 10 調整到 50，或改用 AQE `coalescePartitions` 自動決定
- **如果改用 Delta Lake OPTIMIZE？** → 寫入後跑 `OPTIMIZE /delta/yield_report_silver ZORDER BY (component_type, supplier_id)` 可以取代 coalesce，且能加 Z-ORDER 加速查詢
- **如果供應商資料也很大（50 萬家）？** → 不能 broadcast，改用 `repartition` + shuffle join，或先做 aggregation 縮小資料量再 join
