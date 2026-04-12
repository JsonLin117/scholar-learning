# Lazy Evaluation 實作練習

**日期：** 2026-04-12
**科目：** PySpark（主修）
**場景：** ODM 供應鏈 - 全球採購訂單 ETL 效能診斷

---

## 🏭 情境描述

你是某 ODM 公司（年出貨量 5000 萬台）的 Data Engineer。
每天凌晨 2:00 要跑一個批次 ETL：
- 讀取當日所有採購訂單（Parquet，含 60 個欄位）
- 篩選狀態為 CONFIRMED 且金額 > 10,000 的訂單
- 關聯供應商主數據，取供應商名稱和等級
- 計算每家供應商的：訂單數量、總金額、平均單價
- 寫回 Delta Lake

同事寫了第一版代碼，但 QA 說「感覺跑很慢」。你的任務是：
1. 找出代碼中違反 Lazy Evaluation 原則的地方
2. 修正並驗證 execution plan 的優化效果
3. 實作正確版本

---

## 💡 解題思路

問題代碼有 3 個反模式：
1. **迴圈裡的 Action**：每次 count() 都重新觸發完整 DAG
2. **多餘的 show()**：Debug 用的 show() 沒有移除，生產環境每次都多跑一次
3. **沒有 Column Pruning 意識**：select 時機太晚，早期 transformation 攜帶太多欄位

解法：
- 把需要重複使用的 DataFrame 先 cache() 再用
- 生產代碼移除 show()，或改用 explain() 只看 plan
- 在第一個 wide transformation（groupBy）之前就做 select，減少 shuffle 的資料量

---

## 🔧 實作重點

1. **explain() 看 Catalyst 優化結果**：確認 `filter` 和 `select` 被推到最前面
2. **cache() + unpersist() 的正確使用**：在多次查詢同一個 filtered DataFrame 時，先 cache 一次
3. **Narrow vs Wide transformation 的順序**：narrow 操作（filter、select）要放在 wide 操作（groupBy、join）之前

---

## 📊 SA 延伸思考

**如果我是 SA，這個方案還能怎麼改進？**

1. **Partition 策略**：如果訂單資料按 `order_date` 分區，filter 可以直接跳過無關 partition，I/O 更少
2. **Broadcast Join**：供應商主數據通常是小表（< 10MB），強制 broadcast 可以避免 shuffle join
3. **AQE（Adaptive Query Execution）**：Spark 3.0+ 的 AQE 可以在運行時動態優化，但需要了解它何時會 override Catalyst 的靜態計劃
4. **Z-ORDER on Delta Lake**：訂單資料如果用 Z-ORDER by supplier_id，之後按供應商 filter 的速度可以再提升

**Trade-off**：
- cache() 會佔 memory，如果資料量大過 executor memory，反而造成 spill to disk，更慢
- 在 Databricks 上，Delta Lake 的 file skipping 有時比 Spark 的 predicate pushdown 更有效，需要測試對比
