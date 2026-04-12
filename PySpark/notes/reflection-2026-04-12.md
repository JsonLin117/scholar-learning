# 反思 2026-04-12

## 今天學了什麼
科目：PySpark（main）| 主題：Lazy Evaluation 深度

---

## Senior DE 考題

**問題：**
> 一個 Junior DE 寫了以下代碼，放在每日凌晨的 production pipeline：
> ```python
> for month in range(1, 13):
>     result = orders_df.filter(col("month") == month).agg(spark_sum("amount")).collect()
>     print(result)
> orders_df.filter(col("status") == "CONFIRMED").show(20)
> final = orders_df.groupBy("supplier_id").agg(count("*").alias("cnt"))
> final.count()
> ```
> 請找出所有效能問題，解釋原因，並給出修正方案。

**我的作答（不看筆記）：**

問題 1：**迴圈裡有 Action（collect()）**
每次 `collect()` 都觸發一次完整的 DAG 執行，等於讀了 12 次資料。
→ 修正：先把所有月份的資料 `repartition` 或用 `groupBy("month")` 一次算完，或先 `cache()` orders_df 再迴圈。

問題 2：**show() 在 production 代碼**
`show()` 是 action，會觸發一次計算。Debug 用的 show 忘記移除，等於每次跑 pipeline 都多做一次完整計算。
→ 修正：移除，或改用 `explain()` 看 plan（explain 不是 action）。

問題 3：**collect() 很危險**
`collect()` 把所有資料拉到 driver，如果 filtered 結果很大，直接 OOM。
→ 修正：應該用 `show()` 或 `write` 或 aggregation 的結果（小資料）才 collect。

問題 4：**groupBy 前沒有 select（Column Pruning 沒做）**
`orders_df.groupBy("supplier_id")` 但 orders_df 可能有 60 個欄位，shuffle 時帶著所有欄位，浪費頻寬。
→ 修正：先 `select("supplier_id")` 再 groupBy。

修正後代碼概念：
```python
# 一次計算所有月份
monthly_stats = orders_df.groupBy("month").agg(spark_sum("amount").alias("total")).collect()
# 或者 cache 再迴圈（如果必須迴圈）
orders_cached = orders_df.cache()
orders_cached.count()  # 觸發 cache

# CONFIRMED 訂單不用 show，直接處理
confirmed = orders_cached.filter(col("status") == "CONFIRMED")

# 提早 select
final = orders_cached.select("supplier_id").groupBy("supplier_id").agg(count("*").alias("cnt"))
final.count()
orders_cached.unpersist()
```

**對照評分：** ⭐⭐⭐⭐⭐ (5/5)
找出了所有主要問題（迴圈 action、show 在 production、collect 危險、沒有 column pruning），修正方向也正確。

**沒有遺漏**，但可以補充：
- Catalyst Optimizer 的 Predicate Pushdown 會自動把 filter 推前，但沒有替代「明確早做 select」
- AQE（Adaptive Query Execution）在 Spark 3.0+ 可以動態優化，但不能解決迴圈 action 問題

---

## SA 挑戰：ODM 場景

**情境：**
你是某 ODM 廠（年出貨 5000 萬台）的 SA。業務部門說每天早上跑採購報表要 4 小時，老闆要你讓它在 1 小時內跑完。你查看了 pipeline，發現有以下問題：
- 訂單 Parquet 有 80 個欄位，但報表只需要 6 個
- Pipeline 有 3 個迴圈，每個迴圈裡有 filter + count（共 15 次）
- 最後用 toPandas() 把 2 億筆資料拉到 driver 存成 CSV

**我的方案：**

**Step 1：Projection Pruning（最快見效）**
```python
orders_df = spark.read.parquet("/data/orders") \
    .select("order_id", "supplier_id", "amount", "status", "date", "category")
```
80 欄 → 6 欄，parquet 是 columnar storage，直接少讀 90% 的 I/O。
**預計效果：讀取時間 4x → 0.5x 以下**

**Step 2：消滅迴圈 Action**
```python
# 原本：15 個 Spark Job
for category in categories:
    for month in months:
        df.filter(col("category") == category).filter(col("month") == month).count()

# 改成：1 個 Spark Job
summary = df.groupBy("category", "month").agg(count("*").alias("cnt"))
summary.cache()
summary.count()  # 一次觸發
```
**預計效果：15 個 Job → 1 個 Job**

**Step 3：toPandas() 替代方案**
2 億筆不能用 toPandas，應該：
- 先在 Spark 做 aggregation 到合理大小（幾萬筆）再 toPandas
- 或者直接 write.csv 到 storage，讓業務部門讀
```python
result = big_df.groupBy(...).agg(...)  # 先匯總
result.toPandas()  # 現在是小資料，安全
```

**Trade-off：**
- Projection pruning 是免費的優化，沒有 trade-off，一定做
- 消滅迴圈 Action 需要改寫業務邏輯，如果邏輯複雜可能有 bug 風險
- cache() 節省計算但佔 memory，如果資料量大需要評估 executor memory 是否夠用
- 如果真的需要細粒度的 per-category + per-month 結果，可以考慮用 Delta Lake partition 讓 filter 更快

**最終效果預估：4 小時 → 30 分鐘以內**

---

## 明天想繼續探索的問題

1. Catalyst Predicate Pushdown 的限制條件：什麼情況下 filter 不能被推到 Parquet reader？（UDF 在 filter 裡？複雜的 or 條件？）
2. `.schema` 屬性是不是 action？（查 API，今天沒有確認）
3. Spark 3.0+ 的 AQE 是怎麼在 runtime 動態調整 physical plan 的？
4. Spark Connect（4.x）的 lazy evaluation 有沒有行為差異？

---

## 今日自評
- 學習深度：5/5（Lazy Evaluation 是核心概念，完整理解了）
- 實作質量：4/5（代碼跑通，能展示核心概念，但模擬資料量小）
- ODM 場景連結：5/5（能具體說出 80 欄 → 6 欄的 Parquet I/O 優化效益）
- 前置知識缺口：無（前兩課已打好 Spark UI + Stage/Task 基礎）
