"""
PySpark Lazy Evaluation 實作練習
場景：ODM 供應鏈 - 採購訂單 ETL 效能診斷與修正

日期：2026-04-12
科目：PySpark（主修）
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, avg, broadcast
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, DateType
)
import time

# ============================================================
# 初始化 SparkSession
# ============================================================
spark = SparkSession.builder \
    .appName("ODM-LazyEval-Demo") \
    .master("local[2]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("🏭 ODM 採購訂單 ETL - Lazy Evaluation 練習")
print("=" * 60)

# ============================================================
# 建立測試資料（模擬 ODM 採購訂單）
# ============================================================

# 訂單資料（模擬有 60 個欄位，我們只用到幾個）
orders_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("supplier_id", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("status", StringType(), False),
    StructField("unit_price", DoubleType(), False),
    StructField("quantity", IntegerType(), False),
    # 以下模擬多餘欄位（實際場景可能有 60 個欄位）
    StructField("extra_col_1", StringType(), True),
    StructField("extra_col_2", StringType(), True),
    StructField("extra_col_3", StringType(), True),
])

orders_data = [
    # order_id, supplier_id, amount, status, unit_price, quantity, ...
    ("ORD-001", "SUP-A", 15000.0, "CONFIRMED", 150.0, 100, "x", "x", "x"),
    ("ORD-002", "SUP-B", 8000.0,  "PENDING",   80.0,  100, "x", "x", "x"),  # 金額太低
    ("ORD-003", "SUP-A", 22000.0, "CONFIRMED", 220.0, 100, "x", "x", "x"),
    ("ORD-004", "SUP-C", 50000.0, "CONFIRMED", 500.0, 100, "x", "x", "x"),
    ("ORD-005", "SUP-B", 12000.0, "CONFIRMED", 120.0, 100, "x", "x", "x"),
    ("ORD-006", "SUP-A", 5000.0,  "CONFIRMED",  50.0, 100, "x", "x", "x"),  # 金額太低
    ("ORD-007", "SUP-C", 35000.0, "CANCELLED", 350.0, 100, "x", "x", "x"),  # CANCELLED
    ("ORD-008", "SUP-D", 18000.0, "CONFIRMED", 180.0, 100, "x", "x", "x"),
    ("ORD-009", "SUP-D", 25000.0, "CONFIRMED", 250.0, 100, "x", "x", "x"),
    ("ORD-010", "SUP-B", 11000.0, "CONFIRMED", 110.0, 100, "x", "x", "x"),
]

# 供應商主數據（小表，適合 broadcast）
suppliers_data = [
    ("SUP-A", "Alpha Electronics", "Grade-A"),
    ("SUP-B", "Beta Components", "Grade-B"),
    ("SUP-C", "Gamma Materials", "Grade-A"),
    ("SUP-D", "Delta Tech", "Grade-C"),
]

orders_df = spark.createDataFrame(orders_data, schema=orders_schema)
suppliers_df = spark.createDataFrame(suppliers_data, ["supplier_id", "supplier_name", "grade"])

print(f"\n✅ 測試資料建立完成")
print(f"   訂單資料：{len(orders_data)} 筆，{len(orders_schema.fields)} 個欄位")
print(f"   供應商資料：{len(suppliers_data)} 筆（小表，適合 broadcast）")

# ============================================================
# ❌ 反模式示範：違反 Lazy Evaluation 原則的代碼
# ============================================================

print("\n" + "=" * 60)
print("❌ 反模式：同事寫的「問題代碼」")
print("=" * 60)

def bad_code_example():
    """
    問題 1：迴圈裡有 Action
    問題 2：多餘的 show()（debug 用，忘記拿掉）
    問題 3：沒有提早 select，浪費 shuffle bandwidth
    """

    statuses = ["CONFIRMED", "PENDING", "CANCELLED"]

    # ❌ 問題 1：每次 count() 都觸發一次完整的 DAG（3 個 Job！）
    for status in statuses:
        cnt = orders_df.filter(col("status") == status).count()
        print(f"  {status} 訂單數: {cnt}")
    # 以上會觸發 3 個獨立的 Spark Job，資料讀了 3 次

    # ❌ 問題 2：Debug 用的 show() 沒有移除
    filtered = orders_df.filter(col("status") == "CONFIRMED") \
                         .filter(col("amount") > 10000)
    filtered.show(5)  # ← 這是 Action！觸發一次計算

    # ❌ 問題 3：groupBy 之前沒有做 select（帶著所有欄位做 shuffle）
    result = filtered.groupBy("supplier_id").agg(
        count("order_id").alias("order_count"),
        spark_sum("amount").alias("total_amount"),
        avg("unit_price").alias("avg_price")
    )
    return result

print("\n  開始執行問題代碼...")
t1 = time.time()
bad_result = bad_code_example()
bad_result.count()  # 最後還有一個 Action
t2 = time.time()
print(f"\n  問題代碼執行時間: {t2-t1:.2f}s（共觸發了 5 個 Spark Job）")

# ============================================================
# ✅ 正確示範：遵守 Lazy Evaluation 原則
# ============================================================

print("\n" + "=" * 60)
print("✅ 正確示範：理解 Lazy Evaluation 後的優化代碼")
print("=" * 60)

def good_code_example():
    """
    優化 1：把需要重複使用的 DataFrame cache 起來
    優化 2：移除多餘的 show()，改用 explain() 看 plan
    優化 3：在 groupBy 之前先 select，減少 shuffle 的資料量
    優化 4：使用 broadcast join 避免小表 shuffle
    """

    # Step 1：先做所有 narrow transformation（不觸發計算）
    # 注意：filter 和 select 都是 transformation，不會立刻執行
    filtered = orders_df \
        .filter(col("status") == "CONFIRMED") \
        .filter(col("amount") > 10000) \
        .select("order_id", "supplier_id", "amount", "unit_price")  # ← 提早 select，剩 4 欄
    # ↑ 這裡什麼都還沒執行，只是定義了執行計劃

    # ✅ 優化 1：cache 一次，後面多次使用
    filtered.cache()
    # cache 本身也是 lazy，要等第一個 action 才真正 cache

    # 用一個 action 觸發計算並建立 cache
    total_count = filtered.count()  # ← 這次觸發計算 + 建立 cache
    print(f"\n  符合條件的訂單總數: {total_count}")

    # 各狀態訂單數（現在讀的是 cached data，不是重新計算）
    statuses = ["CONFIRMED", "PENDING", "CANCELLED"]
    for status in statuses:
        # ⚠️ 注意：orders_df 還沒 cache，所以這裡還是讀原始資料
        # 但 filtered（CONFIRMED + amount>10000）是 cached 的
        cnt = orders_df.filter(col("status") == status).count()
        print(f"  {status} 訂單數 (從原始資料): {cnt}")

    # ✅ 優化 2：不用 show()，用 explain() 看 Catalyst 優化後的 plan
    print("\n  📋 Optimized Execution Plan（節錄）：")
    filtered.explain()  # explain() 不是 action，不觸發計算

    # ✅ 優化 3：已經在前面的 select 裡提早做了 column pruning
    # groupBy 只攜帶 4 個欄位，不是原始的 9 個欄位
    supplier_stats = filtered.groupBy("supplier_id").agg(
        count("order_id").alias("order_count"),
        spark_sum("amount").alias("total_amount"),
        avg("unit_price").alias("avg_price")
    )

    # ✅ 優化 4：Broadcast Join（供應商是小表）
    # broadcast() 讓 Spark 把小表廣播到每個 executor，避免 shuffle join
    result = supplier_stats.join(
        broadcast(suppliers_df),  # ← 明確指定 broadcast
        on="supplier_id",
        how="inner"
    ).select(
        "supplier_id", "supplier_name", "grade",
        "order_count", "total_amount", "avg_price"
    )

    # 記得釋放 cache
    filtered.unpersist()

    return result

print("\n  開始執行優化代碼...")
t1 = time.time()
good_result = good_code_example()
# 最後的 show 是刻意的：展示最終結果
good_result.show()
t2 = time.time()
print(f"\n  優化代碼執行時間: {t2-t1:.2f}s")

# ============================================================
# 展示 explain()：看 Catalyst 如何優化
# ============================================================

print("\n" + "=" * 60)
print("🔍 深度：explain() 展示 Catalyst 的優化魔法")
print("=" * 60)

# 建立一個更清晰的例子
demo_df = orders_df \
    .filter(col("status") == "CONFIRMED") \
    .select("order_id", "supplier_id", "amount") \
    .filter(col("amount") > 10000)  # ← 故意把第二個 filter 放後面

print("\n原始代碼邏輯：filter(status) → select → filter(amount)")
print("Catalyst 優化後的實際執行順序：（注意兩個 filter 會被合併或重新排序）")
demo_df.explain()

# ============================================================
# 關鍵學習：什麼時候 DataFrame 操作是 action？
# ============================================================

print("\n" + "=" * 60)
print("📚 總結：Transformation vs Action 速查")
print("=" * 60)

examples = {
    "Transformations（Lazy，不立刻執行）": [
        "filter() / where()",
        "select() / withColumn() / drop()",
        "groupBy() + agg()",
        "join()",
        "orderBy() / sort()",
        "repartition() / coalesce()",
        "cache() / persist()",
        "union() / unionByName()",
    ],
    "Actions（立刻觸發完整 DAG 計算）": [
        "count()",
        "show(n)",
        "collect() ← 危險！大資料會 OOM",
        "take(n) / first() / head(n)",
        "write.parquet() / write.save()",
        "toPandas() ← 危險！大資料會 OOM",
        "foreach() / foreachPartition()",
        "reduce()",
    ]
}

for category, ops in examples.items():
    print(f"\n  {category}:")
    for op in ops:
        print(f"    - {op}")

print("\n" + "=" * 60)
print("✅ 練習完成！核心概念：")
print("  1. Transformation = 記計劃，不執行")
print("  2. Action = 觸發整個 DAG 執行")
print("  3. 迴圈裡避免 Action → 用 cache()")
print("  4. 提早 select → 減少 shuffle 資料量")
print("  5. 小表 join → 用 broadcast()")
print("=" * 60)

spark.stop()
