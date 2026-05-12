"""
Broadcast Join 實作練習 — ODM 採購分析
2026-05-13 | PySpark Lesson 7

場景：500 萬筆採購明細 JOIN 200 個供應商，比較 Broadcast vs Sort-Merge Join
"""

import time
import random
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col, sum as spark_sum, count, avg, round as spark_round
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, DateType

# ============================================================
# 1. 初始化 Spark（刻意壓低 broadcast threshold 做對比實驗）
# ============================================================
spark = SparkSession.builder \
    .appName("BroadcastJoinPractice") \
    .master("local[*]") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("🏭 ODM 採購分析：Broadcast Join vs Sort-Merge Join")
print("=" * 60)

# ============================================================
# 2. 生成模擬資料
# ============================================================
print("\n📊 生成模擬資料...")

# Dimension: 200 個供應商
countries = ["Taiwan", "China", "Japan", "Korea", "USA", "Germany", "Vietnam", "Thailand", "Malaysia", "Mexico"]
categories = ["IC", "PCB", "Connector", "Capacitor", "Resistor", "Memory", "SSD", "Fan", "Cable", "Chassis"]

suppliers = []
for i in range(1, 201):
    suppliers.append((
        f"SUP-{i:04d}",
        f"Supplier_{i}",
        random.choice(countries),
        random.choice(categories),
        random.choice(["A", "B", "C"]),  # tier
    ))

dim_supplier = spark.createDataFrame(suppliers, ["supplier_id", "supplier_name", "country", "category", "tier"])
dim_supplier.cache()

supplier_ids = [f"SUP-{i:04d}" for i in range(1, 201)]

# Fact: 500 萬筆採購明細
print("  生成 500 萬筆採購明細（這可能需要幾秒）...")
NUM_ROWS = 5_000_000

# 用 Spark 直接生成（避免 Python 迴圈太慢）
from pyspark.sql.functions import (
    expr, lit, rand, floor, concat, lpad, date_add, to_date
)

fact_po = spark.range(0, NUM_ROWS) \
    .withColumn("po_id", concat(lit("PO-"), lpad(col("id").cast("string"), 8, "0"))) \
    .withColumn("supplier_idx", (floor(rand() * 200) + 1).cast("int")) \
    .withColumn("supplier_id", concat(lit("SUP-"), lpad(col("supplier_idx").cast("string"), 4, "0"))) \
    .withColumn("amount", (rand() * 10000 + 100).cast("float")) \
    .withColumn("quantity", (floor(rand() * 1000) + 1).cast("int")) \
    .withColumn("order_date", date_add(to_date(lit("2025-01-01")), (floor(rand() * 365)).cast("int"))) \
    .drop("id", "supplier_idx")

fact_po.cache()
fact_count = fact_po.count()  # 觸發 cache
dim_count = dim_supplier.count()

print(f"  ✅ fact_purchase_order: {fact_count:,} rows")
print(f"  ✅ dim_supplier: {dim_count:,} rows")

# ============================================================
# 3. 實驗 A：Broadcast Join（強制）
# ============================================================
print("\n" + "=" * 60)
print("🔬 實驗 A：Broadcast Join（使用 broadcast() hint）")
print("=" * 60)

# 強制 broadcast
t0 = time.time()
result_broadcast = fact_po.join(broadcast(dim_supplier), "supplier_id") \
    .groupBy("supplier_name", "country", "category", "tier") \
    .agg(
        spark_sum("amount").alias("total_spend"),
        count("*").alias("po_count"),
        spark_round(avg("amount"), 2).alias("avg_amount")
    ) \
    .orderBy(col("total_spend").desc())

# 先看 Physical Plan
print("\n📋 Physical Plan（Broadcast Join）:")
result_broadcast.explain(True)

# 執行並收集結果
rows_broadcast = result_broadcast.collect()
t1 = time.time()
print(f"\n⏱️  Broadcast Join 耗時: {t1 - t0:.3f} 秒")
print(f"   結果行數: {len(rows_broadcast)}")
print(f"\n   🏆 Top 5 供應商:")
for i, row in enumerate(rows_broadcast[:5]):
    print(f"   {i+1}. {row['supplier_name']} ({row['country']}/{row['category']}) "
          f"— ${row['total_spend']:,.0f} ({row['po_count']:,} POs)")

# ============================================================
# 4. 實驗 B：Sort-Merge Join（停用 broadcast）
# ============================================================
print("\n" + "=" * 60)
print("🔬 實驗 B：Sort-Merge Join（停用 auto broadcast）")
print("=" * 60)

# 停用 auto broadcast（threshold 設 -1）
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
spark.conf.set("spark.sql.adaptive.enabled", "false")  # 也停 AQE，避免動態轉換

t0 = time.time()
result_smj = fact_po.join(dim_supplier, "supplier_id") \
    .groupBy("supplier_name", "country", "category", "tier") \
    .agg(
        spark_sum("amount").alias("total_spend"),
        count("*").alias("po_count"),
        spark_round(avg("amount"), 2).alias("avg_amount")
    ) \
    .orderBy(col("total_spend").desc())

print("\n📋 Physical Plan（Sort-Merge Join，注意有 Exchange/Sort）:")
result_smj.explain(True)

rows_smj = result_smj.collect()
t1 = time.time()
print(f"\n⏱️  Sort-Merge Join 耗時: {t1 - t0:.3f} 秒")

# ============================================================
# 5. 實驗 C：AQE 動態轉換觀察
# ============================================================
print("\n" + "=" * 60)
print("🔬 實驗 C：AQE 動態轉換（threshold=-1 但 AQE 開啟）")
print("=" * 60)

# threshold 仍是 -1，但開啟 AQE → 看 AQE 是否自動轉 broadcast
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
spark.conf.set("spark.sql.adaptive.enabled", "true")

t0 = time.time()
result_aqe = fact_po.join(dim_supplier, "supplier_id") \
    .groupBy("supplier_name", "country", "category", "tier") \
    .agg(
        spark_sum("amount").alias("total_spend"),
        count("*").alias("po_count"),
        spark_round(avg("amount"), 2).alias("avg_amount")
    ) \
    .orderBy(col("total_spend").desc())

print("\n📋 Physical Plan（AQE 動態優化）:")
result_aqe.explain(True)

rows_aqe = result_aqe.collect()
t1 = time.time()
print(f"\n⏱️  AQE 動態轉換耗時: {t1 - t0:.3f} 秒")

# ============================================================
# 6. 總結比較
# ============================================================
print("\n" + "=" * 60)
print("📊 三種 Join 策略比較總結")
print("=" * 60)
print("""
| 策略              | Shuffle? | 適用場景            |
|-------------------|----------|---------------------|
| Broadcast Join    | ❌ 無     | 大表 JOIN 小表      |
| Sort-Merge Join   | ✅ 有     | 大表 JOIN 大表      |
| AQE 動態轉換      | 視情況    | 開啟 AQE 自動判斷   |

🎯 關鍵觀察：
  - Broadcast Join 的 Physical Plan 有 BroadcastExchange，無 Exchange/Sort
  - Sort-Merge Join 的 Physical Plan 有 Exchange（Shuffle）+ Sort
  - AQE 可能在 runtime 把 SMJ 轉成 Broadcast（看實際資料大小）

🏭 ODM 供應鏈實戰建議：
  - fact_purchase_order JOIN dim_supplier → 永遠 Broadcast（dim 表極小）
  - fact_production JOIN dim_factory → 永遠 Broadcast（100 個工廠）
  - fact_inventory JOIN dim_part → 注意！如果 BOM 物料 > 10MB，需要調整策略
""")

# 恢復預設設定
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")
spark.conf.set("spark.sql.adaptive.enabled", "true")

spark.stop()
print("✅ 練習完成！")
