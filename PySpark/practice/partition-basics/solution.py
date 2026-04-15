"""
PySpark Partition 基礎實作練習
場景：緯穎 ODM 廠庫存異動資料的 Partition 策略設計

學習重點：
1. 觀察不同 Partition 策略的資料分布
2. 體驗 Data Skew（超級大客戶問題）
3. 用 AQE 自動處理 Skew
4. 正確的 partitionBy 基數選擇

執行：
    cd /Users/json/Projects/scholar-learning
    JAVA_HOME=/opt/homebrew/opt/openjdk@17 .venv/bin/python3 \
        PySpark/practice/partition-basics/solution.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DoubleType, DateType
)
import random
from datetime import date, timedelta

# ============================================================
# 建立 SparkSession（本地模式，4 個虛擬 CPU）
# ============================================================
spark = SparkSession.builder \
    .appName("ODM-Inventory-Partition-Demo") \
    .master("local[4]") \
    .config("spark.sql.shuffle.partitions", "10") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64m") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ============================================================
# Step 1：模擬 MATDOC 資料（物料憑證）
# 緯穎廠：5 個主要客戶，Microsoft 佔 60%
# ============================================================

def generate_matdoc_data(n_records=100_000):
    """
    模擬 SAP MATDOC 表的庫存異動記錄
    真實場景：每天約 500 萬筆，這裡用 10 萬筆示意
    """
    # 模擬超級大客戶的資料傾斜
    customers = {
        "MSFT": 0.60,   # Microsoft — 超級大客戶，60%
        "META": 0.15,   # Meta
        "AMZN": 0.10,   # Amazon AWS
        "GOOG": 0.10,   # Google
        "OTHER": 0.05   # 其他小客戶
    }
    
    materials = [f"MAT-{i:05d}" for i in range(200)]   # 200 個料號
    plants = ["WTR-A", "WTR-B", "WTR-C"]               # 3 個緯穎廠區
    movements = ["101", "261", "201", "311"]             # SAP MM 移動類型
    
    random.seed(42)
    rows = []
    base_date = date(2025, 1, 1)
    
    # 根據客戶比例生成資料
    for customer, ratio in customers.items():
        n = int(n_records * ratio)
        for _ in range(n):
            posting_date = base_date + timedelta(days=random.randint(0, 364))
            rows.append({
                "mblnr": f"490{random.randint(10_000_000, 99_999_999)}",  # 憑證號
                "posting_date": posting_date,
                "year_month": posting_date.strftime("%Y-%m"),
                "customer_id": customer,
                "material_id": random.choice(materials),
                "plant": random.choice(plants),
                "movement_type": random.choice(movements),
                "quantity": round(random.uniform(1, 500), 2),
                "amount_usd": round(random.uniform(100, 50000), 2),
            })
    
    return rows

print("=" * 60)
print("ODM 庫存異動資料 Partition 策略示範")
print("=" * 60)

# 建立 DataFrame
data = generate_matdoc_data(100_000)
df = spark.createDataFrame(data)

print(f"\n📦 資料總筆數: {df.count():,}")
print(f"📊 讀取後 partition 數: {df.rdd.getNumPartitions()}")

# ============================================================
# Step 2：展示 Data Skew 問題
# — 如果按 customer_id 做 partitionBy，會是什麼情況？
# ============================================================
print("\n" + "=" * 60)
print("示範 1：Data Skew — 客戶分布嚴重不均")
print("=" * 60)

customer_dist = df.groupBy("customer_id").count() \
    .withColumn("pct", F.round(F.col("count") / df.count() * 100, 1)) \
    .orderBy("count", ascending=False)

print("\n客戶資料分布（模擬緯穎的超級大客戶問題）：")
customer_dist.show()

# 用 glom() 查看各 partition 的資料量分布（不均勻就是 Skew）
partition_sizes = df.rdd.glom().map(len).collect()
print(f"目前各 partition 大小（筆數）: {sorted(partition_sizes, reverse=True)}")
print(f"最大 partition: {max(partition_sizes):,} 筆")
print(f"最小 partition: {min(partition_sizes):,} 筆")
print(f"📊 不均勻比例: {max(partition_sizes)/min(partition_sizes):.1f}x — 差距越大，Skew 越嚴重")

# ============================================================
# Step 3：正確的 partitionBy 策略 — 用低基數欄位 year_month
# ============================================================
print("\n" + "=" * 60)
print("示範 2：正確的 partitionBy 策略 — year_month（低基數）")
print("=" * 60)

# 看 year_month 有幾種值（基數）
year_month_count = df.select("year_month").distinct().count()
print(f"\nyear_month 基數（不同值數量）: {year_month_count}  ← 低基數，適合做 partitionBy")
print(f"customer_id 基數: 5  ← 看起來低，但資料分布極度不均，不適合")

# 月份資料分布
year_month_dist = df.groupBy("year_month").count().orderBy("year_month")
print("\n月份資料分布（相對均勻）：")
year_month_dist.show(5)

# ============================================================
# Step 4：執行月度庫存分析（核心 aggregation）
# 展示 shuffle.partitions 和 AQE 的作用
# ============================================================
print("\n" + "=" * 60)
print("示範 3：月度庫存消耗分析（groupBy + sum）")
print("=" * 60)

monthly_analysis = df.groupBy("year_month", "material_id", "plant") \
    .agg(
        F.sum("quantity").alias("total_qty"),
        F.sum("amount_usd").alias("total_amount"),
        F.count("*").alias("transaction_count")
    ) \
    .orderBy("year_month", "total_amount", ascending=[True, False])

# explain() 可以看到 Exchange hashpartitioning
print("\n執行計劃（Physical Plan 節錄）：")
monthly_analysis.explain(mode="simple")

print(f"\n月度分析結果 partition 數: {monthly_analysis.rdd.getNumPartitions()}")
print("（AQE 啟用時，Spark 會動態 coalesce 過小的 partition）")

# 顯示前 10 行
print("\n月度庫存消耗 Top 10（按金額排序）：")
monthly_analysis.show(10, truncate=False)

# ============================================================
# Step 5：repartition vs coalesce 對比
# ============================================================
print("\n" + "=" * 60)
print("示範 4：repartition vs coalesce 差異")
print("=" * 60)

original_partitions = df.rdd.getNumPartitions()
print(f"\n原始 partition 數: {original_partitions}")

# repartition：有 shuffle，可以增加或減少，資料均勻分布
df_repartitioned = df.repartition(20)
print(f"repartition(20) 後: {df_repartitioned.rdd.getNumPartitions()} 個")

# coalesce：無 shuffle，只能減少，相鄰 partition 合併
df_coalesced = df.coalesce(4)
print(f"coalesce(4) 後: {df_coalesced.rdd.getNumPartitions()} 個")

# 比較：如果要增加 partition，只有 repartition 有效
df_coalesce_increase = df.coalesce(100)  # 嘗試增加，實際不會超過原來數量
print(f"coalesce(100) 嘗試增加: {df_coalesce_increase.rdd.getNumPartitions()} 個 ← 無法增加！")

print("""
選擇原則：
  repartition()  → 需要增加並行度，或要均勻打散資料（有 shuffle 成本）
  coalesce()     → 寫入前減少 partition 數量，避免小檔案（無 shuffle 成本）
""")

# ============================================================
# Step 6：顯示 AQE 配置
# ============================================================
print("=" * 60)
print("AQE（Adaptive Query Execution）配置確認")
print("=" * 60)
print(f"""
AQE 狀態：
  spark.sql.adaptive.enabled                    = {spark.conf.get("spark.sql.adaptive.enabled")}
  spark.sql.adaptive.coalescePartitions.enabled = {spark.conf.get("spark.sql.adaptive.coalescePartitions.enabled")}
  spark.sql.adaptive.skewJoin.enabled           = {spark.conf.get("spark.sql.adaptive.skewJoin.enabled")}
  advisoryPartitionSizeInBytes                  = {spark.conf.get("spark.sql.adaptive.advisoryPartitionSizeInBytes")}
  shuffle.partitions (設定值)                   = {spark.conf.get("spark.sql.shuffle.partitions")}

Spark 版本: {spark.version}
""")

print("✅ Partition 基礎練習完成！")
print("""
重點複習：
  1. 讀檔預設 128MB/partition，shuffle 預設 200（可設定）
  2. 高基數欄位 partitionBy → 小檔案災難
  3. 超級大客戶 → Data Skew → AQE skewJoin 自動處理
  4. repartition = 有 shuffle（可增減）；coalesce = 無 shuffle（只能減少）
  5. Spark 3.2+ AQE 預設開啟，動態優化 partition 數量
""")

spark.stop()
