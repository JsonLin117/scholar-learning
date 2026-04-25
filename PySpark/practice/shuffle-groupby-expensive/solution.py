"""
PySpark Shuffle 優化實作：供應商月度採購分析
情境：緯穎 ODM 廠 ERP PO 資料，找出 shuffle 瓶頸並優化
日期：2026-04-26
"""

import builtins
builtins_sum = builtins.sum  # 保存 Python 原生 sum，避免被 pyspark 的 sum 覆蓋

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    sum, count, col, broadcast, when,
    rand, concat_ws, lit
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import time


def create_spark_session():
    """建立 SparkSession，調整 shuffle 相關設定"""
    return SparkSession.builder \
        .appName("ShuffleOptimizationDemo") \
        .master("local[4]") \
        .config("spark.sql.shuffle.partitions", "20") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.autoBroadcastJoinThreshold", "50mb") \
        .getOrCreate()


def generate_po_data(spark, num_records=100000):
    """
    模擬 ERP PO 資料
    - 假設有 80 個供應商，實際大廠可能 50~200 個直接供應商
    - 有資料傾斜：supply_001 的訂單量 5 倍於其他供應商（台積電效應）
    """
    from pyspark.sql import Row
    import random

    suppliers = [f"SUP_{str(i).zfill(3)}" for i in range(1, 81)]
    categories = ["CPU", "Memory", "PCB", "Connector", "Other"]
    months = ["2026-01", "2026-02", "2026-03"]

    rows = []
    for i in range(num_records):
        # 製造資料傾斜：SUP_001 出現機率 5 倍
        if random.random() < 0.5:
            supplier = "SUP_001"
        else:
            supplier = random.choice(suppliers[1:])

        rows.append({
            "po_id": f"PO{str(i).zfill(10)}",
            "supplier_id": supplier,
            "month": random.choice(months),
            "category": random.choice(categories),
            "amount": round(random.uniform(5000, 500000), 2),
            "qty": random.randint(10, 1000),
        })

    schema = StructType([
        StructField("po_id", StringType(), False),
        StructField("supplier_id", StringType(), False),
        StructField("month", StringType(), False),
        StructField("category", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("qty", IntegerType(), False),
    ])

    return spark.createDataFrame(rows, schema=schema)


def generate_supplier_master(spark):
    """
    模擬供應商 Master Data（幾十 ~ 幾百筆，必須 broadcast）
    """
    suppliers = [(f"SUP_{str(i).zfill(3)}", f"供應商_{i}", "Taiwan") for i in range(1, 81)]
    schema = StructType([
        StructField("supplier_id", StringType(), False),
        StructField("supplier_name", StringType(), False),
        StructField("country", StringType(), False),
    ])
    return spark.createDataFrame(suppliers, schema=schema)


# ============================================================
# 方案 A：❌ 糟糕的寫法（RDD groupByKey）
# ============================================================
def bad_approach_groupbykey(df_po):
    """
    反模式：groupByKey 不做 map-side aggregation
    問題：全量 shuffle，每個 (supplier_id, month) key 的所有 amount 都要傳過網路
    """
    print("\n=== ❌ 方案A: RDD groupByKey（反模式） ===")
    start = time.time()

    rdd = df_po.rdd.map(lambda row: ((row.supplier_id, row.month), row.amount))

    # groupByKey：全量收集，不壓縮，OOM 風險
    def group_agg(vals):
        items = list(vals)  # 先 materialize（危險！大 key 會 OOM）
        return (builtins_sum(items), len(items))

    result = (
        rdd
        .groupByKey()
        .mapValues(group_agg)  # ← 要 materialize 整個 iterator
        .collect()
    )

    elapsed = time.time() - start
    print(f"✗ 完成，耗時: {elapsed:.2f}s，記錄數: {len(result)}")
    return result


# ============================================================
# 方案 B：✅ 好的寫法（reduceByKey，map-side combine）
# ============================================================
def better_approach_reducebykey(df_po):
    """
    改善：reduceByKey 在 map 端先聚合
    優點：每個 mapper 先把本地 partition 的 amount 加總，shuffle 的資料量大幅降低
    """
    print("\n=== ✅ 方案B: RDD reduceByKey（改善） ===")
    start = time.time()

    rdd = df_po.rdd.map(lambda row: ((row.supplier_id, row.month), (row.amount, 1)))

    # reduceByKey：在 map 端先做局部聚合，只傳 (sum, count) 的中間值
    result = (
        rdd
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
        .collect()
    )

    elapsed = time.time() - start
    print(f"✓ 完成，耗時: {elapsed:.2f}s，記錄數: {len(result)}")
    return result


# ============================================================
# 方案 C：✅✅ 最佳寫法（DataFrame API + broadcast join）
# ============================================================
def best_approach_dataframe(df_po, df_supplier_master):
    """
    最佳：DataFrame API（Catalyst 自動優化）+ broadcast 小表
    
    1. groupBy().agg() → Catalyst 自動做 partial aggregation（partial sum 在 map 端）
    2. broadcast(small_table) → 供應商 master 廣播，PO 大表不做任何 shuffle
    3. AQE 開啟 → 動態合併 shuffle 後的小 partition
    """
    print("\n=== ✅✅ 方案C: DataFrame + broadcast（最佳）===")
    start = time.time()

    # Step 1: 按供應商 + 月份 + 分類做彙總（Catalyst 自動做 partial aggregation）
    summary = df_po.groupBy("supplier_id", "month", "category").agg(
        sum("amount").alias("total_amount"),
        sum("qty").alias("total_qty"),
        count("*").alias("po_count")
    )

    # Step 2: join 供應商 master，強制 broadcast 小表（幾百筆，不值得 shuffle 大表）
    result = summary.join(
        broadcast(df_supplier_master),  # ← 廣播供應商 master，完全沒有 shuffle join
        on="supplier_id",
        how="left"
    )

    # Step 3: 計算總計
    final = result.select(
        "supplier_name",
        "month",
        "category",
        "total_amount",
        "total_qty",
        "po_count"
    ).orderBy("total_amount", ascending=False)

    count_result = final.count()
    elapsed = time.time() - start
    print(f"✓ 完成，耗時: {elapsed:.2f}s，記錄數: {count_result}")
    final.show(5)

    return final


# ============================================================
# 示範 AQE 動態 partition 合併
# ============================================================
def demo_aqe_coalesce(spark, df_po):
    """
    示範 AQE 的動態 partition 合併效果
    spark.sql.shuffle.partitions = 200（預設），但實際只有 80 個供應商
    AQE 會把空的/小的 partition 合併，避免 200 個小 task 的 overhead
    """
    print("\n=== AQE 動態 Partition 合併示範 ===")

    # 查看 AQE 是否開啟
    aqe_enabled = spark.conf.get("spark.sql.adaptive.enabled")
    print(f"AQE enabled: {aqe_enabled}")

    # 設回預設 200（模擬生產環境）
    spark.conf.set("spark.sql.shuffle.partitions", "200")

    result = df_po.groupBy("supplier_id").agg(
        sum("amount").alias("total"),
        count("*").alias("cnt")
    )

    # explain 看 AQE plan
    print("\nPhysical Plan（注意有沒有 AdaptiveSparkPlan）:")
    result.explain(mode="formatted")

    result.count()  # 觸發 action，AQE 才真正運作
    print("✓ AQE 動態合併完成（實際 partition 數遠少於 200）")


if __name__ == "__main__":
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("="*60)
    print("PySpark Shuffle 優化實作：緯穎供應商採購分析")
    print("="*60)

    # 生成模擬資料
    print("\n📊 生成模擬 PO 資料...")
    df_po = generate_po_data(spark, num_records=50000)
    df_supplier_master = generate_supplier_master(spark)
    df_po.cache()  # 多次使用，cache 避免重複讀取
    df_po.count()  # 觸發 cache
    print(f"✓ PO 資料: {df_po.count()} 筆，供應商 master: {df_supplier_master.count()} 筆")

    # 執行三種方案對比
    bad_approach_groupbykey(df_po)
    better_approach_reducebykey(df_po)
    best_approach_dataframe(df_po, df_supplier_master)

    # AQE 示範
    demo_aqe_coalesce(spark, df_po)

    print("\n" + "="*60)
    print("📝 效能總結：")
    print("  ❌ groupByKey: 全量 shuffle，OOM 風險，不推薦")
    print("  ✅ reduceByKey: map-side combine，shuffle 量大幅下降")
    print("  ✅✅ DataFrame + broadcast: Catalyst 自動優化 + 消除 join shuffle")
    print("="*60)

    spark.stop()
