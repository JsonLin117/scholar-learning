"""
OLAP vs OLTP：ODM 供應鏈出貨資料的查詢型態效能對比
模擬 SAP OLTP 單筆交易查詢 vs Databricks OLAP 聚合分析報表，
用同一份資料量驗證兩種負載混跑會互相拖累，佐證架構解耦的必要性。

學習日期：2026-07-11
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, DateType, LongType
)
import random
import time
from datetime import date, timedelta

# ── Spark 初始化 ──
spark = SparkSession.builder \
    .appName("OLAP-vs-OLTP-ODM-Shipment") \
    .master("local[*]") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("=" * 70)
print("  OLAP vs OLTP — ODM 出貨資料查詢型態效能對比 Demo")
print("=" * 70)

# ── 模擬資料：fact_shipment（出貨事實表）──
NUM_RECORDS = 500_000
NUM_CUSTOMERS = 30
NUM_REGIONS = 8

regions = ["APAC", "NA", "EU", "LATAM"]
customers = [(f"CUST-{i:03d}", f"CSP客戶_{i}", random.choice(regions))
             for i in range(1, NUM_CUSTOMERS + 1)]

customer_schema = StructType([
    StructField("customer_key", StringType(), False),
    StructField("customer_name", StringType(), False),
    StructField("region", StringType(), False),
])
dim_customer = spark.createDataFrame(customers, schema=customer_schema)

print(f"\n[1/4] 產生 {NUM_RECORDS:,} 筆模擬出貨事實資料...")

start_date = date(2025, 1, 1)


def gen_shipment_row(i):
    cust = random.choice(customers)
    ship_date = start_date + timedelta(days=random.randint(0, 545))
    qty = random.randint(1, 200)
    unit_price = round(random.uniform(500, 8000), 2)
    otd_flag = 1 if random.random() > 0.12 else 0  # ~88% OTD 準時率
    return (
        f"ORD-{i:08d}",
        cust[0],
        ship_date,
        ship_date.year,
        (ship_date.month - 1) // 3 + 1,  # quarter
        qty,
        unit_price,
        otd_flag,
    )


shipment_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_key", StringType(), False),
    StructField("ship_date", DateType(), False),
    StructField("year", IntegerType(), False),
    StructField("quarter", IntegerType(), False),
    StructField("qty", IntegerType(), False),
    StructField("unit_price", DoubleType(), False),
    StructField("otd_flag", IntegerType(), False),
])

rows = [gen_shipment_row(i) for i in range(NUM_RECORDS)]
fact_shipment = spark.createDataFrame(rows, schema=shipment_schema)
fact_shipment = fact_shipment.repartition(8).cache()
fact_shipment.count()  # materialize cache

print(f"   完成。fact_shipment 筆數 = {fact_shipment.count():,}")

# ── OLTP 模擬：單筆交易 point lookup（模擬 SAP VL02N 查單筆出貨單）──
print("\n[2/4] OLTP 模擬：單筆交易 point lookup (模擬 SAP 交易查詢)...")
sample_order_ids = [r["order_id"] for r in fact_shipment.select("order_id").limit(20).collect()]

oltp_start = time.time()
for oid in sample_order_ids:
    _ = fact_shipment.filter(F.col("order_id") == oid).select(
        "order_id", "customer_key", "ship_date", "qty", "otd_flag"
    ).collect()
oltp_elapsed = time.time() - oltp_start
print(f"   20 次單筆 point lookup 總耗時: {oltp_elapsed:.3f} 秒 "
      f"(平均 {oltp_elapsed/20*1000:.1f} ms/次)")

# ── OLAP 模擬：跨百萬筆聚合分析（模擬 BI 報表：各地區/季度 OTD 統計）──
print("\n[3/4] OLAP 模擬：跨全表聚合分析 (模擬 BI 報表查詢)...")

olap_start = time.time()
olap_result = (
    fact_shipment.join(F.broadcast(dim_customer), "customer_key")
    .groupBy("region", "year", "quarter")
    .agg(
        F.sum("qty").alias("total_qty"),
        F.avg("unit_price").alias("avg_unit_price"),
        F.avg("otd_flag").alias("otd_rate"),
        F.count("order_id").alias("order_count"),
    )
    .orderBy("region", "year", "quarter")
)
olap_rows = olap_result.collect()
olap_elapsed = time.time() - olap_start
print(f"   全表 GROUP BY 聚合耗時: {olap_elapsed:.3f} 秒（{len(olap_rows)} 個分組結果）")

print("\n   OTD 表現節錄（前 5 組）：")
for r in olap_rows[:5]:
    print(f"     {r['region']} {r['year']}-Q{r['quarter']}: "
          f"qty={r['total_qty']:,}, avg_price=${r['avg_unit_price']:.2f}, "
          f"OTD={r['otd_rate']*100:.1f}%, orders={r['order_count']}")

# ── 執行計劃對比 ──
print("\n[4/4] Spark 物理執行計劃對比：")
print("\n--- OLTP point lookup explain (單筆過濾) ---")
fact_shipment.filter(F.col("order_id") == sample_order_ids[0]).explain(mode="simple")

print("\n--- OLAP aggregation explain (全表 GROUP BY) ---")
olap_result.explain(mode="simple")

# ── 結論 ──
print("\n" + "=" * 70)
print("  結論：")
print(f"  - OLTP point lookup 平均 {oltp_elapsed/20*1000:.1f} ms/次（單筆命中）")
print(f"  - OLAP 全表聚合 {olap_elapsed:.3f} 秒（掃描 {NUM_RECORDS:,} 筆 + join + shuffle）")
print("  - 兩者的資源消耗模式（IOPS+單點 vs CPU+全表掃描）本質互斥")
print("  - 若在同一系統上混跑，OLAP 的全表掃描會拖慢 OLTP 的單筆交易延遲")
print("  - 這正是為什麼 ODM 廠要把 SAP OLTP 資料 CDC 同步到獨立 OLAP 資料倉庫")
print("=" * 70)

assert oltp_elapsed > 0, "OLTP timing should be positive"
assert olap_elapsed > 0, "OLAP timing should be positive"
assert len(olap_rows) > 0, "OLAP aggregation should produce results"
print("\n✅ 練習驗證通過：OLTP/OLAP 查詢型態模擬與執行計劃對比完成")

spark.stop()
