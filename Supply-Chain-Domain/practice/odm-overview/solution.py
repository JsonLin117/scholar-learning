"""
ODM 多客戶 BOM 資料架構設計 - SCD Type 2 實作
2026-04-14 | Supply-Chain-Domain Lesson 1

情境：緯穎（Wiwynn）ODM 廠，服務 Microsoft/Google/Meta/AWS，
     Microsoft 緊急發 ECO 改散熱設計，需要更新 BOM 維度表（SCD Type 2）
     且確保歷史採購訂單報表不受影響。
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, when, monotonically_increasing_id
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    BooleanType, TimestampType, LongType
)
from datetime import datetime

spark = SparkSession.builder \
    .appName("Wiwynn_ODM_BOM_SCD2") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("ODM 多客戶 BOM 資料架構 - SCD Type 2 示範")
print("情境：緯穎同時服務 Microsoft/Google/Meta/AWS")
print("=" * 60)

# ============================================================
# Step 1: 建立初始 BOM Dimension Table（代表歷史快照）
# ============================================================
print("\n📋 Step 1: 初始 BOM 維度表（ECO 前）")

# 模擬 4 家 CSP 客戶的初始 BOM 配置
initial_bom_data = [
    # (bom_sk, bom_natural_key, customer_code, product_line, cpu_model, memory_gb, cooling_design, valid_from, valid_to, is_current, change_reason)
    (1, "MSFT-HB120-V3", "MSFT", "Server", "Intel Xeon 8380", 512, "Air Cooling",    "2025-01-01", None, True,  "Initial Release"),
    (2, "GOOGL-A3-V2",   "GOOGL", "HPC",   "AMD EPYC 9654",   768, "Air Cooling",    "2025-02-01", None, True,  "Initial Release"),
    (3, "META-RS720-V1",  "META",  "Server", "Intel Xeon 8480+", 256, "Liquid Cooling", "2025-03-01", None, True,  "Initial Release"),
    (4, "AMZN-UL430-V2",  "AMZN", "Storage","AMD EPYC 9354P",  128, "Air Cooling",    "2025-04-01", None, True,  "Initial Release"),
]

bom_schema = StructType([
    StructField("bom_sk",          LongType(),    False),  # 代理鍵
    StructField("bom_natural_key", StringType(),  False),  # 自然鍵
    StructField("customer_code",   StringType(),  False),  # CSP 代碼
    StructField("product_line",    StringType(),  False),  # Server/Storage/HPC
    StructField("cpu_model",       StringType(),  True),
    StructField("memory_gb",       IntegerType(), True),
    StructField("cooling_design",  StringType(),  True),   # 散熱設計（ECO 變更點）
    StructField("valid_from",      StringType(),  False),  # SCD Type 2 有效期
    StructField("valid_to",        StringType(),  True),   # NULL = 現行版本
    StructField("is_current",      BooleanType(), False),  # 是否現行版本
    StructField("change_reason",   StringType(),  True),   # 變更原因（ECO 編號）
])

bom_dim_df = spark.createDataFrame(initial_bom_data, schema=bom_schema)

print("初始 BOM 維度表（4 家 CSP，每家 1 個產品）：")
bom_dim_df.select("bom_natural_key", "customer_code", "cooling_design", "is_current").show()

# ============================================================
# Step 2: 模擬採購 Fact Table（ECO 前的歷史訂單）
# ============================================================
print("📦 Step 2: 採購 Fact Table（ECO 前歷史訂單）")

po_data = [
    # (po_id, bom_sk, supplier_id, order_date, quantity, unit_price_usd_cents, total_amount_usd_cents)
    ("PO-2025-001", 1, "SUP-INTEL-TW", "2025-06-01", 500, 8000_00, 400_000_00),  # MSFT Air Cooling 時期
    ("PO-2025-002", 1, "SUP-INTEL-TW", "2025-09-15", 300, 8100_00, 243_000_00),  # MSFT Air Cooling 時期
    ("PO-2025-003", 2, "SUP-AMD-US",   "2025-07-01", 200, 12000_00, 240_000_00), # Google 訂單
    ("PO-2025-004", 3, "SUP-INTEL-TW", "2025-08-01", 400, 9500_00, 380_000_00),  # Meta 訂單
]

po_schema = StructType([
    StructField("po_id",                  StringType(),  False),
    StructField("bom_sk",                 LongType(),    False),   # 指向 bom_dim 的代理鍵
    StructField("supplier_id",            StringType(),  False),
    StructField("order_date",             StringType(),  False),
    StructField("quantity",               IntegerType(), False),
    StructField("unit_price_usd_cents",   LongType(),    False),   # 用 cents 避免浮點問題
    StructField("total_amount_usd_cents", LongType(),    False),
])

po_fact_df = spark.createDataFrame(po_data, schema=po_schema)
print("歷史採購訂單（MSFT 有 2 筆，都是 Air Cooling 時期）：")
po_fact_df.show()

# ============================================================
# Step 3: ECO 發生！Microsoft 改散熱設計
# ============================================================
print("⚡ Step 3: ECO 發生！Microsoft 發 ECO-2026-001")
print("   MSFT-HB120-V3：Air Cooling → Direct Liquid Cooling")
print("   原因：NVIDIA GB200 TDP 提升，Air Cooling 散熱不足")

eco_date = "2026-01-15"  # ECO 生效日期

# SCD Type 2 邏輯：
# 1. 找到 MSFT 的現行版本（is_current=True），將其 valid_to = eco_date，is_current = False
# 2. 插入新版本（cooling_design = Liquid Cooling），valid_from = eco_date，is_current = True，new bom_sk

# 模擬 SCD Type 2 更新（非 Delta Lake MERGE，純 PySpark 示範）
from pyspark.sql.functions import expr

# 舊版本：標記為 is_current = False，設定 valid_to
bom_old = bom_dim_df.withColumn(
    "valid_to", when(
        (col("bom_natural_key") == "MSFT-HB120-V3") & (col("is_current") == True),
        lit(eco_date)
    ).otherwise(col("valid_to"))
).withColumn(
    "is_current", when(
        (col("bom_natural_key") == "MSFT-HB120-V3") & (col("is_current") == True),
        lit(False)
    ).otherwise(col("is_current"))
)

# 新版本：bom_sk = 5（下一個 ID）
new_bom_row = spark.createDataFrame([
    (5, "MSFT-HB120-V3", "MSFT", "Server", "Intel Xeon 8380", 512, 
     "Direct Liquid Cooling",  # 👈 ECO 變更點
     eco_date, None, True, "ECO-2026-001: GB200 TDP increase")
], schema=bom_schema)

# 合併：舊表（含更新）+ 新版本
bom_dim_updated = bom_old.union(new_bom_row)

print("\n更新後的 BOM 維度表（MSFT 現在有 2 個版本）：")
bom_dim_updated.filter(col("customer_code") == "MSFT") \
    .select("bom_sk", "customer_code", "cooling_design", "valid_from", "valid_to", "is_current", "change_reason") \
    .show(truncate=False)

# ============================================================
# Step 4: 驗證歷史採購訂單不受影響
# ============================================================
print("✅ Step 4: 驗證歷史查詢正確性（ECO 前的訂單應顯示 Air Cooling）")

# 歷史訂單 JOIN 當時有效的 BOM 版本（根據 order_date 落在 valid_from ~ valid_to 之間）
po_with_bom = po_fact_df.alias("po").join(
    bom_dim_updated.alias("bom"),
    on=col("po.bom_sk") == col("bom.bom_sk"),
    how="left"
).select(
    col("po.po_id"),
    col("po.order_date"),
    col("po.quantity"),
    col("bom.customer_code"),
    col("bom.cooling_design"),
    col("bom.is_current"),
    col("bom.valid_from"),
    col("bom.valid_to"),
)

print("歷史採購訂單 × BOM 版本 JOIN 結果：")
po_with_bom.show(truncate=False)

print("""
🎯 關鍵驗證點：
   - PO-2025-001 和 PO-2025-002（MSFT 舊訂單）→ bom_sk=1，cooling_design = Air Cooling ✅
   - 若未來有新訂單（bom_sk=5），會顯示 Direct Liquid Cooling ✅
   - SCD Type 2 確保歷史報表不被 ECO 影響 ✅
""")

# ============================================================
# Step 5: SA 延伸 - 影響分析（ECO 後有多少在途訂單受影響）
# ============================================================
print("🏗️  Step 5: SA 延伸 - ECO 影響分析")
print("   問題：ECO 發生時，有多少 MSFT 訂單是 Air Cooling 設計的？")

# 模擬：找出使用舊版本 BOM（is_current=False, customer_code=MSFT）的訂單
affected_orders = po_fact_df.alias("po").join(
    bom_dim_updated.filter(
        (col("customer_code") == "MSFT") & (col("is_current") == False)
    ).alias("old_bom"),
    on=col("po.bom_sk") == col("old_bom.bom_sk"),
    how="inner"
).agg(
    {"quantity": "sum", "total_amount_usd_cents": "sum", "po_id": "count"}
).withColumnRenamed("sum(quantity)", "total_units_affected") \
 .withColumnRenamed("sum(total_amount_usd_cents)", "total_amount_cents") \
 .withColumnRenamed("count(po_id)", "num_orders_affected")

print("ECO 前 MSFT 的 Air Cooling 訂單統計：")
affected_orders.show()

total = affected_orders.collect()[0]
print(f"   受影響訂單數：{total['num_orders_affected']}")
print(f"   受影響總量：{total['total_units_affected']} 台")
print(f"   受影響金額：US${total['total_amount_cents']/100:,.2f}")
print(f"\n   ⚠️  SA 建議：檢查這 {total['total_units_affected']} 台是否已出貨/在製，")
print(f"   如有在製品（WIP），需要評估是否需要 Rework 或 Re-design")

print("\n" + "=" * 60)
print("✅ 練習完成！Supply-Chain-Domain Lesson 1: ODM 全景 + SCD Type 2")
print("=" * 60)

spark.stop()
