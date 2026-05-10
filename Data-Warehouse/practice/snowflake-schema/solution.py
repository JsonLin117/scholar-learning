"""
Snowflake Schema vs Star Schema：ODM 供應鏈採購分析
比較兩種維度建模的 JOIN 複雜度和 Spark 執行計劃

學習日期：2026-05-10
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, DateType, LongType
)
import random
from datetime import date, timedelta

# ── Spark 初始化 ──
spark = SparkSession.builder \
    .appName("Star-vs-Snowflake-Schema") \
    .master("local[*]") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("=" * 70)
print("  Star Schema vs Snowflake Schema — ODM 採購分析 Demo")
print("=" * 70)

# ── 模擬資料 ──
NUM_RECORDS = 500_000
NUM_PARTS = 2000
NUM_SUPPLIERS = 50
NUM_REGIONS = 8

# 基礎資料
regions = [
    ("R01", "台灣", "亞太"),
    ("R02", "中國大陸", "亞太"),
    ("R03", "日本", "亞太"),
    ("R04", "韓國", "亞太"),
    ("R05", "美國", "北美"),
    ("R06", "墨西哥", "北美"),
    ("R07", "德國", "歐洲"),
    ("R08", "馬來西亞", "亞太"),
]

suppliers = [
    (f"SUP-{i:03d}", f"供應商_{i}", random.choice(regions)[0])
    for i in range(1, NUM_SUPPLIERS + 1)
]

categories = [
    ("CPU", "處理器", "主動元件"),
    ("DRAM", "記憶體", "主動元件"),
    ("SSD", "儲存裝置", "主動元件"),
    ("PCB", "印刷電路板", "被動元件"),
    ("FAN", "散熱風扇", "機構件"),
    ("CASE", "機殼", "機構件"),
    ("PSU", "電源供應器", "主動元件"),
    ("CABLE", "線材", "連接件"),
    ("HEATSINK", "散熱器", "機構件"),
    ("CONN", "連接器", "連接件"),
]

parts = [
    (
        f"MAT-{i:05d}",
        f"零件_{i}",
        random.choice(suppliers)[0],  # supplier_code
        random.choice(categories)[0],  # category_code
        random.choice([True, False]) if random.random() < 0.15 else False,  # is_critical
    )
    for i in range(1, NUM_PARTS + 1)
]

# ── 方案 A：Star Schema（攤平維度） ──
print("\n📦 方案 A：建立 Star Schema（維度攤平）...")

# 建立攤平的 dim_part_star
region_map = {r[0]: r for r in regions}
supplier_map = {s[0]: s for s in suppliers}
category_map = {c[0]: c for c in categories}

dim_part_star_data = []
for i, (mat_id, mat_name, supp_code, cat_code, is_crit) in enumerate(parts, 1):
    supp = supplier_map[supp_code]
    region = region_map[supp[2]]
    cat = category_map[cat_code]
    dim_part_star_data.append((
        i,                # part_sk
        mat_id,           # part_id
        mat_name,         # part_name
        supp[0],          # supplier_code
        supp[1],          # supplier_name（攤平！）
        region[1],        # region_name（攤平！）
        region[2],        # continent（攤平！）
        cat[0],           # category_code
        cat[1],           # category_name（攤平！）
        cat[2],           # category_group（攤平！）
        is_crit,          # is_critical
    ))

dim_part_star = spark.createDataFrame(
    dim_part_star_data,
    ["part_sk", "part_id", "part_name", "supplier_code", "supplier_name",
     "region_name", "continent", "category_code", "category_name",
     "category_group", "is_critical"]
)
dim_part_star.createOrReplaceTempView("dim_part_star")

# ── 方案 B：Snowflake Schema（正規化維度） ──
print("❄️  方案 B：建立 Snowflake Schema（維度正規化）...")

# dim_region
dim_region = spark.createDataFrame(
    [(r[0], r[1], r[2]) for r in regions],
    ["region_sk", "region_name", "continent"]
)
dim_region.createOrReplaceTempView("dim_region")

# dim_supplier
dim_supplier = spark.createDataFrame(
    [(s[0], s[1], s[2]) for s in suppliers],
    ["supplier_code", "supplier_name", "region_sk"]
)
dim_supplier.createOrReplaceTempView("dim_supplier")

# dim_category
dim_category = spark.createDataFrame(
    [(c[0], c[1], c[2]) for c in categories],
    ["category_code", "category_name", "category_group"]
)
dim_category.createOrReplaceTempView("dim_category")

# dim_part_snowflake（只存 FK）
dim_part_sf_data = [
    (i, mat_id, mat_name, supp_code, cat_code, is_crit)
    for i, (mat_id, mat_name, supp_code, cat_code, is_crit) in enumerate(parts, 1)
]
dim_part_sf = spark.createDataFrame(
    dim_part_sf_data,
    ["part_sk", "part_id", "part_name", "supplier_code", "category_code", "is_critical"]
)
dim_part_sf.createOrReplaceTempView("dim_part_snowflake")

# ── 共用 Fact Table ──
print("📊 建立 Fact Table（50 萬筆採購記錄）...")
random.seed(42)

base_date = date(2025, 1, 1)
fact_data = [
    (
        j,
        random.randint(1, NUM_PARTS),
        (base_date + timedelta(days=random.randint(0, 365))).isoformat(),
        random.randint(1, 5000),
        round(random.uniform(0.5, 500.0), 2),
    )
    for j in range(1, NUM_RECORDS + 1)
]

fact_procurement = spark.createDataFrame(
    fact_data,
    ["procurement_id", "part_sk", "order_date", "quantity", "unit_price"]
).withColumn("amount", F.col("quantity") * F.col("unit_price"))

fact_procurement.createOrReplaceTempView("fact_procurement")

print(f"   ✅ fact_procurement: {fact_procurement.count():,} rows")
print(f"   ✅ dim_part_star: {dim_part_star.count():,} rows")
print(f"   ✅ dim_part_snowflake: {dim_part_sf.count():,} rows")
print(f"   ✅ dim_supplier: {dim_supplier.count():,} rows")
print(f"   ✅ dim_region: {dim_region.count():,} rows")
print(f"   ✅ dim_category: {dim_category.count():,} rows")

# ── 業務查詢：按供應商地區統計採購金額 ──
print("\n" + "=" * 70)
print("  查詢：按供應商地區和零件類別統計採購金額")
print("=" * 70)

# Star Schema 查詢（1 JOIN）
print("\n⭐ Star Schema 查詢（1 JOIN）:")
star_query = """
SELECT
    d.region_name,
    d.continent,
    d.category_group,
    COUNT(*) AS order_count,
    SUM(f.amount) AS total_amount,
    AVG(f.unit_price) AS avg_unit_price
FROM fact_procurement f
JOIN dim_part_star d ON f.part_sk = d.part_sk
GROUP BY d.region_name, d.continent, d.category_group
ORDER BY total_amount DESC
"""
star_result = spark.sql(star_query)
star_result.show(10, truncate=False)

# Snowflake Schema 查詢（3 JOINs）
print("\n❄️  Snowflake Schema 查詢（3 JOINs）:")
snow_query = """
SELECT
    r.region_name,
    r.continent,
    c.category_group,
    COUNT(*) AS order_count,
    SUM(f.amount) AS total_amount,
    AVG(f.unit_price) AS avg_unit_price
FROM fact_procurement f
JOIN dim_part_snowflake p ON f.part_sk = p.part_sk
JOIN dim_supplier s ON p.supplier_code = s.supplier_code
JOIN dim_region r ON s.region_sk = r.region_sk
JOIN dim_category c ON p.category_code = c.category_code
GROUP BY r.region_name, r.continent, c.category_group
ORDER BY total_amount DESC
"""
snow_result = spark.sql(snow_query)
snow_result.show(10, truncate=False)

# ── 執行計劃對比 ──
print("\n" + "=" * 70)
print("  執行計劃對比")
print("=" * 70)

print("\n⭐ Star Schema 邏輯計劃：")
spark.sql(star_query).explain(mode="simple")

print("\n❄️  Snowflake Schema 邏輯計劃：")
spark.sql(snow_query).explain(mode="simple")

# ── 結論 ──
print("\n" + "=" * 70)
print("  📝 結論")
print("=" * 70)
print("""
1. Star Schema：1 個 JOIN → 1 次 Exchange（如果觸發 Shuffle）
   - SQL 簡潔，分析師友善
   - dim_part 有冗餘字串（supplier_name 等重複），但 Parquet 壓縮後影響極小

2. Snowflake Schema：3 個 JOIN → 多達 3 次 Exchange
   - SQL 複雜，每多一個 JOIN 就多一次 Shuffle 風險
   - 小維度表（dim_supplier: 50 行、dim_region: 8 行）會被 Broadcast
   - 但在大規模生產環境中，維度表可能不小（10 萬+ 供應商）

3. SA 選型結論：
   ✅ 99% 場景用 Star Schema — 特別是 Lakehouse（Delta Lake + Parquet）
   ⚠️ Snowflake Schema 只在維度有極深階層且各層獨立變動時考慮
   🔥 現代趨勢是往 OBT（One Big Table）走，而不是更正規化
""")

spark.stop()
