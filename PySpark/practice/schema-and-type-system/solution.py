"""
PySpark Schema 與型別系統 — 實作練習
======================================
場景：ODM BOM 資料的 Schema 防線設計
目標：
  1. Explicit Schema vs Inference 效能與正確性對比
  2. DecimalType vs DoubleType 精度在多層 BOM 展開的誤差
  3. nullable 約束的「不檢查」陷阱
  4. Schema 序列化/反序列化（版本控制）
  5. Schema Evolution（ECO 加欄位後的合併）
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, DecimalType, DateType, TimestampType, BooleanType,
    ArrayType, MapType
)
from pyspark.sql.functions import col, lit, when, sum as spark_sum, round as spark_round
from decimal import Decimal
import json
import time
import os
import tempfile
import shutil

spark = SparkSession.builder \
    .appName("SchemaAndTypeSystem") \
    .master("local[*]") \
    .config("spark.sql.session.timeZone", "UTC") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 70)
print("  PySpark Schema 與型別系統 — ODM BOM 場景實作")
print("=" * 70)

# ====================================================================
# 1. Explicit Schema 定義 — ODM BOM 資料結構
# ====================================================================
print("\n📐 Step 1: Explicit Schema 定義")

bom_schema = StructType([
    StructField("bom_id", StringType(), nullable=False),
    StructField("parent_material", StringType(), nullable=False),
    StructField("child_material", StringType(), nullable=False),
    StructField("quantity_per", DecimalType(10, 4), nullable=False),
    StructField("bom_level", IntegerType(), nullable=False),
    StructField("plant", StringType(), nullable=False),
    StructField("effective_from", StringType(), nullable=False),
    StructField("effective_to", StringType(), nullable=True),
    StructField("scrap_rate", DecimalType(5, 2), nullable=True),
])

print(f"  Schema 欄位數：{len(bom_schema.fields)}")
print(f"  StructType tree:")
for f in bom_schema.fields:
    null_str = "nullable" if f.nullable else "NOT NULL"
    print(f"    ├─ {f.name}: {f.dataType.simpleString()} ({null_str})")

# DDL 字串等價定義
bom_ddl = """
    bom_id STRING,
    parent_material STRING,
    child_material STRING,
    quantity_per DECIMAL(10,4),
    bom_level INT,
    plant STRING,
    effective_from STRING,
    effective_to STRING,
    scrap_rate DECIMAL(5,2)
"""
schema_from_ddl = spark.createDataFrame([], bom_ddl).schema
print(f"\n  DDL 定義的欄位數：{len(schema_from_ddl.fields)}")
print(f"  DDL vs Programmatic 欄位名稱一致：{[f.name for f in schema_from_ddl.fields] == [f.name for f in bom_schema.fields]}")

# ====================================================================
# 2. Schema Inference vs Explicit Schema — 效能與正確性對比
# ====================================================================
print("\n\n📊 Step 2: Schema Inference vs Explicit Schema 比較")

# 產生測試 BOM 資料（JSON）
tmp_dir = tempfile.mkdtemp(prefix="schema_test_")
json_path = os.path.join(tmp_dir, "bom_data")

bom_rows = []
for i in range(50000):
    row = {
        "bom_id": f"BOM-{i:06d}",
        "parent_material": f"FG-{i % 100:04d}",
        "child_material": f"RM-{i:06d}",
        "quantity_per": round(1.0 + (i % 10) * 0.2345, 4),
        "bom_level": i % 5 + 1,
        "plant": f"P{i % 3 + 1:02d}",
        "effective_from": "2026-01-01",
        "effective_to": None if i % 7 == 0 else "2026-12-31",
        "scrap_rate": round((i % 20) * 0.5, 2) if i % 5 != 0 else None,
    }
    bom_rows.append(row)

# 用 Spark 自己寫出 JSON（避免 pandas 依賴）
rdd = spark.sparkContext.parallelize([json.dumps(r) for r in bom_rows], 4)
rdd.saveAsTextFile(json_path)
print(f"  寫入 {len(bom_rows)} 筆 BOM JSON 到 {json_path}")

# -- 2a. Schema Inference（Spark 掃兩次）
t0 = time.time()
df_inferred = spark.read.json(json_path)
_ = df_inferred.count()  # force evaluation
t_infer = time.time() - t0

# -- 2b. Explicit Schema（Spark 只掃一次）
t0 = time.time()
df_explicit = spark.read.schema(bom_schema).json(json_path)
_ = df_explicit.count()
t_explicit = time.time() - t0

print(f"\n  Schema Inference：{t_infer:.3f}s")
print(f"  Explicit Schema ：{t_explicit:.3f}s")
print(f"  差異：{(t_infer - t_explicit) / t_explicit * 100:+.1f}%")

# -- 2c. 型別推斷比較
print("\n  型別推斷差異：")
for fi, fe in zip(df_inferred.schema.fields, df_explicit.schema.fields):
    match = "✅" if fi.dataType == fe.dataType else "❌"
    if fi.dataType != fe.dataType:
        print(f"    {match} {fi.name}: inferred={fi.dataType.simpleString()} vs explicit={fe.dataType.simpleString()}")

# quantity_per 可能被推斷為 double 而非 decimal
inferred_qty_type = df_inferred.schema["quantity_per"].dataType.simpleString()
explicit_qty_type = df_explicit.schema["quantity_per"].dataType.simpleString()
print(f"\n  💡 quantity_per 型別：")
print(f"     Inference  → {inferred_qty_type}")
print(f"     Explicit   → {explicit_qty_type}")
if inferred_qty_type != explicit_qty_type:
    print(f"     ⚠️ 型別不一致！Inference 推斷為 {inferred_qty_type}，可能導致精度問題")

# ====================================================================
# 3. DecimalType vs DoubleType — BOM 多層展開精度累積誤差
# ====================================================================
print("\n\n🔬 Step 3: DecimalType vs DoubleType — BOM 展開精度比較")
print("  模擬 10 層 BOM 展開，每層 quantity_per = 1.1234")
print("  理論值：1.1234^10 = ?")

# 純 Python 計算參考值
from decimal import Decimal as PyDec, getcontext
getcontext().prec = 50
ref_value = PyDec("1.1234") ** 10
print(f"  Python Decimal 參考值：{ref_value}")

# Spark DataFrame 模擬
levels = [(i, 1.1234, Decimal("1.1234")) for i in range(1, 11)]
level_schema = StructType([
    StructField("level", IntegerType()),
    StructField("qty_double", DoubleType()),
    StructField("qty_decimal", DecimalType(20, 10)),
])
level_df = spark.createDataFrame(levels, level_schema)

# 用 reduce 模擬逐層乘法
double_result = 1.0
decimal_result = Decimal("1.0")
double_chain = []
decimal_chain = []

for i in range(10):
    double_result *= 1.1234
    decimal_result *= Decimal("1.1234")
    double_chain.append(double_result)
    decimal_chain.append(float(decimal_result))

print(f"\n  逐層累積比較（每層 × 1.1234）：")
print(f"  {'層級':>4}  {'DoubleType':>22}  {'DecimalType':>22}  {'誤差':>15}")
print(f"  {'─' * 4}  {'─' * 22}  {'─' * 22}  {'─' * 15}")
for i in range(10):
    err = abs(double_chain[i] - decimal_chain[i])
    flag = "🔴" if err > 1e-10 else "🟢"
    print(f"  {i+1:4d}  {double_chain[i]:22.15f}  {decimal_chain[i]:22.15f}  {err:.2e} {flag}")

final_err = abs(double_chain[-1] - float(ref_value))
print(f"\n  💡 10 層展開後 Double 累積誤差：{final_err:.2e}")
print(f"     如果 BOM 中一顆 GPU $15,000，10 層後的數量誤差可能影響 ${final_err * 15000:.4f}")
print(f"     ⚠️ 結論：財務和 BOM 相關計算必須用 DecimalType！")

# ====================================================================
# 4. nullable 約束的「不檢查」陷阱
# ====================================================================
print("\n\n⚠️ Step 4: nullable=False 的「不檢查」陷阱")

strict_schema = StructType([
    StructField("vendor_id", StringType(), nullable=False),
    StructField("vendor_name", StringType(), nullable=False),
    StructField("avl_status", StringType(), nullable=False),
])

# PySpark 4.x nullable 檢查行為深度探索

# 示範 1：createDataFrame(verifySchema=True) 會擋住 null
try:
    dirty_data = [("V001", "Vendor A", "active"), ("V002", None, "active")]
    df_dirty = spark.createDataFrame(dirty_data, strict_schema)
    print("  ❌ 預期應該報錯但沒有")
except Exception as e:
    print(f"  ✅ createDataFrame(verifySchema=True) 擋住了 null: {type(e).__name__}")

# 示範 2：verifySchema=False → JVM codegen NPE（更危險！）
try:
    df_dirty2 = spark.createDataFrame(
        [("V001", "Vendor A", "active"), ("V002", None, "active")],
        strict_schema, verifySchema=False
    )
    df_dirty2.show()  # 這裡會 NPE
    print("  ❌ 預期應該 NPE 但沒有")
except Exception as e:
    print(f"  ✅ verifySchema=False 繞過 Python 檢查，但 JVM codegen 爬不過去：")
    print(f"     NullPointerException！Spark 4.x codegen 假設 non-nullable 欄位不會是 null")

# 示範 3：從 JSON 檔案讀取 — 這是生產最常見的場景
json_null_path = os.path.join(tmp_dir, "vendor_null")
null_json = ['{"vendor_id":"V001","vendor_name":"A","avl_status":"active"}',
             '{"vendor_id":"V002","vendor_name":null,"avl_status":"active"}',
             '{"vendor_id":null,"vendor_name":"C","avl_status":null}']
spark.sparkContext.parallelize(null_json, 1).saveAsTextFile(json_null_path)

# 用 nullable=True 的 schema 讀取（安全）
safe_schema = StructType([
    StructField("vendor_id", StringType(), nullable=True),
    StructField("vendor_name", StringType(), nullable=True),
    StructField("avl_status", StringType(), nullable=True),
])
df_from_file = spark.read.schema(safe_schema).json(json_null_path)
print("\n  從 JSON 讀取（nullable=True schema，資料有 null）：")
df_from_file.show()

null_counts = df_from_file.select(
    spark_sum(when(col("vendor_id").isNull(), 1).otherwise(0)).alias("vendor_id_nulls"),
    spark_sum(when(col("vendor_name").isNull(), 1).otherwise(0)).alias("vendor_name_nulls"),
    spark_sum(when(col("avl_status").isNull(), 1).otherwise(0)).alias("avl_status_nulls"),
).collect()[0]
print(f"  Null 計數：vendor_id={null_counts['vendor_id_nulls']}, "
      f"vendor_name={null_counts['vendor_name_nulls']}, "
      f"avl_status={null_counts['avl_status_nulls']}")

print(f"\n  💡 PySpark 4.x nullable 行為總結：")
print(f"     1. createDataFrame(verifySchema=True)：Python 層檢查，擋住 null ✅")
print(f"     2. createDataFrame(verifySchema=False)：JVM codegen NPE 💥")
print(f"        Spark 4.x codegen 假設 non-nullable 不會是 null，直接 crash")
print(f"     3. read.schema().json()：JSON reader 會把 null 填入，不管 nullable flag")
print(f"        → 生產最常見場景！Schema 控制不了來源資料的品質")
print(f"     → 結論：nullable 是 Catalyst 優化提示 + codegen 安全提示")
print(f"     → 資料品質檢查必須用 GX/dbt test，不能依賴 nullable 標記")

# ====================================================================
# 5. Schema 序列化/反序列化（版本控制）
# ====================================================================
print("\n\n📦 Step 5: Schema 序列化 — 版本控制與跨系統傳遞")

schema_json_str = bom_schema.json()
schema_dict = json.loads(schema_json_str)
print(f"  Schema JSON 大小：{len(schema_json_str)} bytes")
print(f"  欄位數：{len(schema_dict['fields'])}")

# 反序列化
restored = StructType.fromJson(schema_dict)
print(f"  反序列化後欄位數：{len(restored.fields)}")
print(f"  原始 == 反序列化：{bom_schema == restored}")

# 存到檔案（模擬 Git 版本控制）
schema_file = os.path.join(tmp_dir, "bom_schema_v1.json")
with open(schema_file, "w") as f:
    json.dump(schema_dict, f, indent=2)
print(f"  Schema 已存到：{schema_file}")
print(f"  💡 實務：Schema JSON 存入 Git，每次 schema 變更都有 commit history")

# ====================================================================
# 6. Schema Evolution — ECO 加欄位
# ====================================================================
print("\n\n🔄 Step 6: Schema Evolution — 模擬 ECO 加欄位")

# v1 schema 資料
v1_data = [("BOM-001", "FG-001", "RM-001", Decimal("2.5000"), 1, "P01")]
v1_schema = StructType([
    StructField("bom_id", StringType()),
    StructField("parent_material", StringType()),
    StructField("child_material", StringType()),
    StructField("quantity_per", DecimalType(10, 4)),
    StructField("bom_level", IntegerType()),
    StructField("plant", StringType()),
])
df_v1 = spark.createDataFrame(v1_data, v1_schema)

# v2 schema（ECO 加了 eco_number 和 eco_date）
v2_data = [("BOM-002", "FG-001", "RM-002", Decimal("3.0000"), 1, "P01", "ECO-2026-042", "2026-05-30")]
v2_schema = StructType([
    StructField("bom_id", StringType()),
    StructField("parent_material", StringType()),
    StructField("child_material", StringType()),
    StructField("quantity_per", DecimalType(10, 4)),
    StructField("bom_level", IntegerType()),
    StructField("plant", StringType()),
    StructField("eco_number", StringType()),
    StructField("eco_date", StringType()),
])
df_v2 = spark.createDataFrame(v2_data, v2_schema)

print("  v1 Schema（原始）：")
df_v1.printSchema()
print("  v2 Schema（ECO 加欄位後）：")
df_v2.printSchema()

# 合併：v1 缺的欄位自動填 null
from pyspark.sql.functions import lit as spark_lit
for field in v2_schema.fields:
    if field.name not in [f.name for f in v1_schema.fields]:
        df_v1 = df_v1.withColumn(field.name, spark_lit(None).cast(field.dataType))

merged = df_v1.unionByName(df_v2)
print("  合併後（v1 + v2）：")
merged.show(truncate=False)
print("  💡 Schema Evolution 策略：")
print("     Additive（加欄位）→ 安全，舊資料填 null")
print("     Type Change → 需要 Type Widening（Int→Long 安全，String→Int 不安全）")
print("     Delete Column → 危險，下游依賴可能斷掉")

# ====================================================================
# 7. 巢狀 Schema（Nested StructType）
# ====================================================================
print("\n\n🏗️ Step 7: 巢狀 Schema — 供應商 + 地址 + 認證")

vendor_schema = StructType([
    StructField("vendor_id", StringType(), False),
    StructField("vendor_name", StringType(), False),
    StructField("address", StructType([
        StructField("city", StringType()),
        StructField("country", StringType()),
        StructField("postal_code", StringType()),
    ]), True),
    StructField("certifications", ArrayType(StringType()), True),
    StructField("contacts", MapType(StringType(), StringType()), True),
])

vendor_data = [
    ("V001", "Samsung", ("Seoul", "KR", "06235"), ["ISO9001", "IATF16949"], {"sales": "kim@samsung.com"}),
    ("V002", "SK Hynix", ("Icheon", "KR", "17336"), ["ISO9001"], {"sales": "lee@skhynix.com", "tech": "park@skhynix.com"}),
    ("V003", "Micron", ("Boise", "US", "83716"), None, None),
]
df_vendor = spark.createDataFrame(vendor_data, vendor_schema)

print("  巢狀 Schema：")
df_vendor.printSchema()
print("  存取巢狀欄位：")
df_vendor.select(
    "vendor_id",
    "vendor_name",
    col("address.city").alias("city"),
    col("address.country").alias("country"),
).show()

# 展開 Array
from pyspark.sql.functions import explode_outer
print("  展開 certifications Array：")
df_vendor.select("vendor_id", explode_outer("certifications").alias("cert")).show()

# ====================================================================
# 總結
# ====================================================================
print("\n" + "=" * 70)
print("  📝 總結：Schema 與型別系統的 5 個關鍵教訓")
print("=" * 70)
print("""
  1. 生產環境永遠用 Explicit Schema
     → 避免 inference 的效能損失和型別不確定性

  2. 財務/BOM 計算用 DecimalType，不用 DoubleType
     → 10 層 BOM 展開，Double 的累積誤差可能影響財務報表

  3. nullable=False 不是約束，是優化提示
     → Spark 不會拒絕 null 值，需要額外的 data quality check

  4. Schema JSON 存入 Git 做版本控制
     → 每次 ECO / schema 變更都有 audit trail

  5. Schema Evolution 只做 additive change
     → 加欄位安全，改型別需要 Type Widening，刪欄位需要 deprecation 期
""")

# 清理
shutil.rmtree(tmp_dir, ignore_errors=True)
spark.stop()
print("✅ 練習完成")
