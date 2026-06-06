"""
Practice: Table Format 概念 — Parquet vs Delta Lake 對比
場景：ODM 庫存異動資料的 Schema Evolution、ACID、Time Travel

目標：體會為什麼 Table Format（Delta Lake / Iceberg）是 Lakehouse 的必要層
"""

import os
import shutil
from datetime import datetime, date
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType, DateType, DoubleType
)
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# ── Spark Session (with Delta Lake jars) ──
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder
    .appName("TableFormatConcept")
    .master("local[2]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalogExtension")
    .config("spark.sql.warehouse.dir", "/tmp/scholar-table-format-practice")
    .config("spark.driver.memory", "2g")
    .config("spark.ui.showConsoleProgress", "false")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

BASE_DIR = "/tmp/scholar-table-format-practice"
PARQUET_PATH = f"{BASE_DIR}/parquet_inventory"
DELTA_PATH = f"{BASE_DIR}/delta_inventory"

# 清理舊資料
for p in [PARQUET_PATH, DELTA_PATH]:
    if os.path.exists(p):
        shutil.rmtree(p)


# ═══════════════════════════════════════════════
# Part 1: 初始 Schema — 庫存異動資料（SAP MATDOC 風格）
# ═══════════════════════════════════════════════

schema_v1 = StructType([
    StructField("material_doc_id", StringType(), False),
    StructField("material_number", StringType(), False),
    StructField("plant", StringType(), False),
    StructField("storage_location", StringType(), False),
    StructField("movement_type", StringType(), False),   # 101=GR, 261=Issue, 601=GI
    StructField("quantity", IntegerType(), False),
    StructField("posting_date", DateType(), False),
])

data_v1 = [
    ("5000001", "MAT-CPU-001", "TW01", "RM01", "101", 500, date(2026, 6, 1)),
    ("5000002", "MAT-CPU-001", "TW01", "RM01", "261", -100, date(2026, 6, 2)),
    ("5000003", "MAT-MEM-002", "TW01", "RM01", "101", 1000, date(2026, 6, 1)),
    ("5000004", "MAT-SSD-003", "TW01", "RM02", "101", 200, date(2026, 6, 3)),
    ("5000005", "MAT-CPU-001", "TW01", "WIP01", "261", -50, date(2026, 6, 4)),
]

df_v1 = spark.createDataFrame(data_v1, schema=schema_v1)

print("=" * 70)
print("Part 1: 初始寫入 — Parquet vs Delta")
print("=" * 70)

# 寫入 Parquet
df_v1.write.mode("overwrite").parquet(PARQUET_PATH)
print(f"✅ Parquet 寫入完成: {PARQUET_PATH}")

# 寫入 Delta
df_v1.write.format("delta").mode("overwrite").save(DELTA_PATH)
print(f"✅ Delta  寫入完成: {DELTA_PATH}")


# ═══════════════════════════════════════════════
# Part 2: Schema Evolution — 新增欄位
# 場景：需要加入 batch_number（批次號）欄位
# ═══════════════════════════════════════════════

print("\n" + "=" * 70)
print("Part 2: Schema Evolution — 新增 batch_number 欄位")
print("=" * 70)

# 新資料帶 batch_number
data_v2 = [
    ("5000006", "MAT-CPU-001", "TW01", "RM01", "101", 300, date(2026, 6, 5), "BATCH-2026-06-A"),
    ("5000007", "MAT-MEM-002", "TW01", "RM01", "261", -200, date(2026, 6, 5), "BATCH-2026-06-B"),
]

schema_v2 = StructType(schema_v1.fields + [
    StructField("batch_number", StringType(), True),
])

df_v2 = spark.createDataFrame(data_v2, schema=schema_v2)

# --- Parquet: Schema Evolution 是痛苦的 ---
print("\n📦 Parquet Schema Evolution:")
try:
    # Append 新 schema 到舊 Parquet → Schema 不匹配
    # Parquet 可以用 mergeSchema，但需要 Spark 特殊選項
    df_v2.write.mode("append").parquet(PARQUET_PATH)
    print("  Parquet append 成功（但 schema 可能不一致）")

    # 讀回時可能遇到問題
    df_read = spark.read.option("mergeSchema", "true").parquet(PARQUET_PATH)
    print(f"  讀回 schema 欄位數: {len(df_read.columns)}")
    print(f"  ⚠️  Parquet 需要 mergeSchema 選項，且不保證所有引擎都支援")
    print(f"  ⚠️  舊資料的 batch_number 自動為 null，但沒有 Schema Enforcement")
except Exception as e:
    print(f"  ❌ Parquet Schema Evolution 失敗: {e}")

# --- Delta: Schema Evolution 是乾淨的 ---
print("\n🔷 Delta Schema Evolution:")
try:
    df_v2.write.format("delta").mode("append") \
        .option("mergeSchema", "true") \
        .save(DELTA_PATH)
    print("  ✅ Delta mergeSchema append 成功")

    dt = DeltaTable.forPath(spark, DELTA_PATH)
    print(f"  Schema 自動演進，新欄位數: {len(dt.toDF().columns)}")
    print(f"  Schema: {[f.name for f in dt.toDF().schema.fields]}")
    print(f"  ✅ Delta 有 Schema Enforcement：如果型別不對會直接報錯（不像 Parquet 靜默出錯）")
except Exception as e:
    print(f"  ❌ Delta Schema Evolution 失敗: {e}")


# ═══════════════════════════════════════════════
# Part 3: Time Travel — 回到過去
# ═══════════════════════════════════════════════

print("\n" + "=" * 70)
print("Part 3: Time Travel")
print("=" * 70)

# --- Parquet: 沒有 Time Travel ---
print("\n📦 Parquet Time Travel:")
print("  ❌ Parquet 沒有版本概念。覆寫就沒了。")
print("  ❌ 如果今天的 append 有問題，只能手動找備份恢復。")

# --- Delta: Version-based Time Travel ---
print("\n🔷 Delta Time Travel:")
dt = DeltaTable.forPath(spark, DELTA_PATH)
history = dt.history()
print("  版本歷史：")
history.select("version", "timestamp", "operation", "operationMetrics").show(truncate=False)

# 查看 Version 0（初始寫入，沒有 batch_number）
df_v0 = spark.read.format("delta").option("versionAsOf", 0).load(DELTA_PATH)
print(f"  Version 0 欄位: {df_v0.columns}")
print(f"  Version 0 筆數: {df_v0.count()}")

# 查看 Version 1（加了 batch_number）
df_v1_delta = spark.read.format("delta").option("versionAsOf", 1).load(DELTA_PATH)
print(f"  Version 1 欄位: {df_v1_delta.columns}")
print(f"  Version 1 筆數: {df_v1_delta.count()}")


# ═══════════════════════════════════════════════
# Part 4: ACID — MERGE Upsert（Delta 才能做）
# 場景：修正一筆庫存異動的數量（更正錯帳）
# ═══════════════════════════════════════════════

print("\n" + "=" * 70)
print("Part 4: ACID MERGE — 更正錯帳")
print("=" * 70)

# --- Parquet: 沒有 MERGE ---
print("\n📦 Parquet MERGE:")
print("  ❌ Parquet 不支援 MERGE。要修改一筆資料 =")
print("     1. 讀出全量 → 2. 過濾+修改 → 3. 覆寫全表")
print("     → 如果中途崩潰 = 資料丟失")

# --- Delta: MERGE 是原子操作 ---
print("\n🔷 Delta MERGE:")

correction_data = [("5000002", -120)]  # 修正：原本 -100 → -120
correction_df = spark.createDataFrame(
    correction_data,
    schema=StructType([
        StructField("material_doc_id", StringType(), False),
        StructField("corrected_qty", IntegerType(), False),
    ])
)

dt = DeltaTable.forPath(spark, DELTA_PATH)
dt.alias("target").merge(
    correction_df.alias("source"),
    "target.material_doc_id = source.material_doc_id"
).whenMatchedUpdate(set={
    "quantity": "source.corrected_qty"
}).execute()

print("  ✅ MERGE 完成（原子操作，不怕中途崩潰）")

# 驗證修正結果
result = spark.read.format("delta").load(DELTA_PATH) \
    .filter(F.col("material_doc_id") == "5000002") \
    .select("material_doc_id", "quantity")
result.show()
print("  ✅ quantity 已從 -100 修正為 -120")


# ═══════════════════════════════════════════════
# Part 5: Transaction Log 結構觀察
# ═══════════════════════════════════════════════

print("\n" + "=" * 70)
print("Part 5: Delta Transaction Log 結構")
print("=" * 70)

import glob

log_files = sorted(glob.glob(f"{DELTA_PATH}/_delta_log/*.json"))
print(f"  Transaction Log 檔案數: {len(log_files)}")
for f in log_files:
    print(f"  📄 {os.path.basename(f)}")

# 讀最新 log 內容看結構
if log_files:
    with open(log_files[-1], 'r') as f:
        lines = f.readlines()[:5]
    print(f"\n  最新 log ({os.path.basename(log_files[-1])}) 前 5 行：")
    for line in lines:
        print(f"    {line.strip()[:120]}...")


# ═══════════════════════════════════════════════
# Summary
# ═══════════════════════════════════════════════

print("\n" + "=" * 70)
print("Summary: 為什麼需要 Table Format")
print("=" * 70)
print("""
┌──────────────────┬────────────────────┬────────────────────┐
│ 能力              │ 裸 Parquet         │ Table Format       │
│                   │                    │ (Delta/Iceberg)    │
├──────────────────┼────────────────────┼────────────────────┤
│ ACID Transaction │ ❌ 不支援           │ ✅ 原子性 MERGE     │
│ Schema Evolution │ ⚠️ 手動 merge      │ ✅ 自動+Enforcement │
│ Time Travel      │ ❌ 覆寫即丟失       │ ✅ VERSION AS OF    │
│ 並行安全          │ ❌ 互踩風險         │ ✅ Optimistic CC    │
│ 查詢規劃          │ O(n) 目錄掃描      │ O(1) Manifest/Log  │
│ 更新/刪除         │ 全量覆寫            │ 行級 MERGE/DELETE  │
└──────────────────┴────────────────────┴────────────────────┘

結論：Table Format 是 Data Lakehouse 的基石。
      沒有它，Data Lake 只是一堆檔案。
      有了它，Data Lake 變成具備 DW 能力的 Lakehouse。
""")

spark.stop()
print("✅ Practice 完成")
