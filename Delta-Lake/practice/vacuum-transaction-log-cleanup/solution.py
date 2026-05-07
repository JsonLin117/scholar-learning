"""
Delta Lake VACUUM 實作練習
場景：ODM 供應鏈 — 庫存管理 Lakehouse 的 VACUUM 策略

技術重點：
- VACUUM 清理過期的舊版本 Parquet 檔
- 保留期間設計（考慮 Streaming 限制）
- OPTIMIZE + VACUUM 排程策略
"""

import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from datetime import datetime

# ===== 初始化 Spark Session (Delta Lake 模式) =====
builder = (SparkSession.builder
    .appName("ODM_Inventory_VACUUM_Demo")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))

spark = configure_spark_with_delta_pip(builder).getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ===== 準備測試路徑 =====
import tempfile, shutil
TABLE_PATH = "/tmp/odm_inventory_vacuum_demo"
if os.path.exists(TABLE_PATH):
    shutil.rmtree(TABLE_PATH)

print("=" * 60)
print("🏭 ODM 庫存表 VACUUM 策略實作")
print("=" * 60)

# ===== Step 1：建立模擬的庫存 Delta Table =====
print("\n📦 Step 1：建立全球零件庫存表（模擬 ODM 供應鏈）")

schema = StructType([
    StructField("part_id", StringType(), False),       # 零件 ID（如 CPU/DRAM）
    StructField("location", StringType(), False),      # 工廠代碼（TW01, CN02...）
    StructField("qty_on_hand", IntegerType(), True),   # 手上庫存量
    StructField("last_updated", StringType(), True),   # 最後更新時間
])

# 模擬 100 個零件在 3 個工廠的庫存
initial_data = []
for part_num in range(1, 11):  # 10 個零件（示範用，實際幾百萬）
    for factory in ["TW01", "CN02", "MX03"]:
        initial_data.append((
            f"PART-{part_num:04d}",
            factory,
            1000 + part_num * 10,
            "2026-05-07 07:00:00"
        ))

df = spark.createDataFrame(initial_data, schema)
df.write.format("delta").save(TABLE_PATH)
print(f"  ✅ 初始資料寫入：{df.count()} 筆")

# ===== Step 2：模擬頻繁的庫存更新（產生舊版本） =====
print("\n🔄 Step 2：模擬高頻庫存異動（每次更新產生舊版本 Parquet 檔）")

delta_table = DeltaTable.forPath(spark, TABLE_PATH)

for update_round in range(1, 4):  # 模擬 3 輪更新
    # 每輪更新所有零件在 TW01 工廠的庫存
    update_time = f"2026-05-07 0{6+update_round}:00:00"
    
    # UPDATE：庫存異動（進貨/出貨）
    delta_table.update(
        condition="location = 'TW01'",
        set={
            "qty_on_hand": f"qty_on_hand + {update_round * 50}",
            "last_updated": f"'{update_time}'"
        }
    )
    print(f"  🔁 Round {update_round}：更新 TW01 庫存，qty +{update_round * 50}")

print(f"\n  ✅ 模擬完成：產生了 3 個版本的舊 Parquet 檔（這些就是 VACUUM 要清理的垃圾）")

# ===== Step 3：用 DESCRIBE HISTORY 診斷問題 =====
print("\n🔍 Step 3：診斷 — DESCRIBE HISTORY 查看版本歷史")

history_df = spark.sql(f"DESCRIBE HISTORY delta.`{TABLE_PATH}`")
history_df.select("version", "timestamp", "operation").show(10, truncate=False)

# 查看目前有多少檔案（含舊版本）
import glob
parquet_files = glob.glob(f"{TABLE_PATH}/**/*.parquet", recursive=True)
print(f"  📊 目前底層 Parquet 檔案數量（含舊版本垃圾）：{len(parquet_files)} 個")

# ===== Step 4：VACUUM DRY RUN — 確認哪些檔案會被刪除 =====
print("\n🧪 Step 4：VACUUM DRY RUN（先確認，不實際刪除）")

# 注意：測試環境允許 retention = 0，生產環境絕對不能這樣設！
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

dry_run_df = spark.sql(f"""
    VACUUM delta.`{TABLE_PATH}` 
    RETAIN 0 HOURS 
    DRY RUN
""")
files_to_delete = dry_run_df.count()
print(f"  🗑️  DRY RUN 結果：共 {files_to_delete} 個檔案會被刪除")
if files_to_delete > 0:
    dry_run_df.show(5, truncate=False)

# ===== Step 5：設計生產環境 VACUUM 策略 =====
print("\n🏗️ Step 5：生產環境 VACUUM 策略設計")
print("""
  策略設計（考慮 Streaming 的 ODM 庫存 pipeline）：
  
  情境：
  - Spark Structured Streaming 最長可能停機 3 天後重啟
  - 重啟後需要從 checkpoint 回補最多 3 天的資料
  
  結論：
  - VACUUM retention 必須 > 3 天 → 設為 14 天（雙倍 buffer，安全）
  - OPTIMIZE 排程：每天一次（高頻寫入表）
  - VACUUM 排程：每週一次（retention = 14 天）
  
  ⚠️  反模式（不要這樣做）：
  - stream log retention = 30 天，但 VACUUM retention = 7 天
    → Streaming 以為可以回溯 30 天，但實際檔案只有 7 天 → 假安全感！
""")

# ===== Step 6：設定 Table Properties =====
print("\n⚙️  Step 6：設定 Table Properties")

spark.sql(f"""
    ALTER TABLE delta.`{TABLE_PATH}`
    SET TBLPROPERTIES (
        'delta.deletedFileRetentionDuration' = 'interval 14 days',
        'delta.logRetentionDuration' = 'interval 30 days',
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
""")

props_df = spark.sql(f"SHOW TBLPROPERTIES delta.`{TABLE_PATH}`")
props_df.filter("key LIKE 'delta.%Retention%'").show(truncate=False)
print("  ✅ 已設定保留期：deletedFile=14天，log=30天（> Streaming 最長停機 3 天）")

# ===== Step 7：實際執行 VACUUM（測試環境用 retention=0 清空） =====
print("\n🚿 Step 7：執行 VACUUM（測試環境 retention=0，清理所有舊版本）")

before_files = len(glob.glob(f"{TABLE_PATH}/**/*.parquet", recursive=True))

delta_table.vacuum(retentionHours=0)

after_files = len(glob.glob(f"{TABLE_PATH}/**/*.parquet", recursive=True))

print(f"  📊 清理前 Parquet 檔案數：{before_files}")
print(f"  📊 清理後 Parquet 檔案數：{after_files}")
print(f"  🗑️  刪除了 {before_files - after_files} 個廢棄舊版本檔案")

# ===== Step 8：驗證資料完整性 =====
print("\n✅ Step 8：驗證資料完整性")

current_df = spark.read.format("delta").load(TABLE_PATH)
print(f"  📦 當前庫存資料筆數：{current_df.count()} 筆（應保持不變）")
print("\n  TW01 工廠庫存（最新版）：")
current_df.filter("location = 'TW01'").select("part_id", "qty_on_hand", "last_updated").show(5)

# ===== 總結 =====
print("\n" + "=" * 60)
print("📋 VACUUM 實作總結")
print("=" * 60)
print("""
  ✅ 核心學到的：
  1. VACUUM 清理「已作廢 + 超過 retention」的舊 Parquet 檔
  2. 每次 UPDATE/DELETE 都產生舊版本（Soft Delete），需要 VACUUM 清理
  3. 保留期 > Streaming 最長停機時間（避免任務崩潰）
  4. OPTIMIZE（製造垃圾）永遠要配 VACUUM（清理垃圾）
  
  ⚠️  生產環境警告：
  - 永遠不要 retention=0（除非測試）
  - 先 DRY RUN，再實際執行
  - 排程 VACUUM 前確認 Streaming checkpoint 狀態
""")

spark.stop()
