"""
Z-ORDER 多維度聚簇實作練習
場景：ODM 工廠庫存異動表的查詢優化

學習日期：2026-05-09
科目：Delta-Lake（第 3 課）
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, TimestampType, DateType
)
import random
import json
import os
import shutil

# ===== 初始化 Spark（本地模式 + Delta Lake） =====
from delta.pip_utils import configure_spark_with_delta_pip

builder = (SparkSession.builder
    .appName("Z-ORDER Practice - ODM Inventory")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # 設定較小的檔案大小以便觀察效果
    .config("spark.databricks.delta.optimize.maxFileSize", str(32 * 1024 * 1024))  # 32MB
    .config("spark.sql.shuffle.partitions", "8")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ===== 模擬資料：ODM 庫存異動 =====
TABLE_PATH = "/tmp/delta-zorder-practice/inventory_movements"

# 清理舊資料
if os.path.exists("/tmp/delta-zorder-practice"):
    shutil.rmtree("/tmp/delta-zorder-practice")

# 15 個廠區（模擬緯穎的全球佈局）
PLANTS = [f"PLANT-{i:03d}" for i in range(1, 16)]

# 3000 種料號（模擬伺服器零件：CPU、RAM、SSD、PSU、NIC 等）
PART_CATEGORIES = ["CPU", "RAM", "SSD", "PSU", "NIC", "HDD", "FAN", "CABLE", "BOARD", "CHASSIS"]
PARTS = [f"{cat}-{i:04d}" for cat in PART_CATEGORIES for i in range(1, 301)]

# 異動類型
MOVEMENT_TYPES = ["GR", "GI", "TR", "ADJ"]  # 收貨/發料/轉撥/調整

print("=" * 60)
print("🏭 ODM 庫存異動表 Z-ORDER 實作練習")
print("=" * 60)

# ===== Step 1：產生模擬資料（50 萬筆，模擬 10 天的異動） =====
print("\n📊 Step 1: 產生模擬庫存異動資料...")

NUM_RECORDS = 500_000
dates = [f"2026-05-{d:02d}" for d in range(1, 11)]  # 10 天

# 用 Spark 產生隨機資料
data_rows = []
for _ in range(NUM_RECORDS):
    data_rows.append((
        random.choice(dates),
        random.choice(PLANTS),
        random.choice(PARTS),
        random.choice(MOVEMENT_TYPES),
        round(random.uniform(1, 1000), 2),  # 數量
        random.choice(["EA", "KG", "M"]),   # 單位
    ))

schema = StructType([
    StructField("date", StringType(), False),
    StructField("plant_id", StringType(), False),
    StructField("part_number", StringType(), False),
    StructField("movement_type", StringType(), False),
    StructField("quantity", DoubleType(), False),
    StructField("unit", StringType(), False),
])

df = spark.createDataFrame(data_rows, schema)

# 寫入 Delta Lake（按 date 分區）
(df.write
    .format("delta")
    .partitionBy("date")
    .mode("overwrite")
    .save(TABLE_PATH)
)

print(f"  ✅ 寫入 {NUM_RECORDS:,} 筆資料，分區 by date（{len(dates)} 天）")
print(f"  ✅ {len(PLANTS)} 個廠區，{len(PARTS)} 種料號")

# ===== Step 2：查詢效能基準（無 Z-ORDER） =====
print("\n📊 Step 2: 查詢效能基準（無 Z-ORDER）...")

# 註冊為 SQL 表
spark.sql(f"CREATE TABLE IF NOT EXISTS inventory_delta USING delta LOCATION '{TABLE_PATH}'")

# 典型查詢：某廠某天某料號的庫存消耗
query_result_before = spark.sql("""
    SELECT plant_id, part_number, movement_type,
           SUM(quantity) as total_qty, COUNT(*) as record_count
    FROM delta.`{path}`
    WHERE date = '2026-05-05'
      AND plant_id = 'PLANT-007'
      AND part_number = 'CPU-0042'
    GROUP BY plant_id, part_number, movement_type
""".format(path=TABLE_PATH))

# 執行查詢並收集 metrics
query_result_before.collect()

# 讀取 _delta_log 了解檔案結構
from delta.tables import DeltaTable
dt = DeltaTable.forPath(spark, TABLE_PATH)
detail_before = dt.detail().collect()[0]

print(f"  表大小：{detail_before['sizeInBytes'] / 1024 / 1024:.1f} MB")
print(f"  檔案數：{detail_before['numFiles']}")

# 統計 date='2026-05-05' 分區的檔案數
log_df = spark.read.format("delta").load(TABLE_PATH)
partition_files_before = (log_df
    .filter(F.col("date") == "2026-05-05")
    .inputFiles()
)
print(f"  date='2026-05-05' 分區的檔案數：{len(partition_files_before)}")

# ===== Step 3：執行 OPTIMIZE ZORDER BY =====
print("\n⚡ Step 3: 執行 OPTIMIZE ZORDER BY (plant_id, part_number)...")

optimize_result = spark.sql(f"""
    OPTIMIZE delta.`{TABLE_PATH}`
    WHERE date = '2026-05-05'
    ZORDER BY (plant_id, part_number)
""")

# 顯示 OPTIMIZE 結果
opt_metrics = optimize_result.collect()[0]
print(f"  結果：")
print(f"    {opt_metrics}")

# 重新檢查檔案結構
detail_after = dt.detail().collect()[0]
partition_files_after = (spark.read.format("delta").load(TABLE_PATH)
    .filter(F.col("date") == "2026-05-05")
    .inputFiles()
)
print(f"\n  OPTIMIZE 後：")
print(f"    表大小：{detail_after['sizeInBytes'] / 1024 / 1024:.1f} MB")
print(f"    總檔案數：{detail_after['numFiles']}")
print(f"    date='2026-05-05' 分區檔案數：{len(partition_files_after)}")

# ===== Step 4：查詢效能對比（有 Z-ORDER） =====
print("\n📊 Step 4: 查詢效能對比（有 Z-ORDER）...")

query_result_after = spark.sql("""
    SELECT plant_id, part_number, movement_type,
           SUM(quantity) as total_qty, COUNT(*) as record_count
    FROM delta.`{path}`
    WHERE date = '2026-05-05'
      AND plant_id = 'PLANT-007'
      AND part_number = 'CPU-0042'
    GROUP BY plant_id, part_number, movement_type
""".format(path=TABLE_PATH))

results = query_result_after.collect()
print(f"  查詢結果：{len(results)} 筆")
for r in results:
    print(f"    {r['movement_type']}: qty={r['total_qty']:.1f}, records={r['record_count']}")

# ===== Step 5：檢查 _delta_log 的統計資訊 =====
print("\n📊 Step 5: 檢查 _delta_log 的 data skipping 統計...")

import glob

log_dir = os.path.join(TABLE_PATH, "_delta_log")
json_files = sorted(glob.glob(os.path.join(log_dir, "*.json")))

if json_files:
    latest_log = json_files[-1]
    print(f"  最新 log: {os.path.basename(latest_log)}")
    
    with open(latest_log, 'r') as f:
        for line in f:
            entry = json.loads(line)
            if 'add' in entry:
                stats = entry['add'].get('stats', '')
                if stats:
                    stats_dict = json.loads(stats) if isinstance(stats, str) else stats
                    path_short = entry['add']['path'][-40:]
                    num_records = stats_dict.get('numRecords', 'N/A')
                    min_vals = stats_dict.get('minValues', {})
                    max_vals = stats_dict.get('maxValues', {})
                    
                    plant_min = min_vals.get('plant_id', 'N/A')
                    plant_max = max_vals.get('plant_id', 'N/A')
                    part_min = min_vals.get('part_number', 'N/A')
                    part_max = max_vals.get('part_number', 'N/A')
                    
                    print(f"\n  檔案: ...{path_short}")
                    print(f"    records: {num_records}")
                    print(f"    plant_id:    [{plant_min} ~ {plant_max}]")
                    print(f"    part_number: [{part_min} ~ {part_max}]")
                    break  # 只顯示第一個 add entry 作為示例

# ===== Step 6：Z-ORDER 欄位數量效果對比 =====
print("\n📊 Step 6: Z-ORDER 欄位數量對比實驗...")
print("  （概念說明：欄位越多，單一欄位的聚簇效果越差）")
print()
print("  建議策略：")
print("  ├── 2 欄位 Z-ORDER (plant_id, part_number)   → 最佳多維範圍查詢")
print("  ├── + Bloom Filter on customer_id             → 補充精確點查需求")
print("  └── 或升級 Liquid Clustering                   → 一勞永逸")

# ===== 總結 =====
print("\n" + "=" * 60)
print("📝 Z-ORDER 實作總結")
print("=" * 60)
print(f"""
1. Partition by date → 物理隔離時間維度
2. Z-ORDER by (plant_id, part_number) → 聚簇高基數查詢欄位
3. Data skipping 透過 min/max 統計跳過不相關檔案
4. 欄位選擇限制 2-3 個，超過效果遞減
5. 需要定期排程 OPTIMIZE ZORDER（離峰時段）

ODM 實務：
- 庫存異動表：Z-ORDER by (plant_id, part_number)
- 採購訂單表：Z-ORDER by (vendor_id, material_group)
- 品質檢測表：Z-ORDER by (production_line, defect_code)

SA 選型結論：
- 新表（Databricks 13.3+）→ 直接用 Liquid Clustering
- 舊表/OSS Delta → Partition + Z-ORDER + Bloom Filter
""")

spark.stop()
print("✅ 練習完成！")
