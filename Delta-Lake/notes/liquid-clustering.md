# Liquid Clustering：取代分區和 Z-ORDER 的下一代資料佈局技術

> 學習日期：2026-05-16 | 科目：Delta-Lake（main）| 進度：第 4 課
> 本科目目標：能設計 production-grade Lakehouse 架構：Medallion 分層、OPTIMIZE 策略、Unity Catalog governance，並能做 Delta Lake 的效能調教和版本管理

## 這是什麼？

Liquid Clustering 是 Delta Lake 的**新一代資料佈局優化技術**，用 `CLUSTER BY` 語法完全取代傳統的 Hive-style 分區和 Z-ORDER。它的核心創新是：**打破實體分區目錄的限制**，在全表範圍內用空間填充曲線（Hilbert Curve）做多維度聚簇，且允許隨時用 `ALTER TABLE` 修改 clustering key，不需要重寫現有資料。

**一句話：Liquid Clustering 讓你不用再煩惱「怎麼切 Partition」，還給你隨時改主意的彈性。**

## 為什麼重要？

Z-ORDER 已經很好了，但它有三個致命痛點：

| Z-ORDER 痛點 | Liquid Clustering 解法 |
|---|---|
| **被困在傳統分區內**：Z-ORDER 只能在單一 partition 內重排，partition key 選錯就完蛋 | 完全淘汰分區，在全表範圍做聚簇 |
| **改不掉**：partition 方式一旦決定，要改就得重寫整張表 | `ALTER TABLE ... CLUSTER BY (new_cols)` 隨時改，已有資料不需重寫 |
| **純事後優化**：Z-ORDER 100% 依賴手動排程 `OPTIMIZE ZORDER` | **Eager Clustering**：寫入時就做初步聚簇，減輕後續 OPTIMIZE 負擔 |
| **寫入衝突多**：partition 邊界導致多個寫入/OPTIMIZE 操作互搶同一 partition | **Row-level concurrency**：無分區界線 + Deletion Vectors，衝突大幅降低 |

## 怎麼運作？

### 核心機制：Hilbert Curve + Eager Clustering

**1. Hilbert Curve（比 Z-ORDER 的 Morton Curve 更好的空間填充曲線）**
- Morton Curve（Z-ORDER）：在空間中走 Z 字形，相鄰的轉彎處會有「跳躍」
- Hilbert Curve：連續且不跳躍，資料局部性（data locality）更好
- 結果：同樣的 clustering key，Hilbert Curve 產生的檔案 min/max 範圍更緊湊 → data skipping 更有效

**2. Eager Clustering（寫入時初步聚簇）**
- 當新寫入資料量 < 512 GB（預設門檻），在寫入當下就在記憶體內做初步排序
- 這是「盡力而為（best-effort）」的機制——不會拖慢寫入速度
- 結果：新寫入的資料立刻有不錯的聚簇品質，不需要等 OPTIMIZE

**3. OPTIMIZE 仍然需要（深度掃除）**
- Eager Clustering 只做初步整理
- 定期執行 `OPTIMIZE` 進行深度 compaction + 重新聚簇
- `OPTIMIZE FULL`：改過 clustering key 後必須跑，強制全表重新聚簇

**4. Row-level Concurrency**
- 無分區目錄 → 不存在「兩個操作搶同一 partition」的問題
- 搭配 Deletion Vectors：刪除/更新時先標記，不立刻重寫檔案
- 只有同一時間修改同一列（row）才會衝突

### 支援的資料類型（Clustering Key）

`Date`, `Timestamp`, `TimestampNTZ`（14.3+）, `String`, `Integer/Long/Short/Byte`, `Float/Double/Decimal`

### 限制

- 最多 **4 個** clustering key
- 欄位必須有 statistics collected（預設前 32 欄）
- **不能與 partitioning 或 ZORDER 共存**（建表時二選一）
- Delta writer v7 / reader v3 → 舊版 client 無法讀取
- 小於 10TB 的表用 4 個 key 可能比 2 個 key 慢（key 越多，單一欄位過濾效能越差）

## 程式碼範例

```sql
-- ===== 建表時啟用 =====
CREATE TABLE inventory_movements (
    movement_date DATE,
    factory_id STRING,
    part_number STRING,
    movement_type STRING,
    quantity INT,
    warehouse_zone STRING
) CLUSTER BY (movement_date, factory_id, part_number);

-- ===== CTAS（從現有資料建表）=====
CREATE TABLE inventory_movements_v2
CLUSTER BY (movement_date, factory_id, part_number)
AS SELECT * FROM inventory_movements_legacy;

-- ===== 對現有無分區表啟用 =====
ALTER TABLE purchase_orders
CLUSTER BY (order_date, vendor_id);

-- ===== 修改 clustering key（業務需求改變）=====
ALTER TABLE inventory_movements
CLUSTER BY (movement_date, supplier_id);
-- 未來的 OPTIMIZE 自動用新 key，舊資料不重寫

-- ===== 改完 key 後強制全表重新聚簇 =====
OPTIMIZE inventory_movements FULL;

-- ===== 移除 clustering =====
ALTER TABLE inventory_movements CLUSTER BY NONE;

-- ===== 查看 clustering 設定 =====
DESCRIBE DETAIL inventory_movements;

-- ===== Automatic Liquid Clustering（15.4+，讓 Databricks 自動選 key）=====
CREATE TABLE auto_clustered_table (...)
CLUSTER BY AUTO;

-- ===== Structured Streaming + Liquid Clustering（16.0+）=====
-- 先建表
CREATE TABLE streaming_inventory (
    event_time TIMESTAMP,
    factory_id STRING,
    part_number STRING,
    quantity INT
) CLUSTER BY (event_time, factory_id);
-- 然後用 writeStream 寫入
```

```python
# ===== Python DataFrame API（建表 / overwrite 時）=====
from delta.tables import DeltaTable

(df.write
   .format("delta")
   .clusterBy("movement_date", "factory_id", "part_number")
   .saveAsTable("inventory_movements"))

# ===== 注意：append mode 不能用 DataFrame API 設 clustering key
# ===== 要改 key 必須用 SQL ALTER TABLE
```

## 什麼時候不用 / 常見坑

### ❌ 不適合 Liquid Clustering 的場景

1. **舊版 Delta client**：writer v7 / reader v3 是硬需求，如果下游有舊版 Spark（< 3.4）或非 Databricks 引擎讀表 → 需確認相容性
2. **已有 partition 的舊表**：必須先移除 partition 才能啟用（或用 CTAS 重建），直接 ALTER 會失敗
3. **跨引擎開放生態**：如果需要 Flink/Trino/Presto 等多引擎讀寫，Iceberg Hidden Partitioning 相容性更好

### ⚠️ 反模式

| 反模式 | 問題 | 解法 |
|--------|------|------|
| 4+ 個 clustering key | 小表查詢效能反而下降 | 只選 2-3 個最高頻 WHERE 欄位 |
| 超大表（>10TB）一次性 CTAS 遷移 | 第一次 OPTIMIZE 爆炸式資源消耗 | 分批遷移（batch by date range） |
| 啟用後不跑 OPTIMIZE | Eager Clustering 只是初步整理 | 定期排程 OPTIMIZE（或啟用 Predictive Optimization）|
| 改 key 後不跑 OPTIMIZE FULL | 舊資料還是按舊 key 排列 | ALTER 後立即跑 OPTIMIZE FULL |
| 同時宣告 PARTITION BY + CLUSTER BY | 語法直接報錯 | 二選一，新表一律用 CLUSTER BY |

### 🔥 遷移策略（Z-ORDER → Liquid Clustering）

| 原始方案 | 遷移建議 |
|----------|----------|
| `PARTITION BY (date)` + `ZORDER BY (factory_id, part_number)` | `CLUSTER BY (date, factory_id, part_number)` |
| `PARTITION BY (date)` 只有 partition | `CLUSTER BY (date)` |
| Generated column `date_col = date(timestamp_col)` | 直接用原始 `timestamp_col`，不需 generated column |

**遷移步驟（大表安全做法）：**
1. `CREATE TABLE new_table CLUSTER BY (...) AS SELECT * FROM old_table WHERE date BETWEEN ... AND ...`（分批）
2. 驗證資料完整性
3. 對 new_table 執行 `OPTIMIZE`
4. 切換下游讀取到 new_table
5. `DROP TABLE old_table`

## 🏗️ SA 視角

### ODM 供應鏈場景

**庫存異動歷史表（每天數千萬筆）**：
- 傳統做法：`PARTITION BY (date)` + `ZORDER BY (plant_id, part_number)`
  - 痛點：採購部要跨廠區查「某料號過去一年消耗量」→ 掃描所有 partition
- Liquid Clustering：`CLUSTER BY (date, plant_id, part_number)`
  - Hilbert Curve 同時對三個維度聚簇 → 任何維度組合的查詢都有效
  - 半年後採購部要按 `supplier_id` 查？→ `ALTER TABLE ... CLUSTER BY (date, supplier_id)` 即可

**其他適用場景：**
- 採購訂單表：`CLUSTER BY (order_date, vendor_id)` → 隨時可改成 `(order_date, material_group)`
- 出貨紀錄表：`CLUSTER BY (ship_date, customer_id)` → 旺季可改成 `(ship_date, destination_country)`
- 品質紀錄表：`CLUSTER BY (inspection_date, production_line)` → 品質問題追蹤

### 數位轉型場景

**導入門檻：**
- 新表：零門檻，建表時加 `CLUSTER BY` 就好
- 舊表遷移：需要 CTAS 重建（大表分批），有一次性成本
- 需要 Databricks Runtime 15.2+（GA）
- 下游讀取端必須支援 Delta protocol v7

**替代方案比較：**
- **Z-ORDER**：舊版 Databricks 或 OSS Spark 的退路，不需要高版本 protocol
- **Iceberg Hidden Partitioning**：跨引擎/多雲場景首選，但 partition 演進比 Liquid Clustering 笨重
- **Automatic Liquid Clustering（15.4+）**：讓 Databricks AI 自動選 key，適合不確定查詢模式的場景

### Solution Architect Trade-off

| 維度 | Liquid Clustering | Z-ORDER | Iceberg Hidden Partition |
|------|-------------------|---------|--------------------------|
| **彈性** | ⭐⭐⭐⭐⭐（隨時改 key） | ⭐⭐（改 partition 要重寫） | ⭐⭐⭐⭐（改 spec 不重寫舊資料） |
| **自動化** | ⭐⭐⭐⭐（Eager + Predictive Opt） | ⭐⭐（純手動排程） | ⭐⭐⭐（依賴 compaction 排程） |
| **多維查詢** | ⭐⭐⭐⭐⭐（Hilbert Curve） | ⭐⭐⭐⭐（Morton Curve） | ⭐⭐⭐（transform 函數組合） |
| **跨引擎相容** | ⭐⭐（Databricks 生態系） | ⭐⭐⭐（較舊 protocol） | ⭐⭐⭐⭐⭐（開放標準） |
| **並發寫入** | ⭐⭐⭐⭐⭐（Row-level） | ⭐⭐⭐（Partition-level） | ⭐⭐⭐⭐（Snapshot isolation） |

**SA 決策流程（更新版）：**
1. 全 Databricks 生態？→ **Liquid Clustering**（所有新表）
2. 需要跨引擎（Flink/Trino/Presto）？→ **Iceberg Hidden Partitioning**
3. 舊 Databricks Runtime / OSS Spark？→ **Partition + Z-ORDER**（退路）
4. 不確定查詢模式？→ **Automatic Liquid Clustering**（15.4+）

## 我的理解 / 待解疑問

- ✅ Liquid Clustering 的核心機制：Hilbert Curve + Eager Clustering + Row-level Concurrency
- ✅ 與 Z-ORDER 的本質差異：全表聚簇 vs partition 內聚簇
- ✅ 遷移策略和反模式
- ✅ SA 選型框架：Delta Liquid vs Iceberg Hidden Partition
- ❓ Hilbert Curve vs Morton Curve 的具體 benchmark 數據（NotebookLM 沒有提供量化差異）
- ❓ Automatic Liquid Clustering 的 AI 演算法是怎麼根據查詢歷史選 key 的？
- ❓ Liquid Clustering 對 Iceberg managed tables（16.4+ Public Preview）的支援程度和限制？

---

> 📚 來源：NotebookLM DE Core Notebook（5 輪對話，conv: 37a0d9d2-fe35-4156-a590-9dfb8b0be7a8）+ Databricks Liquid Clustering Docs + Delta Lake OSS Docs
