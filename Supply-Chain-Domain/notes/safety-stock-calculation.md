# Safety Stock 計算：為什麼需要安全庫存

> 學習日期：2026-05-22 | 供應鏈每日課 #20
> 分類：生產計劃

## 一句話定義

**Safety Stock（安全庫存）** 是在正常庫存水位之上額外持有的緩衝庫存，用來吸收需求波動和供應不確定性，防止缺料（stockout）。

## 為什麼重要（業務影響）

1. **防止停線**：ODM 工廠一條產線停一小時，損失可達數十萬台幣。Safety Stock 是最後防線。
2. **服務水準保證**：CSP 客戶要求 OTD（On-Time Delivery）> 95%，沒有 SS 幾乎不可能達標。
3. **財務兩面刃**：太多 SS = 資金積壓 + 倉儲成本 + 呆料風險；太少 = 缺料 + 停線 + 客訴 + Premium Freight（加急運費）。
4. **MRP 關鍵參數**：SAP MRP 計算 Net Requirement 時，SS 是不可動用的「地板庫存」，低於 SS 才會觸發採購建議。

## ODM/緯穎場景

### 為什麼 ODM 的 Safety Stock 特別複雜

| 挑戰 | 說明 |
|------|------|
| **CSP Forecast 不穩定** | Hyperscaler 的 Rolling Forecast 經常大幅變動（±30%），需求標準差很高 |
| **零件 Lead Time 長且不穩** | GPU（NVIDIA）LT 可達 26-52 週，供應端 σ_LT 很大 |
| **BOM 層級深** | 一台 AI Server 數百個零件，每個零件都需要各自的 SS 設定 |
| **ABC 分類不同策略** | A 類零件（GPU/CPU/HBM）用高 SS + VMI；C 類（螺絲/線材）用低 SS + 定期補貨 |
| **NPI vs MP 差異** | NPI 階段：BOM 不穩定，SS 用較高的固定天數（如 14 天用量）；MP 階段：用統計方法 |

### 緯穎實際 SS 設定邏輯

```
A 類零件（GPU/CPU/Memory）：
  - SS = CSP Firm PO 的 2 週用量
  - 或用 VMI（寄售），SS 由供應商承擔
  - 要考慮 Allocation（分配制），有時候想備也備不到

B 類零件（PCB/Connector/Power Supply）：
  - SS = Z × σ_D × √LT
  - Service Level 通常設 95%（Z=1.65）
  - 每月由 PMC 在 S&OP 檢討

C 類零件（螺絲/貼紙/包材）：
  - SS = 固定天數（如 7 天用量）
  - 定期定量補貨（Min-Max 或 ROP）
```

## 統計計算方法

### 基本公式（需求不確定，Lead Time 固定）

```
SS = Z × σ_D × √LT

其中：
  Z    = 服務水準對應的 Z-score（95% → 1.65, 99% → 2.33）
  σ_D  = 每日（或每週）需求的標準差
  LT   = 供應商 Lead Time（天數或週數，與 σ_D 的時間單位一致）
```

### 進階公式（需求和 Lead Time 都不確定）

```
SS = Z × √(LT × σ_D² + D_avg² × σ_LT²)

其中：
  D_avg = 平均每日需求
  σ_LT  = Lead Time 的標準差（天）
  σ_D   = 每日需求的標準差
```

> ⚠️ 這些公式假設需求是常態分布的獨立隨機變數。實際上 CSP 的需求是 lumpy（間歇性大批量），常態分布假設可能不成立。A 類零件可能需要用 Croston method 或 simulation。

### Service Level 對照表

| Service Level | Z-score | 含義 |
|--------------|---------|------|
| 90% | 1.28 | 10% 機率缺料 |
| 95% | 1.65 | 5% 機率缺料（ODM 常用） |
| 97.5% | 1.96 | 2.5% 機率缺料 |
| 99% | 2.33 | 1% 機率缺料（A 類零件） |
| 99.9% | 3.09 | 極端保守（金融級） |

## 相關 SAP T-Code（如適用）

| T-Code | 功能 | SS 相關 |
|--------|------|---------|
| **MM02** | 修改物料主數據 | 設定 Safety Stock 數量（MRP 2 view） |
| **MD04** | MRP Stock/Requirements List | 查看 SS 是否足夠，Net Requirement 計算 |
| **MD01/MD02** | MRP Run | SS 作為計算參數，低於 SS → 產生 Planned Order |
| **MMBE** | 庫存概覽 | 查看當前庫存 vs SS 水位 |
| **MC.9** | 消耗統計分析 | 提取 σ_D 的歷史數據來源 |

## Data Engineer 視角：背後的資料

### SS 計算需要的資料

| 資料 | 來源 | 用途 |
|------|------|------|
| 每日/週需求歷史 | SAP MATDOC / Work Order consumption | 計算 σ_D |
| Lead Time 歷史 | SAP PO → GR 的時間差 | 計算 avg_LT 和 σ_LT |
| BOM 結構 | SAP CS03 / BOM master | 把 FG 需求展開成零件需求 |
| Service Level 設定 | PMC 決策 | 決定 Z-score |
| 庫存快照 | SAP MMBE / MB52 | 判斷是否低於 SS |

### DE 可以做什麼

```python
# 用 PySpark 計算各零件的建議 Safety Stock
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import math

# 計算每個零件的需求統計量
demand_stats = (
    daily_consumption
    .groupBy("material_number")
    .agg(
        F.avg("qty").alias("avg_daily_demand"),
        F.stddev("qty").alias("sigma_d"),
    )
)

# JOIN Lead Time 統計
lt_stats = (
    po_gr_history
    .groupBy("material_number")
    .agg(
        F.avg("lead_time_days").alias("avg_lt"),
        F.stddev("lead_time_days").alias("sigma_lt"),
    )
)

# 計算 Safety Stock（Z=1.65 for 95% service level）
Z = 1.65
suggested_ss = (
    demand_stats.join(lt_stats, "material_number")
    .withColumn("safety_stock",
        F.lit(Z) * F.sqrt(
            F.col("avg_lt") * F.pow(F.col("sigma_d"), 2) +
            F.pow(F.col("avg_daily_demand"), 2) * F.pow(F.col("sigma_lt"), 2)
        )
    )
)
```

> 💡 **這就是 DE 在供應鏈最有價值的輸出之一**：把「拍腦袋定 SS」變成「數據驅動的 SS 建議」。每月產出 SS 建議報表 → PMC 在 S&OP 會議上檢討 → 調整 SAP MM02 的 SS 設定。

## 常見術語搭配

- **ROP（Reorder Point）= 平均需求 × LT + Safety Stock**
- **Weeks of Supply（WOS）**：當前庫存能撐幾週 = (On-hand + On-order) / 週均需求
- **E&O（Excess & Obsolete）**：SS 設太高 → 需求下降 → 呆滯料
- **Fill Rate**：訂單滿足率，與 Service Level 相關但不完全等同
- **Buffer Stock**：Safety Stock 的另一個說法
- **DDMRP Buffer**：Demand-Driven MRP 中的動態 Buffer = Green + Yellow + Red zone

## See Also

- [[Supply-Chain-Domain/mrp-material-requirements-planning]] — MRP 的 Net Requirement 計算依賴 SS
- [[Supply-Chain-Domain/mps-master-production-schedule]] — MPS 驅動 MRP，間接影響 SS 消耗
- [[Supply-Chain-Domain/lead-time-types]] — LT 是 SS 計算的核心參數
- [[Supply-Chain-Domain/capacity-planning]] — Capacity 不足時 SS 可能建不起來
- [[Supply-Chain-Domain/sop-sales-operations-planning]] — S&OP 會議每月檢討 SS 策略
