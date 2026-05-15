# BOM（Bill of Materials）— 物料清單

> 學習日期：2026-05-16 | 供應鏈每日課 #14
> 分類：術語 / 生產計劃 / 採購

## 一句話定義

BOM（Bill of Materials，物料清單）是一份**階層式的零件清單**，記錄製造一個成品所需的所有原物料、子組件、中間組件及其數量，是 MRP 計算、採購、成本估算、生產排程的核心資料。

## 為什麼重要（業務影響）

**BOM 是供應鏈的 DNA**——幾乎所有供應鏈系統的起點：

| 流程 | BOM 的角色 |
|------|-----------|
| **MRP（物料需求計劃）** | MRP 拿 BOM × 生產計劃 = 各零件的需求量，扣除在手庫存和在途 = 採購建議 |
| **採購** | BOM 決定要買什麼、買多少、向誰買（AVL 綁在 BOM 層級） |
| **成本估算** | 每個零件有單價，BOM 展開 = 產品成本（用於報價） |
| **品質管理** | BOM 變更（ECO）觸發品質驗證流程 |
| **生產排程** | BOM + Routing（工藝路線）= 生產工單內容 |
| **出貨包裝** | Packing BOM 決定每台伺服器出貨時附帶什麼配件 |

**BOM 出錯的連鎖反應**：BOM 上少一顆螺絲 → MRP 不會計算需求 → 產線缺料停線 → 交期延誤 → 客戶罰款。

## ODM/緯穎場景

### Engineering BOM (EBOM) vs Manufacturing BOM (MBOM)

| 類型 | 誰維護 | 用途 | 結構特點 |
|------|--------|------|----------|
| **EBOM（工程 BOM）** | RD/設計部門 | 反映設計意圖，列出功能性零件 | 以功能模組為單位（CPU Module、Memory Module） |
| **MBOM（製造 BOM）** | ME/製造工程 | 反映實際組裝流程，包含製程所需的所有物料 | 以工站/工序為單位，包含螺絲、貼紙、包材、治具消耗品 |

**ODM 伺服器的 BOM 特點：**
- **BOM 層級深**：伺服器 BOM 通常 4-6 層（成品 → 主板模組 → PCB → IC 零件）
- **Configurable BOM（CBOM）**：CSP 客戶（微軟/Google）的伺服器高度客製化，一個 Model 可能有幾百種 configuration
- **Phantom BOM（虛擬 BOM）**：中間組件不實際入庫，只是邏輯層級（例如「主板模組」不會有獨立庫存）
- **ECO 頻繁**：NPI 階段 BOM 每週都在改（EVT→DVT→PVT），每次 ECO 都影響備料計劃

### BOM 與緯穎系統的關係

```
SAP MM/PP 中的 BOM：
├── CS01/CS02/CS03：建立/修改/查看 BOM
├── CS11：單層展開（看直接子件）
├── CS12：多層展開（看所有層級到最底層）
├── CS15：反查（某個零件被用在哪些 BOM？）
├── Production Version：連結 BOM + Routing
└── MRP Run（MD01）：讀 BOM → 計算需求 → 產生 PR/Planned Order
```

### BOM 在 ODM 的常見問題

1. **EBOM→MBOM 轉換落差**：RD 設計時沒考慮產線需要的包材/螺絲，MBOM 多出很多 items
2. **ECO 時效性**：BOM 改了但 MRP 用的是舊版本 → 備了錯的料
3. **Phantom Item 管理**：虛擬件的處理每家 ERP 不同，SAP 的 Phantom Assembly 不消耗庫存但會往下展
4. **Alternative BOM**：同一產品因客戶需求不同（AMD/Intel CPU），有多版 BOM

## 相關 SAP T-Code（如適用）

| T-Code | 功能 | 使用場景 |
|--------|------|----------|
| CS01 | 建立 BOM | 新產品導入 |
| CS02 | 修改 BOM | ECO 變更 |
| CS03 | 查看 BOM | 日常查詢 |
| CS11 | 單層 BOM 展開 | 看直接子件 |
| CS12 | 多層 BOM 展開 | 完整零件清單 |
| CS15 | Where-used（反查） | 某零件被哪些產品使用？ |
| CA01 | 建立 Routing | 搭配 BOM 的工藝路線 |

## Data Engineer 視角：背後的資料

**BOM 是最經典的階層式資料結構**，在資料倉庫中需要特別處理：

```
BOM 資料結構（簡化）：
├── material_number（成品/父件料號）
├── component_number（子件料號）
├── quantity_per（每個父件需要幾個子件）
├── uom（單位）
├── bom_level（層級）
├── valid_from / valid_to（版本有效期）
├── eco_number（變更單號）
├── phantom_flag（是否為虛擬件）
└── alternative_bom（替代 BOM 識別碼）
```

**DE 處理 BOM 的挑戰：**
1. **遞迴展開**：BOM 是樹狀結構，需要遞迴 CTE（WITH RECURSIVE）或 PySpark 的 GraphFrame 展開
2. **版本管理**：同一產品不同時間點有不同 BOM → SCD Type 2 追蹤
3. **成本計算**：BOM 展開 × 各層零件單價 = 產品成本（向上捲起）
4. **ECO 影響分析**：某零件改了，影響哪些產品？→ Where-used 反查（CS15 的資料版本）

**Delta Lake 上的 BOM 表設計建議：**
- `CLUSTER BY (material_number)` — 最常見查詢是「某產品的 BOM 展開」
- Time Travel 追蹤 BOM 版本歷史（什麼時候改了什麼）
- MERGE 處理 ECO 更新（SCD Type 2）

## 常見術語搭配

| 術語 | 意義 |
|------|------|
| **BOM Explosion** | BOM 展開（向下展到最底層零件） |
| **BOM Implosion** | BOM 反查（某零件被用在哪些產品） |
| **Phantom Assembly** | 虛擬組件（邏輯層級，不實際入庫） |
| **Configurable BOM (CBOM)** | 可配置 BOM（一個 Model 對應多種 configuration） |
| **Multi-level BOM** | 多層 BOM（完整階層結構） |
| **Single-level BOM** | 單層 BOM（只看直接子件） |
| **ECO (Engineering Change Order)** | 工程變更單（觸發 BOM 修改） |
| **Production Version** | SAP 中 BOM + Routing 的組合，MRP 用它排程 |
| **Where-used** | 反查：某零件被用在哪些 BOM 中 |
