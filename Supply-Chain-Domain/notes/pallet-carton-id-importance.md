# Pallet ID 和 Carton ID 為什麼重要

> 學習日期：2026-05-15 | 供應鏈每日課 #13
> 分類：術語/倉庫/追蹤

## 一句話定義

Pallet ID 和 Carton ID 是倉庫/物流追蹤系統的**唯一識別碼**——沒有它們，從產線末端到客戶手上的每一個環節都無法精準追蹤。

## 為什麼重要（業務影響）

### 1. 正向追蹤（Forward Traceability）
從生產到交付的每個環節，都靠 ID 串聯：

```
產線包裝貼 Carton Label
  → 碼板時掃 Carton ID 綁定 Pallet ID
    → 入倉掃 Pallet ID 上架
      → 出貨掃 Pallet/Carton ID 裝車
        → 客戶收貨掃 ID 驗收
```

每一次「掃碼」都產生一筆 timestamp + location + operator 的紀錄，形成完整的 **audit trail**。

### 2. 反向追溯（Backward Traceability）
客訴時最關鍵的能力：

| 場景 | 追溯路徑 |
|------|---------|
| 客戶報伺服器硬碟故障 | Serial Number → Carton ID → Work Order → 產線/班次 → 零件批號 → 供應商 |
| 海關扣留一板貨 | Pallet ID → 所有 Carton IDs → 每箱的 Commercial Invoice → 品項明細 |
| 物流遺失 | Pallet ID → 最後掃碼地點 → 3PL 簽收紀錄 |

### 3. 庫存準確度（Inventory Accuracy）
- **沒有 ID**：盤點只能數量對照，發現差異不知道差在哪裡
- **有 ID**：系統帳（Book）vs 實體（Physical）可以精準到「哪個 Carton 不見了」

### 4. 出貨對帳（Shipping Reconciliation）
- Packing List 上列的 Carton IDs 必須跟實際出貨 100% 吻合
- 一箱多發 → 成本損失；一箱少發 → 客戶投訴/罰款
- CSP 大客戶（微軟/Meta）通常有**自動化收貨系統**，掃描每個 Carton ID 跟 ASN（Advance Shipping Notice）比對

### 5. 合規與保險
- 出口文件（Commercial Invoice、Packing List、Bill of Lading）都需要 Pallet/Carton 粒度的資訊
- 保險理賠時需要提供完整的包裝追蹤紀錄
- 危險品（DG，如鋰電池）管制要追蹤到 Carton 級

## ODM/緯穎場景

### ID 的生成和結構

```
Pallet ID 格式（範例）：
PL-TW01-20260515-0001
│  │     │         │
│  │     │         └─ 當天流水號
│  │     └─────────── 日期
│  └───────────────── 工廠代碼
└──────────────────── Prefix（Pallet）

Carton ID 格式（範例）：
CT-TW01-WO240515001-001
│  │     │            │
│  │     │            └─ 箱序號
│  │     └────────────── 工單號
│  └──────────────────── 工廠代碼
└─────────────────────── Prefix（Carton）
```

### 關鍵綁定關係

```
1 個 Pallet ID → 綁定 N 個 Carton IDs（碼板時建立）
1 個 Carton ID → 綁定 M 個 Serial Numbers（包裝時建立）
1 個 Serial Number → 綁定零件 BOM（生產時建立）
```

這個三層綁定是整個追蹤系統的骨架。

### 緯穎場景的特殊複雜度

1. **多種伺服器混合出貨**：同一個 Pallet 上可能有不同 SKU（同客戶不同配置）
2. **大型 AI Server**：一箱就一台（1:1），但 Carton ID 仍必須存在（追蹤用）
3. **Build-to-Order 特性**：CSP 客戶的 PO 指定到 Serial Number 級，Carton ID 是 PO 和 Serial 的中間連結
4. **跨廠出貨合併**：台灣、中國、墨西哥不同工廠的貨，要在 Hub 倉合併成一個 Shipment → Pallet ID 需要能跨廠識別

## 相關 SAP T-Code

| T-Code | 功能 | 與 ID 的關係 |
|--------|------|-------------|
| **HU02** | 顯示 Handling Unit | Pallet/Carton 在 SAP 中用 HU（Handling Unit）管理 |
| **HUMO** | HU Monitor | 查看 HU 的完整歷史（建立/移動/出貨） |
| **MIGO** | 收貨 | 收貨時掃描 Pallet/Carton ID，建立庫存 |
| **VL01N** | 建立交貨單 | 出貨時綁定 HU（Pallet/Carton）到交貨單 |
| **LM76** | 確認轉儲 | 倉庫內部移動掃描 |

## Data Engineer 視角：背後的資料

### 核心資料表設計

```sql
-- Carton 追蹤表
CREATE TABLE fact_carton_tracking (
    carton_id       STRING NOT NULL,   -- PK
    pallet_id       STRING,            -- FK → fact_pallet
    work_order_id   STRING,            -- 生產工單
    plant_code      STRING,            -- 工廠
    sku             STRING,            -- 產品型號
    qty_units       INT,               -- 箱內數量
    packed_at       TIMESTAMP,         -- 包裝完成時間
    palletized_at   TIMESTAMP,         -- 碼板時間
    shipped_at      TIMESTAMP,         -- 出貨時間
    received_at     TIMESTAMP,         -- 客戶收貨時間
    status          STRING             -- packed/palletized/shipped/delivered
);

-- Pallet 追蹤表
CREATE TABLE fact_pallet_tracking (
    pallet_id       STRING NOT NULL,
    shipment_id     STRING,
    total_cartons   INT,
    total_units     INT,
    gross_weight_kg DECIMAL(10,2),
    pallet_spec     STRING,            -- US48x40/EUR1200x1000
    created_at      TIMESTAMP,
    shipped_at      TIMESTAMP,
    delivered_at    TIMESTAMP,
    status          STRING
);
```

### GX 驗證要點（連結 [[great-expectations-core-concepts]]）

```python
# Carton ID 驗證
expect_column_values_to_not_be_null("carton_id")
expect_column_values_to_be_unique("carton_id")
expect_column_values_to_match_regex("carton_id", r"^CT-[A-Z]{2}\d{2}-WO\d{9}-\d{3}$")

# Pallet-Carton 一致性驗證
expect_column_pair_values_a_to_be_greater_than_b("pallet.total_cartons", 0)
# 實際 Carton 數 vs Pallet 記錄的 total_cartons 要一致
```

## 常見術語搭配

| 術語 | 說明 |
|------|------|
| **ASN（Advance Shipping Notice）** | 出貨前通知客戶的電子文件，包含所有 Pallet/Carton ID |
| **SSCC（Serial Shipping Container Code）** | GS1 國際標準的 18 位序列碼，用於全球供應鏈識別 |
| **GS1-128 條碼** | 印在 Pallet/Carton Label 上的條碼格式，包含 SSCC + 其他 AI（Application Identifier） |
| **Handling Unit（HU）** | SAP 中 Pallet/Carton 的抽象概念，支援巢狀結構（HU 內有 HU） |
| **Packing List** | 出貨文件，列出每個 Carton 的內容物 |

## See Also
- [[pallet]] — Pallet 的定義、規格、ISPM 15 合規
- [[carton]] — Carton 的包裝層級結構、客訴追查功能
- [[shop-floor]] — 產線末端包裝工站是 Carton ID 的起點
- [[great-expectations-core-concepts]] — GX 驗證 Carton/Pallet ID 的唯一性和非空
