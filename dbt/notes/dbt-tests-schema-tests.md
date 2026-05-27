# dbt Tests：Schema Tests 怎麼寫，確保資料品質

> 學習日期：2026-05-28 | 科目：dbt（main）| 進度：第 4 課
> 本科目目標：能在 Databricks 上用 dbt 管理整個 transformation layer
> 今日選題依據：正常輪換 P4 — dayCount=37, 37%3=1, mainRotation[4]=dbt, topics[3]
> ⏰ 今日 SR 到期：13 張（Recovery 已消化 5 張，剩餘 8 張留 Phase B）

## 這是什麼？

**dbt tests** 是你對 models（以及 sources、seeds、snapshots）做的**資料品質斷言（assertions）**。執行 `dbt test` 時，dbt 告訴你每個 test 通過或失敗。

核心原理極簡：**每個 test 本質上是一條 SELECT 語句，查出「違反斷言」的記錄。回傳 0 行 = 通過，非 0 行 = 失敗。**

dbt 有 **三種** test 類型：

| 類型 | 定義方式 | 複用性 | 適用場景 |
|------|---------|--------|---------|
| **Generic Data Test**（原名 schema test）| YAML 宣告 + Jinja macro | ✅ 高度複用 | 80% 場景：NOT NULL、UNIQUE、關聯 |
| **Singular Data Test** | 獨立 `.sql` 檔案 | ❌ 一次性 | 複雜業務邏輯驗證 |
| **Unit Test**（v1.8+）| YAML 定義靜態輸入/輸出 | ✅ 可複用 | SQL 邏輯正確性（開發/CI 階段）|

> ⚠️ 術語演變：dbt 官方已將 "tests" 改名為 "data tests"，YAML key 從 `tests:` 改為 `data_tests:`（`tests:` 仍向後兼容）。

## 為什麼重要？

### 沒有 dbt tests 會怎樣？

想像 ODM 廠的 ERP 資料流入 Delta Lake：

```
SAP MATDOC (庫存異動) → Bronze → Silver (dbt) → Gold (dbt) → BI Dashboard
```

**沒有 tests 的災難場景：**

1. **Silent Data Corruption**：SAP 匯出的 `material_number` 有些是 NULL（資料提取 bug），dbt 照跑不報錯 → Silver 層出現 NULL key → Gold 層的 JOIN 靜默丟掉這些行 → BI 報表的庫存數字偏低 → 財務部門做錯採購決策
2. **Referential Integrity 破洞**：有人改了 vendor 主數據但沒通知 DE → `vendor_id` 在 fact 表裡找不到對應 dim → LEFT JOIN 產出 NULL vendor name → 報表不知道哪個供應商
3. **重複資料不察覺**：ETL job retry 導致同一筆 GR（收貨單）重複寫入 → 庫存多算 → 觸發假性超額採購

**dbt tests 的價值**：在 `dbt run` 之後、BI 看到之前，攔截這些問題。

## 怎麼運作？

### 1. Generic Data Tests（核心 80% 場景）

dbt 內建 **4 個** generic tests，直接在 YAML 宣告即可：

```yaml
# models/staging/_stg_schema.yml
models:
  - name: stg_sap_orders
    columns:
      - name: order_id
        data_tests:
          - unique          # 無重複
          - not_null         # 無 NULL
      - name: status
        data_tests:
          - accepted_values:
              arguments:
                values: ['created', 'released', 'confirmed', 'delivered', 'closed']
      - name: vendor_id
        data_tests:
          - not_null
          - relationships:   # 參照完整性
              arguments:
                to: ref('stg_sap_vendors')
                field: vendor_id
```

**底層原理**——每個 generic test 展開成 SQL：

```sql
-- unique 展開：
SELECT order_id
FROM stg_sap_orders
GROUP BY order_id
HAVING COUNT(*) > 1;

-- not_null 展開：
SELECT * FROM stg_sap_orders WHERE order_id IS NULL;

-- accepted_values 展開：
SELECT status FROM stg_sap_orders
WHERE status NOT IN ('created','released','confirmed','delivered','closed')
  AND status IS NOT NULL;

-- relationships 展開：
SELECT child.vendor_id
FROM stg_sap_orders AS child
WHERE child.vendor_id IS NOT NULL
  AND child.vendor_id NOT IN (SELECT vendor_id FROM stg_sap_vendors);
```

**注意**：`relationships` 和 `accepted_values` 自動排除 NULL（符合 SQL foreign key 語意），要攔截 NULL 需額外加 `not_null` test。

### 2. Custom Generic Tests（組織自定義）

當 4 個內建 test 不夠時，寫自己的 test macro：

```sql
-- tests/generic/test_positive_value.sql
{% test positive_value(model, column_name) %}

SELECT *
FROM {{ model }}
WHERE {{ column_name }} < 0

{% endtest %}
```

使用：
```yaml
columns:
  - name: quantity
    data_tests:
      - positive_value    # 庫存數量不能為負
```

**ODM 常用自定義 test 範例：**

```sql
-- tests/generic/test_valid_material_number.sql
{% test valid_material_number(model, column_name) %}
-- 物料號碼必須符合 SAP 18 位格式（前導零填充）
SELECT *
FROM {{ model }}
WHERE {{ column_name }} IS NOT NULL
  AND (LENGTH({{ column_name }}) != 18
       OR {{ column_name }} NOT RLIKE '^[0-9]{18}$')
{% endtest %}
```

### 3. Singular Data Tests（一次性驗證）

放在 `tests/` 目錄下的獨立 SQL 檔案，適合跨表複雜邏輯：

```sql
-- tests/assert_po_amount_positive.sql
-- 每筆採購單的總金額應該 >= 0（退貨為負數但有上限）
SELECT
    po_number,
    SUM(net_amount) AS total_amount
FROM {{ ref('fct_purchase_orders') }}
GROUP BY 1
HAVING total_amount < -1000000  -- 允許正常退貨，但不該有超過百萬的負數
```

### 4. Unit Tests（v1.8+，SQL 邏輯測試）

驗證你的 SQL 轉換邏輯，不依賴真實資料：

```yaml
# models/unit_tests.yml
unit_tests:
  - name: test_vendor_score_calculation
    model: int_vendor_scorecard
    given:
      - input: ref('stg_sap_deliveries')
        rows:
          - {vendor_id: V001, promised_date: '2026-05-01', actual_date: '2026-05-01'}  # on-time
          - {vendor_id: V001, promised_date: '2026-05-02', actual_date: '2026-05-05'}  # late
    expect:
      rows:
        - {vendor_id: V001, otif_rate: 0.5}
```

**何時用 Unit Test？**
- 複雜 CASE WHEN 邏輯
- Window function（rank、lag）
- 正則表達式解析
- 重構前確認行為不變

**何時不用？** `min()`, `sum()` 等基本聚合 — warehouse 已經測過。

### 5. Test 配置：severity、store_failures

```yaml
columns:
  - name: vendor_id
    data_tests:
      - not_null:
          config:
            severity: warn         # 警告但不中斷
      - unique:
          config:
            severity: error        # 錯誤，中斷 pipeline
            error_if: ">1000"      # > 1000 筆重複才報 error
            warn_if: ">10"         # > 10 筆就警告
            store_failures: true   # 把失敗記錄存到表中供分析
```

**severity 判斷邏輯：**
```
severity=error → 檢查 error_if（預設 !=0）→ 不滿足 → 檢查 warn_if → 滿足就 warn
severity=warn  → 跳過 error_if → 直接檢查 warn_if → 滿足就 warn
```

### 6. dbt 第三方 Test 套件

| 套件 | 用途 | 範例 |
|------|------|------|
| **dbt-utils** | 常用工具 test | `expression_is_true`, `equal_row_count`, `recency` |
| **dbt-expectations** | GX 風格（完整資料品質）| `expect_column_values_to_be_between`, `expect_table_row_count_to_be_between` |
| **dbt-date** | 日期相關 | 日期連續性、工作日驗證 |

```yaml
# dbt-utils 範例
models:
  - name: fct_inventory
    data_tests:
      - dbt_utils.recency:
          arguments:
            datepart: day
            field: updated_at
            interval: 1   # 最近 1 天必須有資料
      - dbt_utils.equal_row_count:
          arguments:
            compare_model: ref('stg_raw_inventory')  # 行數應一致
```

## 程式碼範例：ODM 供應鏈 dbt test 套件

完整的 staging schema 驗證：

```yaml
# models/staging/sap/_sap_sources.yml
sources:
  - name: sap_raw
    database: bronze
    schema: sap
    freshness:
      warn_after: {count: 12, period: hour}
      error_after: {count: 24, period: hour}
    loaded_at_field: _etl_loaded_at
    tables:
      - name: ekko   # PO Header
      - name: ekpo   # PO Item
      - name: mkpf   # Material Document Header
      - name: mseg   # Material Document Item
      - name: lfa1   # Vendor Master

# models/staging/sap/_stg_schema.yml
models:
  - name: stg_sap_po_header
    description: "SAP 採購訂單表頭 - staging"
    columns:
      - name: po_number
        data_tests:
          - unique
          - not_null
      - name: vendor_id
        data_tests:
          - not_null
          - relationships:
              arguments:
                to: ref('stg_sap_vendor_master')
                field: vendor_id
      - name: po_date
        data_tests:
          - not_null
          - dbt_utils.expression_is_true:
              arguments:
                expression: "po_date >= '2020-01-01'"  # 不該有 2020 年前的 PO
      - name: net_amount
        data_tests:
          - not_null
          - positive_value  # 自定義：金額不為負

  - name: stg_sap_po_item
    columns:
      - name: po_item_key
        description: "po_number || '-' || po_item"
        data_tests:
          - unique
          - not_null
      - name: material_number
        data_tests:
          - not_null:
              config:
                severity: warn  # MRO 採購可能沒有物料號
                where: "item_category = 'DIRECT'"  # 只檢查 Direct 採購
          - valid_material_number:
              config:
                where: "material_number IS NOT NULL"
      - name: quantity
        data_tests:
          - positive_value
      - name: avl_status
        data_tests:
          - accepted_values:
              arguments:
                values: ['active', 'probation', 'suspended', 'pending']
```

## 什麼時候不用 / 常見坑

### ❌ 反模式

1. **過度測試每一欄** — 不是每個欄位都需要 `unique` + `not_null`。focus on **business-critical** 欄位
2. **把資料品質問題混進 unit test** — Unit test 驗證 SQL 邏輯，data test 驗證資料品質，兩者不要混
3. **severity 全部設 error** — Bronze 層的資料「髒」是正常的，用 `warn` 記錄；Silver/Gold 層才用 `error` 硬擋
4. **不用 `where` 過濾** — 舊資料可能有已知問題（已修復），加 `where: "order_date >= '2026-01-01'"` 避免誤報
5. **忽略 `store_failures`** — 失敗記錄不存 = 出問題時要手動查原因，設 `store_failures: true` 自動存入 `_dbt_test__audit` schema

### ⚠️ 常見坑

- **relationships test 不檢查 NULL** — `vendor_id IS NULL` 的行會被跳過，要加 `not_null` test
- **test 命名衝突** — 同一 model 的多個 test 組合可能產生同名（dbt 自動 hash），建議用 `name:` 自定義名稱
- **Incremental model 的 test 只測新資料？** — 不！`dbt test` 預設掃描全表。大表加 `where` 過濾只測最近資料
- **Test 不會自動跑** — `dbt run` 不會跑 test，要 `dbt build`（= run + test）或獨立 `dbt test`

### 🔑 多層測試策略（Medallion 架構）

| 層 | 測試重點 | Severity | 範例 |
|----|---------|----------|------|
| **Bronze → Silver** | Schema 完整性 | warn | `not_null`, `unique`, `freshness` |
| **Silver → Gold** | 業務規則 + 參照完整性 | error | `relationships`, `accepted_values`, 自定義 |
| **Gold → BI** | 聚合正確性 + 數據一致性 | error | `equal_row_count`, `expression_is_true` |

## 🏗️ SA 視角

### ODM 供應鏈場景

在鴻海/緯穎這樣的 ODM 公司：

1. **P2P 資料品質門**：SAP PO 資料流入 Lakehouse 時，`stg_sap_po_header` 的 `vendor_id` 必須 NOT NULL + 必須在 `dim_vendor` 裡找得到 → 這就是 `not_null` + `relationships` 的教科書場景
2. **AVL 狀態驗證**：供應商的 AVL status 只能是 `active/probation/suspended/pending` → `accepted_values` test 攔截異常值
3. **BOM 層級完整性**：`parent_material` 在 BOM explosion 後必須對應到 `material_master` → `relationships` test
4. **庫存不為負**：`quantity_on_hand` 不能 < 0 → 自定義 `positive_value` test

### 數位轉型場景

- **導入門檻**：極低 — 只要在 YAML 加幾行，零額外工具
- **與 GX 的定位差異**：dbt test 是 **transformation layer 內建的品質 gate**，GX 是 **standalone 品質引擎**
  - dbt test：在 dbt DAG 內執行，結果直接影響下游 model 是否建置
  - GX：獨立執行，產出 Data Docs HTML 報告，適合跨團隊品質治理
  - **SA 建議**：小型團隊用 dbt test 即可；大型企業（跨 DE + DA + Business）用 GX 做中央品質平台，dbt test 做 pipeline 內快速 gate

### Solution Architect Trade-off

| 維度 | dbt tests | Great Expectations | dbt-expectations |
|------|-----------|-------------------|------------------|
| 整合度 | ✅ 原生（DAG 內）| ❌ 需外掛整合 | ✅ dbt 套件 |
| 表達力 | 🟡 中（4 built-in + custom）| ✅ 高（200+ expectations）| ✅ 高（GX 風格）|
| 學習曲線 | ✅ 低 | 🟡 中 | ✅ 低 |
| 報告 | 🟡 CLI + manifest.json | ✅ Data Docs HTML | 🟡 同 dbt |
| 適用層 | Transformation layer | 任何資料源 | Transformation layer |
| 推薦場景 | Pipeline 品質 gate | 企業品質治理 | 需要 GX 表達力但不想離開 dbt |

## 我的理解 / 待解疑問

1. ✅ dbt test 的本質很優雅：每個 test = 一條找壞資料的 SELECT。0 行 = 通過。
2. ✅ Generic test 的複用性很高：YAML 宣告比寫 SQL 省 10 倍時間
3. ✅ `severity` + `error_if` + `warn_if` 的三層控制非常實用 — Bronze 層 warn，Gold 層 error
4. ✅ `store_failures: true` 是生產必備 — 失敗記錄存表，事後分析
5. 🤔 **dbt test 在 incremental model 上的效率問題**：大表每次 `dbt test` 全表掃描，是否有辦法只測新增的資料？→ 用 `where` config 限制範圍，但需要手動維護日期邏輯
6. 🤔 **dbt-expectations vs 原生 dbt tests**：什麼規模的團隊值得引入 dbt-expectations？→ 當自定義 generic test 超過 10 個時考慮
7. 🔗 **與 [[dbt-materialization]] 的關聯**：incremental model 的 MERGE 操作需要特別注意 test 設計 — dedup key 的 unique test 可能在 merge 前就有重複
8. 🔗 **與 [[great-expectations-core-concepts]] 的關聯**：GX 做內容驗證（值域、分佈），dbt test 做結構驗證（NULL、唯一、關聯）→ 互補不衝突
