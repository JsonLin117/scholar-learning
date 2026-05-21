# Variable 與 Secret Backend：設定管理的正確方式

> 學習日期：2026-05-22 | 科目：Airflow（main）| 進度：第 5 課
> 本科目目標：能設計企業級 Airflow 架構：動態 DAG、K8s Executor、cross-DAG dependencies、SLA 監控
> 今日選題依據：正常輪換 P4（dayCount=31, mainRotationIndex=1 → Airflow）

⏰ 今日 SR 到期：9 張（Phase B 複習）

## 這是什麼？

**Variable** 是 Airflow 的「全域設定中心」——一個 key/value store，用來存放 DAG 運行時需要的配置參數（非認證類），像是環境名稱、S3 bucket 路徑、feature flag、批次大小等。

**Secret Backend** 是 Airflow 的「保險箱的保險箱」——把 Connection 和 Variable 的存放委託給外部密鑰管理系統（HashiCorp Vault、Azure Key Vault、AWS Secrets Manager 等），而不是存在 Airflow 自己的 metadata DB 裡。

```
DAG 代碼需要配置
  ├── 認證類（帳號密碼）→ Connection（上一課）
  ├── 配置類（路徑、參數）→ Variable ← 本課
  └── 儲存後端選擇 → Secret Backend ← 本課
```

## 為什麼重要？

1. **配置與代碼分離**：DAG 代碼在 Git，設定在 Variable → 改參數不用重新部署 DAG
2. **環境切換零改動**：dev/staging/prod 各有自己的 Variable 值，DAG 代碼完全不變
3. **安全合規**：Secret Backend 讓敏感值永遠不碰 Airflow DB，滿足 SOC2/ISO 審計
4. **密鑰輪換**：Vault/KV 的自動輪換機制，Airflow 不需要重啟就能拿到新密鑰
5. **多團隊隔離**：Airflow 3.x 的 Multi-Team Variable 支持團隊級別的配置隔離

## 怎麼運作？

### Variable 的基本用法

```python
from airflow.sdk import Variable

# ① 直接取值
bucket = Variable.get("s3_landing_bucket")

# ② JSON 自動反序列化
config = Variable.get("etl_config", deserialize_json=True)
# → {"batch_size": 5000, "retry_count": 3, "env": "prod"}

# ③ 帶預設值（變數不存在時不報錯）
feature_flag = Variable.get("enable_new_pipeline", default="false")

# ④ Jinja 模板中使用（BashOperator / templated fields）
# {{ var.value.s3_landing_bucket }}
# {{ var.json.etl_config.batch_size }}
```

### Variable 的三種存儲方式

| 方式 | 語法 | UI 可見 | 適用場景 |
|------|------|---------|---------|
| **Metadata DB** | UI/CLI 設定 | ✅ | 開發環境、非敏感配置 |
| **環境變數** | `AIRFLOW_VAR_{KEY}` 全大寫 | ❌ | K8s/Docker 部署 |
| **Secret Backend** | 配置 `[secrets]` section | ❌ | 企業級、密鑰管理 |

**搜尋順序**（Airflow 3.x）：
```
Worker Secrets Backend → Secrets Backend → 環境變數 → Metadata DB
```

> ⚠️ **重要**：`Variable.set()` 永遠寫到 Metadata DB，即使你配了 Secret Backend。Secret Backend 是**唯讀**的！要更新 Secret Backend 的值，必須直接操作 Vault/KV。

### Variable vs Connection 的選擇

| | Variable | Connection |
|---|---|---|
| **用途** | 存**配置參數** | 存**認證資訊** |
| **典型內容** | 路徑、開關、批次大小 | host/port/login/password |
| **加密** | Fernet 加密（DB 中） | Fernet 加密（password + extra） |
| **結構** | 純 key/value（string 或 JSON） | 有固定 schema（conn_type/host/port...） |
| **Hook 整合** | ❌ 手動取值 | ✅ Hook 自動讀取 |

**黃金法則**：
- 能用 `hook.get_conn()` 就用 Connection（有類型檢查和結構化）
- 剩下的全域配置才用 Variable
- **不要用 Variable 存密碼**——用 Connection 或 Secret Backend

### Variable 的反模式（踩坑）

#### 🚫 反模式 1：在 DAG 頂層讀 Variable

```python
# ❌ 錯誤：每次 scheduler parse DAG 都會觸發 DB 查詢
BUCKET = Variable.get("s3_bucket")  # 頂層 → 30 秒 parse 一次

with DAG(...) as dag:
    task = BashOperator(
        task_id="process",
        bash_command=f"aws s3 cp s3://{BUCKET}/data ."  # 每次 parse 都查 DB
    )
```

```python
# ✅ 正確方式 1：用 Jinja 模板（只在 task 執行時解析）
task = BashOperator(
    task_id="process",
    bash_command="aws s3 cp s3://{{ var.value.s3_bucket }}/data ."
)

# ✅ 正確方式 2：在 task function 裡面讀（TaskFlow API）
@task
def process():
    bucket = Variable.get("s3_bucket")
    # ... 業務邏輯
```

#### 🚫 反模式 2：把 Variable 當 XCom 用

```python
# ❌ 錯誤：Variable 是全域的，會被其他 DAG run 覆蓋
Variable.set("last_processed_id", "12345")  # 如果兩個 DAG run 同時跑...

# ✅ 正確：task 間傳資料用 XCom
@task
def extract():
    return {"last_id": 12345}  # 自動 push 到 XCom
```

#### 🚫 反模式 3：環境變數命名搞混

```bash
# Connection 環境變數：雙底線
AIRFLOW_CONN_MY_DB=postgres://...

# Variable 環境變數：單底線（VAR 前後各一個底線）
AIRFLOW_VAR_MY_CONFIG=value

# Airflow 設定：雙底線
AIRFLOW__CORE__FERNET_KEY=...
```

### Secret Backend 深度

#### 架構原理

```
┌─────────────────────────────────────────┐
│ Airflow Worker                          │
│  ┌─────────────────────────────────┐    │
│  │ Task 執行                        │    │
│  │  Variable.get("api_key")        │    │
│  │        │                        │    │
│  │        ▼                        │    │
│  │  SecretsBackend.get_variable()  │    │
│  └─────────────────────────────────┘    │
│          │                              │
│  ┌───────┴───────────────┐              │
│  │ 搜尋順序               │              │
│  │ 1. Worker Backend     │→ Vault(worker-specific)
│  │ 2. Secrets Backend    │→ Azure Key Vault
│  │ 3. 環境變數           │→ AIRFLOW_VAR_XXX
│  │ 4. Metadata DB        │→ Fernet 加密
│  └───────────────────────┘              │
└─────────────────────────────────────────┘
```

#### 主流 Secret Backend 選型

| Backend | 雲平台 | 特色 | ODM 適用性 |
|---------|--------|------|-----------|
| **Azure Key Vault** | Azure | DefaultAzureCredential、Managed Identity | ⭐⭐⭐ 緯穎用 Azure |
| **AWS Secrets Manager** | AWS | 自動密鑰輪換、cross-account | CSP 客戶端可能用 |
| **AWS SSM Parameter Store** | AWS | 免費額度高、適合非敏感配置 | 配合 Secrets Manager |
| **HashiCorp Vault** | 跨雲 | 開源、最靈活、審計完整 | 多雲架構首選 |
| **GCP Secret Manager** | GCP | IAM 整合好 | Google CSP 場景 |
| **K8s Secrets** | K8s | 原生整合、無額外依賴 | Airflow on K8s |

#### Azure Key Vault 配置範例（Json 的場景）

```ini
# airflow.cfg
[secrets]
backend = airflow.providers.microsoft.azure.secrets.key_vault.AzureKeyVaultBackend
backend_kwargs = {
    "connections_prefix": "airflow-connections",
    "variables_prefix": "airflow-variables",
    "vault_url": "https://odm-airflow-kv.vault.azure.net/"
}
```

```bash
# Azure Key Vault 中的命名
# Connection: airflow-connections-odm-erp-prod  (hyphen 取代 underscore)
# Variable:   airflow-variables-s3-landing-bucket

# Multi-Team（Airflow 3.x）
# Team 級別: airflow-variables-team-de--s3-landing-bucket
# 全域 fallback: airflow-variables-s3-landing-bucket
```

#### Airflow 3.x Worker-Specific Backend（新功能）

```ini
# 全域 Backend：scheduler/webserver/worker 共用
[secrets]
backend = airflow.providers.microsoft.azure.secrets.key_vault.AzureKeyVaultBackend
backend_kwargs = {"vault_url": "https://airflow-global-kv.vault.azure.net/"}

# Worker 專屬 Backend：只有 worker 能看到（更安全）
[workers]
secrets_backend = airflow.providers.microsoft.azure.secrets.key_vault.AzureKeyVaultBackend
secrets_backend_kwargs = {"vault_url": "https://airflow-worker-kv.vault.azure.net/"}
```

**安全意義**：Worker Backend 的密鑰只注入到執行 task 的 worker process，webserver 看不到 → 即使 Airflow UI 被入侵，敏感密鑰也不會洩漏。

### Fernet 加密機制

```python
# Fernet = 對稱加密（AES-128-CBC + HMAC-SHA256）
# 用於加密 Metadata DB 中的 Connection password/extra 和 Variable value

from cryptography.fernet import Fernet
key = Fernet.generate_key()
# → b'YlCKzE2xJPJsqR7MFhYCpf4eMSRp4i3qD1tLFYU5wbs='

# 密鑰輪換（不停機）：
# 1. fernet_key = new_key,old_key
# 2. airflow rotate-fernet-key（重新加密所有值）
# 3. fernet_key = new_key（移除舊 key）
```

## 程式碼範例

### ODM 供應鏈 DAG：用 Variable 管理環境配置

```python
from airflow.sdk import DAG, Variable
from airflow.decorators import task
from datetime import datetime

with DAG(
    dag_id="odm_erp_daily_sync",
    schedule="0 6 * * *",
    start_date=datetime(2026, 1, 1),
) as dag:

    @task
    def extract_erp_data():
        # ✅ 在 task 內部讀 Variable（不在頂層）
        config = Variable.get("erp_sync_config", deserialize_json=True)
        # → {"plants": ["TPE", "KH", "SZ"], "batch_size": 10000,
        #    "lookback_days": 3, "target_schema": "bronze"}

        plants = config["plants"]
        batch_size = config["batch_size"]
        # ... 從 ERP 抽資料

    @task
    def load_to_delta(plant: str):
        # ✅ 混合使用 Variable + Connection
        target_path = Variable.get("delta_lake_base_path")
        # Connection 由 Hook 自動處理（上一課）
        from airflow.providers.databricks.hooks.databricks import DatabricksHook
        hook = DatabricksHook(databricks_conn_id="databricks_prod")
        # ...

    extract_erp_data() >> load_to_delta("TPE")
```

### 批次 Variable 管理（JSON 匯入匯出）

```bash
# 匯出（本地 CLI 限定，安全考量 API 不支援）
airflow variables export variables.json

# 匯入
airflow variables import variables.json

# variables.json 範例
{
    "erp_sync_config": "{\"plants\": [\"TPE\", \"KH\"], \"batch_size\": 10000}",
    "s3_landing_bucket": "odm-data-lake-landing",
    "enable_quality_check": "true",
    "alert_email": "de-team@odm.com"
}
```

## 什麼時候不用 / 常見坑

### 不該用 Variable 的場景

| 場景 | 為什麼不用 Variable | 該用什麼 |
|------|---------------------|---------|
| task 間傳資料 | Variable 是全域的，concurrent DAG run 會覆蓋 | **XCom** |
| 存密碼/token | Variable 加密但結構不佳 | **Connection** |
| 大量資料（>10KB） | DB bloat、parse 慢 | **S3/GCS + XCom 傳路徑** |
| 頻繁更新的值 | DB 鎖競爭 | **外部 cache（Redis）** |

### Secret Backend 的坑

1. **唯讀陷阱**：`Variable.set()` 寫到 DB，但 `Variable.get()` 先查 Backend → 造成「寫了但讀不到」的詭異 bug。Airflow 3.x 會 log warning 但不阻擋。
2. **Key 命名衝突**：Azure Key Vault 不接受底線（`_`），會自動轉成 hyphen（`-`）。`odm_erp_prod` → `airflow-connections-odm-erp-prod`。
3. **UI 不可見**：Secret Backend 的值在 Airflow UI 看不到，debug 時要直接查 Vault/KV。
4. **冷啟動延遲**：每次 `Variable.get()` 都打 Vault API → 要做 caching（大部分 Backend 支援 `cache_ttl_seconds`）。
5. **lookup_pattern 很重要**：如果不設 `variables_lookup_pattern`，每次 `Variable.get()` 都會先打 Backend，即使那個 key 不在 Backend 裡。用 regex 過濾可減少不必要的 API call。

## 🏗️ SA 視角

### ODM 供應鏈場景

**多工廠統一設定管理**：
- TPE/KH/SZ 三個工廠各有自己的 ERP 連線 → **Connection**（每工廠一個 conn_id）
- 共用的 ETL 參數（批次大小、重試次數、landing path）→ **Variable**（一份 JSON 管全部）
- Azure Key Vault 當 Backend → 工廠的 DB 密碼存在 KV 裡，不碰 Airflow DB

**實際配置策略**：
```
Azure Key Vault (Secret Backend)
  ├── airflow-connections-erp-tpe-prod   ← ERP 連線
  ├── airflow-connections-erp-kh-prod
  ├── airflow-connections-databricks-prod
  ├── airflow-variables-etl-config       ← 全域 ETL 設定
  └── airflow-variables-alert-config     ← 告警配置

Airflow DB (UI 可見的非敏感配置)
  ├── feature_flag_new_pipeline = true
  └── dashboard_refresh_interval = 30
```

### 數位轉型場景

導入 Secret Backend 是「從小作坊升級到企業級」的關鍵步驟：
- **Before**：密碼寫在 .env、DAG 代碼、甚至 Slack 裡傳
- **After**：所有密鑰集中在 Vault/KV，有審計日誌、自動輪換、RBAC

**導入門檻**：中等。需要先有 Azure Key Vault 或 HashiCorp Vault 基礎建設。
**替代方案**：K8s Secrets（如果已經在 K8s 上跑 Airflow）。

### Solution Architect Trade-off

| 方案 | 優點 | 缺點 | 推薦場景 |
|------|------|------|---------|
| **純 DB + Fernet** | 簡單、UI 可見 | 密鑰輪換手動、單點故障 | 小團隊、開發環境 |
| **環境變數** | 與 K8s 原生整合 | 散落各處、版本控制難 | 容器化部署 |
| **Azure Key Vault** | 自動輪換、審計、RBAC | 需要 Azure 基建、API 延遲 | Azure 生態的企業 |
| **HashiCorp Vault** | 跨雲、最靈活、動態 secrets | 運維成本高、需專人管理 | 多雲架構、金融級安全 |
| **混合模式** | 敏感進 KV、非敏感留 DB | 架構複雜度高 | 大型企業（推薦） |

**SA 選型建議**：
- 如果只用 Azure → **Azure Key Vault**（DefaultAzureCredential 零配置）
- 如果多雲/混合雲 → **HashiCorp Vault**
- 如果 Airflow on K8s 且安全需求中等 → **K8s Secrets Backend**
- **不管選哪個，Worker-Specific Backend 都建議開**（Airflow 3.x 限定）

## 我的理解 / 待解疑問

1. **Variable 頂層讀取的效能衝擊**：在有 100+ DAG 的環境，如果每個 DAG 頂層都讀 Variable → scheduler 每 30 秒 parse 所有 DAG → 100+ 次 DB 查詢。這是 Airflow 效能瓶頸的常見根因。**解法**：Jinja template 或 task 內讀取。
2. **Secret Backend 的 cache_ttl**：如果 Vault 裡的密鑰被輪換，cache TTL 到期前 task 會用舊密鑰 → 可能失敗。需要在 TTL 和即時性之間取平衡。
3. **Multi-Team Variable 隔離**：Airflow 3.x 新功能，team-scoped Variable 的 Backend 命名用 `--` 分隔（如 `airflow-variables-team-de--config`）。這對多團隊 ODM 組織很有價值。
4. 待探索：Variable.get() 在 deferred task（async）中的行為——是在 triggerer 還是 worker 上解析？

## See Also

- [[Airflow/connection-and-hook]] — Connection 是「認證存儲」，Variable 是「配置存儲」，兩者互補
- [[Airflow/xcom-mechanism]] — XCom 傳 task 間資料，Variable 傳全域配置
- [[Airflow/xcom-limits-best-practices]] — 不要用 Variable 當 XCom
