# Connection 與 Hook：怎麼設定資料庫連線，不寫死密碼

> 學習日期：2026-05-15 | 科目：Airflow（main）| 進度：第 4 課
> 本科目目標：能設計企業級 Airflow 架構：動態 DAG、K8s Executor、cross-DAG dependencies、SLA 監控

## 這是什麼？

**Connection** 是 Airflow 的「認證保險箱」——集中儲存外部系統的連線參數（host、port、login、password、schema、extra），每個 Connection 有唯一的 `conn_id`。

**Hook** 是「拿著鑰匙的搬運工」——封裝底層 API 呼叫的 Python 類，自動從 Connection 讀取認證，建立原生連線物件（DB cursor、HTTP client 等），讓你專注在業務邏輯。

```
Connection（認證存儲）  →  Hook（連線封裝）  →  Operator（任務執行）
     conn_id              get_conn()            execute()
```

## 為什麼重要？

1. **安全**：密碼不進 Git、不寫死在程式碼——Airflow 用 **Fernet 對稱加密**保護 DB 中的 password 和 extra 欄位
2. **集中管理**：100 個 DAG 共用同一個 DB 連線 → 改一個 Connection 就好
3. **環境切換**：dev/staging/prod 用不同 Connection，DAG 代碼不變
4. **權限隔離**：不同 DAG 用不同 conn_id，可以做到最小權限

## 怎麼運作？

### Connection 的結構

```python
# Connection 的核心欄位
{
    "conn_id": "odm_erp_prod",        # 唯一識別名
    "conn_type": "postgres",          # 連線類型（決定 Hook）
    "host": "erp-db.odm-factory.com",
    "login": "airflow_readonly",
    "password": "***encrypted***",    # Fernet 加密
    "port": 5432,
    "schema": "sap_production",
    "extra": {                        # 任意 JSON，存額外參數
        "sslmode": "require",
        "connect_timeout": 10
    }
}
```

### Connection 的三種儲存方式

| 方式 | 搜尋順序 | UI 可見 | 適用場景 |
|------|---------|---------|---------|
| **Secrets Backend**（Vault/Azure KV/AWS SSM）| 1️⃣ 最先 | ❌ | 企業級、密鑰輪換 |
| **環境變數**（`AIRFLOW_CONN_XXX`）| 2️⃣ 第二 | ❌ | K8s/Docker 部署 |
| **Metadata DB**（UI/CLI 設定）| 3️⃣ 最後 | ✅ | 小團隊、開發環境 |

> Airflow 3.x 新增 **Worker-specific Secrets Backend**（`[workers] secrets_backend`），搜尋順序變成：Worker Backend → Backend → 環境變數 → DB

### Hook 的運作機制

```python
from airflow.providers.postgres.hooks.postgres import PostgresHook

# 在 Operator 的 execute() 裡用
hook = PostgresHook(postgres_conn_id="odm_erp_prod")
# ↑ 不傳 conn_id 會用預設的 postgres_default

conn = hook.get_conn()       # 回傳 psycopg2 connection 物件
cursor = conn.cursor()
cursor.execute("SELECT * FROM sap_mm.purchase_orders WHERE created_date = '2026-05-15'")
rows = cursor.fetchall()

# Hook 也有便捷方法
df = hook.get_pandas_df("SELECT * FROM inventory WHERE plant = 'TW01'")
hook.run("INSERT INTO staging.orders VALUES (%s, %s)", parameters=(order_id, qty))
```

### 自訂 Hook 的結構

```python
from airflow.hooks.base import BaseHook

class ODMFactoryAPIHook(BaseHook):
    """自訂 Hook：連接 ODM 工廠 MES REST API"""
    
    conn_name_attr = "odm_conn_id"
    default_conn_name = "odm_factory_default"
    conn_type = "http"
    hook_name = "ODM Factory API"
    
    def __init__(self, odm_conn_id="odm_factory_default"):
        super().__init__()
        self.odm_conn_id = odm_conn_id
    
    def get_conn(self):
        """建立 HTTP Session，自動帶入認證"""
        conn = self.get_connection(self.odm_conn_id)  # 從 Connection 讀取
        import requests
        session = requests.Session()
        session.headers["Authorization"] = f"Bearer {conn.password}"
        session.base_url = f"https://{conn.host}:{conn.port}"
        return session
    
    def get_wip_status(self, plant_code: str):
        """取得工廠 WIP 狀態"""
        session = self.get_conn()
        resp = session.get(f"{session.base_url}/api/v1/plants/{plant_code}/wip")
        resp.raise_for_status()
        return resp.json()
```

### Variable vs Connection

| 面向 | Variable | Connection |
|------|----------|------------|
| **用途** | 全域 key/value 設定 | 外部系統認證+連線 |
| **存什麼** | 閾值、路徑、feature flag、環境名 | DB 密碼、API token、host/port |
| **存取方式** | `Variable.get("key")` | 透過 Hook（`XXXHook(conn_id=...)`） |
| **Scope** | 全域（3.x 支援 Team-scoped）| 全域 |
| **正確用法** | 真正 runtime-dependent 的設定 | 所有外部系統連線資訊 |
| **反模式** | ❌ 存大量資料（用 XCom）<br>❌ DAG 頂層呼叫（每次 parse 都 DB 查詢）| ❌ 密碼寫死程式碼<br>❌ 給 Admin 權限 |

## 程式碼範例

### 完整的 ODM 供應鏈 DAG（Connection + Hook 實戰）

```python
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.hooks.http import HttpHook
from airflow.sdk import Variable
from datetime import datetime

@dag(
    schedule="0 6 * * *",  # 每天早上 6:00
    start_date=datetime(2026, 1, 1),
    tags=["odm", "supply-chain"],
)
def odm_daily_shortage_check():
    """每日缺料檢查：ERP 訂單 vs MES 庫存"""
    
    @task
    def extract_erp_orders():
        # ✅ 在 task 內部才建立 Hook（不在 DAG 頂層）
        hook = PostgresHook(postgres_conn_id="odm_erp_prod")
        df = hook.get_pandas_df("""
            SELECT po_number, material_id, required_qty, required_date
            FROM sap_mm.purchase_orders 
            WHERE required_date = CURRENT_DATE + INTERVAL '3 days'
        """)
        return df.to_dict()
    
    @task
    def extract_mes_inventory():
        hook = HttpHook(http_conn_id="odm_mes_api", method="GET")
        response = hook.run(endpoint="/api/v1/inventory/current")
        return response.json()
    
    @task
    def calculate_shortage(orders, inventory):
        threshold = int(Variable.get("shortage_alert_threshold", default_var="100"))
        # ... 缺料計算邏輯 ...
        shortages = []
        for order in orders:
            inv = next((i for i in inventory if i["material_id"] == order["material_id"]), None)
            gap = order["required_qty"] - (inv["on_hand"] if inv else 0)
            if gap > threshold:
                shortages.append({**order, "gap": gap})
        return shortages
    
    @task
    def alert_shortage(shortages):
        if shortages:
            hook = HttpHook(http_conn_id="odm_teams_webhook")
            hook.run(endpoint="", data={"text": f"⚠️ 缺料警報：{len(shortages)} 項"})
    
    orders = extract_erp_orders()
    inventory = extract_mes_inventory()
    shortages = calculate_shortage(orders, inventory)
    alert_shortage(shortages)

odm_daily_shortage_check()
```

## 什麼時候不用 / 常見坑

### ❌ 反模式清單

1. **密碼寫死** — 推到 Git 的那一刻起就是公開的
2. **DAG 頂層建 Hook** — Scheduler 每 30s parse 一次 DAG，頂層的 Hook 會不斷連線
3. **Connection 用 Admin 權限** — 最小權限原則：只讀 task 就只給 SELECT
4. **自己用 requests 造輪子** — 不用現成 Hook，失去重試/logging/connection pool 等好處
5. **Variable 存大資料** — Variable 是設定用，不是 XCom 替代品
6. **忘記設 Fernet Key** — 沒設 `AIRFLOW__CORE__FERNET_KEY` 的話密碼是明文存的

### ✅ 最佳實踐

- **Connection 命名規範**：`<系統>_<環境>_<用途>`，如 `erp_prod_readonly`
- **Secrets Backend > 環境變數 > DB**：生產環境一定要用外部 Secrets Backend
- **Hook 在 execute() 內建立**：永遠不在 `__init__` 或 DAG 頂層
- **conn_id 不硬寫**：用 Variable 或 DAG params 傳入，方便 dev/prod 切換
- **定期輪換密碼**：用 Vault/Azure KV 的自動輪換功能

## 🏗️ SA 視角

### ODM 供應鏈場景

在鴻海/緯穎這樣的 ODM 公司，一個典型的資料平台需要連接：
- **SAP ERP**（JDBC/ODBC）— 採購訂單、物料主數據、庫存
- **MES/SFCS**（REST API）— 產線 WIP、良率、工站狀態
- **WMS**（REST/SOAP）— 倉庫收發貨、庫存位置
- **客戶 Portal API**（REST + OAuth）— CSP 客戶的 Forecast/PO
- **物流系統**（EDI/REST）— 出貨追蹤、報關文件

每個系統 × 每個環境（dev/staging/prod）× 每個工廠（TW/CN/MX）= **幾十個 Connection**。沒有集中管理會是災難。

### 數位轉型場景

- **導入門檻**：低。Connection + Hook 是 Airflow 最基本的功能。
- **真正的門檻**：**Secrets Backend 的基礎設施**——需要先有 Vault 或 Azure Key Vault，以及 IAM/RBAC 設計。
- 傳統 ODM 廠常見：密碼在 Excel 表裡、FTP 帳密寫死在 shell script → Connection + Secrets Backend 是第一步數位化。

### Solution Architect Trade-off

| 選型 | 推薦場景 | 代價 |
|------|---------|------|
| **Airflow DB + Fernet** | PoC、單團隊 | 手動輪換、UI 權限粗 |
| **Azure Key Vault** | Azure 原生環境、已有 AAD | Azure lock-in |
| **HashiCorp Vault** | 多雲、嚴格合規 | 額外運維、學習曲線 |
| **K8s Secrets + 環境變數** | 純 K8s 部署 | UI 看不到、管理分散 |

**SA 建議**：新專案直接用 Azure Key Vault（已有 Azure 環境）+ Managed Identity（無密碼認證），Connection 只存 conn_id 和 host，password 由 Vault 動態提供。

## 我的理解 / 待解疑問

- [x] Connection 是認證儲存，Hook 是連線封裝 — 清楚
- [x] 三種儲存方式的搜尋順序和 trade-off — 清楚
- [x] Variable vs Connection 的定位差異 — 清楚
- [ ] 實際在 Azure Key Vault 配 Secrets Backend 的步驟（等 Cloud-Architecture 課深挖）
- [ ] Airflow 3.x 的 Multi-Team 如何影響 Connection 權限隔離
- [ ] 大型 ODM 工廠幾十個 Connection 的命名規範和治理最佳實踐

## See Also
- [[Airflow/xcom-mechanism]] — XCom 負責 task 間資料傳遞，Connection/Hook 負責外部系統連線
- [[Airflow/sensor-filesensor-externaltasksensor]] — Sensor 也用 Hook（如 SqlSensor 用 DbApiHook）
- [[DevOps-CICD/cicd-what-is-it]] — CI/CD 中 Secrets 管理跟 Airflow Secrets Backend 同源
