# Variable & Secret Backend 實作練習

**日期：** 2026-05-22  
**科目：** Airflow  
**場景：** ODM 供應鏈 / Airflow 設定與密鑰管理

## 🏭 情境描述

你是 ODM 伺服器廠的 Data Engineer，要把 TPE / KH / SZ 三個工廠的 ERP 同步 DAG 標準化。DAG 需要讀取非敏感 ETL 參數（批次大小、lookback days、目標 schema）、告警 channel，以及工廠專屬的受限設定。練習用 dependency-free Python 模擬 Airflow Variable 搜尋順序：Worker Secret Backend → Global Secret Backend → Environment Variable → Metadata DB，並示範 Secret Backend 唯讀與頂層 `Variable.get()` 的 scheduler 成本。

## 💡 解題思路

這題不是要真的啟動 Airflow，而是把今天的核心概念拆成可驗證模型：

1. `VariableResolver.get()` 模擬 Airflow runtime lookup order。
2. `SecretBackend` 支援 prefix、底線轉 hyphen、lookup pattern 與 TTL cache。
3. `MetadataDB` 模擬 Airflow DB，觀察頂層讀 Variable 造成 parse-time 查詢放大。
4. `VariableResolver.set()` 永遠寫 DB，用 phantom write 示範「寫了但讀不到」的坑。

## 🔧 實作重點

- **搜尋順序可觀察**：每次讀取會回傳來源（worker/global/env/db）與 API/DB hit counters。
- **反模式量化**：120 個 DAG、20 次 parse cycle，頂層 `Variable.get()` 會打 2,400 次 DB；改成 Jinja/runtime 只在 task 執行時讀。
- **Secret Backend 唯讀陷阱**：Backend 已有 `etl_config` 時，`Variable.set("etl_config", ...)` 寫進 DB，但 `get()` 仍被 Backend 覆蓋。

## 📊 SA 延伸思考

在緯穎這類 ODM 場景，推薦混合模式：敏感或跨環境配置放 Azure Key Vault / HashiCorp Vault，非敏感且需要營運人員 UI 調整的配置留在 Metadata DB。SA 要特別定義命名規範、lookup pattern、cache TTL，以及「禁止 DAG 頂層讀 Variable」的 code review rule，否則 Airflow 規模上來後 scheduler 和 Vault API 會先成為瓶頸。
