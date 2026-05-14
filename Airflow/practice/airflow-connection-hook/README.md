# Airflow Connection & Hook 練習

> 日期：2026-05-15  
> 主題：Connection 與 Hook — 不把密碼寫死在 DAG 裡

## 場景

ODM 供應鏈每日缺料檢查：

1. 從 **SAP ERP** 讀取未來 3 天到料需求（DB Connection）
2. 從 **MES/SFCS API** 讀取產線 WIP / 庫存（HTTP Connection）
3. 計算 shortage gap
4. 用 **Teams Webhook** 發出告警

重點不是 Airflow DAG 語法，而是把 Airflow 的設計概念拆出來實作：

```text
Connection Resolver  →  Hook.get_conn()  →  Task business logic
Secrets Backend          native client       shortage calculation
Env Var
Metadata DB
```

## 這次實作驗證什麼

- Connection 是集中化連線設定，不在 DAG 寫 host/password/token
- Connection 搜尋順序：Worker-specific Secrets → Global Secrets → Env Var → Metadata DB
- Hook 在 task 執行時才建立 native connection，避免 Scheduler parse DAG 時連外部系統
- Variable 存 runtime threshold，Connection 存認證/連線資訊
- SA 視角：prod 用 Secrets Backend；Metadata DB 只適合 dev / PoC / 非敏感 webhook placeholder

## 執行

```bash
cd /Users/json/Projects/scholar-learning
JAVA_HOME=/opt/homebrew/opt/openjdk@17 .venv/bin/python3 Airflow/practice/airflow-connection-hook/solution.py
```

## 預期結果

程式會印出：

- Scheduler parse 階段沒有 connection lookup
- 三個 Connection 分別從不同 backend 解析
- ERP + MES shortage 計算結果
- Teams 告警 payload
- 最後通過一組 assertions
