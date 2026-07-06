# DAG 參數化練習：ODM 庫存對帳 Ad-hoc DAG 的 Params + Trigger UI 表單模擬

## 場景

緯穎倉管每天早上發現特定料號（Part Number）在特定倉別（RM/WIP/FG）帳實不符，
需要臨時觸發「庫存對帳 DAG」，只針對那個料號和倉別重算，不想動到全量排程。

這個 DAG 需要透過 Airflow `Param` 機制，讓倉管不用寫 code、直接在 Trigger UI
表單填資料就能安全觸發，同時要擋掉：
- 空的料號（避免誤觸發全量掃描）
- 不合法的倉別代碼（只能是 RM/WIP/FG）
- 型別錯誤（例如把布林值填成字串）

## 練習目標

不安裝真實 Airflow，純 Python 模擬 `Param` 的 JSON Schema 驗證邏輯 + 觸發流程，
驗證對「什麼樣的輸入會通過 / 被擋下」的理解，並模擬四種觸發來源
（Web UI 表單、CLI `--conf`、REST API、TriggerDagRunOperator 的 `conf`）如何
merge 出最終生效的 params。

## 涵蓋知識點

1. `Param` 的型別驗證（string/integer/boolean/enum/array/object）
2. DAG-level vs Task-level vs 使用者觸發覆寫的優先順序
3. JSON Schema 驗證失敗時的行為（擋在觸發前，不是 task 跑到一半才炸）
4. `dag_run_conf_overrides_params=False` 時鎖定為唯讀常數的行為
5. 多種觸發來源的 conf 合併邏輯

## 執行方式

```bash
cd /Users/json/Projects/scholar-learning
JAVA_HOME=/opt/homebrew/opt/openjdk@17 .venv/bin/python3 Airflow/practice/dag-parameterization-params-ui-trigger/solution.py
```

## 預期輸出

腳本會跑過一系列測試案例（合法輸入、空料號、非法倉別、型別錯誤、
`dag_run_conf_overrides_params=False` 鎖定情境），並印出每個案例的驗證結果
（PASS/REJECTED + 原因），最後印出一個彙總報告。
