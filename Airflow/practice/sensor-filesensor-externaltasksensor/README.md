# Airflow Sensor 實作練習

**日期：** 2026-04-29
**科目：** Airflow（主修，第 3 課）
**場景：** ODM 供應鏈 — ERP 落檔等待 + 跨 DAG 依賴

---

## 🏭 情境描述

你是緯穎（Wistron）的 Data Engineer。每天凌晨，SAP ERP 系統會把前一天的採購訂單（PO）
匯出成 parquet 檔存到 Azure Blob Storage，但 ERP 匯出的時間不固定（通常在 02:00 ~ 03:30 之間）。

你需要：
1. **DAG 1 (`erp_po_ingestion`)**: 等 ERP PO 檔案出現 → 把 parquet 讀進 Delta Lake Bronze 層
2. **DAG 2 (`supply_chain_kpi`)**: 等 DAG 1 的清洗任務完成 → 才能計算供應商 OTIF 指標

不能用「定時猜」，因為 ERP 延遲時下游就會拿到昨天的資料。

---

## 💡 解題思路

- `erp_po_ingestion` DAG 用 **FileSensor**（reschedule mode）等 parquet 出現
- `supply_chain_kpi` DAG 用 **ExternalTaskSensor** 等 `erp_po_ingestion` 的 `validate_data` task 完成
- 兩者都設 `timeout`，避免 Sensor 永遠等

---

## 🔧 實作重點

1. **reschedule mode** — 長等待不佔 worker slot，關鍵效能改進
2. **timeout 必設** — 防止 Sensor 掛住整個 Slot Pool
3. **FileSensor 用 Connection** — 路徑透過 `fs_conn_id` 設定，不寫死

---

## 📊 SA 延伸思考

**如果 ERP 某天凌晨 4:00 都沒出檔？**
- timeout=7200 觸發 → DAG FAIL
- 設 `on_failure_callback` 發 Teams/Slack 告警
- 下游 `supply_chain_kpi` 因 ExternalTaskSensor 等不到，也會 timeout FAIL

**Airflow 3.2 改進方向：**
- 用 Asset Partitioning 取代 ExternalTaskSensor
- 宣告 `Asset("abfs://datalake/silver/po/{{ ds }}")` 
- 解耦上下游 DAG，不需要知道 DAG id

**何時不用 Sensor？**
- ERP 固定 02:00 出檔，且 SLA 允許 2:30 才跑 → 直接設 schedule，不需 Sensor
- 追求毫秒級反應 → 換 Kafka + Flink，Airflow 不合適
