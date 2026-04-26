# 反思 2026-04-26

## 今天學了什麼

科目：Airflow（main）| 主題：XCom 機制（第 1 課）
SC Daily #2：緯穎 CSP 客戶結構（微軟/Meta/AWS/Google）

---

## Senior DE 考題

**題目：**
> 「一個 Airflow DAG 有 5 個 task，前 3 個 task 的輸出需要傳給第 4 個 task 合併處理，你怎麼設計 XCom 的資料傳遞？如果其中一個 task 傳的資料量是 10MB 的 DataFrame，你怎麼處理？」

**我的作答（不看筆記）：**

用 TaskFlow API 讓 3 個上游 task 各自 return 值，第 4 個 task 的 @task 函式接收 3 個參數。Airflow 自動 pull XCom。

10MB DataFrame → **不能存 XCom**（MySQL 64KB，PostgreSQL 雖然大但也不適合）。正確做法：把 DataFrame 存 S3 parquet，XCom 只傳 S3 路徑。進階：Custom XCom Backend 指向 S3，讓大物件透明序列化到 blob store。

**對照評分：⭐⭐⭐⭐⭐ (5/5)**

| 要點 | 是否答對 |
|------|---------|
| XCom 存 metadata DB | ✅ |
| TaskFlow API 語法 | ✅ |
| MySQL 64KB 限制 | ✅ |
| S3 路徑替代方案 | ✅ |
| Custom XCom Backend | ✅ |

**漏掉的：** 無明顯遺漏，但沒提 Jinja template 裡的 XCom 引用方式（`{{ task_instance.xcom_pull(...) }}`）。

---

## NotebookLM 追問：KubernetesExecutor 下 XCom 的效能問題

**書中觀點（Fundamentals of Data Engineering）：**
- Airflow Metadata DB 是「**無法橫向擴展的核心元件**」，是整個系統的效能瓶頸
- 把大資料塞進 XCom = 直接拖垮 Scheduler + DB，可能導致 Airflow 當機
- 最佳實踐：控制流（Airflow / XCom 傳路徑）和資料流（S3 存資料）完全分離
- 書也提到 Prefect / Dagster 在嘗試解決這個架構痛點（Scheduler 和 DB 的 scalability 問題）

**新的理解：**
KubernetesExecutor 的每個 task 在獨立 Pod 跑 → XCom 的讀寫跨 Pod 必須經過 metadata DB → 在高並發情況下（很多 task 同時 push/pull），DB 會成為 SPOF（Single Point of Failure）。解法是 Custom XCom Backend（S3）+ 只傳輕量 metadata。

---

## SA 挑戰：ODM 場景

**情境：**
> 緯穎有一個 Airflow DAG，每天凌晨批次處理當日出貨資料：
> - task_1：從 SAP GR 讀取今天所有出貨記錄（約 5 萬筆，20MB）
> - task_2：從 MES 讀取今天的生產良率（約 2MB）
> - task_3：合併以上兩份資料，計算每個 SKU 的 OTIF
> - task_4：把結果寫入 Delta Lake
>
> 工程師直接用 XCom 傳 DataFrame，偶爾 Scheduler 很慢，不知道為什麼。你怎麼改？

**我的方案：**

```
❌ 現況：task_1/task_2 → XCom（DataFrame 20MB/2MB） → task_3 → XCom → task_4

✅ 改法：
task_1 → 寫 ADLS Bronze parquet → XCom 傳路徑 "abfss://bronze/sap-gr/20260426.parquet"
task_2 → 寫 ADLS Bronze parquet → XCom 傳路徑 "abfss://bronze/mes-yield/20260426.parquet"
task_3 → 從 ADLS 讀兩個 parquet → 計算 OTIF → 寫 ADLS Silver parquet → XCom 傳 Silver 路徑
task_4 → 從 Silver 路徑 MERGE 進 Delta Lake
```

**Trade-off：**
- ✅ Metadata DB 不再承受資料壓力，Scheduler 恢復正常
- ✅ 每個 task 的輸出都有 parquet 存底，方便 debug 和 backfill
- ⚠️ 多了 ADLS 讀寫的 I/O 成本（小問題，Cloud storage 便宜且快）
- ⚠️ 路徑管理要規範（建議用 `{{ ds }}` 日期參數注入路徑）

**SA 進一步思考：**
如果 Airflow Scheduler 本身已成瓶頸（日後規模更大），可以評估：
- Lakeflow Jobs（Databricks 原生 orchestrator，和 Delta Lake 深度整合，Scheduler 非 metadata DB 依賴）
- 但緯穎目前的 SAP + 多系統整合，Airflow 的 Sensor 和 Operator 生態更豐富，短期不換

---

## 明天想繼續探索的問題

1. XCom Custom Backend（S3）怎麼設定？有沒有範例 config？
2. Airflow 的 Metadata DB 有 high availability 方案嗎（read replica + connection pool）？
3. Prefect/Dagster 的 "data-native" 是什麼意思，怎麼解決 Airflow 的資料傳遞問題？

---

## SC Daily 連結

今天學的 CSP 客戶結構（Forecast → Firm PO → Pull 的夾心困境）和 XCom 有什麼連結？

**供應鏈 pipeline 設計啟發：**
緯穎的 Airflow DAG 需要處理 CSP 的 Rolling Forecast CSV（每週更新），Firm PO 資料（每日 EDI），兩者都要合併做庫存計算。
- Forecast CSV 可能 50MB+ → 走 ADLS + XCom 傳路徑
- 不同客戶（微軟/Meta）的 EDI 格式不同 → 每個客戶的 parse task 並行執行，結果存 Silver parquet，再由 task_merge 讀取計算
- XCom 只在各 task 間傳遞 `{"customer": "Microsoft", "path": "abfss://..."}` 這種輕量 JSON
