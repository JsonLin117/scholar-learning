# XCom 限制與最佳實踐 — 實作練習

**日期：** 2026-04-28
**科目：** Airflow（主修第 2 課）
**場景：** ODM 供應鏈採購資料 Pipeline

---

## 🏭 情境描述

你是緯穎的 Data Engineer，需要每日從 SAP MM 系統抽取採購訂單資料（PR→PO 的狀態），
做資料品質驗證，計算是否有缺料風險，並通知 SCM 團隊。

上週你的前任工程師寫了一個 DAG，把整個庫存 DataFrame（約 50 萬筆）通過 XCom 傳遞，
導致 Airflow metadata DB（PostgreSQL）因為 XCom 累積佔滿磁碟，每天凌晨都有 scheduler 警報。

你需要：
1. 識別原始 DAG 的 XCom 反模式
2. 重寫為「傳路徑不傳資料」的最佳實踐版本
3. 模擬執行，驗證 XCom 大小符合預期（< 1 KB）

---

## 💡 解題思路

核心原則：XCom 是 orchestration metadata，不是 data transport。

- 原始問題：50 萬筆 DataFrame 序列化後約 50-200 MB，直接寫進 PostgreSQL 的 `xcom.value` 欄位
- 解法：DataFrame 寫到 Delta Lake（或 mock 路徑），XCom 只傳 dict（路徑 + 統計數字）
- 驗證：序列化後的 XCom 值應 < 1 KB

---

## 🔧 實作重點

1. **`@task(multiple_outputs=True)`**：return dict 讓每個 key 成為獨立 XCom entry
2. **路徑命名規則**：用 `data_interval_start` 確保 idempotency（可重跑）
3. **XCom 大小估算**：用 `sys.getsizeof(json.dumps(xcom_value))` 驗證
4. **Retry 安全**：每個 task 都是 idempotent（重跑不會產生重複資料）

---

## 📊 SA 延伸思考

如果 metadata DB 是 MySQL（64 KB 上限），今天的最佳實踐仍然不夠：
- 即使只傳路徑 dict，如果有 critical_parts 清單很長（幾百個 part number），
  JSON 可能達到 100 KB，MySQL backend 直接炸掉
- 解法：改用 Object Storage XCom Backend，threshold=10240（10 KB）
  任何 > 10 KB 的 XCom 自動流到 Azure Blob Storage

```
生產環境架構：
Airflow metadata DB (PostgreSQL)
  → XCom < 10 KB: 存在 metadata DB
  → XCom > 10 KB: 自動存到 Azure Blob Storage
  → 對 task 程式碼透明（push/pull 不需改動）
```
