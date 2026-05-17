# SCD Type 1 實作練習

**日期：** 2026-05-18  
**科目：** Data-Warehouse  
**場景：** ODM 供應鏈 / 供應商維度主檔同步

## 🏭 情境描述

你是某 ODM 伺服器公司的 Data Engineer。SAP 與供應商 Portal 每天送來 `vendor_master_updates`，要同步到 Gold layer 的 `dim_vendor`。這張維度表主要給採購、品質、CF 報表查詢「目前最新的供應商資訊」。本練習要實作 SCD Type 1 merge：同一供應商只保留一筆最新狀態，修正電話、email、顯示名稱等「不需要歷史」欄位；但地址、付款條件、AVL 狀態這類會影響稽核/財務/供應商評鑑的欄位不能悄悄覆寫，必須被標示為需要 Type 2。

## 💡 解題思路

SCD Type 1 的核心不是「看到資料就 update」，而是三件事：

1. **Source 去重**：同一批來源可能有同一個 `vendor_id` 多筆更新，先取 `source_update_ts` 最新、`ingest_seq` 最大的一筆。
2. **條件更新**：只有 Type 1 欄位真的改變時才 update，避免 Delta Lake / Lakehouse 的無效 Copy-on-Write。
3. **欄位級 SCD 策略**：不是整張 dim_vendor 都 Type 1；SA 要逐欄位判斷哪些可覆寫、哪些要保留歷史。

## 🔧 實作重點

- `deduplicate_latest()` 模擬 SQL `ROW_NUMBER() OVER (PARTITION BY vendor_id ORDER BY update_time DESC)`。
- `merge_scd_type1()` 模擬 `MERGE INTO dim_vendor` 的 `WHEN MATCHED THEN UPDATE` / `WHEN NOT MATCHED THEN INSERT`。
- `HISTORY_SENSITIVE_FIELDS` 會被寫入 audit log，而不是直接覆寫，避免把 Type 2 需求錯做成 Type 1。

## 📊 SA 延伸思考

Production Lakehouse 裡可以把這個練習映射成：

- Bronze：原始 SAP/Portal vendor updates，保留每次 ingest。
- Silver：去重、標準化、欄位級 SCD 策略檢查。
- Gold：`dim_vendor_current` 用 Type 1 快速服務目前狀態查詢。
- 另一張 Gold：`dim_vendor_history` / dbt snapshot 用 Type 2 保留地址、付款條件、AVL 狀態歷史。

Trade-off：Type 1 查詢簡單、成本低，但不能回答「當時值是多少」。正式設計時應該用欄位級策略文件明確標註，避免採購報表和財務稽核在不同時間看到不可重現的結果。
