# SCD Type 2 實作練習

**日期：** 2026-05-27
**科目：** Data-Warehouse
**場景：** ODM 供應鏈

## 🏭 情境描述

你是緯穎的 Data Engineer。採購部門需要追蹤供應商 AVL 狀態的完整歷史（Active→Probation→Suspended→Obsolete），因為年度供應商績效報告需要知道「某供應商在 Probation 期間的交期和良率表現」。用 SCD Type 2 在 Delta Lake 上實作 dim_vendor 的完整歷史追蹤。

## 💡 解題思路

1. 用 dependency-free PySpark + Delta Lake 建立 dim_vendor 維度表
2. 實作 UNION 展開法的 MERGE（一次 MERGE 完成 close + insert，而非兩步法）
3. 用 hash-based Surrogate Key（sha2(vendor_id + effective_from)）確保冪等
4. 模擬多輪 CDC 更新（AVL 狀態變更、地址搬遷、新增供應商）
5. 驗證 Point-in-Time Query 的正確性
6. 對比欄位級 SCD 策略：Type 2 欄位（avl_status, lead_time）vs Type 1 欄位（phone, contact_person）

## 🔧 實作重點

- **UNION 展開法**：source 展開為 close_rows (merge_key=vendor_id) + insert_rows (merge_key=NULL)，一次 MERGE 原子完成
- **Hash-based SK**：`sha2(vendor_id || effective_from)` 讓重跑結果不變
- **去重防呆**：同日多次 CDC → ROW_NUMBER() 只取最新
- **9999-12-31 vs NULL**：統一用 9999-12-31 避免 BETWEEN 查詢問題

## 📊 SA 延伸思考

- Type 2 的表膨脹問題：OPTIMIZE + Liquid Clustering(vendor_id) 是最佳搭配
- dbt snapshot 可以零手寫 MERGE 做 SCD Type 2，但了解底層 MERGE 邏輯是 SA 必備
- 混合 SCD 策略（同一張 dim 表有 Type 1 和 Type 2 欄位）是實際生產環境的常態
