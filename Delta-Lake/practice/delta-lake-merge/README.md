# Delta Lake MERGE 操作實作練習

**日期：** 2026-06-02
**科目：** Delta-Lake
**場景：** ODM 供應鏈 — 供應商主數據 CDC 同步

## 🏭 情境描述

某 ODM 伺服器製造商（年出貨 5000 萬台）的 SAP ERP 每天產出供應商主數據的 CDC 事件（INSERT/UPDATE/DELETE）。
需要用 Delta Lake MERGE 將 CDC 事件應用到 Silver 層的 `dim_vendor` 表，支援：
1. **Basic Upsert**：新供應商 INSERT，舊供應商 UPDATE
2. **Conditional Update**：只在真正有變更時才觸發 CoW 重寫
3. **Insert-Only Dedup**：採購訂單事件去重寫入
4. **SCD Type 2**：追蹤供應商 AVL 狀態和 Lead Time 的變更歷史
5. **Incremental Sync + Soft Delete**：source 裡消失的供應商標記為 inactive

## 💡 解題思路

用 dependency-free 的 Mini Delta 模擬器（延續 time-travel 練習的 MiniDelta），
模擬 Delta Lake MERGE 的五大模式，觀察每種模式的行為差異：
- CoW 重寫的觸發條件（有/無 conditional update 的差別）
- Source 重複 key 的錯誤處理
- SCD Type 2 的 UNION 展開法
- WHEN NOT MATCHED BY SOURCE 的安全使用

## 🔧 實作重點

1. **MiniMerge 引擎**：模擬 MERGE 的四個 clause（MATCHED/NOT MATCHED/NOT MATCHED BY SOURCE）
2. **CoW 計數器**：追蹤每次 MERGE 實際重寫了多少行（對比 conditional vs unconditional）
3. **Source Dedup 防呆**：偵測 source 重複 key 並 raise error
4. **SCD Type 2 MERGE**：UNION 展開法，一次 MERGE 同時 close old + insert new
5. **Metrics 輸出**：每種模式的 rows_inserted / rows_updated / rows_deleted / files_rewritten

## 📊 SA 延伸思考

- Conditional Update 減少的 CoW 重寫量在 ODM 場景下是多少？（供應商主數據 90% 不變）
- SCD Type 2 的存儲增長率：如果 5% 供應商每月有屬性變更，dim_vendor 一年增長多少？
- MERGE + Liquid Clustering 的最佳組合：ON 條件欄位是否應該都加入 clustering key？
