# Delta Lake Time Travel 實作練習

**日期：** 2026-05-25
**科目：** Delta-Lake
**場景：** ODM 供應鏈 — 庫存管理 Time Travel

## 🏭 情境描述

你是某 ODM 公司（年出貨量 5000 萬台 3C 產品）的 Data Engineer。
倉庫庫存表（dim_inventory）每天被 MRP Run 和 GR/GI 操作頻繁更新。
今天發現昨晚的 MERGE 操作有 bug（一批 GPU 的 Safety Stock 被錯誤清零），
需要用 Time Travel 查看歷史版本、比較差異、並回滾到正確狀態。

## 💡 解題思路

1. 模擬 Delta Table 的多版本寫入（模擬 _delta_log 版本鏈）
2. 實作 VERSION AS OF 和 TIMESTAMP AS OF 查詢
3. 實作跨版本 diff（找出被 bug 影響的行）
4. 實作 RESTORE 回滾
5. 展示 DESCRIBE HISTORY 和 retention 設定的影響

## 🔧 實作重點

- **Mini Delta Log 模擬器**：用 JSON files 模擬 _delta_log 的 add/remove 語意
- **兩種 Time Travel 查詢**：VERSION AS OF（精確版本）vs TIMESTAMP AS OF（時間戳）
- **版本 Diff**：找出兩個版本間的 insert/update/delete 差異
- **VACUUM 模擬**：展示 VACUUM 如何破壞 Time Travel 能力
- **Retention 控制**：deletedFileRetentionDuration vs logRetentionDuration

## 📊 SA 延伸思考

- 如果是 SA，會建議 ODM 廠的庫存表設多長的 retention？（考慮月結/季結/年審的需求）
- RESTORE 在生產環境要不要加入 Airflow DAG 的 error handling？
- Shallow Clone vs Time Travel 在 disaster recovery 場景的 trade-off
