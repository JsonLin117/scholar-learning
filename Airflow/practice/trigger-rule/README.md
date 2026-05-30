# Trigger Rule 實作練習

**日期：** 2026-05-31
**科目：** Airflow
**場景：** ODM 供應鏈 — 多源 ERP ETL Pipeline + 錯誤處理 + 分支合併

## 🏭 情境描述

某 ODM 公司（年出貨 5000 萬台 3C 產品）的 Data Engineer 需要設計一個每日 ETL DAG：
1. 從 3 個數據源平行拉取（SAP ERP、MES、WMS）
2. 任一來源成功就先生成部分報告（`one_success`）
3. 根據是否有品質異常走不同處理路徑（branching）
4. 分支合併時不被 skip cascade 影響（`none_failed_min_one_success`）
5. 不管成功失敗都通知 PMC（`all_done`）
6. 第一時間偵測失敗並告警（`one_failed`）

## 💡 解題思路

用 dependency-free 的 Mini Airflow DAG Scheduler 模擬 11 種 trigger rule 的行為，
不需要安裝 Airflow。重點在於：
1. 理解每種 trigger rule 的判斷邏輯
2. 模擬 skip cascading 在 branching 後的傳播
3. 對比「正確」和「錯誤」的 trigger rule 選擇

## 🔧 實作重點

1. **MiniScheduler**：模擬 Airflow scheduler 對 trigger rule 的判斷邏輯
2. **Skip Cascade**：`all_success` 遇到 skipped upstream 時的傳播行為
3. **Eager Evaluation**：`one_success`/`one_failed` 不等待所有上游完成
4. **Branch Simulation**：模擬 `@task.branch` 的 skip 注入行為

## 📊 SA 延伸思考

如果 DAG 越來越複雜（20+ tasks），trigger rule 的組合可能讓行為難以預測。
SA 建議：
- 用 Setup/Teardown 取代部分 `all_done` 的清理工作
- 用 `on_failure_callback` 取代 `one_failed` 告警 task
- 在 DAG 文件加 docstring 說明每個 trigger rule 的設計原因
