# TaskFlow API 實作練習

**日期：** 2026-05-24  
**科目：** Airflow  
**場景：** ODM 供應鏈 / Airflow TaskFlow API 機制模擬

## 🏭 情境描述

你是 ODM 廠的 Data Engineer，要設計一條每日採購資料 pipeline：從 SAP 抽取 PO 變更、依供應商與金額分類、寫入 Delta Lake，並更新供應商 scorecard。這條 DAG 適合用 TaskFlow API，因為它是典型的 Python ETL 資料流：上游函式的輸出會餵給下游函式，依賴關係可以自然從函式呼叫推導出來。

本練習不需要安裝 Airflow，而是實作一個極簡 TaskFlow runtime，讓 Json 能直接看見 `@task` 背後的幾件事：呼叫 task 不會立刻執行，而是回傳 `XComArg`；`XComArg` 形成 DAG edge；真正 run 時才依 dependency 執行並傳遞結果。

## 💡 解題思路

我把 TaskFlow API 拆成四個可觀察的核心概念：

1. `@task` 把普通函式包成 task definition。
2. 呼叫 task 時只建立 `TaskInvocation`，回傳 `XComArg`，不立刻跑業務邏輯。
3. 下游 task 接收 `XComArg` 作為參數時，自動產生 upstream dependency。
4. runtime 按拓撲順序執行 task，並用 XCom store 傳遞小型控制資料。

另外加上 `multiple_outputs=True`、`.override()` 與 `expand()`，對應今天筆記裡提到的 TaskFlow 進階用法。

## 🔧 實作重點

- **XComArg 物件化依賴**：`extract()` 回傳的是引用，不是真資料；下游參數引用會自動建 edge。
- **multiple_outputs 模擬**：task 回傳 dict 時，可用 `result["direct_pos"]` 產生 key-level XComArg。
- **Dynamic Task Mapping 模擬**：依供應商清單動態展開 scorecard task，展示 TaskFlow + `expand()` 的設計精神。

## 📊 SA 延伸思考

TaskFlow 很適合新建的 Python ETL DAG，尤其是 ODM 的採購、庫存、需求預測 pipeline。但 SA 要守住兩條邊界：

- 不要用 XCom 傳大資料，PO 明細、DataFrame、檔案內容應該放 ADLS/S3/Delta，只在 XCom 傳路徑或版本號。
- 不要為了追求 Pythonic 而把所有 Operator 都改成 TaskFlow；SparkSubmitOperator、Sensor、複雜 trigger rule 仍可用傳統 Operator 混搭。
