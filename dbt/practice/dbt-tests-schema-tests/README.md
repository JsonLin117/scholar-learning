# dbt Tests / Schema Tests 實作練習

**日期：** 2026-05-28  
**科目：** dbt  
**場景：** ODM 供應鏈 / P2P + AVL 資料品質 Gate

## 🏭 情境描述

你是 ODM 廠資料平台的 Data Engineer。SAP PO、供應商主檔與 AVL 狀態每天進入 Lakehouse，dbt 負責把 staging layer 轉成供應鏈分析用的 mart。今天的任務是實作一個「mini dbt test runner」，在資料進入 Gold 報表前攔截：NULL key、重複採購單、未知供應商、非法 AVL 狀態、過舊資料與供應商 Scorecard 計算錯誤。

## 💡 解題思路

dbt test 的本質是「找出違反規則的列」：回傳 0 筆代表通過，非 0 筆代表 fail/warn。本練習用純 Python 模擬 generic data tests、singular data tests、unit tests，並支援 `severity`、`warn_if`、`error_if`、`where` 與 `store_failures`，對應真實 dbt 專案裡常見的資料品質配置。

## 🔧 實作重點

1. **Generic tests**：實作 `not_null`、`unique`、`accepted_values`、`relationships`，展示 `relationships` 不檢查 NULL、需搭配 `not_null` 的坑。
2. **Severity gate**：Bronze/Silver 可用 `warn`，Gold 層關鍵商業規則用 `error` 中斷 pipeline。
3. **Unit + singular tests**：用靜態資料驗證 Vendor Scorecard SQL 邏輯，並用跨表規則找出高金額採購單使用未核准供應商的風險。

## 📊 SA 延伸思考

若落地到企業級 Lakehouse，dbt tests 適合做 transformation layer 的快速 gate；Great Expectations 或 Deequ 可補足跨系統、跨團隊的中央品質治理。架構上應把測試分層：Bronze 偏觀測與 warn、Silver 做結構與參照完整性、Gold 做業務規則與 BI 一致性，避免「全部 error」造成 pipeline 過度脆弱。
