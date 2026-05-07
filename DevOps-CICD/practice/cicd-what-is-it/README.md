# CI/CD 基礎實作練習

**日期：** 2026-05-08
**科目：** DevOps-CICD
**場景：** ODM 供應鏈 — Airflow DAG 的 CI 驗證

## 🏭 情境描述

你是某 ODM 公司（年出貨量 5000 萬台 3C 產品）的 Data Engineer。團隊有 5 位 DE 共同開發 Airflow DAG，管理從 ERP 抽取資料到 Lakehouse 的 ETL pipeline。

目前痛點：DAG 部署到 production 前沒有自動檢查，曾發生過：
1. 有人 push 了一個 import error 的 DAG，導致整個 DagBag 載入失敗
2. 有人忘記設 `catchup=False`，trigger 了 6 個月的 backfill
3. 密碼被寫在 DAG 裡推到 GitHub

你的任務：**寫一套 DAG 驗證測試 + 一個模擬的 GitHub Actions workflow**。

## 💡 解題思路

用 pytest 框架寫 DAG validation tests，模擬 CI pipeline 中最基本的品質閘門：
1. DAG import error 檢查（最重要！）
2. DAG 屬性合規檢查（catchup、tags、owner）
3. 敏感資訊掃描（簡易版 secret detection）

## 🔧 實作重點

1. **DagBag 載入測試**：用 Airflow 的 DagBag 載入所有 DAG，檢查 `import_errors`
2. **屬性驗證**：確保每個 DAG 符合團隊規範
3. **Secret detection**：regex 掃描 DAG 原始碼中的密碼/API Key pattern

## 📊 SA 延伸思考

如果我是 SA：
- 這只是 CI 的第一步（validation）。完整的 pipeline 還需要 unit test（轉換邏輯）+ integration test（端到端）
- 生產環境應該用 Continuous Delivery（有 approval gate），不是 Continuous Deployment
- 未來可以加入 Data Contract 驗證 — 不只測程式碼，還測資料品質
