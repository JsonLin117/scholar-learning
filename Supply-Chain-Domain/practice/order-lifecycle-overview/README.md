# Order Lifecycle Overview 實作練習

**日期：** 2026-06-24
**科目：** Supply-Chain-Domain
**場景：** ODM 供應鏈

## 🏭 情境描述

某 ODM 伺服器廠（年出貨 50 萬台 AI Server）需要建立訂單生命週期追蹤系統。
目前各階段資料分散在 CRM（RFQ/Quote）、SAP（PO→出貨→收款）、自建系統（生產排程），
管理層需要一個統一的 Dashboard 分析：OTD（準時交貨率）、Quote Win Rate、Cash-to-Cash Cycle。

## 💡 解題思路

用 Python 模擬完整的 Order Lifecycle Data Model，實作：
1. **多源資料整合**：CRM + SAP + MES 三個系統的訂單事件合併
2. **階段轉換追蹤**：Event Sourcing 風格記錄每個狀態變更
3. **KPI 計算引擎**：OTD、Win Rate、C2C Cycle、Lead Time 分析
4. **風險預警**：CRD-CSD Gap 偵測、齊料延遲預警

## 🔧 實作重點

1. **Event Sourcing Pattern**：每個訂單狀態變更都是一個 Event，可回溯完整歷史
2. **跨系統 Join**：用 order_id 作為唯一 key 串接三個系統的資料
3. **KPI Dashboard**：計算業務方最關心的 5 個指標

## 📊 SA 延伸思考

如果要做成 real-time dashboard：
- CRM 資料用 REST API 拉取（低頻，每小時）
- SAP 資料用 Debezium CDC 串流到 Kafka → Delta Lake
- MES 資料用 Kafka event streaming（高頻，每分鐘）
- Dashboard 用 Databricks SQL + Power BI
