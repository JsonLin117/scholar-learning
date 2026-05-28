# 供應鏈組織架構 — RACI 矩陣 + 資訊流延遲模擬 + Scorecard 引擎

**日期：** 2026-05-29
**科目：** Supply-Chain-Domain（suppA 正課第 3 課）+ SC Daily #27
**場景：** ODM 供應鏈組織

## 🏭 情境描述

你是某 ODM 伺服器廠（年出貨 500 萬台）的 Data Engineer，需要設計一套供應鏈組織資料架構：

1. **RACI 矩陣引擎**：根據決策事項自動判斷誰有審核權、誰需要被通知，用於 workflow 路由和 RBAC 設計
2. **資訊流延遲模擬**：模擬 batch vs streaming 整合對業務決策的影響——缺料 Alert 遲到 1 天的代價是什麼？
3. **供應商 Scorecard 引擎**：計算 QCDS 四維度分數、月度 OTIF/PPM、自動觸發 AVL 狀態變更

## 💡 解題思路

- RACI 矩陣用 dict-of-dicts 建模，支援按 decision_type 查詢每個部門的角色
- 資訊流延遲用 event timestamp 模擬，比較 T+0（streaming）vs T+1（batch）對缺料決策的影響
- Scorecard 用加權平均算總分，根據等級矩陣自動產生 AVL action 建議
- 三者組合展示「組織架構如何驅動系統設計」

## 🔧 實作重點

1. **RACI 矩陣** → 系統權限設計的藍本（A=審核權限, R=操作權限, C=只讀, I=通知）
2. **延遲影響量化** → batch delay 讓 shortage alert 平均遲到 14 小時，每次 premium freight 成本 $5,000-$15,000
3. **Scorecard 自動化** → 月度 batch pipeline 計算，觸發 Probation/Suspended 的 AVL 狀態機

## 📊 SA 延伸思考

- RACI 矩陣是 Bounded Context 的組織映射 — 每個 A 欄位對應一個微服務的 data owner
- 資訊流延遲容忍度決定技術選型：S&OP 月度 = Airflow DAG，缺料 Alert = Kafka/Flink
- Conway's Law 在實踐中的體現：系統邊界 ≈ 組織邊界
