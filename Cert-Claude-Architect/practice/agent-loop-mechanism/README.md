# Agent Loop 機制 實作練習

**日期：** 2026-06-01
**科目：** Cert-Claude-Architect
**場景：** ODM 供應鏈 — 智慧缺料告警 Agent

## 🏭 情境描述

你是某 ODM 公司的 Data Engineer，需要設計一個 AI Agent，自動偵測缺料風險並決定應對策略。
Agent 透過 Agent Loop 機制，自動查詢 SAP 庫存 → 查 AVL 供應商 → 查 Lead Time → 判斷嚴重程度 → 發出告警。

## 💡 解題思路

用 Python 模擬完整的 Agent Loop，包含：
1. 模擬 Claude API 的 `stop_reason` 協議
2. 工具結果附回對話歷史的完整流程
3. 展示三大反模式 vs 正確實作的對比
4. 模擬 ODM 缺料檢查的多步驟工具呼叫

## 🔧 實作重點

1. **協議驅動**：只看 `stop_reason`，不解析文本
2. **Stateless API 模擬**：每次迴圈送完整 messages
3. **反模式對比**：用計數器展示三種錯誤做法的問題

## 📊 SA 延伸思考

Agent Loop 在 ODM 的限制：
- 高風險操作（Expedite PO、Premium Freight 審批）不適合讓 Agent 自動決策
- 應該在 escalate_to_human 工具處設計 human-in-the-loop
- 工具結果太大（SAP 回傳整個 BOM 表）會浪費 context window → 需要精簡回傳欄位
