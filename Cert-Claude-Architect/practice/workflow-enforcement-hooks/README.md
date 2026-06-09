# Workflow Enforcement & Handoff 實作練習

**日期：** 2026-06-10
**科目：** Cert-Claude-Architect (D1 Task 1.4 + 1.5)
**場景：** ODM 供應鏈 + Claude Agent SDK Hook 模擬

## 🏭 情境描述

某 ODM 伺服器製造商（年出貨 500 萬台）導入 AI Agent 處理採購部門的日常查詢。
Agent 有以下工具：`check_inventory`、`lookup_vendor`、`create_po`、`approve_po`、`escalate_to_buyer`。
需要用 Hook 機制確保：
1. 建立 PO 前必須先驗證供應商（PreToolUse prerequisite）
2. PO 金額 > $50,000 必須攔截並升級（PreToolUse threshold）
3. 不同系統返回的日期/幣值格式必須統一（PostToolUse normalization）
4. 升級時產生結構化交接摘要（Structured Handoff）

## 💡 解題思路

模擬 Claude Agent SDK 的 Hook lifecycle：
- PreToolUse: 攔截工具呼叫，檢查前置條件和政策
- PostToolUse: 正規化工具返回結果
- 對比 Hook 確定性 vs Prompt 概率性的差異
- 實作結構化交接協議

## 🔧 實作重點

1. Hook registry + matcher pattern matching
2. PreToolUse deny → 替代流程觸發
3. PostToolUse result transformation（日期/幣值/狀態碼）
4. 升級決策矩陣（何時升級、何時自行處理）
5. 5 題 CCA 模擬考

## 📊 SA 延伸思考

Hook pattern 就是 SAP Approval Workflow 的 AI 版本。金額閾值攔截 = SAP 的 Release Strategy。
PostToolUse 正規化 = ETL Bronze→Silver 層的資料清洗。
思考：如果 ODM 廠把所有 SAP 審批流程搬到 AI Agent，哪些用 Hook、哪些用 Prompt？
