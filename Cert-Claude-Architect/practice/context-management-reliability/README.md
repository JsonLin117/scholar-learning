# Context Management & Reliability Practice

## CCA Domain 5 — Tasks 5.1 through 5.6

### 練習模組

1. **CaseFactsManager** — 模擬漸進摘要中的數字精度丟失，實作 Case Facts Block 持久化
2. **LostInMiddleDetector** — 模擬 lost-in-middle 效應，驗證 position-aware ordering 的效果
3. **ToolOutputTrimmer** — 40+ 欄位 → 5 欄位的 PostToolUse Hook 模擬
4. **EscalationEngine** — 升級決策引擎，驗證 sentiment-based 是反模式
5. **ErrorPropagationSimulator** — 多 Agent 錯誤傳播：structured vs generic，local recovery
6. **MockExamQuestions** — 5 題 CCA D5 模擬考

### ODM 場景
採購異常追蹤 Agent：多系統查詢（SAP + WMS + MES）→ 結構化錯誤 → 部分結果聚合 → 升級決策
