# Hub-and-Spoke 多 Agent 架構模擬

## 場景：ODM PO 缺料分析 Orchestrator

模擬一個 Hub-and-Spoke 多 Agent 系統：

- **Coordinator**：接收缺料報告請求，分解任務、委派 subagent、聚合結果
- **InventoryAgent**：查詢物料庫存（on-hand, allocated, available）
- **AVLAgent**：查詢供應商 AVL 狀態和 Lead Time
- **MRPAgent**：查詢 MRP 計劃中的在途 PO

## 練習重點

1. **Context 隔離**：Subagent 不繼承 coordinator 對話歷史，必須顯式傳遞
2. **平行 vs 串行**：三個查詢 subagent 平行執行，綜合 subagent 串行（需要所有結果）
3. **AgentDefinition 配置**：description、prompt、tools、model 各欄位
4. **結構化 Context 傳遞**：用 JSON 格式分離 content 和 metadata
5. **Structured Handoff**：生成結構化升級摘要
6. **Hook 強制**：PreToolUse 攔截超過閾值的緊急採購
7. **PostToolUse 正規化**：統一不同系統回傳的日期格式
