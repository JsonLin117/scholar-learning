# CCA Practice: Subagent Configuration & Context Passing

## 場景
模擬 ODM 供應鏈多 Agent 系統：

1. **AgentDefinition 配置驗證**：測試 description/prompt 必填、tools 繼承/白名單/黑名單三種模式
2. **Context 隔離驗證**：Subagent 不繼承 parent conversation history
3. **顯式 Context 傳遞**：Content vs Metadata 分離格式
4. **allowedTools 行為**：Coordinator 不含 "Agent" → 無法自動 invoke
5. **Subagent 巢狀限制**：Subagent 不能生成 sub-subagent
6. **Dynamic Agent Factory**：根據 runtime 條件（urgency level）動態配置 model/prompt
7. **Context Budget 修剪**：上游 agent verbose → structured summary → 下游 agent
8. **CCA 模擬考 5 題**

## 執行
```bash
cd /Users/json/Projects/scholar-learning
JAVA_HOME=/opt/homebrew/opt/openjdk@17 .venv/bin/python3 Cert-Claude-Architect/practice/subagent-context-passing/solution.py
```
