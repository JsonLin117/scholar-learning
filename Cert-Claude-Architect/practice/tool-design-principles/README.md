# CCA Practice: Tool Design 原則（D2 Task 2.1）

## 場景
ODM 供應鏈缺料告警系統的 Agent 工具設計。模擬：
1. **Tool Description Quality Scorer** — 評估工具描述好壞，給出改善建議
2. **tool_choice Strategy Selector** — 根據場景選擇 auto/any/specific
3. **Schema Design Validator** — 檢查 schema 是否符合 CCA 最佳實踐
4. **Tool Consolidation Analyzer** — 分析碎片化工具並建議整合
5. **Error Response Quality Checker** — 評估錯誤回應的結構化程度
6. **CCA 模擬考 5 題**

## 執行
```bash
cd /Users/json/Projects/scholar-learning
JAVA_HOME=/opt/homebrew/opt/openjdk@17 .venv/bin/python3 Cert-Claude-Architect/practice/tool-design-principles/solution.py
```
