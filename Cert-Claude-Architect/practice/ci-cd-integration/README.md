# CI/CD Integration Practice — CCA D3 Task 3.6

## 場景
模擬 Claude Code 在 CI/CD pipeline 中的各種整合模式：
1. **CLI Flag 模擬器**：驗證 `-p`、`--bare`、`--output-format` 等旗標行為
2. **Multi-Pass Review Engine**：模擬逐檔 + 跨檔整合審查
3. **Self-Review vs Independent Review**：量化 confirmation bias
4. **Batch API 選型決策器**：根據 workflow 特性推薦 API 模式
5. **Duplicate Comment Prevention**：模擬先前結果 context 注入
6. **CCA 模擬考 5 題**

## 執行
```bash
cd /Users/json/Projects/scholar-learning
JAVA_HOME=/opt/homebrew/opt/openjdk@17 .venv/bin/python3 Cert-Claude-Architect/practice/ci-cd-integration/solution.py
```
