# Validation, Retry & Batch API — Practice

## 場景
ODM 供應鏈公司需要從大量供應商文件中提取結構化資料。
練習涵蓋：
1. **RetryWithFeedback** — 語義驗證 + retry-with-error-feedback 循環
2. **SelfCorrectionEngine** — calculated vs stated total 差異偵測
3. **BatchAPIDecisionEngine** — 同步 vs 批次 API 選型 + SLA 計算
4. **MultiInstanceReviewSimulator** — 自我審查偏差 vs 獨立實例審查
5. **5 題 CCA Mock Exam**

## 執行
```bash
cd /Users/json/Projects/scholar-learning
JAVA_HOME=/opt/homebrew/opt/openjdk@17 .venv/bin/python3 Cert-Claude-Architect/practice/validation-retry-batch-api/solution.py
```
