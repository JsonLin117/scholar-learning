# Delta Lake MERGE 操作練習

**場景**：ODM 伺服器廠採購主數據 CDC 同步

模擬以下四種 MERGE 模式（dependency-free，不需要 PySpark）：

1. **Basic Upsert** — 供應商主數據每日 dump 同步
2. **Conditional Update** — 只在有真實變更時才寫（避免 no-op CoW）
3. **Insert-Only Dedup** — Kafka CDC 事件去重插入
4. **SCD Type 2（UNION 展開法）** — 供應商 AVL 狀態歷史追蹤

**Bonus**：模擬 Multi-Table Transaction（Catalog Commits 概念）

執行方式：
```bash
cd /Users/json/Projects/scholar-learning
JAVA_HOME=/opt/homebrew/opt/openjdk@17 .venv/bin/python3 Delta-Lake/practice/delta-lake-merge/solution.py
```
