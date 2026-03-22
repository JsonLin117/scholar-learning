# Scholar Learning Journey

**角色**：ODM 供應鏈公司的 Data Engineer，往 Solution Architect 轉型
**目標**：掌握 DE 技術棧 + 建立 SA 架構思維

## 進度總覽（更新：2026-03-22）

### 主修（DE Core）
| 科目 | Topics 完成 | 總 Topics | 目標完成日 |
|------|------------|----------|-----------|
| PySpark | 2 | 15 | 2026-05-06 |
| Airflow | 1 | 15 | 2026-05-06 |
| Delta-Lake | 1 | 14 | 2026-05-01 |
| Data-Warehouse | 1 | 13 | 2026-05-01 |
| dbt | 1 | 12 | 2026-04-26 |
| Great-Expectations | 0 ⚠️ | 10 | 2026-04-21 |

### 輔修（SA / Advanced）
Cloud-Architecture · Docker-K8s · DevOps-CICD · System-Design · Supply-Chain-Domain · API-Integration · Kafka · Flink · Iceberg

## 本週（W12）學習摘要

- PySpark：Spark 執行模型（Job→Stage→Task）、Spark UI 診斷
- Airflow：XCom 機制與 TaskFlow API
- Delta-Lake：OPTIMIZE 小檔案合併
- Data-Warehouse：Fact Table vs Dimension Table（Kimball Dimensional Modeling）
- dbt：dbt 核心概念，ELT 的 T
- ⚠️ 本週零實作代碼，下週必須補齊

## 資料夾結構

```
<subject>/
├── README.md          # 科目總覽
├── notes/             # 學習筆記（Obsidian 同步版）
└── practice/          # 實作練習代碼
    └── <topic-slug>/
        ├── README.md  # 場景描述 + 解題思路
        └── solution.py (或 .sql / .yaml / ...)
```

## 每日學習由 Scholar Agent 自動更新
