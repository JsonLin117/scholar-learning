# Scholar Learning Journey

**角色**：ODM 供應鏈公司的 Data Engineer，往 Solution Architect 轉型
**目標**：掌握 DE 技術棧 + 建立 SA 架構思維

## 進度總覽（更新：2026-04-12）

### 主修（DE Core）
| 科目 | Topics 完成 | 總 Topics | 目標完成日 | 狀態 |
|------|------------|----------|-----------|------|
| PySpark | 3 | 15 | 2026-05-06 | 🔴 落後（執行模型、Spark UI、Lazy Eval） |
| Airflow | 1 | 15 | 2026-05-06 | 🔴 落後（XCom） |
| Delta-Lake | 1 | 14 | 2026-05-01 | 🔴 落後（OPTIMIZE） |
| Data-Warehouse | 2 | 13 | 2026-05-01 | 🟡 部分（Fact/Dim + Star Schema） |
| dbt | 1 | 12 | 2026-04-26 | 🔴 落後 |
| Great-Expectations | 0 ⚠️ | 10 | 2026-04-21 | 🔴 未開始 |

### 輔修A（SA 核心）
| 科目 | Topics 完成 | 總 Topics | 狀態 |
|------|------------|----------|------|
| System-Design | 1 | 12 | 🟡（CAP Theorem ✅） |
| Supply-Chain-Domain | 0 | 12 | 🔴 未開始（明天！） |

### 輔修B（技術廣度）
| 科目 | Topics 完成 | 狀態 |
|------|------------|------|
| Cloud-Architecture | 1 | 🟡（Azure 全景 ✅） |
| 其他 7 科 | 0 | ⬜ 未開始 |

## 本週（W15）學習摘要

- PySpark：Lazy Evaluation 深度 — Catalyst filter merge 實驗驗證，ODM 採購 ETL 反模式診斷
- 第一個有 git commit 的練習代碼！PySpark/practice/lazy-evaluation/

## W13–W14 回顧

- System-Design：CAP Theorem（CP vs AP 選型）
- Delta-Lake：OPTIMIZE 小檔案 bin-packing
- Data-Warehouse：Fact/Dim 設計 + Star Schema Kimball 四步法（ERD 設計完整）
- Cloud-Architecture：Azure 服務全景分類

## ⚠️ 緊急事項

- **Airflow 2 EOL = 2026-04-22（剩 10 天）** → `airflow-vs-lakeflow-migration` Evergreen 本週內完成

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
