# Scholar Learning Journey

**角色**：ODM 供應鏈公司的 Data Engineer，往 Solution Architect 轉型
**目標**：掌握 DE 技術棧 + 建立 SA 架構思維
**更新：2026-06-01（5 月月總結）**

---

## 🗺️ 進度總覽

### 🔵 主修（DE Core）

| 科目 | Topics 完成 | 總 Topics | 進度 | 狀態 |
|------|------------|----------|------|------|
| PySpark | **9** | 15 | 60% | 🟢 Schema & Type System ✅（本週）|
| Airflow | **7** | 15 | 47% | 🟢 Trigger Rule ✅（本週）|
| Delta-Lake | **5** | 14 | 36% | 🟡 Time Travel ✅（本週）|
| Data-Warehouse | **5** | 13 | 38% | 🟡 SCD Type 2 ✅（本週）|
| dbt | **4** | 12 | 33% | 🟡 Tests ✅（本週）|

### 🟠 輔修A（SA 核心）

| 科目 | Topics 完成 | 本週 | 狀態 |
|------|------------|------|------|
| System-Design | **3** | — | 🟡（CAP / ACID vs BASE / Microservices vs Monolith）|
| Supply-Chain-Domain | **3** suppA + **29** daily | #23~#29（採購完整鏈）| 🟢 持續進行 |

### ⚪ 輔修B（技術廣度）

| 科目 | Topics 完成 | 本週 | 狀態 |
|------|------------|------|------|
| Flink | **1** | Stream Processing vs Batch ✅ | 🟡 開箱 |
| Kafka | **1** | — | 🟡 消息佇列基礎 |
| Great-Expectations | **1** | — | 🟡 核心概念 |
| Cloud-Architecture | 1 | — | 🟠 待繼續 |
| 其他 4 科 | 0 | — | ⬜ 未開始 |

---

## 📅 W22 學習足跡（2026-05-25 ~ 2026-05-31）✅ 全勤 7/7 天

| 日期 | 軌道 | 科目 | 主題 | 自評 | 練習 |
|------|------|------|------|------|------|
| 05-25 | 🔵 main | Delta-Lake | Time Travel：VERSION/TIMESTAMP AS OF, RESTORE, Retention | 4/5 | ✅ |
| 05-26 | ⚪ suppB | Flink | Stream Processing vs Batch：Event Time / Watermark / Checkpoint | 4/5 | ✅ |
| 05-27 | 🔵 main | Data-Warehouse | SCD Type 2：UNION 展開法 MERGE、Surrogate Key | 4/5 | ✅ |
| 05-28 | 🔵 main | dbt | Tests：Generic / Singular / Unit + severity / store_failures | 4/5 | ✅ |
| 05-29 | 🟠 suppA | Supply-Chain | 組織架構深度：RACI Matrix + RBAC 映射 + AVL 狀態機 | 4/5 | ✅ |
| 05-30 | 🔵 main | PySpark | Schema 與型別系統：StructType / DecimalType / nullable 陷阱 | 4/5 | ✅ |
| 05-31 | 🔵 main | Airflow | Trigger Rule：11 種規則、Skip Cascade、分支合併 | 4/5 | ✅ |

**SC Daily 加課（#23~#29）**：P2P 流程 / 採購分類 / MRO / AVL / 供應商績效 / LTA合約 / 備料策略

**本週最大洞察：**
> **ODM 採購資料品質管線雛形**
>
> 技術棧（Airflow Trigger Rules + Delta Lake Time Travel + SCD Type 2 + dbt Tests + Flink）
> 與本週 SC Daily（P2P→AVL→供應商績效→備料策略）天然組成一個完整架構：
>
> `P2P 採購事件` → `Airflow 容錯策略` → `Delta Lake SCD Type 2 供應商歷史` → `dbt Tests 資料品質` → `Time Travel 審計` / `Flink 實時短缺告警`
>
> 這是 SA 思維的體現：看到技術的業務連結，而不只是孤立的工具。

---

## 📅 W21 學習足跡（2026-05-18 ~ 2026-05-24）✅ 全勤

| 日期 | 軌道 | 科目 | 主題 | 自評 | 練習 |
|------|------|------|------|------|------|
| 05-18 | 🔵 main | Data-Warehouse | SCD Type 1：直接覆寫（MERGE upsert）| 4/5 | ✅ |
| 05-19 | 🔵 main | dbt | Materialization：5 種物化策略 + 4 種 incremental 策略 | 5/5 | ✅ |
| 05-20 | ⚪ suppB | Kafka | 為什麼需要消息佇列：同步 vs 非同步 + 三層解耦 | 4/5 | ✅ |
| 05-21 | 🔵 main | PySpark | cache() vs persist()：DataFrame 快取機制 | 4/5 | ✅ |
| 05-22 | 🔵 main | Airflow | Variable 與 Secret Backend：設定管理四層搜尋 | 4/5 | ✅ |
| 05-23 | 🟠 suppA | System-Design | Microservices vs Monolith：架構光譜選型 | 4/5 | ✅ |
| 05-24 | 🔵 main | Airflow | TaskFlow API：@task 裝飾器 + XComArg + expand() | 4/5 | ✅ |

**SC Daily（#16~#22）**：MRP / MPS / S&OP / Capacity Planning / Safety Stock / 需求預測 / DDMRP

---

## 🧠 Knowledge Graph（2026-05-31）

**節點總數：** ~52 個概念節點
**邊總數：** ~80+ 個關係邊
**本週新增節點：** time-travel-version-history / stream-processing-vs-batch / scd-type-2 / dbt-tests-schema-tests / sc-org-architecture-deep / schema-and-type-system / trigger-rule

**主要知識連結（本週新增）：**
- `scd-type-2` → `enables` → `vendor-avl-history-tracking`
- `delta-time-travel` → `enables` → `procurement-audit-trail`
- `dbt-tests` → `contrasts_with` → `great-expectations-checkpoint`
- `trigger-rule` → `used_in` → `odm-shortage-handling-pipeline`
- `stream-processing-vs-batch` → `contrasts_with` → `spark-structured-streaming`

---

## 🔁 Spaced Repetition 狀態（2026-05-31）

- **總卡片數：** 56 張
- **本週新增：** 16 張（採購知識鏈佔大多數）
- **下週到期（06-01~06-07）：** ~15 張
- **積壓警告：** SR 卡片仍偏多，持續每天 Phase B 消化

---

## 🏆 認證備考路線圖

| 認證 | 目標日期 | 狀態 |
|------|---------|------|
| Databricks Data Engineer Associate | 2026-09-30 | ⬜ 準備中 |
| dbt Analytics Engineering | 2026-09-30 | ⬜ 準備中 |
| Claude Certified Architect Foundations | 2026-08-31 | ⬜ 準備中 |
| AWS Cloud Practitioner | 2026-10-31 | ⬜ 準備中 |
| AWS Data Engineer Associate | 2026-11-30 | ⬜ 準備中 |

certRotationFrequency=1（每次 suppB 輪次都插入一次認證備考）

**認證進度：**
- Cert-Claude-Architect：topics[1]/16 — D1 Agent Loop 機制 ✅（2026-06-01）

---

---

## 📅 W23 W1 學習足跡（2026-06-01）🆕 六月開始

| 日期 | 軌道 | 科目 | 主題 | 自評 | 練習 |
|------|------|------|------|------|------|
| 06-01 | 🏆 cert | Cert-Claude-Architect | D1 Agent Loop 機制：stop_reason 協議、三大反模式、Hub-and-Spoke | 4/5 | ✅ |

**SC Daily #30**：缺料管理（Expedite / Premium Freight / 根因分析）

---

## 📊 2026 年 5 月月總結

> 涵蓋：2026-05-01 ~ 2026-05-31 | 完整報告：`Scholar/Monthly/2026-05-summary.md`

### 月份關鍵數字

| 指標 | 數值 |
|------|------|
| 有效學習日 | **25 天**（月中後連續 25 天全勤）|
| 學習主題數 | **25 個**技術 topics |
| SC Daily 完成 | **25 個**（#5 → #29，採購完整鏈）|
| dayCount 增量 | **14 → 40**（+26 天）|
| Knowledge Graph | 11 節點 / 14 邊 → **57+ 節點 / 180+ 邊** |
| SR 卡片累積 | 12 張 → **48 張** |

### 本月最重要進展

1. **第一個完整 DE Pipeline 架構圖成形**
   `Source (Airflow)` → `Validation (GX)` → `Transform (dbt)` → `Storage (Delta Lake LC)` → `Query (PySpark Broadcast Join)`

2. **採購資料品質管線雛形**（W22 核心發現）
   P2P 採購事件 × Airflow Trigger Rules × SCD Type 2 × dbt Tests × Time Travel × Flink 實時告警

3. **供應鏈知識密度關鍵轉折**：首次能描述「一台伺服器的一生」完整流程

4. **認證備考啟動**：certRotationFrequency 改為 1，Claude Architect 第一個開始

### 月度 SA 自省

> 5 月是從「學技術」到「看架構」的轉折月。
> 技術×業務交叉點開始自然出現——不再是硬湊場景，而是學完技術後自然想到 ODM 應用。

---

## 📋 每週進度歷史

| 週次 | 主要主題 | 完成天數 |
|------|---------|---------|
| W12（3/16~3/22）| 課程啟動，PySpark 執行模型 | 7/7 |
| W15（4/6~4/12）| 出差補記期，部分中斷 | — |
| W17（4/20~4/26）| Airflow XCom / Sensor | 7/7 |
| W18（4/27~5/3）| Delta Lake OPTIMIZE / Z-ORDER | 7/7 |
| W19（5/4~5/10）| DW Star Schema / Snowflake Schema | 7/7 |
| W20（5/11~5/17）| GX / Airflow Connection / PySpark Broadcast | 7/7 |
| W21（5/18~5/24）| SCD Type 1 / dbt Materialization / Kafka | 7/7 |
| W22（5/25~5/31）| **採購知識鏈完整 + 資料管線品質技術棧** | **7/7** |
