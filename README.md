# Scholar Learning Journey

**角色**：ODM 供應鏈公司的 Data Engineer，往 Solution Architect 轉型
**目標**：掌握 DE 技術棧 + 建立 SA 架構思維
**更新：2026-05-17（W20 週總結）**

---

## 🗺️ 進度總覽

### 🔵 主修（DE Core）

| 科目 | Topics 完成 | 總 Topics | 目標完成日 | 狀態 |
|------|------------|----------|-----------|------|
| PySpark | **7** | 15 | 延後 | 🟢 Milestone 2 ✅（Shuffle→Broadcast Join 掌握）|
| Airflow | **4** | 15 | 延後 | 🟡 進行中（XCom ✅ Sensor ✅ Connection&Hook ✅）|
| Delta-Lake | **4** | 14 | 延後 | 🟡 進行中（OPTIMIZE ✅ VACUUM ✅ Z-ORDER ✅ Liquid Clustering ✅）|
| Data-Warehouse | **3** | 13 | 延後 | 🔴 進度落後（Fact/Dim ✅ Star ✅ Snowflake ✅）|
| dbt | **2** | 12 | 延後 | 🟡 進行中（概念入門 ✅ Models/Sources/Refs ✅）|
| Great-Expectations | **1** | 10 | 延後 | 🟡 本週開啟（核心概念 ✅）|

### 🟠 輔修A（SA 核心）

| 科目 | Topics 完成 | 狀態 |
|------|------------|------|
| System-Design | **2** | 🟡（CAP Theorem ✅ ACID vs BASE ✅）|
| Supply-Chain-Domain | **2** suppA + **15** daily | 🟢 SC Daily 持續進行中 |

### ⚪ 輔修B（技術廣度）

| 科目 | Topics 完成 | 狀態 |
|------|------------|------|
| Cloud-Architecture | 1 | 🟡（Azure 全景 ✅）|
| DevOps-CICD | 1 | 🟡（CI/CD 基礎 ✅）|
| Docker-K8s | 1 | 🟠 待繼續 |
| 其他 5 科 | 0 | ⬜ 未開始（Kafka 下週開箱！）|

---

## 📅 W20 學習足跡（2026-05-11 ~ 2026-05-17）✅ 全勤

| 日期 | 軌道 | 科目 | 主題 | 自評 | 練習 |
|------|------|------|------|------|------|
| 05-11 | 🟠 suppA | System-Design | ACID vs BASE：資料庫一致性模型 | 4/5 | ✅ |
| 05-12 | 🔵 main | dbt | dbt 核心概念：Models / Sources / Refs | 4/5 | ✅ |
| 05-13 | 🔵 main | PySpark | Broadcast Join：消滅大表 JOIN 的 Shuffle | **5/5 🏆** | ✅ |
| 05-14 | ⚪ suppB | Great-Expectations | GX 核心概念 | 4/5 | ✅ |
| 05-15 | 🔵 main | Airflow | Connection & Hook + Secrets Backend | 4/5 | ✅ |
| 05-16 | 🔵 main | Delta-Lake | Liquid Clustering：下一代資料佈局 | 4/5 | ✅ |
| 05-17 | 🟠 suppA | Supply-Chain | CSP 客戶結構（Hyperscaler 需求特性）| 4/5 | ✅ |

**SC Daily 加課（#9~#15）**：PMC / Shop Floor / Pallet / Carton / Pallet-Carton ID / BOM / Lead Time

**本週最大洞察：**
> 本週七個 topic 打通了 DE Pipeline 全鏈路：
> Airflow Connection（Secrets）→ GX（品質驗證）→ dbt（ELT Transform）→ Delta Lake Liquid Clustering（儲存）→ PySpark Broadcast Join（查詢）→ ACID/BASE（一致性）

---

## 🧠 Knowledge Graph（2026-05-17）

**節點：27 | 邊：63**（W19 結束時：11/14）

**本週新增 16 節點：**
`acid-vs-base` · `dbt-core-concepts` · `broadcast-join` · `great-expectations-core-concepts` · `airflow-connection-hook` · `liquid-clustering` · `csp-customer-structure` · `lead-time-types` · `pmc` · `shop-floor` · `pallet` · `carton` · `pallet-carton-id` · `bom` · `ramp-up-time`（前週）

**關鍵 Graph 邏輯：**
- `broadcast-join` → solved_by → `pyspark-shuffle`（消滅 Shuffle）
- `liquid-clustering` → extends → `z-order-clustering`（進化路線）
- `csp-customer-structure` → depends_on → `bom-bill-of-materials`（業務複雜度根源）
- `airflow-connection-hook` → used_in → `great-expectations-core-concepts`（Pipeline 協作）

---

## 📚 歷史週次足跡

| 週次 | 主要學習 |
|------|---------|
| W12 | PySpark 執行模型、Spark UI、Lazy Evaluation |
| W13 | PySpark Partition、repartition vs coalesce |
| W14 | PySpark Shuffle、Airflow XCom |
| W15-W16 | Delta Lake OPTIMIZE、Cloud-Architecture |
| W17 | Docker-K8s 基礎、System-Design CAP Theorem |
| W18 | Airflow XCom 限制 + Sensor；Delta Lake 深度 |
| W19 | Delta Lake VACUUM + Z-ORDER；Data-Warehouse Snowflake Schema；DevOps-CICD 入門 |
| **W20** | **ACID/BASE, dbt, Broadcast Join, GX, Airflow Hook, Liquid Clustering, CSP** |

---

## 🗂️ 資料夾結構

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
