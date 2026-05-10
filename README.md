# Scholar Learning Journey

**角色**：ODM 供應鏈公司的 Data Engineer，往 Solution Architect 轉型
**目標**：掌握 DE 技術棧 + 建立 SA 架構思維
**更新：2026-05-10（W19 週總結）**

---

## 🗺️ 進度總覽

### 🔵 主修（DE Core）

| 科目 | Topics 完成 | 總 Topics | 目標完成日 | 狀態 |
|------|------------|----------|-----------|------|
| PySpark | 6 | 15 | 2026-05-20 | 🟡 進行中（Shuffle → Broadcast Join 下一課）|
| Airflow | 3 | 15 | 2026-05-20 | 🟡 進行中（XCom ✅ XCom限制 ✅ Sensor ✅）|
| Delta-Lake | 3 | 14 | 2026-05-20 | 🟡 進行中（OPTIMIZE ✅ VACUUM ✅ Z-ORDER ✅）|
| Data-Warehouse | 3 | 13 | 2026-05-20 | 🟡 進行中（Fact/Dim ✅ Star ✅ Snowflake ✅）|
| dbt | 1 | 12 | 延後 | 🟠 待繼續 |
| Great-Expectations | 0 | 10 | 延後 | ⬜ 未開始 |

### 🟠 輔修A（SA 核心）

| 科目 | Topics 完成 | 狀態 |
|------|------------|------|
| System-Design | 1 | 🟡（CAP Theorem ✅，ACID vs BASE 下一課）|
| Supply-Chain-Domain | 7（daily）| 🟢 SC Daily 持續進行中 |

### ⚪ 輔修B（技術廣度）

| 科目 | Topics 完成 | 狀態 |
|------|------------|------|
| Cloud-Architecture | 1 | 🟡（Azure 全景 ✅）|
| DevOps-CICD | 1 | 🟡（CI/CD 基礎 ✅，本週開箱！）|
| Docker-K8s | 1 | 🟠 待繼續 |
| 其他 5 科 | 0 | ⬜ 未開始 |

---

## 📅 W19 學習足跡（2026-05-04 ~ 2026-05-10）

| 日期 | 軌道 | 科目 | 主題 | 練習 |
|------|------|------|------|------|
| 05-07 | 🔵 main | Delta-Lake | VACUUM：歷史版本清理 | ✅ |
| 05-08 | ⚪ suppB | DevOps-CICD | CI/CD 是什麼 | ✅ |
| 05-09 | 🔵 main | Delta-Lake | Z-ORDER 多維聚簇 | ✅ |
| 05-10 | 🔵 main | Data-Warehouse | Snowflake Schema | ✅ |

**SC Daily 加課**：Ramp-up Time (#5) / Trial Run (#6) / Ramp-down (#7)

**本週最大洞察：**
> Star Schema（1 JOIN）+ Delta Lake OPTIMIZE/VACUUM/Z-ORDER = Lakehouse 最優架構公式

---

## 📚 W19 之前累積足跡

| 週次 | 主要學習 |
|------|---------|
| W12 | PySpark 執行模型、Spark UI、Lazy Evaluation |
| W13 | CAP Theorem (System-Design), Delta-Lake OPTIMIZE, Star Schema, Azure 全景 |
| W15 | PySpark Partition, repartition vs coalesce, Data-Warehouse Fact/Dim |
| W16 | PySpark Shuffle, Data-Warehouse Star Schema（Kimball 四步法）|
| W17 | PySpark Broadcast Join, Airflow XCom |
| W18 | Airflow XCom 最佳實踐, Delta-Lake OPTIMIZE（深度）|
| W19 | Delta-Lake VACUUM + Z-ORDER, DevOps-CICD 開箱, Snowflake Schema |

---

## 🧠 Knowledge Graph（2026-05-10）

**節點：11 | 邊：14**

核心連結（已建立）：
- `delta-lake-optimize` → solved_by → `pyspark-shuffle`（小檔案影響 Spark 讀取效能）
- `z-order-clustering` → contrasts_with → `partition`（file-level vs partition-level）
- `snowflake-schema` → depends_on → `pyspark-shuffle`（多 JOIN = 多 Shuffle）
- `snowflake-schema` → contrasts_with → `z-order-clustering`（多 JOIN 吃掉 Z-ORDER 效益）

缺口：liquid-clustering（被引用但節點不存在），Airflow 3 nodes 未入圖

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
