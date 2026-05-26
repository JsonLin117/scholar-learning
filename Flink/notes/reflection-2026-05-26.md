# 反思 2026-05-26

## 今天學了什麼

**科目：** Flink（輔修B）| **主題：** Stream Processing vs Batch 根本差異

---

## 🔁 Spaced Repetition 複習

| 主題 | 科目 | 自評 | 下次複習 | 備注 |
|------|------|------|----------|------|
| Broadcast Join（大表JOIN小表） | PySpark | 4/5 | 2026-06-14 | trade-off 清楚，AQE 自動觸發細節可再深 |
| 消息佇列 Why（同步vs非同步、三層解耦） | Kafka | 4/5 | 2026-06-02 | 三層解耦記得清楚，ODM 多系統廣播場景 |
| Microservices vs Monolith | System-Design | 4/5 | 2026-05-29 | Strangler Fig 路徑清楚，Conway's Law 邊界 |

> **遞延：** demand-forecasting-odm → 2026-05-27

---

## 🎯 Senior DE 面試考題（Flink）

**問題：** 解釋 Event Time、Processing Time、Ingestion Time 的差異，以及 Flink 為什麼需要 Watermark？在 ODM 工廠感測器監控場景下如何設定？

**我的作答（不看筆記）：**

| 時間類型 | 定義 | 準確性 | 缺點 |
|----------|------|--------|------|
| Event Time | 事件**實際發生**的時間（感測器打時間戳）| 最準確 | 需要處理 late data |
| Ingestion Time | 事件**進入 Flink** 的時間 | 中等 | 受 source 端延遲影響 |
| Processing Time | 事件**被 task 處理**的時間 | 最簡單（no watermark）| 不反映現實時序，Window 結果不可重現 |

**Watermark：**
- 一個遞增時間戳，代表「Event Time T 之前的事件我認為都已到達」
- 觸發 Window 關閉的信號
- Late data：超過 Watermark 的事件可透過 `allowedLateness` 或 side output 處理

**ODM SMT 溫度監控設定建議：**
```python
WatermarkStrategy
  .forBoundedOutOfOrderness(Duration.ofSeconds(30))  # 工廠 Wi-Fi 最多 30 秒延遲
  .withTimestampAssigner(event -> event.device_timestamp)
```

**對照自評：** ⭐⭐⭐⭐ (4/5)  
**漏掉的：** Chandy-Lamport 分散式快照演算法的具體步驟（Checkpoint 如何插入 Barrier）

---

## 🔧 SA 挑戰：ODM 場景

**情境：** ODM 廠 SMT 生產線 200 台設備每秒上報溫度。現有 Airflow + PySpark Batch 每小時跑一次，告警延遲 30-60 分鐘。要求縮短到 < 1 分鐘。

**我的方案（Flink + Kafka 架構）：**

```
設備感測器 → IoT Gateway
    → Kafka Topic `factory.smt.temperature`（200 partitions，一台設備一個 partition）
    → Flink Job
        - Source: Kafka Consumer
        - keyBy(device_id)
        - Window: 5分鐘 Tumbling Window（Event Time）
        - Watermark: 30s bounded out-of-order
        - Aggregation: avg(temperature) per window per device
        - Filter: avg > 85°C
        - Sink → Kafka `factory.alerts` + MES API
    → 告警系統（Teams Webhook / MES 工單）
```

**關鍵設計決策：**
- `keyBy(device_id)` 確保同一設備的 state 集中在同一 TaskManager slot
- Keyed State 存儲每台設備的 running window（StateBackend: RocksDB）
- Checkpoint 每 30s，存 S3/ADLS，exact-once 保證

**Trade-off：**

| 維度 | Flink 方案 | 繼續用 Spark Batch |
|------|-----------|------------------|
| 告警延遲 | < 1 分鐘 ✅ | 30-60 分鐘 ❌ |
| 運維複雜度 | 高（Flink cluster + RocksDB）| 低（Databricks managed）|
| 成本 | 額外 Flink 集群費用 | 現有 Databricks 即可 |
| 替代方案 | — | Spark Structured Streaming（若允許 5min 延遲）|

**SA 建議：** 若業務接受 5 分鐘延遲 → Spark Structured Streaming（公司已有 Databricks，技術棧統一）。若必須 < 1 分鐘 → Flink。不要為了 streaming 而 streaming。

---

## 💡 新洞察

- **Stream Processing ≠ 就是快**：Flink 的優勢是 **低延遲 + Stateful computation + Exactly-once**，不只是速度
- **Kappa Architecture 的賭注**：把 Batch 當成 Stream 的特例，但 batch reprocessing 成本高
- **State Management 是核心**：Flink 的 keyed state + RocksDB 讓有狀態計算變得可靠，這才是 Flink 的護城河

---

## 明天想繼續探索的問題

1. Flink Checkpoint 的 Barrier 機制：ExactlyOnce 如何在 operator pipeline 中 propagate？
2. RocksDB StateBackend vs HashMapStateBackend：ODM 場景何時用哪個？
3. Flink SQL 的 Materialized Table（2.3 新功能）：streaming 和 batch 統一寫法

---

## 明天安排（2026-05-27）

- **Phase A：** dayCount=36, 36%3=0 → main → Data-Warehouse topics[4]（SCD Type 2）
- **SC Daily #25：** Supply-Chain-Domain topics[24]（MRO 採購）
- **⏰ SR 到期：** demand-forecasting-odm + pallet + sop + taskflow-api + ddmrp（最多 3 張）
