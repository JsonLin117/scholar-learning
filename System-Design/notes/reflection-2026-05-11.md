# 反思 2026-05-11

## 今天學了什麼

**主修（suppA）：** System-Design | 主題：ACID vs BASE 一致性模型（第 2 課）
**SC Daily Bonus #8：** Supply-Chain-Domain | PMC（生產與物料控制）

---

## 🔁 Spaced Repetition 複習

| 主題 | 到期日 | 狀態 |
|------|--------|------|
| Snowflake Schema | 2026-05-13 | 未到期 |
| Ramp-down（EOL 庫存管理）| 2026-05-13 | 未到期 |
| ACID vs BASE | 2026-05-14 | 未到期（今天剛學）|
| PMC（生產與物料控制）| 2026-05-14 | 未到期（今天剛學）|

→ **今日無到期 SR 卡片，跳過複習。**

---

## Senior DE 面試考題（不看筆記作答）

### 題目
> 你在設計一個跨微服務的訂單系統：客戶下單後需要同步更新「庫存服務」和「財務服務」。
> 如果庫存扣成功但財務更新失敗，怎麼保證資料一致性？
> 請說明你會選 ACID 還是 BASE，以及具體機制。

### 我的作答（不查筆記）

**選 BASE + Saga Pattern**，原因：
- 庫存服務和財務服務是獨立 DB，無法做跨服務的 2PC（代價太高、單點故障風險）
- 這是典型的分散式交易問題，應用 Saga Orchestration

**流程：**
1. 訂單服務是 Saga 協調器（Orchestrator）
2. 先調用「庫存服務 → 扣庫存」（本地 ACID 成功）
3. 再調用「財務服務 → 建立應收款」（本地 ACID）
4. 財務失敗 → 觸發 compensating transaction 回調「庫存服務 → 還庫存」
5. 最終結果：要嘛兩個都成功，要嘛兩個都補償回去

**關鍵點：**
- 中間過渡期系統處於「部分完成」狀態（BASE 的 Soft State）
- 需要冪等性（Idempotency）：補償交易可能被重試，不能重複執行
- 需要事件日誌：記錄 Saga 進度，系統重啟後能從上次繼續

**我不確定的部分：**
- Choreography vs Orchestration 的邊界怎麼選？（有沒有明確標準？）
- Outbox Pattern 如何確保事件不遺失？

### 對照評分：⭐⭐⭐⭐ (4/5)

**對的部分：**
- ✅ 正確選 BASE + Saga（不是 2PC）
- ✅ Compensating Transaction 概念正確
- ✅ 提到冪等性

**漏掉的：**
- ❌ 沒有明確說 2PC 的問題（coordinator 成為瓶頸、腦裂問題）
- ❌ 沒有提 Quorum 機制（雖然這題不太用到）
- ❌ Choreography vs Orchestration 的選型標準還模糊（前者適合簡單流程，後者適合複雜業務邏輯）

---

## SA 挑戰：ODM 場景

### 情境

> 緯穎正在建一個「全球庫存可視化系統」，需要整合台灣、墨西哥、捷克三個廠的實時庫存。
> 目前狀況：三個廠各自有 SAP，庫存資料每天 EOD 才同步一次（最終一致性長達 24 小時）。
> 業務訴求：希望庫存延遲從 24 小時降到 5 分鐘以內。
> 技術問題：這個系統要用 ACID 還是 BASE？如何設計？

### 我的方案

**架構選擇：混合模型（ACID 在 source + BASE 在 analytical layer）**

```
SAP ERP（台灣/墨西哥/捷克）
    ↓ 各自 ACID（本地交易強一致）
    ↓ CDC（Debezium 或 SAP CDC Connector）
Kafka（統一事件流）
    ↓ 消費延遲 < 1 分鐘
Flink 或 Spark Structured Streaming（即時聚合）
    ↓
Delta Lake Silver Layer（全球庫存統一視圖）
    ↓ Eventual Consistency，延遲 < 5 分鐘
Dashboard（Control Tower）
```

**ACID/BASE 的邊界：**
- SAP 以內：ACID（採購、庫存扣減等業務交易不能錯）
- SAP 往外的分析層：BASE（允許 5 分鐘的最終一致）
- 這條線是 **SAP 的 commit log**（CDC 捕捉 committed events）

**補充設計考量：**
- Quorum 設定：庫存 dashboard 讀最新 2/3 節點的資料（避免讀到未同步的舊節點）
- 衝突處理：同一料號在不同廠可能有版本衝突 → 用 Event Time + Last-Write-Wins
- 業務規則：庫存不可為負 → 在 Silver Layer 建 GE（Great Expectations）校驗

### Trade-off

| 方案 | 優點 | 缺點 |
|------|------|------|
| 維持 24h 批次 | 簡單、無技術風險 | 業務無法用、搶單時已過時 |
| 全部改 ACID 即時同步 | 強一致 | 三個廠 SAP 做分散式交易，成本/複雜度極高 |
| **CDC + BASE（我的選擇）** | 5 分鐘延遲、不改 SAP、可擴展 | 需要 CDC 基礎設施、開發複雜度提升 |

**SA 結論：** 混合模型是唯一可行的工業實踐路徑。關鍵不是選哪個模型，而是**畫清 ACID 邊界**（SAP 內部），在邊界外用 CDC bridge 連接 BASE 的分析層。

---

## 明天想繼續探索的問題

1. **Saga + Outbox Pattern** 如何結合確保 exactly-once 語意？（今天的弱點）
2. **Choreography 的死鎖問題**：兩個服務互相等對方事件，怎麼偵測和打破？
3. **緯穎的 CDC 實作**：SAP 有官方的 CDC Connector 嗎？還是用 Debezium 捕 DB log？
