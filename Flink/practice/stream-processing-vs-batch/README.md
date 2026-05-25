# Flink 練習：Stream Processing vs Batch

**日期：** 2026-05-26  
**科目：** Flink  
**主題：** Stream Processing 是什麼：跟 Batch 的根本差異  
**場景：** ODM 工廠 SMT 機台溫度事件流

## 🏭 情境描述

ODM 工廠的 SMT 機台每 10 秒送出一次溫度事件。舊做法是每天用 batch job 重算報表；新目標是用 stream processing 在事件到達後持續更新每分鐘平均溫度，並對過熱機台即時告警。

這個練習不用安裝 Flink，而是用 dependency-free Python 實作一個 **MiniFlink**，模擬 Flink 入門最重要的概念：

- **Bounded vs Unbounded**：Batch 等全部資料到齊；Stream 逐筆處理事件。
- **Event Time**：依事件實際發生時間分窗，而不是依收到時間。
- **Watermark**：宣告 event-time 進度，決定何時關窗輸出。
- **Stateful Processing**：每個 `(machine_id, minute_window)` 都維護 sum/count/max state。
- **Checkpoint + Replay**：模擬失敗後從 checkpoint 還原，source replay，避免重複輸出。
- **Late Event Side Output**：太晚到的事件不默默污染已關窗結果，而是進 side output 等待補償流程。

## 💡 解題思路

1. 先產生一批帶有 `event_time` 和 `arrival_time` 的 SMT sensor events。
2. 用 batch processor 對完整資料做「最終正確答案」。
3. 用 MiniFlink 依 `arrival_time` 逐筆處理：
   - 更新 `max_event_time_seen`
   - 計算 `watermark = max_event_time_seen - allowed_lateness`
   - 將 on-time event 更新到 keyed window state
   - 當 `window_end <= watermark` 時 finalize window
4. 在第 72 筆 source offset 故意注入一次失敗，從最近 checkpoint replay。
5. 對比 batch final report 與 streaming finalized results，觀察 late events 造成的補償需求。

## 🔧 執行方式

```bash
cd /Users/json/Projects/scholar-learning
JAVA_HOME=/opt/homebrew/opt/openjdk@17 .venv/bin/python3 Flink/practice/stream-processing-vs-batch/solution.py
```

## 🧠 SA 延伸思考

- Stream processing 的價值不是「跑比較快」而是**改變反應模式**：設備異常不必等 T+1 batch 才知道。
- Event Time + Watermark 是架構正確性的核心：處理亂序資料時仍能讓同一批事件產生可重現結果。
- Late events 必須有治理策略：丟棄、side output、retraction/correction、或提高 allowed lateness。
- Checkpoint 本質上是 stateful system 的恢復契約：source offset + operator state + sink idempotency 要一起設計。
