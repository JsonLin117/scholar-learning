# Message Queue 練習：同步 vs 非同步 ODM 事件流模擬

**日期：** 2026-05-20
**科目：** Kafka
**場景：** ODM 供應鏈 / 技術練習

## 🏭 情境描述

你是某 ODM 公司的 Data Engineer。工廠 MES 每天產生數千個工單事件（建立/開工/完工/關閉），需要通知 4 個下游系統：倉庫扣料、產線排程、品質檢測、ERP 回報。

**問題**：目前用同步 API 串接，一個下游掛了就全部卡住。要設計一個基於 Message Queue 的非同步事件流架構。

## 💡 解題思路

不用安裝 Kafka。用 Python 原生 `queue.Queue` 模擬 Kafka 的核心行為：
- Topic = named queue
- Partition = 多個 queue（按 key hash 分配）
- Producer = 發事件到 queue
- Consumer Group = 多個 consumer 平分 partition
- Offset tracking = consumer 記錄已消費位置

對比同步（sequential API calls）vs 非同步（event-driven via queue）的：
1. 總延遲
2. 故障隔離能力
3. 吞吐量

## 🔧 實作重點

1. **MiniKafka class**：模擬 Topic、Partition、Offset 機制
2. **同步模式**：Producer 直接呼叫 4 個下游的 handler，任一失敗全部失敗
3. **非同步模式**：Producer 只發事件到 MiniKafka，4 個 Consumer 各自消費
4. **故障注入**：隨機讓某個下游 handler 失敗，觀察兩種模式的行為差異
5. **Metrics 收集**：延遲、成功率、吞吐量對比

## 📊 SA 延伸思考

- 同步模式的可用性 = min(所有下游) ≈ 0.99^4 = 96%
- 非同步模式的 Producer 可用性 ≈ Kafka 可用性 ≈ 99.99%
- 但非同步帶來的 trade-off：最終一致性、debug 複雜度、message 重複消費
