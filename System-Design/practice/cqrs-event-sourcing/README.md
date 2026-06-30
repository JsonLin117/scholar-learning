# CQRS & Event Sourcing — ODM 供應鏈採購追蹤

## 場景
模擬 ODM 工廠的採購訂單（PO）全生命週期管理，使用 Event Sourcing 記錄 PO 狀態變更，
CQRS 分離寫入（Command）和查詢（Query）模型。

## 練習重點
1. **Event Store**：Append-only 事件存儲，按 aggregate ID 分組
2. **Rehydration**：從事件流重建 PO 當前狀態
3. **Snapshot**：避免每次重播全部事件
4. **CQRS Projector**：事件 → 多個讀模型（PO 列表、供應商統計、異常偵測）
5. **Compensating Events**：PO 取消的補償邏輯
6. **Eventual Consistency**：寫入後讀取延遲的模擬
7. **ODM Yield/OEE Integration**：QC 事件影響供應商評分與備料計算
