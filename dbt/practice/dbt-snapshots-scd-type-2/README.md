# dbt Snapshots SCD Type 2 Practice

## 場景
ODM 供應鏈的供應商主數據和 PO 狀態追蹤。模擬 dbt snapshot 的兩種策略（timestamp/check），包含 hard_deletes、dbt_valid_to_current 配置，以及 snapshot vs incremental SCD2 的對比。

## 練習重點
1. **MiniSnapshotEngine**：模擬 dbt snapshot 的 MERGE 邏輯
2. **Timestamp 策略** vs **Check 策略**：偵測變更的差異
3. **Hard Deletes 三種模式**：ignore / invalidate / new_record
4. **dbt_valid_to_current**：NULL vs 9999-12-31 對 JOIN 的影響
5. **Snapshot vs Delta MERGE SCD2**：自動化 vs 手寫的 trade-off
6. **中間變更壓縮問題**：Snapshot 的固有限制展示
