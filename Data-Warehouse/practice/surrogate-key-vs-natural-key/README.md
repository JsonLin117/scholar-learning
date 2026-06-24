# Surrogate Key vs Natural Key 練習

## 場景
ODM 供應商維度管理：模擬 SAP 供應商主數據載入 DW。

## 實作內容
1. **Natural Key 問題展示**：Key 回收、格式變更、SCD 衝突
2. **三種 SK 生成策略比較**：自增整數、Hash-based、UUID
3. **SCD Type 2 + Surrogate Key**：追蹤供應商歷史版本
4. **JOIN 效能對比**：Integer SK vs VARCHAR NK 的 JOIN 差異
5. **跨系統整合**：SAP + MES 不同 vendor_id 映射到同一個 SK
6. **特殊 SK 處理**：SK=0（未知）、SK=-1（不適用）
