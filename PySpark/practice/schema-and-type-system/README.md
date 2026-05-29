# Schema 與型別系統 實作練習

**日期：** 2026-05-30
**科目：** PySpark
**場景：** ODM 供應鏈

## 🏭 情境描述

某 ODM 公司的 DE 團隊收到 SAP 匯出的 BOM 資料（JSON 格式），需要建立 Bronze→Silver 的 Schema 防線。
練習：比較有/無 explicit schema 的效能差異、型別安全檢查、DecimalType 精度驗證、Schema Evolution 處理。

## 💡 解題思路

1. **Schema Inference vs Explicit Schema**：量化 two-pass read 的額外開銷
2. **型別安全**：模擬 SchemaInference 推斷錯型別的場景（混合 null 導致 String 推斷）
3. **DecimalType vs DoubleType**：BOM 多層展開的精度累積誤差
4. **Schema Evolution**：模擬 ECO 加欄位後的新舊資料合併

## 🔧 實作重點

- StructType/StructField 程式化定義 + DDL 字串定義
- DecimalType 精度在 BOM 10 層展開後的誤差放大
- nullable constraint 的「不檢查」陷阱
- Schema JSON 序列化/反序列化（版本控制）

## 📊 SA 延伸思考

- 在企業級數據平台中，Schema 應該集中管理（Schema Registry / Unity Catalog）
- Data Contract = Schema 定義 + Quality Tests + SLA，是跨團隊協作的基礎
- Schema Evolution 策略決定了 pipeline 的靈活性 vs 穩定性 trade-off
