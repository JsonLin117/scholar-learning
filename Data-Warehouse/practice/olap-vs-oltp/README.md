# OLAP vs OLTP 架構解耦實作練習

**日期：** 2026-07-11
**科目：** Data-Warehouse
**場景：** ODM 供應鏈 — SAP OLTP 出貨交易 vs Databricks OLAP 分析報表解耦

## 🏭 情境描述

緯穎（ODM 伺服器製造商）的 SAP S/4HANA 每天處理數萬筆出貨交易（GR/交貨單建立），
供應鏈管理層同時要看「跨月各地區出貨量/OTIF 趨勢」報表。
過去 BI 工具直接連 SAP 生產資料庫跑報表，導致月底報表尖峰時段工廠現場人員反應
MIGO/VL02N 操作明顯卡頓。本練習模擬：
1. OLTP 端：Row-oriented 單筆交易寫入延遲測試
2. OLAP 端：Column-oriented 聚合查詢效能測試
3. 用同一份資料量，比較兩種 schema/查詢型態的效能特徵差異，佐證「為什麼要分離」

## 💡 解題思路

用 PySpark 模擬兩種查詢負載打在同一份 50 萬筆出貨事實資料上：
- **OLTP 模擬**：單筆 row 隨機 point lookup + update（模擬 SAP 交易查詢）
- **OLAP 模擬**：跨百萬筆 group by 聚合（模擬 BI 報表按地區/月份彙總 OTD）

用 `explain()` 對比兩種查詢在 Spark 下的物理計劃差異（全表掃描 vs 索引式查找的類比），
並實測兩種查詢型態混跑在同一資料源時的資源競爭現象，驗證解耦的必要性。

## 🔧 實作重點

1. **模擬 fact_shipment 表**（50 萬筆出貨記錄）＋ dim_customer/dim_region 維度表
2. **OLTP 查詢**：`filter(order_id == X)` 單筆查找，模擬交易系統的 point lookup
3. **OLAP 查詢**：`groupBy(region, quarter).agg(sum, avg)` 全表聚合，模擬分析報表
4. **效能對比**：分別計時兩種查詢型態，說明為何 OLAP 需要 Column-oriented + 反正規化才能在大資料量下維持可接受的回應時間

## 📊 SA 延伸思考

如果我是 SA，會建議用 CDC（如 Debezium/SAP SLT）把 SAP OLTP 資料同步到 Databricks/Snowflake，
在 OLAP 端做 Star Schema 建模，BI 報表全部指向 OLAP 端，SAP 生產系統只保留交易查詢負載。
Trade-off：多一層 ETL pipeline 的維運成本，換取交易系統效能不受分析查詢影響——
對 ODM 廠這種交易量大、報表需求也重的場景，這個 trade-off 幾乎必然值得。

## ⚙️ 執行方式（環境備註）

本機 `.venv` 使用 python3.14，但 PySpark worker 預設會 fallback 到系統 `/usr/bin/python3`（3.9），
兩者版本不一致會導致 worker 啟動失敗（`GeographyType` TypeError）。需明確指定 worker python：

```bash
JAVA_HOME=/opt/homebrew/opt/openjdk@17 \
PYSPARK_PYTHON=$(pwd)/.venv/bin/python3.14 \
PYSPARK_DRIVER_PYTHON=$(pwd)/.venv/bin/python3.14 \
.venv/bin/python3 Data-Warehouse/practice/olap-vs-oltp/solution.py
```
