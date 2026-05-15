# Liquid Clustering 實作練習

**日期：** 2026-05-16  
**科目：** Delta-Lake  
**場景：** ODM 供應鏈 / Lakehouse 資料佈局模擬

## 🏭 情境描述

某 ODM 廠每天產生大量庫存異動資料，採購、PMC、倉庫會用不同查詢模式看資料：按日期查每日異動、按料號查全年消耗、按工廠+供應商查缺料風險。傳統 `PARTITION BY movement_date + ZORDER BY (plant_id, part_number)` 對日期查詢很好，但跨日期查料號會掃很多 partition。本練習用純 Python 模擬不同資料佈局對 data skipping 的影響。

## 💡 解題思路

Liquid Clustering 的核心不是「真的多一個索引」，而是把資料檔案按多個高頻查詢欄位聚在一起，讓每個檔案的 min/max statistics 更緊，查詢時可以跳過更多檔案。練習中比較三種 layout：

1. **Unoptimized**：原始寫入順序，檔案統計很散。
2. **Partition + ZORDER**：先按日期分區，再在分區內按 plant/part 排序。
3. **Liquid Clustered**：不建立實體分區，直接用 `(movement_date, plant_id, part_number)` 做全表聚簇。

## 🔧 實作重點

- 用 synthetic ODM inventory movement 產生器建立 25,000 筆資料。
- 模擬 Delta file statistics：每個 data file 保存各 clustering key 的 min/max。
- 對多種 query predicate 估算 file pruning：掃描檔案數、跳過檔案數、skip ratio。
- 模擬 `ALTER TABLE ... CLUSTER BY (movement_date, supplier_id)` + `OPTIMIZE FULL`，觀察查詢模式改變時的重新聚簇效果。

## 📊 SA 延伸思考

Liquid Clustering 適合「查詢模式會變」的大型 Lakehouse table：例如庫存異動初期常按日期+工廠查，半年後供應商風險管理成熟後改按 supplier 查。SA 要關注兩個 trade-off：

- **彈性 vs 相容性**：Liquid Clustering 彈性高，但需要較新 Delta protocol；跨引擎場景仍要評估 Iceberg Hidden Partitioning。
- **Key 數量 vs 單欄效能**：key 越多不代表越好，通常 2-3 個高頻 WHERE 欄位比塞滿 4 個更穩。
