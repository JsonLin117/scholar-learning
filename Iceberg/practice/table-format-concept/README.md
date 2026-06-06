# Practice: Table Format 概念理解 — Parquet vs Delta Lake 對比

## 場景
ODM 廠要設計 SAP 庫存異動資料的 Lakehouse 儲存方案。

## 練習目標
1. 示範裸 Parquet 的問題：Schema 不可演進、沒有 ACID、沒有 Time Travel
2. 對比 Delta Lake（作為 Table Format 的代表）如何解決這些問題
3. 體會為什麼 Table Format 是 Lakehouse 的必要層

## 思路
- 用 PySpark 建立一個 Parquet 表和 Delta 表
- 在兩者上嘗試：Schema Evolution、Concurrent Write、Time Travel
- 觀察差異
