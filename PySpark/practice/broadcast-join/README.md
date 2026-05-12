# Broadcast Join 實作練習

**日期：** 2026-05-13
**科目：** PySpark
**場景：** ODM 供應鏈 — 採購分析

## 🏭 情境描述

你是緯穎的 Data Engineer。採購部門需要一份報表：「各供應商的月度採購金額排行 + 供應商所在國家分布」。Fact 表 `fact_purchase_order` 有 500 萬筆（模擬），Dim 表 `dim_supplier` 有 200 個供應商。你需要比較 Broadcast Join vs Sort-Merge Join 的效能差異。

## 💡 解題思路

1. 生成模擬資料：500 萬筆採購訂單 + 200 個供應商
2. 分別用 Broadcast Join 和 Sort-Merge Join 做相同查詢
3. 比較 Physical Plan 差異（有沒有 Exchange/Shuffle）
4. 測量執行時間差異

## 🔧 實作重點

1. `broadcast()` 函數的使用方式
2. 用 `explain(True)` 查看 Physical Plan，確認是 BroadcastHashJoin 還是 SortMergeJoin
3. AQE 動態轉換的觀察

## 📊 SA 延伸思考

如果 `dim_part`（零件維度）有 50 萬行、超過 10MB threshold，怎麼辦？
→ 選項 A：調高 `autoBroadcastJoinThreshold`（風險：其他查詢也受影響）
→ 選項 B：用 `broadcast()` hint 針對性強制（風險：表長大後 OOM）
→ 選項 C：信任 AQE 動態判斷（推薦）
→ 選項 D：對 `dim_part` 做 Bucketing（頻繁 JOIN 場景）
