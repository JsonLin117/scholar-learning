# Medallion Architecture 深度 — ODM 採購資料 Bronze/Silver/Gold Pipeline

> 學習日期：2026-07-08 | 科目：Delta-Lake | Topic: Medallion Architecture 深度
> 對應筆記：`second-brain/Scholar/Delta-Lake/medallion-architecture-deep-dive.md`

## 場景

緯穎的採購資料來自三個系統：SAP ECC（PO）、SAP LFA1（供應商主數據）、MES（生產過站，用來確認交期）。
本練習模擬把這些原始資料，依照 Medallion Architecture 的三層設計原則，逐步提煉成可供業務方直接消費的月度採購分析報表。

## 練習目標

1. 實作 **Bronze 層**：原始資料 1:1 落地，只加 metadata，不做任何清洗
2. 實作 **Silver 層**：跨系統資料對齊（PO + 供應商主數據 join）、去重、schema 標準化
3. 實作 **Gold 層**：業務聚合（供應商月度採購金額、OTD 達成率）
4. 對比「跳過 Silver 直接 Bronze→Gold」的反例，展示為什麼會出問題（邏輯重複、口徑不一致）
5. 模擬 CDF 概念：只重算「有變化」的供應商，而非每次全表重算

## 執行方式

```bash
cd /Users/json/Projects/scholar-learning
JAVA_HOME=/opt/homebrew/opt/openjdk@17 .venv/bin/python3 Delta-Lake/practice/medallion-architecture-deep-dive/solution.py
```

## 核心設計決策

- **Bronze 不做 join、不去重**：保留 replay 能力，任何時候都能從 Bronze 重建下游
- **Silver 是唯一做「跨系統對齊」的地方**：SAP 供應商代碼 + MES 料號統一映射到 `material_id`，這是全專案裡最耗工但也最有價值的一層
- **Gold 只做聚合，不做清洗**：所有清洗邏輯集中在 Silver，Gold 專心處理業務聚合規則
- **反例對照**：`demo_skip_silver_antipattern()` 示範跳過 Silver 直接從 Bronze 算 Gold 會導致的兩個具體問題（重複去重邏輯寫兩次、confirmed 使用 SAP 和 MES 兩套不同判斷條件造成口徑不一致）
