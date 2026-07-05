# Kimball vs Inmon 實作練習

**日期：** 2026-07-06
**科目：** Data-Warehouse
**場景：** ODM 供應鏈 / Bus Matrix 治理工具

## 🏭 情境描述

你是緯穎（ODM 伺服器製造商）的 Data Engineer。公司正在用 Kimball Bus Architecture
方式漸進式擴建資料倉庫：先做「採購」Data Mart，接著「收貨」「供應商績效」「庫存」。
問題是：不同團隊各自建表時，很容易在沒注意的情況下對同一個維度（例如 `dim_vendor`）
建立出定義不一致的版本（欄位不同、Surrogate Key 型別不同），導致 Data Mart 之間無法
JOIN，退化成孤島。你被要求做一個輕量工具，在新增 Fact Table 定義時，自動檢查它引用
的維度是否都是「已註冊的 Conformed Dimension」，並產生 Bus Matrix 報表。

## 💡 解題思路

把 Kimball Bus Architecture 的核心治理機制寫成程式碼：
1. 維護一份 **Conformed Dimension Registry**（維度的欄位規格 + 型別）
2. 每個業務流程的 Fact Table 定義只能「引用」註冊過的維度，不能自建同名但不同規格的版本
3. 檢查機制：新 Fact 引用的維度如果欄位規格對不上 registry → 直接報錯，阻止孤島產生
4. 產生 **Bus Matrix**：業務流程 × 共享維度的矩陣，視覺化目前的擴充藍圖

## 🔧 實作重點

1. **Conformed Dimension Registry**：用 dataclass 定義每個維度的規格（欄位名 + 型別），
   作為單一事實來源（這裡故意讓程式碼本身扮演 Inmon 精神的「中央治理」角色）。
2. **Fact Table 註冊時的一致性檢查**：模擬「收貨 Data Mart」團隊如果誤建了一個欄位對不上
   的 `dim_vendor` 版本，程式應該偵測出來並拒絕，逼團隊改用共享定義（這就是 Bus Matrix
   紀律的自動化版本）。
3. **Bus Matrix 產出**：把「業務流程 × 使用的 Conformed Dimension」列成矩陣，模擬
   Kimball Group 建議的規劃工具，用於後續擴充新 Data Mart 時檢查覆蓋範圍。

## 📊 SA 延伸思考

- 如果我是 SA，這個 Registry 概念可以進一步落地成 dbt 的 `dbt_project.yml` + 
  `sources.yml` 中的 **contract（column-level contract）**，讓 dbt 在 CI 階段就能
  自動擋下不符合 Conformed Dimension 規格的 model。
- Trade-off：中心化 Registry 提供一致性保證，但也意味著新增/修改共享維度需要走
  治理流程（不能單一團隊隨意改），這是 Kimball Bus Architecture 「治理紀律成本」
  的具體展現——跟 Inmon EDW 集中治理在精神上其實是一致的，只是作用範圍限縮在
  「維度」而非「全部資料」。
