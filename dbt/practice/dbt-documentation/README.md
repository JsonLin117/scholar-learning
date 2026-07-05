# dbt Documentation 實作練習

**日期：** 2026-07-06
**科目：** dbt
**場景：** ODM 供應鏈

## 🏭 情境描述

你是某 ODM 公司的 Data Engineer，Data Platform 團隊要求：所有 `marts/` 層的 model
與 PK/FK column 都必須有 description，否則 CI 擋下 PR；同時要把 dbt description
透過 `persist_docs` 同步進倉庫的 `COMMENT ON TABLE/COLUMN`，讓不用 dbt 的 BI 分析師
也能在 Databricks Catalog Explorer 直接看到欄位說明。

本練習不依賴真的 dbt CLI，用 dependency-free Python 模擬：
1. Doc Block Registry（`{% docs %}` / `{{ doc() }}` 解析與跨檔引用）
2. Description Coverage Checker（dbt-coverage 風格，marts 必寫 + PK/FK 必寫）
3. persist_docs → `COMMENT ON TABLE/COLUMN` SQL 產生器
4. CI Gate 模擬：文件不足時 exit code 非 0，補齊後轉綠

## 💡 解題思路

1. 建一個 mini ODM dbt project metadata：staging / intermediate / marts 三層，
   模型：`stg_sap_po`（staging，1:1 映射，可選寫）、`int_po_line_status`（intermediate）、
   `fct_purchase_orders` + `dim_vendors`（marts，必寫）。
2. 用 `DocBlockRegistry` 模擬 `plant_code`、`vendor_id`、`po_status` 等共用欄位定義，
   YAML description 用 `{{ doc("xxx") }}` 語法引用，registry 負責 resolve。
3. `CoverageChecker` 套用「必寫 vs 選寫」矩陣：
   - marts model + description → 必寫
   - marts column + description → 必寫
   - PK/FK column（不論哪層）→ 必寫
   - staging/intermediate 一般 column → 選寫，不計入 fail
4. 模擬兩輪 CI：Round 1（vendor_id FK 缺 description）→ FAIL；
   Round 2（補齊後）→ PASS，並印出 coverage %。
5. `persist_docs` 模擬：只有 `+persist_docs: {relation: true, columns: true}` 的
   mart model 才產生 `COMMENT ON TABLE` / `COMMENT ON COLUMN` DDL，
   staging model 因為沒開 persist_docs 所以不產生。

## 🔧 實作重點

1. **doc() 解析**：description 字串中若匹配 `{{ doc("name") }}` 就去 registry 查表替換，
   查不到的 doc block 名稱要丟出明確錯誤（模擬 dbt compile 失敗）。
2. **Coverage 規則引擎**：分層 + PK/FK 判斷這種「條件式必寫」比單純算全域覆蓋率更貼近
   真實 CI gate（`dbt_meta_testing` / `dbt-coverage --model-path marts`）的做法。
3. **persist_docs DDL 產生器**：只处理 description 不为空的 relation/column，
   且只对声明 persist_docs 的 model 生效，體現「文件即代碼，但同步是選擇性的」。

## 📊 SA 延伸思考

`persist_docs` 只在 **table / incremental** materialization 上可靠——view 每次
`CREATE OR REPLACE VIEW` 都會重建，Databricks 的 view COMMENT 可能被沖掉，
所以「文件同步到倉庫 metadata」這件事本身也是一個架構決策：
你要嘛把 mart 全部設計成 table/incremental materialization 換取穩定的 COMMENT，
要嘛接受 view 型 mart 的 metadata 必須靠 dbt docs 網站而非倉庫 native metadata 呈現。
這正是「文件系統設計」跟「materialization 策略」互相牽制的一個具體例子。
