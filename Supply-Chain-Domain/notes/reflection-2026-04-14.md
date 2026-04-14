# 反思 2026-04-14

## 今天學了什麼
科目：Supply-Chain-Domain（suppA）| 主題：ODM 是什麼：ODM vs OEM vs OBM，緯穎的角色與產品線

---

## Senior DE / SA 考題

**問題：**
> 你在緯穎擔任 Data Engineer，Sales 突然說 Microsoft 發了 ECO（工程變更單），要把某款 AI Server 的散熱設計從 Air Cooling 改為 Direct Liquid Cooling。
>
> 問題一（DE）：這個 ECO 對你現有的「採購 Fact Table + BOM Dimension Table」設計有什麼影響？你怎麼處理？
>
> 問題二（SA）：如果今天緯穎同時服務 Microsoft、Google、Meta、AWS，四家客戶的 BOM 資料如何設計架構才能既隔離又共用？請說明你的核心設計原則。

---

**我的作答（不看筆記）：**

**問題一（DE）：ECO 的資料影響**

ECO 改的是 BOM 規格，對資料的衝擊有兩層：

1. **Dimension Table 要做 SCD Type 2**
   - 不能直接 update 舊版本，因為歷史訂單需要知道「當時用的是 Air Cooling」
   - 做法：把舊版本的 `is_current` 設為 False，填上 `valid_to = ECO 生效日`，再 INSERT 一筆新版本（new bom_sk, cooling = Liquid, valid_from = ECO date, is_current = True）
   - `change_reason` 欄位存 ECO 編號，方便日後查詢為什麼有這個版本

2. **Fact Table 不需要動**
   - Fact Table 只存 `bom_sk`（代理鍵），歷史訂單的 `bom_sk` 仍指向舊版本（Air Cooling）
   - ECO 後的新訂單用新的 `bom_sk` → 自動對應到 Liquid Cooling 版本

3. **ECO 衝擊分析**（SA 追加）
   - 需要查：還有多少在製/在途訂單用的是舊版本 BOM？是否需要 Rework？
   - 如果 WIP 數量大，影響交期，需要告警

**問題二（SA）：多客戶 BOM 架構**

核心設計原則：

1. **嚴格資料隔離**
   - `customer_code` 是每筆 BOM 記錄的必填欄位
   - 不同 CSP 的資料絕不混在一張表沒有區隔（因為 Microsoft 和 Google 是競爭對手！）
   
2. **Surrogate Key 不帶業務意義**
   - Fact Table 只存 `bom_sk`，不存任何客戶規格欄位
   - 歷史查詢時 JOIN BOM Dim，才拿到具體規格 → 確保 Fact 表可以跨版本

3. **MDM（主數據管理）統一零件命名**
   - Meta 說「Intel-CPU」，Google 說「Intel CPU」要對應到同一個零件
   - 這是跨客戶共用數據的核心挑戰，沒有 MDM 就算不出「這個零件我買了多少台」

4. **Hub-and-Spoke 架構**
   - Hub：零件 MDM、BOM 版本、供應商主數據 → 集中管
   - Spoke：各 CSP 的 Data Mart → 各部門自己維護（採購部看採購 Mart，CF 部看交付 Mart）
   - 不推薦 Data Mesh（太分散，難以算跨客戶 BOM 共用零件的量）

---

**對照評分：** ⭐⭐⭐⭐⭐ (5/5)

問題一完全答到：SCD Type 2 機制、Surrogate Key 保護 Fact Table、ECO 衝擊分析。  
問題二核心三點都答到：隔離、MDM、Hub-and-Spoke。甚至補了「不推薦 Data Mesh 原因」。

唯一可以更深的地方：
- MDM 的具體實作（模糊比對 / fuzzy matching）沒有展開
- 法律層面：不同 CSP 之間的 NDA 如何映射到資料架構隔離設計（這是遺留的待解疑問）

---

## SA 挑戰：ODM 場景

**情境：**
你是緯穎剛入職的資深 SA。老闆說：「現在採購、倉庫、PMC 三個部門各自建了自己的 Excel + Access 資料庫追 BOM 版本，常常資料對不上，我要你設計一套方案讓大家共用一份 BOM 數據。但每個部門又有自己的特殊欄位需求。」

**你的方案：**

**Step 1：建立 BOM Hub（集中式主數據）**
- 設計一張 `bom_dim` 表，存所有客戶的 BOM 規格 + SCD Type 2 歷史版本
- 這張表由 Engineering 部門擁有（owner 要明確！），其他部門唯讀
- 歷史版本追蹤依賴 `valid_from / valid_to / is_current`
- ECO 變更時，Engineering 更新 BOM Hub，所有部門的下游自動拿到最新版

**Step 2：三個部門的 Spoke（Data Mart）**
- 採購 Mart：BOM Hub + 採購 Fact（PO 金額、交期）
- 倉庫 Mart：BOM Hub + 庫存 Fact（在手量、在途量）
- PMC Mart：BOM Hub + 生產 Fact（計劃產量、實際產量）
- 各部門的特殊欄位（如倉庫的 Bin Location、採購的 AVL 資訊）放在自己的 Mart 擴展表

**Step 3：MDM 導入**
- 第一步：讓三部門統一使用一套物料編號（緯穎的 Internal Part Number）
- 第二步：建立 IPN ↔ 各部門舊編號的 mapping 表，舊資料才能正確遷移

**Trade-off：**
- 好處：資料一致，跨部門溝通用同一套語言，BOM 變更只需維護一處
- 壞處：需要跨部門的「BOM 所有權」共識（最難！通常是組織問題而非技術問題）
- 如果強推沒有業務方配合，很容易變成「多了一個沒人維護的 Hub」
- 建議：從最痛的一條流程（如採購 ↔ PMC 的 BOM 確認流程）開始 pilot，成功後再推廣

---

## 明天想繼續探索的問題

1. 緯穎實際使用的 ERP 是 SAP S/4HANA 嗎？SAP 的 BOM 管理（CS03/CS01）和我設計的 bom_dim 如何對應？
2. 多客戶資料隔離在 Data Layer 以外，還有哪些層次的保護（網路隔離、Databricks Workspace 隔離？）
3. CSP 的 Demand Signal（Forecast）格式：EDI？API？各家都不同嗎？
4. 「先進先出（FIFO）」在 BOM 版本管理有沒有類似概念？（先進廠的舊物料先用？）

---

## 今日自評
- 學習深度：5/5（ODM/OEM/OBM 架構清晰，BOM + SCD Type 2 + MDM 應用扎實）
- 實作質量：5/5（PySpark SCD Type 2 模擬跑通，包含 ECO 影響分析）
- ODM 場景連結：5/5（今天本身就是供應鏈科目，直接在緯穎場景下練習）
- 前置知識串接：✅ 把 PySpark（Lazy Eval, 前幾課）+ Data-Warehouse（SCD Type 2 概念）整合進了今天的供應鏈練習
- 明天計劃：PySpark topics[3] — Partition 是什麼（main 輪）
