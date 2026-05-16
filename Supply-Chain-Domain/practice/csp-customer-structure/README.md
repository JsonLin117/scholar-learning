# CSP Customer Structure 實作練習

**日期：** 2026-05-17  
**科目：** Supply-Chain-Domain  
**場景：** ODM 供應鏈 / CSP Forecast 轉 PO 風險評估

## 🏭 情境描述

某 ODM 伺服器廠服務三個 Hyperscaler 客戶。CSP 會先給 12 週 Rolling Forecast，之後才變成 Firm PO 或 Pull Signal；但 GPU/CPU 的採購 Lead Time 可能比 PO 提前量更長。練習目標是建立一個小型決策引擎：把 Forecast 轉化率、BOM 需求、庫存/在途 PO、Lead Time 缺口合併，找出哪些料需要提前備、哪些訂單已經有交期風險。

## 💡 解題思路

CSP 供應鏈的核心不是「客戶下單後再採購」，而是根據不確定的 Forecast 提前下注。本練習用純 Python 模擬：

1. 依客戶歷史可靠度把 Rolling Forecast 折算成 expected demand。
2. 用 BOM 展開每台 server 需要的 CPU/GPU/NIC/冷板數量。
3. 比較需求日期、物料 Lead Time、庫存與 open PO ETA，判斷缺料與 LT gap。
4. 產出 SA/PMC 可看的 action：coverable、expedite、buy-ahead、negotiate CSD。

## 🔧 實作重點

- 建模 CSP demand signal：Rolling Forecast / Firm PO / Pull Signal 三層確定性。
- 模擬 BOM explosion，把 server-level demand 轉成 component-level requirement。
- 建立 availability ledger：on-hand inventory + open POs by ETA。
- 用 Lead Time gap 和 coverage ratio 生成風險分數與建議動作。

## 📊 SA 延伸思考

如果我是 SA，這個練習可以擴成正式平台：Bronze 層保留每版 CSP Forecast，Silver 層標準化 demand signal 與 BOM，Gold 層輸出 material risk dashboard。Trade-off 是：越早備料越能守住 OTD，但越容易產生 E&O / working capital 壓力；因此 Forecast 版本化與客戶可靠度分數是架構核心，不是附加功能。
