# AKS vs ACA vs Azure Functions — ODM 供應鏈平台選型模擬器

## 場景

緯穎（ODM 伺服器製造商）要在 Azure 上建置一個「供應商 Portal + 即時缺料預警」平台，包含三個工作負載：

1. **供應商 Portal API**（同步、中等流量、多供應商在上班時間查詢交期/PO 狀態）
2. **MRP 缺料事件通知**（事件驅動、Service Bus 訊息、單次處理秒級完成，含**含 DG（危險品）貨物訂單需優先標記**的邏輯）
3. **未來 ML 需求預測模型**（GPU 推論，需自訂 batch scheduler）

身為 SA，需要：
- 用今天學到的 Decision Tree（Kubernetes API 需求 → GPU 需求 → 事件驅動短生命週期 → 一般容器化服務）自動判斷每個工作負載該選哪個 Azure 運算服務
- 模擬三種服務的月成本（依流量/idle 時間），驗證「ACA 是 2026 年新預設值」這個結論在真實工作負載下是否成立
- 整合今天 SC Daily 學到的 DG 分類概念：缺料通知事件如果牽涉含鋰電池（DG）貨物，需要在事件 payload 標記 `dg_classification`，讓下游系統能提前套用較長的 Buffer Lead Time

## 執行

```bash
cd /Users/json/Projects/scholar-learning
JAVA_HOME=/opt/homebrew/opt/openjdk@17 .venv/bin/python3 Cloud-Architecture/practice/aks-vs-aca-vs-azure-functions/solution.py
```

## 練習重點

1. **Decision Tree 實作**：把筆記中的「依序判斷、第一個 yes 就是起點」邏輯寫成可測試的純函式
2. **成本模擬**：用不同的請求量/idle 比例模擬三種服務的月費，驗證「低流量、無 K8s API 需求 → ACA 最省」的結論
3. **DG 事件標記整合**：缺料通知 payload 中加入 `dg_classification` 欄位，模擬 PI965/966/967 三種分類如何影響下游 Buffer Lead Time 計算（呼應今天 SC Daily #51 內容）
4. 全部用 `assert` 驗證關鍵推論成立，不依賴外部服務（dependency-free，純模擬）
