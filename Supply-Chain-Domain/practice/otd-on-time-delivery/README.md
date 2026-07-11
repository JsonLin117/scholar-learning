# ODM OTD（On-Time Delivery）計算與根因分析引擎

> 學習日期：2026-07-12 | Scholar Day 74 | 科目：Supply-Chain-Domain（🟠 suppA）
> 對應筆記：`second-brain/Scholar/Supply-Chain-Domain/otd-on-time-delivery.md`

## 場景

緯穎規模 ODM 廠每天要對三個 CSP 客戶（CSP-MSFT / CSP-GOOG / CSP-META）算 OTD，
但 PMC、CF、客戶端各自用不同口徑算，數字對不上，這支程式示範口徑差異的實際影響，
並模擬 `otd-on-time-delivery.md` 提到的架構建議：`fact_otd_daily` 每日預聚合表。

## 練習重點

1. **雙口徑 OTD 計算**：CSD 基準（業界標準，供應商自己承諾的日期）vs CRD 基準（客戶希望日，通常嚴格得多）——實際跑出來 CRD 基準的 OTD 明顯低於 CSD 基準，驗證筆記裡「兩者中間隔著整段評估緩衝，數字可能差 10-20 個百分點」的說法（實測差距甚至超過 40 個百分點，因為模擬資料刻意放大了 CRD-CSD gap）。
2. **訂單層級 vs 訂單行層級**：一張 PO 有多行，只要一行遲到全單就算不準時（Order-level），比逐行計算（Line-level）嚴格很多，驗證程式碼裡有 assert 確保 Order-level ≤ Line-level。
3. **根因分類**：呼應 `cf-core-pain-points.md` 的三重壓力——插單衝突（rush_conflict）、ECO 衝擊（eco_impact）、缺料、產能瓶頸、物流延誤、品質重工，六種分類統計每種原因造成的延誤筆數。
4. **跨系統風險評分（Composite Risk Score）**：模擬把「短交期壓力」「插單」「活躍 ECO 數量」三個訊號匯總成單一分數，找出 PMC 應該優先處理的高風險訂單。
5. **fact_otd_daily 預聚合表**：模擬每日批次跑出的聚合表設計（按 customer × delay_reason 分組），Dashboard 只讀這張表而非每次重新 JOIN 原始資料。

## 執行方式

```bash
cd /Users/json/Projects/scholar-learning
python3 Supply-Chain-Domain/practice/otd-on-time-delivery/solution.py
```

純 Python 標準庫實作（`dataclasses`/`enum`/`datetime`/`statistics`/`random`），無需 PySpark 或 JAVA_HOME，因為這是業務邏輯層練習而非分散式運算練習。

## 驗證結果（實測）

```
✅ 所有驗證通過：資料量正確、OTD% 範圍合理、Order-level <= Line-level、fact table 非空
```

- 200 筆模擬出貨紀錄
- CSD 基準 Line-level OTD：CSP-GOOG 88.2% / CSP-META 92.2% / CSP-MSFT 83.8%
- CRD 基準明顯更嚴格（36.8%~53.1%），驗證了「對比基準搞錯」是常見坑的說法
- Order-level（68.7%）明顯低於 Line-level（88.0%），符合預期
- 根因分布顯示插單衝突（rush_conflict）是最大延誤來源，符合 CF 三重壓力筆記的預期
