# ODM 供應鏈全景：多客戶 BOM 資料架構設計

**日期：** 2026-04-14
**科目：** Supply-Chain-Domain（suppA）
**場景：** ODM 多客戶 BOM 管理資料架構

---

## 🏭 情境描述

你是緯穎（Wiwynn）的 Data Engineer。緯穎同時服務 Microsoft（Azure）、Google Cloud、Meta、AWS 四家 CSP，每家都訂製了不同規格的 AI 伺服器（CPU 型號、記憶體、散熱設計各不相同）。

本月 Microsoft 緊急提交 ECO（Engineering Change Order），把 Azure HB-120rs v3 伺服器的散熱設計從「Air Cooling」改為「Direct Liquid Cooling」，原因是 NVIDIA GB200 的 TDP 提高了。

你需要：
1. 設計能追蹤多客戶 BOM 版本變更的資料模型（SCD Type 2）
2. 確保 ECO 前的採購訂單歷史報表不受影響
3. 演示如何用 PySpark + Delta Lake 實作 BOM 維度表的 MERGE 操作

---

## 💡 解題思路

**核心概念應用：**
- **SCD Type 2**：保留舊版本（Air Cooling）+ 新增新版本（Liquid Cooling），歷史 PO Fact 仍能正確 join 到當時的規格
- **Surrogate Key**：Fact Table（採購訂單）只存 `bom_sk`，不直接存規格，避免規格更新後歷史記錄失真
- **ODM 多客戶隔離**：`customer_code` 作為 Dimension 的 partition key，確保 Microsoft 資料不跟 Google 混

**Data Engineer 的挑戰**：
- ECO 發生在半夜（台灣時間），自動 pipeline 必須能接收 ECO 更新並更新 BOM Dim
- 歷史採購訂單的報表不能因為 ECO 而改變數字

---

## 🔧 實作重點

### 1. BOM 維度表 SCD Type 2 MERGE 邏輯

```python
# MERGE statement (Delta Lake)
bom_updates.alias("updates").merge(
    bom_dim_table.alias("current"),
    "current.bom_natural_key = updates.bom_natural_key AND current.is_current = true"
).whenMatchedUpdate(  # 找到現行版本 → 將其 valid_to 設為現在
    set={
        "is_current": "false",
        "valid_to": "updates.valid_from"
    }
).whenNotMatchedInsert(  # 插入新版本
    values={
        "bom_sk": "updates.new_bom_sk",
        ...
    }
).execute()
```

### 2. Fact Table 查詢不受影響
```sql
-- 查詢 2025 年 Q4 的採購訂單，join 當時有效的 BOM 規格
SELECT po.po_id, po.order_date, po.quantity, b.cooling_design
FROM po_fact po
JOIN bom_dim b 
  ON po.bom_sk = b.bom_sk
WHERE po.order_date BETWEEN '2025-10-01' AND '2025-12-31'
-- cooling_design 會顯示 Air Cooling（舊版本）而非 Liquid Cooling（新版本）
```

---

## 📊 SA 延伸思考

**如果是我做 SA 的方案設計：**

1. **ECO 自動觸發**：SAP MM 的 ECO 事件 → Kafka event → Airflow DAG 觸發 BOM Dim MERGE
2. **版本比對工具**：給 PMC 和採購人員的 Web 界面，可以比較 BOM v3 vs v4 的差異
3. **影響分析**：ECO 後，有多少 in-transit（在途）採購訂單、多少 WIP（在製品）受影響 → 自動計算 exposure
4. **Trade-off**：如果每次 ECO 都要 MERGE 整個 BOM Dim，對大表的效能壓力如何？
   - 解法：Partition by customer_code + event_date，配合 Delta Lake Z-ORDER by bom_natural_key

---

## 執行方式

```bash
cd /Users/json/Projects/scholar-learning
JAVA_HOME=/opt/homebrew/opt/openjdk@17 .venv/bin/python3 Supply-Chain-Domain/practice/odm-overview/solution.py
```
