# 反思 2026-05-27

## 今天學了什麼
科目：Data-Warehouse（main）| 主題：SCD Type 2  
進度：Data-Warehouse 第 5 課（topics[4]）

---

## 🔁 Spaced Repetition 複習

| 主題 | 自評 | 下次複習 | 備注 |
|------|------|----------|------|
| 需求預測在 ODM（Rolling/Firm/Pull/MAPE） | 4/5 | 2026-05-30 | 首次複習通過，三層 signal 清楚 |
| Pallet（棧板/物流基本單位） | 4/5 | 2026-06-14 | 規格和追蹤意義都說得出來 |
| GX 核心概念（Expectation/Suite/Checkpoint/Data Docs） | 5/5 | 2026-06-15 | 四個概念和 flow 倒背如流 |

> ⏰ 剩餘到期卡（今日未複習）：sop, taskflow-api, ddmrp → 下次 Phase B 補

---

## Senior DE 考題

**問題：** 請說明 SCD Type 2 的 UNION 展開法，以及為什麼比兩步法更好？

**我的作答（不看筆記）：**

兩步法的問題：第一步 UPDATE（關閉舊行），第二步 INSERT（插新行），兩步法中間有窗口——如果 Step 1 成功、Step 2 失敗，dim 表裡舊行被關閉但新行沒插進來，資料就不一致了。

UNION 展開法：把 source 在 USING 子句裡展開成兩類行，一類帶 `'close'` 標記（用來 MATCHED UPDATE），一類帶 `'open'` 標記（用來 NOT MATCHED INSERT），整個 MERGE 是一次原子操作：

```sql
MERGE INTO dim_vendor AS target
USING (
  -- 舊行：標記要關閉的
  SELECT vendor_id, 'close' AS _action, current_date() AS effective_to, ...
  FROM staging WHERE change_detected AND is_current = true  
  UNION ALL
  -- 新行：要插入的新版本
  SELECT vendor_id, 'open' AS _action, current_date() AS effective_from, ...
  FROM staging WHERE change_detected OR is_new
) AS staged
ON target.vendor_id = staged.vendor_id
   AND target.is_current = true
   AND staged._action = 'close'
WHEN MATCHED THEN UPDATE SET is_current = false, effective_to = staged.effective_to
WHEN NOT MATCHED AND staged._action = 'open' THEN INSERT (...)
```

**對照評分：** ⭐⭐⭐⭐ (4/5)  
**漏掉的：**
- UNION 展開時 source 的去重邏輯很重要（同一 vendor 同天多筆 CDC，要先 dedupe 取最新）
- hash-based Surrogate Key（`md5(vendor_id || effective_from)`）讓 MERGE 冪等，防止重跑時重複插行
- Mixed SCD 策略：city/payment_terms 用 Type 2，name 用 Type 1 — 兩種策略可混在同一張 dim 表

---

## SA 挑戰：ODM 場景

**情境：** 緯穎採購主管希望分析「過去兩年，哪些核心供應商的 Lead Time 出現惡化趨勢？」但目前 dim_vendor 是 Type 1（只有當前值），歷史資料全丟了。

**我的方案（如果是 SA，我怎麼做）：**

1. **遷移 dim_vendor → SCD Type 2**  
   加三欄：`effective_from DATE`、`effective_to DATE`（預設 9999-12-31）、`is_current BOOLEAN`  
   用 `md5(vendor_id || effective_from)` 做 hash SK（冪等，重跑安全）

2. **每日增量 MERGE**（Airflow DAG，跑在夜間 00:00 after ERP snapshot）  
   UNION 展開法，mixed：Lead Time / AVL Status / City → Type 2；vendor_name → Type 1  

3. **歷史分析 SQL**  
   ```sql
   SELECT v.vendor_id, v.lead_time_days, v.effective_from, v.effective_to,
          AVG(f.actual_lead_time) AS actual_lead_time
   FROM fact_purchase_order f
   JOIN dim_vendor v
     ON f.vendor_id = v.vendor_id
     AND f.order_date BETWEEN v.effective_from AND v.effective_to
   GROUP BY 1,2,3,4
   ORDER BY v.vendor_id, v.effective_from
   ```

4. **效能考量**  
   - dim_vendor 表小（<1萬行版本），可 broadcast hint  
   - Liquid Clustering on `(vendor_id, effective_from)` 讓 date range 查詢快

**Trade-off：**  
- ✅ 完整歷史，審計可用，分析正確  
- ⚠️ 表會慢慢長大（每次變更 +1 行），30 年後有 10 萬行左右也可接受  
- ⚠️ Date range JOIN 比等值 JOIN 慢，但 broadcast 和 Liquid Clustering 可緩解  
- ⚠️ 遷移現有 Type 1 → Type 2：需要一次全量歷史 backfill（通常從 ERP 的 change log 或 audit table 拿）

---

## 明天想繼續探索的問題
1. SCD Type 3 的實際使用場景（只保留「前一個值」，什麼時候夠用？）
2. Surrogate Key 衝突問題：如果 effective_from 在同天有兩次變更，hash SK 會碰撞嗎？（答：需要加 hash_diff 或 timestamp 精度）
3. dbt snapshots 怎麼自動處理 SCD Type 2 — 明天 dbt 課時銜接
