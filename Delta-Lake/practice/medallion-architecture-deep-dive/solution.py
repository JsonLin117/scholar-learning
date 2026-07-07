"""
Medallion Architecture 深度練習：ODM 採購資料 Bronze/Silver/Gold Pipeline
場景：緯穎 SAP ECC（PO）+ SAP LFA1（供應商主數據）+ MES（生產過站確認交期）
      → Bronze 原始落地 → Silver 跨系統對齊清洗 → Gold 業務聚合

依賴：無（dependency-free，純 Python 模擬 Delta Lake 三層 pipeline）
學習日期：2026-07-08
"""

from datetime import datetime
from collections import defaultdict


# ─────────────────────────────────────────────
# 1. 基礎工具：MiniDeltaTable（重用 delta-lake-merge 練習的設計）
# ─────────────────────────────────────────────

class MiniDeltaTable:
    """模擬 Delta Lake Table：只做最小必要的 append/overwrite + 簡易 log"""

    def __init__(self, name: str):
        self.name = name
        self.data: list = []
        self.version: int = 0
        self.log: list = []

    def append(self, rows: list, source_system: str | None = None, process_id: str | None = None):
        """Bronze 專用：原始 append，加 metadata，不做任何清洗"""
        ts = datetime.now().isoformat()
        for r in rows:
            row = dict(r)
            if source_system:
                row["_source_system"] = source_system
            if process_id:
                row["_process_id"] = process_id
            row["_ingested_at"] = ts
            self.data.append(row)
        self._commit(f"APPEND ({len(rows)} rows)")

    def overwrite(self, rows: list):
        self.data = list(rows)
        self._commit(f"OVERWRITE ({len(rows)} rows)")

    def _commit(self, action: str):
        self.version += 1
        entry = {"version": self.version, "action": action, "row_count": len(self.data)}
        self.log.append(entry)
        print(f"  📝 [{self.name}] {action}  (v{self.version}, total={len(self.data)} rows)")

    def show(self, limit=10, title=None, cols=None):
        rows = self.data[:limit]
        if title:
            print(f"\n{'='*70}\n  {title}\n{'='*70}")
        if not rows:
            print("  (empty table)")
            return
        headers = cols or [k for k in rows[0].keys() if not k.startswith("_")]
        widths = {h: max(len(h), max(len(str(r.get(h, ''))) for r in rows)) for h in headers}
        print("  " + " | ".join(h.ljust(widths[h]) for h in headers))
        print("  " + "-+-".join("-" * widths[h] for h in headers))
        for r in rows:
            print("  " + " | ".join(str(r.get(h, '')).ljust(widths[h]) for h in headers))
        print(f"  ({len(self.data)} rows total)")


# ─────────────────────────────────────────────
# 2. BRONZE 層：三個來源系統原始落地
# ─────────────────────────────────────────────

def build_bronze():
    print("\n" + "=" * 70)
    print("  STEP 1：BRONZE 層 — 原始資料 1:1 落地（不清洗、不 join）")
    print("=" * 70)

    bronze_po = MiniDeltaTable("bronze.sap_ekpo")
    bronze_po.append([
        {"EBELN": "PO2026001", "EBELP": "10", "MATNR": "MAT-GPU-A100", "MENGE": 50,  "NETPR": 12000, "LIFNR": "V001"},
        {"EBELN": "PO2026001", "EBELP": "10", "MATNR": "MAT-GPU-A100", "MENGE": 50,  "NETPR": 12000, "LIFNR": "V001"},  # 重複行（source 系統重傳）
        {"EBELN": "PO2026002", "EBELP": "10", "MATNR": "MAT-DDR5-64G", "MENGE": 800, "NETPR": 220,   "LIFNR": "V002"},
        {"EBELN": "PO2026003", "EBELP": "10", "MATNR": "MAT-NIC-100G", "MENGE": 300, "NETPR": 180,   "LIFNR": "V003"},
        {"EBELN": "PO2026004", "EBELP": "10", "MATNR": "MAT-GPU-A100", "MENGE": 20,  "NETPR": 12500, "LIFNR": "V001"},
        {"EBELN": None,        "EBELP": "10", "MATNR": "MAT-BAD-ROW",  "MENGE": 5,   "NETPR": 100,   "LIFNR": "V999"},  # 壞資料：無 PO number
    ], source_system="SAP_ECC", process_id="po_ingest_daily")

    bronze_vendor = MiniDeltaTable("bronze.sap_lfa1")
    bronze_vendor.append([
        {"LIFNR": "V001", "NAME1": "Alpha Semi",  "LAND1": "TW", "avl_status": "active"},
        {"LIFNR": "V002", "NAME1": "Beta Memory", "LAND1": "KR", "avl_status": "active"},
        {"LIFNR": "V003", "NAME1": "Gamma NIC",   "LAND1": "CN", "avl_status": "conditional"},
    ], source_system="SAP_ECC", process_id="vendor_master_sync")

    bronze_mes = MiniDeltaTable("bronze.mes_shipment_confirm")
    bronze_mes.append([
        {"EBELN": "PO2026001", "confirm_status": "SHIPPED",  "confirm_date": "2026-07-05"},
        {"EBELN": "PO2026002", "confirm_status": "DELAYED",  "confirm_date": "2026-07-10"},
        {"EBELN": "PO2026003", "confirm_status": "SHIPPED",  "confirm_date": "2026-07-03"},
        # PO2026004 尚未有 MES 確認記錄（still in production）
    ], source_system="MES", process_id="shipment_confirm_sync")

    bronze_po.show(title="Bronze: sap_ekpo（含重複行 + 1 筆壞資料，原封不動）")
    bronze_vendor.show(title="Bronze: sap_lfa1")
    bronze_mes.show(title="Bronze: mes_shipment_confirm")

    return bronze_po, bronze_vendor, bronze_mes


# ─────────────────────────────────────────────
# 3. SILVER 層：跨系統對齊 + 去重 + 清洗（"just-enough"）
# ─────────────────────────────────────────────

def build_silver(bronze_po, bronze_vendor, bronze_mes):
    print("\n" + "=" * 70)
    print("  STEP 2：SILVER 層 — 去重、跨系統對齊、Enterprise View")
    print("=" * 70)

    # 2.1 去除壞資料 + 去重
    seen = set()
    clean_po_rows = []
    dropped = 0
    for r in bronze_po.data:
        if not r.get("EBELN"):
            dropped += 1
            continue
        key = (r["EBELN"], r["EBELP"])
        if key in seen:
            dropped += 1
            continue
        seen.add(key)
        clean_po_rows.append(r)
    print(f"  🧹 去除壞資料/重複行：{dropped} 筆被丟棄")

    # 2.2 跨系統對齊：PO + 供應商主數據 + MES 確認狀態 join 起來
    vendor_map = {v["LIFNR"]: v for v in bronze_vendor.data}
    mes_map = {m["EBELN"]: m for m in bronze_mes.data}

    silver_po = MiniDeltaTable("silver.purchase_order_detail")
    silver_rows = []
    for r in clean_po_rows:
        vendor = vendor_map.get(r["LIFNR"], {})
        mes = mes_map.get(r["EBELN"], {})
        silver_rows.append({
            "po_number": r["EBELN"],
            "po_line": r["EBELP"],
            "material_id": r["MATNR"],
            "order_qty": r["MENGE"],
            "unit_price": r["NETPR"],
            "vendor_id": r["LIFNR"],
            "vendor_name": vendor.get("NAME1", "UNKNOWN"),
            "vendor_country": vendor.get("LAND1", "UNKNOWN"),
            "vendor_avl_status": vendor.get("avl_status", "UNKNOWN"),
            # 統一判斷「是否已確認出貨」的唯一定義位置（Enterprise view 的價值所在）
            "is_confirmed": mes.get("confirm_status") == "SHIPPED",
            "confirm_date": mes.get("confirm_date"),
        })
    silver_po.overwrite(silver_rows)
    silver_po.show(title="Silver: purchase_order_detail（已 join 供應商 + MES 確認狀態）",
                    cols=["po_number", "material_id", "order_qty", "vendor_name", "vendor_avl_status", "is_confirmed"])

    return silver_po


# ─────────────────────────────────────────────
# 4. GOLD 層：業務聚合（供應商月度採購金額 + OTD 概念）
# ─────────────────────────────────────────────

def build_gold(silver_po):
    print("\n" + "=" * 70)
    print("  STEP 3：GOLD 層 — 業務聚合（供應商月度採購金額 / 確認率）")
    print("=" * 70)

    agg = defaultdict(lambda: {"total_amount": 0, "po_count": 0, "confirmed_count": 0})
    for r in silver_po.data:
        key = r["vendor_name"]
        agg[key]["total_amount"] += r["order_qty"] * r["unit_price"]
        agg[key]["po_count"] += 1
        if r["is_confirmed"]:
            agg[key]["confirmed_count"] += 1

    gold_rows = []
    for vendor_name, stats in agg.items():
        confirm_rate = stats["confirmed_count"] / stats["po_count"] if stats["po_count"] else 0
        gold_rows.append({
            "vendor_name": vendor_name,
            "total_po_amount": stats["total_amount"],
            "po_count": stats["po_count"],
            "confirm_rate": f"{confirm_rate:.0%}",
        })

    gold_vendor_spend = MiniDeltaTable("gold.vendor_monthly_spend")
    gold_vendor_spend.overwrite(sorted(gold_rows, key=lambda x: -x["total_po_amount"]))
    gold_vendor_spend.show(title="Gold: vendor_monthly_spend（consumption-ready，業務方直接可用）")

    return gold_vendor_spend


# ─────────────────────────────────────────────
# 5. 反例：跳過 Silver，直接 Bronze → Gold 會發生什麼？
# ─────────────────────────────────────────────

def demo_skip_silver_antipattern(bronze_po, bronze_vendor, bronze_mes):
    print("\n" + "=" * 70)
    print("  反例：跳過 Silver，直接從 Bronze 算 Gold（不建議！）")
    print("=" * 70)

    vendor_map = {v["LIFNR"]: v for v in bronze_vendor.data}
    mes_map = {m["EBELN"]: m for m in bronze_mes.data}

    # 問題 1：忘記去重（Bronze 有重複行 + 壞資料，直接算的人常常忘記處理）
    naive_total = 0
    naive_po_count = 0
    seen_keys = set()
    for r in bronze_po.data:
        if not r.get("EBELN"):
            continue  # 至少要記得過濾壞資料
        key = (r["EBELN"], r["EBELP"], r["LIFNR"])
        # ⚠️ 這裡故意「忘記」去重複行，模擬真實世界常見疏忽
        naive_total += r["MENGE"] * r["NETPR"]
        naive_po_count += 1

    print(f"  ❌ [Bronze 直接算] 未去重總金額：${naive_total:,.0f}（PO 行數={naive_po_count}，含重複行）")

    # 問題 2：判斷「confirmed」的邏輯要重寫一次，容易跟 Silver 版本的定義不一致
    # 例如這裡不小心漏掉了 vendor_avl_status 過濾，把 conditional 供應商也算進去
    naive_confirmed_vendors = set()
    for r in bronze_po.data:
        if not r.get("EBELN"):
            continue
        mes = mes_map.get(r["EBELN"], {})
        if mes.get("confirm_status") == "SHIPPED":  # 跟 Silver 用同樣條件，但這裡是「第二次重寫」同一段邏輯
            naive_confirmed_vendors.add(r["LIFNR"])

    print(f"  ❌ [Bronze 直接算] confirmed vendor 數：{len(naive_confirmed_vendors)}")
    print("  ⚠️  問題總結：")
    print("     1. 每個 Gold 需求都要重新寫一次「去重 + join + 業務規則判斷」邏輯")
    print("        → 邏輯散落在多個 pipeline，改一個規則要改 N 個地方")
    print("     2. 容易忘記處理 Bronze 層的原始瑕疵（重複行、壞資料）")
    print("        → 本例故意示範：naive 版本金額比 Silver-based 版本多算了一筆重複的 PO2026001")
    print("     3. 沒有一個「唯一事實版本」的 is_confirmed 定義")
    print("        → 不同 Gold 報表可能對「已確認」有不同（甚至互相矛盾）的計算方式")


# ─────────────────────────────────────────────
# 6. Bonus：模擬 CDF 概念 — 只重算「有變化」的供應商
# ─────────────────────────────────────────────

def demo_incremental_gold_refresh(silver_po):
    print("\n" + "=" * 70)
    print("  Bonus：模擬 CDF 增量處理 — 只重算受影響的供應商，不做全表重算")
    print("=" * 70)

    # 假設今天只有 V003（Gamma NIC）的 MES 確認狀態更新了
    changed_vendor_ids = {"V003"}
    affected_rows = [r for r in silver_po.data if r["vendor_id"] in changed_vendor_ids]
    unaffected_count = len(silver_po.data) - len(affected_rows)

    print(f"  📡 模擬 CDF 通知：只有 vendor_id={changed_vendor_ids} 的 Silver 資料有變化")
    print(f"  ✅ 增量重算：只需處理 {len(affected_rows)} 筆受影響的行")
    print(f"  ⏭️  跳過：{unaffected_count} 筆未變化的行（不需要重新掃描）")
    print("  💡 這就是 Delta Lake CDF 在 Medallion 架構中的價值：")
    print("     Silver→Gold 不必每次全表重算，只处理有變化的 key")


# ─────────────────────────────────────────────
# main
# ─────────────────────────────────────────────

def main():
    print("\n" + "🏗️ " * 20)
    print("  Medallion Architecture 深度練習")
    print("  場景：緯穎 SAP ECC + SAP LFA1 + MES 採購資料 Bronze/Silver/Gold Pipeline")
    print("🏗️ " * 20)

    bronze_po, bronze_vendor, bronze_mes = build_bronze()
    silver_po = build_silver(bronze_po, bronze_vendor, bronze_mes)
    gold_vendor_spend = build_gold(silver_po)

    demo_skip_silver_antipattern(bronze_po, bronze_vendor, bronze_mes)
    demo_incremental_gold_refresh(silver_po)

    # ─── 驗證斷言（作為練習正確性的自我檢查） ───
    print("\n" + "=" * 70)
    print("  ✅ 驗證")
    print("=" * 70)
    assert len(bronze_po.data) == 6, "Bronze 應保留原始 6 筆（含壞資料+重複行）"
    assert len(silver_po.data) == 4, "Silver 應去除壞資料+重複行後剩 4 筆"
    assert all(r["vendor_name"] != "UNKNOWN" for r in silver_po.data), "Silver 所有行都應該 join 到供應商名稱"
    gold_total_vendors = len(gold_vendor_spend.data)
    assert gold_total_vendors == 3, f"Gold 應有 3 家供應商彙總，實際 {gold_total_vendors}"
    print("  PASS: Bronze 保留原始 6 筆（含瑕疵資料）")
    print("  PASS: Silver 去重清洗後剩 4 筆，且全部成功 join 供應商資訊")
    print("  PASS: Gold 產出 3 家供應商的月度彙總")
    print("\n🎉 Medallion Architecture Bronze→Silver→Gold Pipeline 練習完成！")


if __name__ == "__main__":
    main()
