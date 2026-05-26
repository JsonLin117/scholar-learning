"""
SCD Type 2 實作練習 — ODM 供應商 AVL 狀態歷史追蹤
2026-05-27 | Data-Warehouse Lesson 5

場景：緯穎 dim_vendor SCD Type 2，追蹤 avl_status / lead_time / city 的歷史變更
技術：Dependency-free 模擬 Delta Lake MERGE (UNION 展開法)

重點：
1. Hash-based Surrogate Key（冪等）
2. UNION 展開法（一次 MERGE = close old + insert new）
3. 欄位級混合 SCD（Type 2 + Type 1 在同一張表）
4. Point-in-Time Query 驗證
5. 去重防呆（同日多次 CDC）
"""

import hashlib
from datetime import date
from copy import deepcopy
from collections import OrderedDict

# ============================================================
# Mini Delta Table Simulator (dependency-free)
# ============================================================
class MiniDeltaTable:
    """
    模擬 Delta Lake MERGE 語意的 in-memory table。
    每次 MERGE 產生新版本（append-only log），支援 Time Travel。
    """

    def __init__(self, name: str, columns: list[str]):
        self.name = name
        self.columns = columns
        self.rows: list[dict] = []
        self.versions: list[list[dict]] = []  # Time Travel: 每次 commit 的快照
        self._version = -1

    def insert(self, rows: list[dict]):
        for r in rows:
            self.rows.append(r)
        self._commit("INSERT")

    def _commit(self, operation: str):
        self._version += 1
        snapshot = [deepcopy(r) for r in self.rows]
        self.versions.append(snapshot)

    @property
    def version(self):
        return self._version

    def current_rows(self, filter_fn=None):
        result = [deepcopy(r) for r in self.rows]
        if filter_fn:
            result = [r for r in result if filter_fn(r)]
        return result

    def at_version(self, v: int):
        return [deepcopy(r) for r in self.versions[v]]

    def merge(self, source_rows, match_condition, when_matched_updates, when_not_matched_insert):
        """
        模擬 Delta Lake MERGE INTO 語意。
        
        match_condition: fn(target_row, source_row) → bool
        when_matched_updates: [(condition_fn, set_fn), ...]
        when_not_matched_insert: fn(source_row) → dict
        """
        matched_target_indices = set()

        for src in source_rows:
            found_match = False
            for i, tgt in enumerate(self.rows):
                if match_condition(tgt, src):
                    found_match = True
                    matched_target_indices.add(i)
                    # 嘗試每個 whenMatched clause（按順序，第一個匹配的生效）
                    for cond_fn, set_fn in when_matched_updates:
                        if cond_fn(tgt, src):
                            set_fn(tgt, src)
                            break

            if not found_match and when_not_matched_insert:
                new_row = when_not_matched_insert(src)
                if new_row:
                    self.rows.append(new_row)

        self._commit("MERGE")

    def show(self, cols=None, filter_fn=None, order_by=None, title=""):
        rows = self.current_rows(filter_fn)
        if order_by:
            rows.sort(key=order_by)
        if cols is None:
            cols = self.columns

        if title:
            print(f"\n{title}")

        # 計算欄位寬度
        widths = {c: max(len(str(c)), max((len(str(r.get(c, ""))) for r in rows), default=4)) for c in cols}
        header = " | ".join(str(c).ljust(widths[c]) for c in cols)
        sep = "-+-".join("-" * widths[c] for c in cols)
        print(header)
        print(sep)
        for r in rows:
            line = " | ".join(str(r.get(c, "")).ljust(widths[c]) for c in cols)
            print(line)
        print(f"({len(rows)} rows)")


# ============================================================
# Helper Functions
# ============================================================
def make_sk(vendor_id: str, effective_from: str) -> str:
    """Hash-based Surrogate Key: sha256(vendor_id|effective_from)[:16]"""
    raw = f"{vendor_id}|{effective_from}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]

FAR_FUTURE = "9999-12-31"

DISPLAY_COLS = ["sk", "vendor_id", "vendor_name", "city", "avl_status",
                "lead_time_days", "contact_person", "is_current",
                "effective_from", "effective_to"]

SHORT_COLS = ["vendor_id", "vendor_name", "avl_status", "lead_time_days",
              "contact_person", "is_current", "effective_from", "effective_to"]


# ============================================================
# SCD Type 2 MERGE Engine (UNION 展開法)
# ============================================================
def scd_type2_merge(table: MiniDeltaTable, cdc_updates: list[dict],
                    today: str, type2_cols: list[str], type1_cols: list[str]):
    """
    SCD Type 2 MERGE — UNION 展開法 (dependency-free)
    
    核心邏輯：
    1. Source 去重（同日多次 CDC 只取最新）
    2. 對比 current 行的 Type 2 欄位，找出有變化的
    3. 把 source 展開為三類：
       - close_rows: merge_key=vendor_id → MATCHED → close old (Type 2)
       - insert_rows: merge_key=NULL → NOT MATCHED → insert new version
       - type1_rows: merge_key=vendor_id → MATCHED → update in-place (Type 1 only)
    4. 一次 MERGE 搞定
    """

    # Step A: 去重（同一 vendor 取 update_time 最大的）
    deduped = {}
    for row in cdc_updates:
        vid = row["vendor_id"]
        if vid not in deduped or row.get("update_time", "") > deduped[vid].get("update_time", ""):
            deduped[vid] = row

    # Step B: 分類 source 行
    current_rows = {r["vendor_id"]: r for r in table.current_rows(lambda r: r["is_current"])}

    close_rows = []    # Type 2 changed → close old
    insert_rows = []   # Type 2 changed new version + brand new vendors
    type1_rows = []    # Type 1 only changed

    for vid, src in deduped.items():
        if vid in current_rows:
            tgt = current_rows[vid]
            # 檢查 Type 2 欄位是否有變化
            has_type2_change = any(src.get(c) != tgt.get(c) for c in type2_cols)
            has_type1_change = any(src.get(c) != tgt.get(c) for c in type1_cols)

            if has_type2_change:
                # Type 2: 需要 close old + insert new
                close_rows.append({**src, "_merge_key": vid, "_action": "close"})
                insert_rows.append({**src, "_merge_key": None, "_action": "insert"})
            elif has_type1_change:
                # Type 1 only: 原地更新
                type1_rows.append({**src, "_merge_key": vid, "_action": "type1"})
        else:
            # 全新供應商
            insert_rows.append({**src, "_merge_key": None, "_action": "insert"})

    # Step C: 組合成 staged source
    staged = close_rows + insert_rows + type1_rows

    # Step D: MERGE
    table.merge(
        source_rows=staged,
        match_condition=lambda tgt, src: (
            tgt["vendor_id"] == src.get("_merge_key") and
            tgt["is_current"] == True
        ),
        when_matched_updates=[
            # Clause 1: Type 2 close
            (
                lambda tgt, src: src["_action"] == "close",
                lambda tgt, src: tgt.update({
                    "is_current": False,
                    "effective_to": today,
                })
            ),
            # Clause 2: Type 1 update
            (
                lambda tgt, src: src["_action"] == "type1",
                lambda tgt, src: tgt.update({
                    c: src[c] for c in type1_cols
                })
            ),
        ],
        when_not_matched_insert=lambda src: (
            {
                "sk": make_sk(src["vendor_id"], today),
                "vendor_id": src["vendor_id"],
                "vendor_name": src["vendor_name"],
                "city": src["city"],
                "avl_status": src["avl_status"],
                "payment_terms": src["payment_terms"],
                "lead_time_days": src["lead_time_days"],
                "contact_person": src["contact_person"],
                "phone": src["phone"],
                "is_current": True,
                "effective_from": today,
                "effective_to": FAR_FUTURE,
            } if src["_action"] == "insert" else None
        ),
    )

    return table


# ============================================================
# STEP 1: Initial Load
# ============================================================
print("=" * 80)
print("STEP 1: Initial Load — 5 位供應商首次載入")
print("=" * 80)

ALL_COLS = ["sk", "vendor_id", "vendor_name", "city", "avl_status",
            "payment_terms", "lead_time_days", "contact_person", "phone",
            "is_current", "effective_from", "effective_to"]

dim = MiniDeltaTable("dim_vendor", ALL_COLS)

initial_data = [
    ("V001", "台積電",   "新竹", "Active",    "NET30", 15, "王小明", "0912-xxx"),
    ("V002", "日月光",   "高雄", "Active",    "NET60", 20, "李大華", "0923-xxx"),
    ("V003", "京元電",   "新竹", "Active",    "NET30", 10, "張三豐", "0934-xxx"),
    ("V004", "Foxconn", "深圳", "Active",    "NET90", 30, "陳四海", "0945-xxx"),
    ("V005", "Samsung", "平澤", "Probation", "NET60", 25, "Kim SH", "0956-xxx"),
]

init_rows = []
for v in initial_data:
    ef = "2024-01-01"
    init_rows.append({
        "sk": make_sk(v[0], ef),
        "vendor_id": v[0], "vendor_name": v[1], "city": v[2],
        "avl_status": v[3], "payment_terms": v[4], "lead_time_days": v[5],
        "contact_person": v[6], "phone": v[7],
        "is_current": True, "effective_from": ef, "effective_to": FAR_FUTURE,
    })

dim.insert(init_rows)
dim.show(SHORT_COLS, title=f"Initial Load (version {dim.version}):")


# ============================================================
# STEP 2: CDC Round 1
# ============================================================
print("\n" + "=" * 80)
print("STEP 2: CDC Round 1 — Samsung Active 恢復, Foxconn 降級, 新增 Micron, 台積電改聯絡人")
print("=" * 80)

cdc_round1 = [
    # V005 Samsung: Probation → Active（品質改善）→ Type 2
    {"vendor_id": "V005", "vendor_name": "Samsung", "city": "平澤", "avl_status": "Active",
     "payment_terms": "NET60", "lead_time_days": 22, "contact_person": "Kim SH", "phone": "0956-xxx",
     "update_time": "2026-05-27 08:00:00"},
    # V004 Foxconn: Active → Probation（交期延遲）→ Type 2
    {"vendor_id": "V004", "vendor_name": "Foxconn", "city": "深圳", "avl_status": "Probation",
     "payment_terms": "NET90", "lead_time_days": 35, "contact_person": "陳四海", "phone": "0945-xxx",
     "update_time": "2026-05-27 09:00:00"},
    # V006 Micron: 全新供應商
    {"vendor_id": "V006", "vendor_name": "Micron", "city": "桃園", "avl_status": "Active",
     "payment_terms": "NET45", "lead_time_days": 18, "contact_person": "林五湖", "phone": "0967-xxx",
     "update_time": "2026-05-27 10:00:00"},
    # V001 台積電: 只改聯絡人（Type 1 only）→ 不該產生新版本行！
    {"vendor_id": "V001", "vendor_name": "台積電", "city": "新竹", "avl_status": "Active",
     "payment_terms": "NET30", "lead_time_days": 15, "contact_person": "王小華", "phone": "0912-yyy",
     "update_time": "2026-05-27 11:00:00"},
]

TYPE2_COLS = ["city", "avl_status", "payment_terms", "lead_time_days"]
TYPE1_COLS = ["contact_person", "phone"]

scd_type2_merge(dim, cdc_round1, "2026-05-27", TYPE2_COLS, TYPE1_COLS)

dim.show(SHORT_COLS, order_by=lambda r: (r["vendor_id"], r["effective_from"]),
         title=f"After Round 1 (version {dim.version}):")


# ============================================================
# 驗證 1: V001 台積電 — Type 1 only（不產生新版本行）
# ============================================================
print("\n" + "=" * 80)
print("驗證 1: V001 台積電 — Type 1 only change (contact_person)")
print("=" * 80)

v001 = [r for r in dim.current_rows() if r["vendor_id"] == "V001"]
assert len(v001) == 1, f"❌ V001 應只有 1 行（Type 1 不產生歷史），實際 {len(v001)}"
assert v001[0]["contact_person"] == "王小華", f"❌ Type 1 應覆寫為 '王小華'"
assert v001[0]["is_current"] == True, "❌ V001 應仍為 current"
print(f"✅ V001: 1 行（Type 1 原地更新，不產生歷史行）")
print(f"✅ contact_person: '{v001[0]['contact_person']}' (覆寫成功)")
print(f"✅ phone: '{v001[0]['phone']}' (覆寫成功)")


# ============================================================
# 驗證 2: V005 Samsung — Type 2（Probation → Active）
# ============================================================
print("\n" + "=" * 80)
print("驗證 2: V005 Samsung — Type 2 change (Probation → Active)")
print("=" * 80)

v005 = sorted([r for r in dim.current_rows() if r["vendor_id"] == "V005"],
              key=lambda r: r["effective_from"])

assert len(v005) == 2, f"❌ V005 應有 2 行（1 old + 1 current），實際 {len(v005)}"

old, new = v005[0], v005[1]
assert old["avl_status"] == "Probation" and old["is_current"] == False, "❌ 舊行應為 Probation + closed"
assert old["effective_to"] == "2026-05-27", f"❌ 舊行 effective_to 應為 2026-05-27，實際 {old['effective_to']}"
assert new["avl_status"] == "Active" and new["is_current"] == True, "❌ 新行應為 Active + current"
assert new["effective_from"] == "2026-05-27", "❌ 新行 effective_from 應為 2026-05-27"
assert new["lead_time_days"] == 22, f"❌ 新行 lead_time 應為 22，實際 {new['lead_time_days']}"

print(f"✅ V005 old: Probation, is_current=False, effective_to=2026-05-27")
print(f"✅ V005 new: Active, is_current=True, effective_from=2026-05-27, LT=22")


# ============================================================
# 驗證 3: V004 Foxconn — Type 2（Active → Probation）
# ============================================================
print("\n" + "=" * 80)
print("驗證 3: V004 Foxconn — Type 2 change (Active → Probation)")
print("=" * 80)

v004 = sorted([r for r in dim.current_rows() if r["vendor_id"] == "V004"],
              key=lambda r: r["effective_from"])

assert len(v004) == 2, f"❌ V004 應有 2 行"
assert v004[0]["avl_status"] == "Active" and v004[0]["is_current"] == False
assert v004[1]["avl_status"] == "Probation" and v004[1]["is_current"] == True
print(f"✅ V004 old: Active → closed")
print(f"✅ V004 new: Probation → current, LT={v004[1]['lead_time_days']}")


# ============================================================
# 驗證 4: V006 Micron — 全新供應商
# ============================================================
print("\n" + "=" * 80)
print("驗證 4: V006 Micron — 全新供應商")
print("=" * 80)

v006 = [r for r in dim.current_rows() if r["vendor_id"] == "V006"]
assert len(v006) == 1, f"❌ V006 應有 1 行"
assert v006[0]["is_current"] == True
assert v006[0]["effective_from"] == "2026-05-27"
print(f"✅ V006: 1 行，Active，effective_from=2026-05-27")


# ============================================================
# STEP 3: Point-in-Time Query
# ============================================================
print("\n" + "=" * 80)
print("STEP 3: Point-in-Time Query — 查詢 2025-06-01 時的供應商狀態")
print("=" * 80)

pit_date = "2025-06-01"
pit = [r for r in dim.current_rows()
       if r["effective_from"] <= pit_date and r["effective_to"] > pit_date]

print(f"\n供應商狀態 @ {pit_date}:")
for r in sorted(pit, key=lambda r: r["vendor_id"]):
    print(f"  {r['vendor_id']} {r['vendor_name']:8s} avl={r['avl_status']:12s} "
          f"LT={r['lead_time_days']:2d}  city={r['city']}")

# V005 在 2025-06-01 應該是 Probation（不是 Active）
pit_v005 = [r for r in pit if r["vendor_id"] == "V005"]
assert len(pit_v005) == 1, "❌ V005 在 PiT 應有 1 行"
assert pit_v005[0]["avl_status"] == "Probation", \
    f"❌ V005 @ {pit_date} 應為 Probation，實際 {pit_v005[0]['avl_status']}"
print(f"\n✅ V005 @ {pit_date}: Probation（正確！不是 Active）")

# V006 在 2025-06-01 不應存在
pit_v006 = [r for r in pit if r["vendor_id"] == "V006"]
assert len(pit_v006) == 0, "❌ V006 @ 2025-06-01 不應存在"
print(f"✅ V006 @ {pit_date}: 不存在（正確！2026-05-27 才新增）")


# ============================================================
# STEP 4: CDC Round 2 — 更多 Type 2 變更 + 去重驗證
# ============================================================
print("\n" + "=" * 80)
print("STEP 4: CDC Round 2 — V003 搬家 + V005 同日兩次更新(去重驗證)")
print("=" * 80)

cdc_round2 = [
    # V003 京元電：新竹 → 苗栗（搬家）→ Type 2
    {"vendor_id": "V003", "vendor_name": "京元電", "city": "苗栗", "avl_status": "Active",
     "payment_terms": "NET30", "lead_time_days": 12, "contact_person": "張三豐", "phone": "0934-xxx",
     "update_time": "2026-06-15 08:00:00"},
    # V005 Samsung：同日兩次 CDC（08:00 改 LT=20，10:00 改 LT=19）→ 只取最新
    {"vendor_id": "V005", "vendor_name": "Samsung", "city": "平澤", "avl_status": "Active",
     "payment_terms": "NET60", "lead_time_days": 20, "contact_person": "Kim SH", "phone": "0956-xxx",
     "update_time": "2026-06-15 08:00:00"},
    {"vendor_id": "V005", "vendor_name": "Samsung", "city": "平澤", "avl_status": "Active",
     "payment_terms": "NET60", "lead_time_days": 19, "contact_person": "Kim SH", "phone": "0956-xxx",
     "update_time": "2026-06-15 10:00:00"},
]

scd_type2_merge(dim, cdc_round2, "2026-06-15", TYPE2_COLS, TYPE1_COLS)

# 驗證去重：V005 應只產生 1 個新版本（LT=19，不是 LT=20）
v005_r2 = sorted([r for r in dim.current_rows() if r["vendor_id"] == "V005"],
                 key=lambda r: r["effective_from"])

print(f"\nV005 Samsung 全部版本：")
for r in v005_r2:
    print(f"  avl={r['avl_status']:12s} LT={r['lead_time_days']:2d} "
          f"current={str(r['is_current']):5s} {r['effective_from']} ~ {r['effective_to']}")

v005_latest = [r for r in v005_r2 if r["is_current"]]
assert len(v005_latest) == 1, "❌ V005 應只有 1 個 current 版本"
assert v005_latest[0]["lead_time_days"] == 19, \
    f"❌ 去重後 LT 應為 19（最新的），實際 {v005_latest[0]['lead_time_days']}"
print(f"✅ V005 去重正確：LT=19（取 10:00 的更新，不是 08:00 的 LT=20）")


# ============================================================
# STEP 5: Surrogate Key 冪等性驗證
# ============================================================
print("\n" + "=" * 80)
print("STEP 5: Surrogate Key 冪等性驗證")
print("=" * 80)

sk1 = make_sk("V005", "2026-05-27")
sk2 = make_sk("V005", "2026-05-27")
sk3 = make_sk("V005", "2026-06-15")  # 不同日期 → 不同 SK

assert sk1 == sk2, "❌ 相同 vendor_id + date 應產生相同 SK"
assert sk1 != sk3, "❌ 不同日期應產生不同 SK"
print(f"✅ sha256('V005|2026-05-27')[:16] = {sk1}")
print(f"✅ sha256('V005|2026-05-27')[:16] = {sk2} (相同 → 冪等)")
print(f"✅ sha256('V005|2026-06-15')[:16] = {sk3} (不同 → 唯一)")


# ============================================================
# STEP 6: Time Travel 模擬
# ============================================================
print("\n" + "=" * 80)
print("STEP 6: Delta Lake Time Travel 模擬")
print("=" * 80)

for v in range(dim.version + 1):
    snapshot = dim.at_version(v)
    current_count = sum(1 for r in snapshot if r["is_current"])
    total_count = len(snapshot)
    print(f"  Version {v}: {total_count} rows (current: {current_count})")


# ============================================================
# STEP 7: 欄位級 SCD 策略矩陣
# ============================================================
print("\n" + "=" * 80)
print("STEP 7: 欄位級 SCD 策略矩陣 (dim_vendor)")
print("=" * 80)

strategy_matrix = [
    ("vendor_name",    "Type 1", "改名只是修正（如更名通知），不影響歷史分析"),
    ("city",           "Type 2", "影響物流成本/Lead Time 分析，搬家是重大事件"),
    ("avl_status",     "Type 2", "核心！Active→Probation→Suspended 的完整稽核軌跡"),
    ("payment_terms",  "Type 2", "影響 Cash Flow 分析，合約條件變更需歷史追蹤"),
    ("lead_time_days", "Type 2", "影響供應商績效 KPI（OTD 計算基準）"),
    ("contact_person", "Type 1", "不影響業務分析，換人只需覆寫"),
    ("phone",          "Type 1", "不影響業務分析"),
]

print(f"  {'欄位':20s} {'SCD':8s} 理由")
print(f"  {'-'*20} {'-'*8} {'-'*50}")
for col_name, scd_type, reason in strategy_matrix:
    marker = "🔵" if scd_type == "Type 2" else "⚪"
    print(f"  {marker} {col_name:18s} {scd_type:8s} {reason}")


# ============================================================
# STEP 8: 效能建議
# ============================================================
print("\n" + "=" * 80)
print("STEP 8: SCD Type 2 效能最佳實踐")
print("=" * 80)

print("""
📋 生產環境建議：

1. 資料佈局：Liquid Clustering CLUSTER BY (vendor_id)
   → 同一 vendor 的所有版本物理上相鄰
   → Point-in-Time 查詢只掃少量 file groups

2. 快速查 current 版本：
   → CREATE VIEW dim_vendor_current AS SELECT * WHERE is_current = true
   → 或 Bloom Filter on is_current（Databricks 支援）

3. OPTIMIZE 排程：
   → 每日 CDC MERGE 後跑 OPTIMIZE（合併 MERGE 產生的小檔案）
   → 不需要手動 VACUUM（SCD Type 2 自帶歷史，Delta Time Travel 可設短）

4. effective_to 使用 9999-12-31，不用 NULL：
   → BETWEEN 查詢不需要 COALESCE
   → JOIN 條件：fact.date BETWEEN dim.effective_from AND dim.effective_to

5. dbt snapshot 自動化：
   → config(strategy='check', check_cols=['city','avl_status','payment_terms','lead_time_days'])
   → 自動處理 hash SK、去重、冪等、有效日期管理

6. MRO 供應商的 AVL 狀態追蹤：
   → Direct 物料供應商和 MRO 供應商都需要 Type 2 追蹤 avl_status
   → MRO 供應商的狀態變更（Active→Probation→Suspended→Obsolete）
     影響備用供應商切換決策
""")


# ============================================================
# SUMMARY
# ============================================================
print("=" * 80)
print("SUMMARY")
print("=" * 80)

all_rows = dim.current_rows()
current = [r for r in all_rows if r["is_current"]]
historical = [r for r in all_rows if not r["is_current"]]
vendors = set(r["vendor_id"] for r in all_rows)

print(f"最終 dim_vendor 狀態：")
print(f"  Total rows:      {len(all_rows)}")
print(f"  Current rows:    {len(current)}")
print(f"  Historical rows: {len(historical)}")
print(f"  Unique vendors:  {len(vendors)}")
print(f"  Delta versions:  {dim.version + 1}")

dim.show(SHORT_COLS, order_by=lambda r: (r["vendor_id"], r["effective_from"]),
         title="Final State:")

print(f"""
✅ 練習完成！SCD Type 2 核心概念全部驗證：

  1. Type 2 欄位變更 → close old + insert new ✅
  2. Type 1 欄位變更 → 原地更新，不產生歷史行 ✅
  3. 混合 SCD（同一張表 Type 1 + Type 2 欄位共存）✅
  4. 全新 vendor → 直接插入 ✅
  5. Point-in-Time Query → 正確返回歷史時間點的狀態 ✅
  6. 同日多次 CDC 去重 → ROW_NUMBER 只取最新 ✅
  7. Hash-based Surrogate Key → 確定性、冪等 ✅
  8. Time Travel → 版本歷史可追溯 ✅
  9. 欄位級策略矩陣 → SA 按欄位決定 SCD 類型 ✅
""")
