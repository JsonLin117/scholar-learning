"""
dbt Snapshots SCD Type 2 Practice
==================================
Scholar Day 61 — dbt #5: Snapshots 自動追蹤 SCD Type 2

模擬 dbt snapshot engine 的核心 MERGE 邏輯，展示：
1. Timestamp 策略 vs Check 策略
2. Hard Deletes 三種模式（ignore/invalidate/new_record）
3. dbt_valid_to_current 配置影響
4. 中間變更壓縮問題（Snapshot 固有限制）
5. Point-in-Time Query

場景：ODM 供應商 AVL 追蹤 + PO 狀態追蹤
"""

import hashlib
from datetime import datetime, timedelta
from copy import deepcopy
from typing import Optional


# ============================================================
# Part 1: Mini Snapshot Engine
# ============================================================

class MiniSnapshotEngine:
    """模擬 dbt snapshot 的核心邏輯。"""

    def __init__(self, unique_key: str, strategy: str,
                 updated_at: str = None,
                 check_cols: list = None,
                 hard_deletes: str = "ignore",
                 dbt_valid_to_current: Optional[str] = None):
        self.unique_key = unique_key
        self.strategy = strategy
        self.updated_at = updated_at
        self.check_cols = check_cols
        self.hard_deletes = hard_deletes
        self.dbt_valid_to_current = dbt_valid_to_current
        self.snapshot_table: list[dict] = []
        self.run_count = 0
        self.stats = {"inserts": 0, "updates": 0, "deletes_tracked": 0}

    def _generate_scd_id(self, row: dict, change_marker: str) -> str:
        """模擬 dbt_scd_id: md5(unique_key || '|' || change_marker)"""
        key_val = str(row[self.unique_key])
        raw = f"{key_val}|{change_marker}"
        return hashlib.md5(raw.encode()).hexdigest()[:12]

    def _get_valid_to_value(self) -> Optional[str]:
        """當前有效記錄的 dbt_valid_to 值"""
        return self.dbt_valid_to_current  # None = NULL

    def _has_changed(self, source_row: dict, snapshot_row: dict) -> bool:
        """根據策略判斷記錄是否有變化。"""
        if self.strategy == "timestamp":
            src_ts = source_row.get(self.updated_at)
            snap_ts = snapshot_row.get("dbt_updated_at")
            if src_ts and snap_ts:
                return src_ts > snap_ts
            return False

        elif self.strategy == "check":
            cols = self.check_cols
            if cols == "all":
                # 排除 meta 欄位
                meta_cols = {"dbt_valid_from", "dbt_valid_to", "dbt_scd_id",
                             "dbt_updated_at", "dbt_is_deleted"}
                cols = [k for k in source_row.keys()
                        if k not in meta_cols and k != self.unique_key]
            for col in cols:
                if source_row.get(col) != snapshot_row.get(col):
                    return True
            return False

        return False

    def _get_current_records(self) -> dict:
        """取得 snapshot 表中所有「當前有效」的記錄（dbt_valid_to = current marker）。"""
        current = {}
        valid_to_marker = self._get_valid_to_value()
        for row in self.snapshot_table:
            if row["dbt_valid_to"] == valid_to_marker:
                key = row[self.unique_key]
                current[key] = row
        return current

    def run(self, source_data: list[dict], snapshot_time: datetime) -> dict:
        """
        執行一次 snapshot run。
        Returns: {"inserts": N, "invalidated": N, "deletes": N}
        """
        self.run_count += 1
        run_stats = {"inserts": 0, "invalidated": 0, "deletes": 0}
        ts_str = snapshot_time.strftime("%Y-%m-%d %H:%M:%S")

        current_records = self._get_current_records()
        source_keys = {row[self.unique_key] for row in source_data}

        # === Process each source row ===
        for src_row in source_data:
            key = src_row[self.unique_key]

            if key not in current_records:
                # New record → INSERT
                new_row = deepcopy(src_row)
                if self.strategy == "timestamp" and self.updated_at:
                    valid_from = src_row.get(self.updated_at, ts_str)
                else:
                    valid_from = ts_str
                new_row["dbt_valid_from"] = valid_from
                new_row["dbt_valid_to"] = self._get_valid_to_value()
                new_row["dbt_updated_at"] = valid_from if self.strategy == "timestamp" else ts_str
                new_row["dbt_scd_id"] = self._generate_scd_id(src_row, str(valid_from))
                if self.hard_deletes == "new_record":
                    new_row["dbt_is_deleted"] = False
                self.snapshot_table.append(new_row)
                run_stats["inserts"] += 1

            else:
                # Existing record → check if changed
                snap_row = current_records[key]

                if self._has_changed(src_row, snap_row):
                    # Invalidate old
                    if self.strategy == "timestamp" and self.updated_at:
                        invalidate_ts = src_row.get(self.updated_at, ts_str)
                    else:
                        invalidate_ts = ts_str
                    snap_row["dbt_valid_to"] = invalidate_ts

                    # Insert new version
                    new_row = deepcopy(src_row)
                    new_row["dbt_valid_from"] = invalidate_ts
                    new_row["dbt_valid_to"] = self._get_valid_to_value()
                    new_row["dbt_updated_at"] = invalidate_ts if self.strategy == "timestamp" else ts_str
                    new_row["dbt_scd_id"] = self._generate_scd_id(src_row, str(invalidate_ts))
                    if self.hard_deletes == "new_record":
                        new_row["dbt_is_deleted"] = False
                    self.snapshot_table.append(new_row)
                    run_stats["invalidated"] += 1
                    run_stats["inserts"] += 1

        # === Handle hard deletes ===
        if self.hard_deletes != "ignore":
            for key, snap_row in current_records.items():
                if key not in source_keys:
                    if self.hard_deletes == "invalidate":
                        snap_row["dbt_valid_to"] = ts_str
                        run_stats["deletes"] += 1

                    elif self.hard_deletes == "new_record":
                        snap_row["dbt_valid_to"] = ts_str
                        # Insert delete marker row
                        del_row = deepcopy(snap_row)
                        del_row["dbt_valid_from"] = ts_str
                        del_row["dbt_valid_to"] = self._get_valid_to_value()
                        del_row["dbt_updated_at"] = ts_str
                        del_row["dbt_is_deleted"] = True
                        del_row["dbt_scd_id"] = self._generate_scd_id(
                            snap_row, f"deleted_{ts_str}")
                        self.snapshot_table.append(del_row)
                        run_stats["deletes"] += 1
                        run_stats["inserts"] += 1

        self.stats["inserts"] += run_stats["inserts"]
        self.stats["updates"] += run_stats["invalidated"]
        self.stats["deletes_tracked"] += run_stats["deletes"]

        return run_stats

    def point_in_time_query(self, as_of: str) -> list[dict]:
        """模擬 Point-in-Time Query：取得指定時間點的有效記錄。"""
        results = []
        for row in self.snapshot_table:
            valid_from = row["dbt_valid_from"]
            valid_to = row["dbt_valid_to"]

            if valid_to is None:
                # NULL means current → valid if as_of >= valid_from
                if as_of >= valid_from:
                    results.append(row)
            elif self.dbt_valid_to_current and valid_to == self.dbt_valid_to_current:
                # Custom current marker → treat as current
                if as_of >= valid_from:
                    results.append(row)
            else:
                # Historical → valid if valid_from <= as_of < valid_to
                if valid_from <= as_of < valid_to:
                    results.append(row)

        # Filter out deleted records for PIT query
        if self.hard_deletes == "new_record":
            results = [r for r in results if not r.get("dbt_is_deleted", False)]

        return results


def print_snapshot_table(table: list[dict], title: str, key_col: str,
                         show_cols: list = None):
    """格式化印出 snapshot 表。"""
    print(f"\n{'='*80}")
    print(f"  {title}")
    print(f"{'='*80}")

    if not table:
        print("  (empty)")
        return

    # Determine columns to show
    if show_cols:
        cols = show_cols
    else:
        meta = ["dbt_valid_from", "dbt_valid_to", "dbt_scd_id", "dbt_updated_at", "dbt_is_deleted"]
        data_cols = [c for c in table[0].keys() if c not in meta]
        cols = data_cols + [m for m in meta if m in table[0]]

    # Print header
    widths = {}
    for c in cols:
        max_w = len(c)
        for row in table:
            val = str(row.get(c, ""))
            if len(val) > max_w:
                max_w = len(val)
        widths[c] = min(max_w, 25)

    header = " | ".join(c.ljust(widths[c]) for c in cols)
    print(f"  {header}")
    print(f"  {'-' * len(header)}")

    # Sort by key + valid_from
    sorted_table = sorted(table, key=lambda r: (str(r.get(key_col, "")), str(r.get("dbt_valid_from", ""))))

    for row in sorted_table:
        vals = []
        for c in cols:
            v = row.get(c)
            if v is None:
                v = "NULL"
            vals.append(str(v).ljust(widths[c]))
        print(f"  {' | '.join(vals)}")


# ============================================================
# Part 2: Demo - Timestamp Strategy with Vendor AVL Tracking
# ============================================================

def demo_timestamp_strategy():
    print("\n" + "█" * 80)
    print("  DEMO 1: Timestamp 策略 — ODM 供應商 AVL 狀態追蹤")
    print("█" * 80)

    engine = MiniSnapshotEngine(
        unique_key="vendor_id",
        strategy="timestamp",
        updated_at="updated_at",
        hard_deletes="new_record",
        dbt_valid_to_current=None  # NULL for current
    )

    # === Run 1: Initial load ===
    t1 = datetime(2026, 1, 15, 8, 0, 0)
    source_r1 = [
        {"vendor_id": "V001", "vendor_name": "Delta Electronics", "avl_status": "Active",
         "lead_time_days": 14, "payment_terms": "NET30", "updated_at": "2026-01-10 10:00:00"},
        {"vendor_id": "V002", "vendor_name": "Foxconn PCB", "avl_status": "Active",
         "lead_time_days": 21, "payment_terms": "NET60", "updated_at": "2026-01-12 09:00:00"},
        {"vendor_id": "V003", "vendor_name": "Nanya DRAM", "avl_status": "Active",
         "lead_time_days": 30, "payment_terms": "NET45", "updated_at": "2026-01-05 14:00:00"},
    ]

    stats = engine.run(source_r1, t1)
    print(f"\n  Run 1 ({t1.strftime('%Y-%m-%d %H:%M')}): Initial load")
    print(f"  Stats: {stats}")
    print_snapshot_table(engine.snapshot_table, "Snapshot after Run 1", "vendor_id",
                         ["vendor_id", "vendor_name", "avl_status", "lead_time_days",
                          "dbt_valid_from", "dbt_valid_to", "dbt_is_deleted"])

    # === Run 2: V001 lead time changed, V003 goes Probation ===
    t2 = datetime(2026, 2, 1, 8, 0, 0)
    source_r2 = [
        {"vendor_id": "V001", "vendor_name": "Delta Electronics", "avl_status": "Active",
         "lead_time_days": 10, "payment_terms": "NET30", "updated_at": "2026-01-28 16:00:00"},
        {"vendor_id": "V002", "vendor_name": "Foxconn PCB", "avl_status": "Active",
         "lead_time_days": 21, "payment_terms": "NET60", "updated_at": "2026-01-12 09:00:00"},
        {"vendor_id": "V003", "vendor_name": "Nanya DRAM", "avl_status": "Probation",
         "lead_time_days": 30, "payment_terms": "NET45", "updated_at": "2026-01-30 11:00:00"},
    ]

    stats = engine.run(source_r2, t2)
    print(f"\n  Run 2 ({t2.strftime('%Y-%m-%d %H:%M')}): V001 lead time 14→10, V003 Active→Probation")
    print(f"  Stats: {stats}")
    print_snapshot_table(engine.snapshot_table, "Snapshot after Run 2", "vendor_id",
                         ["vendor_id", "vendor_name", "avl_status", "lead_time_days",
                          "dbt_valid_from", "dbt_valid_to", "dbt_is_deleted"])

    # === Run 3: V003 deleted from source (hard delete) ===
    t3 = datetime(2026, 3, 1, 8, 0, 0)
    source_r3 = [
        {"vendor_id": "V001", "vendor_name": "Delta Electronics", "avl_status": "Active",
         "lead_time_days": 10, "payment_terms": "NET30", "updated_at": "2026-01-28 16:00:00"},
        {"vendor_id": "V002", "vendor_name": "Foxconn PCB", "avl_status": "Active",
         "lead_time_days": 21, "payment_terms": "NET60", "updated_at": "2026-01-12 09:00:00"},
        # V003 removed from source!
    ]

    stats = engine.run(source_r3, t3)
    print(f"\n  Run 3 ({t3.strftime('%Y-%m-%d %H:%M')}): V003 deleted from source!")
    print(f"  Stats: {stats}")
    print_snapshot_table(engine.snapshot_table, "Snapshot after Run 3 (hard delete tracked)", "vendor_id",
                         ["vendor_id", "vendor_name", "avl_status",
                          "dbt_valid_from", "dbt_valid_to", "dbt_is_deleted"])

    # === Point-in-Time Query ===
    print(f"\n{'='*80}")
    print(f"  Point-in-Time Query: 2026-01-20（V003 還是 Active 的時候）")
    print(f"{'='*80}")
    pit = engine.point_in_time_query("2026-01-20 00:00:00")
    for r in pit:
        print(f"  {r['vendor_id']}: {r['vendor_name']} | AVL={r['avl_status']} | LT={r['lead_time_days']}d")

    print(f"\n  Total snapshot rows: {len(engine.snapshot_table)}")
    print(f"  Cumulative stats: {engine.stats}")


# ============================================================
# Part 3: Demo - Check Strategy with PO Status Tracking
# ============================================================

def demo_check_strategy():
    print("\n\n" + "█" * 80)
    print("  DEMO 2: Check 策略 — PO 狀態追蹤（Source 沒有 updated_at）")
    print("█" * 80)

    engine = MiniSnapshotEngine(
        unique_key="po_number",
        strategy="check",
        check_cols=["po_status", "received_qty"],
        hard_deletes="ignore"
    )

    t1 = datetime(2026, 6, 1, 7, 0, 0)
    source_r1 = [
        {"po_number": "PO-2026-001", "vendor": "V001", "po_status": "Created",
         "total_qty": 500, "received_qty": 0, "amount": 75000},
        {"po_number": "PO-2026-002", "vendor": "V002", "po_status": "Approved",
         "total_qty": 1000, "received_qty": 0, "amount": 120000},
    ]

    stats = engine.run(source_r1, t1)
    print(f"\n  Run 1: Initial → {stats}")

    t2 = datetime(2026, 6, 5, 7, 0, 0)
    source_r2 = [
        {"po_number": "PO-2026-001", "vendor": "V001", "po_status": "Approved",
         "total_qty": 500, "received_qty": 0, "amount": 75000},
        {"po_number": "PO-2026-002", "vendor": "V002", "po_status": "Approved",
         "total_qty": 1000, "received_qty": 300, "amount": 120000},
    ]

    stats = engine.run(source_r2, t2)
    print(f"  Run 2: PO-001 Created→Approved, PO-002 received 300 → {stats}")

    t3 = datetime(2026, 6, 10, 7, 0, 0)
    source_r3 = [
        {"po_number": "PO-2026-001", "vendor": "V001", "po_status": "Partially Received",
         "total_qty": 500, "received_qty": 200, "amount": 75000},
        {"po_number": "PO-2026-002", "vendor": "V002", "po_status": "Fully Received",
         "total_qty": 1000, "received_qty": 1000, "amount": 120000},
    ]

    stats = engine.run(source_r3, t3)
    print(f"  Run 3: PO-001 partial rcv 200, PO-002 fully received → {stats}")

    print_snapshot_table(engine.snapshot_table, "PO Snapshot (Check Strategy)", "po_number",
                         ["po_number", "po_status", "received_qty",
                          "dbt_valid_from", "dbt_valid_to"])

    # Show PO lifecycle analysis
    print(f"\n  --- PO Lifecycle Analysis ---")
    for po in ["PO-2026-001", "PO-2026-002"]:
        versions = sorted(
            [r for r in engine.snapshot_table if r["po_number"] == po],
            key=lambda r: r["dbt_valid_from"]
        )
        print(f"\n  {po}:")
        for v in versions:
            duration = ""
            if v["dbt_valid_to"] and v["dbt_valid_to"] is not None:
                d1 = datetime.strptime(v["dbt_valid_from"], "%Y-%m-%d %H:%M:%S")
                d2 = datetime.strptime(v["dbt_valid_to"], "%Y-%m-%d %H:%M:%S")
                duration = f" ({(d2-d1).days} days)"
            else:
                duration = " (current)"
            print(f"    {v['po_status']:>20s} | rcv={v['received_qty']:>5} | "
                  f"{v['dbt_valid_from']} → {v['dbt_valid_to'] or 'NULL'}{duration}")


# ============================================================
# Part 4: Demo - Change Compression Problem
# ============================================================

def demo_change_compression():
    print("\n\n" + "█" * 80)
    print("  DEMO 3: 中間變更壓縮問題（Snapshot 的固有限制）")
    print("█" * 80)

    print("""
  場景：PO 在兩次 snapshot 之間經歷了多次狀態變更
  
  實際變更序列（source 裡不會保留）：
    06-01 07:00  Created
    06-01 10:00  Approved     ← 兩小時後就批准了
    06-02 14:00  On Hold      ← 客戶要求暫停
    06-03 09:00  Approved     ← 恢復
    06-04 16:00  Shipped      ← 出貨
  
  Snapshot 每週跑一次（06-01 和 06-08）：
    06-01: 看到 Created
    06-08: 看到 Shipped
    
  結論：中間的 Approved → On Hold → Approved → Shipped 全部丟失！
  Snapshot 只能告訴你「Created → Shipped」，中間經歷了什麼不知道。
  """)

    engine = MiniSnapshotEngine(
        unique_key="po_number", strategy="timestamp",
        updated_at="updated_at", hard_deletes="ignore"
    )

    # Weekly Run 1
    source_w1 = [
        {"po_number": "PO-RUSH-001", "po_status": "Created",
         "updated_at": "2026-06-01 07:00:00"},
    ]
    engine.run(source_w1, datetime(2026, 6, 1, 8, 0, 0))

    # Weekly Run 2 (a week later - status jumped to Shipped)
    source_w2 = [
        {"po_number": "PO-RUSH-001", "po_status": "Shipped",
         "updated_at": "2026-06-04 16:00:00"},
    ]
    engine.run(source_w2, datetime(2026, 6, 8, 8, 0, 0))

    print_snapshot_table(engine.snapshot_table, "Weekly Snapshot (中間變更遺失)", "po_number",
                         ["po_number", "po_status", "updated_at",
                          "dbt_valid_from", "dbt_valid_to"])

    print("""
  ⚠️ 解決方案（當需要完整事件歷史時）：
  
  1. 提高 Snapshot 頻率（每小時 → 成本高）
  2. 使用 Delta Lake CDF（行級 CDC，不遺漏）
  3. 使用 Debezium + Kafka（Source 級 CDC）
  4. Source 系統本身保留事件日誌（最佳解）
  
  ✅ 結論：Snapshot 適合「不需要秒級歷史」的維度追蹤，
     如供應商資訊、物料主數據、客戶資訊等變動較慢的維度。
  """)


# ============================================================
# Part 5: Demo - dbt_valid_to_current Impact on JOINs
# ============================================================

def demo_valid_to_current():
    print("\n" + "█" * 80)
    print("  DEMO 4: dbt_valid_to_current 對 JOIN 的影響")
    print("█" * 80)

    # Engine with NULL as current marker
    engine_null = MiniSnapshotEngine(
        unique_key="vendor_id", strategy="timestamp",
        updated_at="updated_at", dbt_valid_to_current=None
    )

    # Engine with 9999-12-31 as current marker
    engine_date = MiniSnapshotEngine(
        unique_key="vendor_id", strategy="timestamp",
        updated_at="updated_at", dbt_valid_to_current="9999-12-31"
    )

    source = [
        {"vendor_id": "V001", "vendor_name": "Delta", "avl_status": "Active",
         "updated_at": "2026-01-01 00:00:00"},
    ]

    for eng in [engine_null, engine_date]:
        eng.run(source, datetime(2026, 1, 1, 8, 0, 0))

    print(f"""
  NULL 模式：
    dbt_valid_to = {engine_null.snapshot_table[0]['dbt_valid_to']}
    
    JOIN 語法（需要 COALESCE）：
      WHERE event_date >= snap.dbt_valid_from
        AND event_date < COALESCE(snap.dbt_valid_to, '9999-12-31')
    
  9999-12-31 模式：
    dbt_valid_to = {engine_date.snapshot_table[0]['dbt_valid_to']}
    
    JOIN 語法（更簡潔）：
      WHERE event_date BETWEEN snap.dbt_valid_from AND snap.dbt_valid_to
  
  ✅ 建議：新 snapshot 一律設定 dbt_valid_to_current = "to_date('9999-12-31')"
     避免 NULL handling 的複雜度，且 BETWEEN 查詢可利用 index/partition pruning。
  """)


# ============================================================
# Part 6: Snapshot vs Manual SCD2 Comparison
# ============================================================

def demo_snapshot_vs_manual_scd2():
    print("\n" + "█" * 80)
    print("  DEMO 5: dbt Snapshot vs 手寫 SCD Type 2 (Delta MERGE)")
    print("█" * 80)

    print("""
  ┌─────────────────────────┬──────────────────────┬──────────────────────┐
  │ 面向                    │ dbt Snapshot          │ 手寫 MERGE SCD2     │
  ├─────────────────────────┼──────────────────────┼──────────────────────┤
  │ 開發時間                │ ✅ 10 行 YAML         │ ❌ 50-100 行 SQL     │
  │ 變更偵測                │ ✅ 內建 2 策略        │ ❌ 自己寫比對邏輯    │
  │ Hard Delete 追蹤        │ ✅ 一行配置           │ ❌ 自己 LEFT JOIN     │
  │ Surrogate Key           │ ✅ 自動 md5           │ ❌ 自己設計 hash      │
  │ Schema Evolution        │ ✅ timestamp 自動適應 │ ⚠️ mergeSchema       │
  │ Meta Column 自定義      │ ✅ v1.9+ 支援         │ ✅ 完全控制           │
  │ 定制化程度              │ ⚠️ 兩種策略+custom    │ ✅ 無限               │
  │ 執行效率（大表）        │ ⚠️ 全表 SELECT        │ ✅ 可用 CDF 增量      │
  │ 適合場景                │ 維度追蹤（慢變）      │ 大量 fact + CDC       │
  │ 除錯難度                │ ⚠️ macro 封裝深       │ ✅ SQL 透明           │
  └─────────────────────────┴──────────────────────┴──────────────────────┘
  
  🎯 SA 選型建議：
  
  1. 維度表（供應商/物料/客戶）→ dbt Snapshot（省力、夠用）
  2. 大型 Fact 表 + CDC         → Delta MERGE 手寫（效能可控）
  3. 需要秒級歷史              → Debezium + Kafka + Streaming
  4. 混合策略                  → Staging 用 Snapshot，Mart 用 Incremental
  """)


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":
    print("=" * 80)
    print("  dbt Snapshots SCD Type 2 Practice")
    print("  Scholar Day 61 — ODM 供應鏈場景")
    print("=" * 80)

    demo_timestamp_strategy()
    demo_check_strategy()
    demo_change_compression()
    demo_valid_to_current()
    demo_snapshot_vs_manual_scd2()

    print("\n" + "=" * 80)
    print("  ✅ All demos completed!")
    print("=" * 80)
