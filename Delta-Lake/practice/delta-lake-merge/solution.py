"""
Delta Lake MERGE 操作練習
場景：ODM 伺服器廠採購主數據 CDC 同步

依賴：無（dependency-free，純 Python 模擬）
學習日期：2026-06-02
"""

from datetime import date, datetime
from typing import Optional
from copy import deepcopy
import json

# ─────────────────────────────────────────────
# 1. 基礎工具：MiniDeltaTable
# ─────────────────────────────────────────────

class MiniDeltaTable:
    """
    模擬 Delta Lake Table 的核心特性：
    - Transaction Log（版本控制）
    - Copy-on-Write（只重寫受影響的 files）
    - MERGE 操作（upsert / conditional / dedup / SCD2）
    """
    def __init__(self, name: str, pk: str):
        self.name = name
        self.pk = pk
        self.data: dict = {}          # {pk_value: row_dict}
        self.log: list = []           # transaction log
        self.version: int = 0

    def _commit(self, action: str, rows_added: int, rows_updated: int, rows_deleted: int):
        entry = {
            "version": self.version,
            "timestamp": datetime.now().isoformat(),
            "action": action,
            "stats": {
                "added": rows_added,
                "updated": rows_updated,
                "deleted": rows_deleted
            }
        }
        self.log.append(entry)
        self.version += 1
        return entry

    def count(self) -> int:
        return len(self.data)

    def scan(self, filter_fn=None) -> list:
        rows = list(self.data.values())
        if filter_fn:
            rows = [r for r in rows if filter_fn(r)]
        return rows

    def show(self, limit=10, title=None):
        rows = list(self.data.values())[:limit]
        if title:
            print(f"\n{'='*60}")
            print(f"  {title}")
            print(f"{'='*60}")
        if not rows:
            print("  (empty table)")
            return
        headers = list(rows[0].keys())
        col_widths = {h: max(len(h), max(len(str(r.get(h, ''))) for r in rows)) for h in headers}
        header_line = " | ".join(h.ljust(col_widths[h]) for h in headers)
        sep = "-+-".join("-" * col_widths[h] for h in headers)
        print(f"  {header_line}")
        print(f"  {sep}")
        for row in rows:
            line = " | ".join(str(row.get(h, '')).ljust(col_widths[h]) for h in headers)
            print(f"  {line}")
        print(f"  ({self.count()} rows total)")

    # ─── MERGE 核心 ───

    def merge(self, source: list, on: str,
              when_matched_update=None,
              when_matched_condition=None,
              when_not_matched_insert=None,
              when_not_matched_by_source=None,
              label="MERGE"):
        """
        模擬 Delta Lake MERGE INTO

        Args:
            source: 來源 rows（list of dict）
            on: JOIN key（欄位名）
            when_matched_update: 匹配時更新的欄位映射 {target_col: fn(target, source) -> value}
            when_matched_condition: 只有滿足此條件才 UPDATE（fn(target, source) -> bool）
            when_not_matched_insert: 未匹配時插入的 row builder（fn(source) -> dict）
            when_not_matched_by_source: target 有但 source 沒有時的處理（fn(target) -> dict or None）
        """
        added = updated = deleted = 0
        source_keys = {s[on] for s in source}

        # 去重 source（每個 key 只取第一筆）
        seen = set()
        deduped_source = []
        for s in source:
            k = s[on]
            if k not in seen:
                deduped_source.append(s)
                seen.add(k)

        # WHEN MATCHED
        for s in deduped_source:
            key = s[on]
            if key in self.data:
                t = self.data[key]
                if when_matched_update:
                    should_update = True
                    if when_matched_condition:
                        should_update = when_matched_condition(t, s)
                    if should_update:
                        for col, fn in when_matched_update.items():
                            self.data[key][col] = fn(t, s)
                        updated += 1

        # WHEN NOT MATCHED (source has, target doesn't)
        for s in deduped_source:
            key = s[on]
            if key not in self.data and when_not_matched_insert:
                new_row = when_not_matched_insert(s)
                if new_row:
                    self.data[key] = new_row
                    added += 1

        # WHEN NOT MATCHED BY SOURCE (target has, source doesn't)
        if when_not_matched_by_source:
            target_only_keys = set(self.data.keys()) - source_keys
            for key in target_only_keys:
                result = when_not_matched_by_source(self.data[key])
                if result is None:
                    del self.data[key]
                    deleted += 1
                else:
                    self.data[key] = result
                    updated += 1

        commit = self._commit(label, added, updated, deleted)
        print(f"\n  📝 {label}: +{added} INSERT, ~{updated} UPDATE, -{deleted} DELETE  (v{commit['version']})")
        return commit


# ─────────────────────────────────────────────
# 2. 模式一：Basic Upsert（供應商主數據每日同步）
# ─────────────────────────────────────────────

def demo_basic_upsert():
    print("\n" + "="*60)
    print("  模式 1：Basic Upsert — 供應商主數據每日同步")
    print("="*60)

    # 初始 target（Silver 層）
    dim_vendor = MiniDeltaTable("silver.dim_vendor", "vendor_id")
    initial_data = [
        {"vendor_id": "V001", "name": "Alpha Semi", "city": "Taipei", "lead_time": 14, "avl_status": "active"},
        {"vendor_id": "V002", "name": "Beta Memory",  "city": "Seoul",  "lead_time": 21, "avl_status": "active"},
        {"vendor_id": "V003", "name": "Gamma PSU",    "city": "Dongguan","lead_time": 10, "avl_status": "active"},
    ]
    for row in initial_data:
        dim_vendor.data[row["vendor_id"]] = row
    dim_vendor._commit("INITIAL_LOAD", 3, 0, 0)
    dim_vendor.show(title="初始狀態（Silver dim_vendor）")

    # 每日 Bronze dump（source）
    bronze_dump = [
        {"vendor_id": "V001", "name": "Alpha Semi",    "city": "Hsinchu", "lead_time": 12, "avl_status": "active"},  # city & LT 改了
        {"vendor_id": "V002", "name": "Beta Memory",   "city": "Seoul",   "lead_time": 21, "avl_status": "active"},  # 沒變
        {"vendor_id": "V004", "name": "Delta NIC",     "city": "Shanghai","lead_time": 18, "avl_status": "active"},  # 新供應商
    ]

    print("\n  [Source] 今日 Bronze dump：")
    for r in bronze_dump:
        print(f"    {r}")

    dim_vendor.merge(
        source=bronze_dump,
        on="vendor_id",
        when_matched_update={
            "city":       lambda t, s: s["city"],
            "lead_time":  lambda t, s: s["lead_time"],
            "avl_status": lambda t, s: s["avl_status"],
        },
        when_not_matched_insert=lambda s: dict(s),
        label="MERGE daily_vendor_dump"
    )
    dim_vendor.show(title="MERGE 後（Basic Upsert）")
    print("  ⚠️  問題：V002 沒有任何變更，但依然被 'updated' 計數！")
    print("         → 在真實 Delta Lake 中，這會觸發不必要的 CoW 重寫！")


# ─────────────────────────────────────────────
# 3. 模式二：Conditional Update（避免 no-op CoW）
# ─────────────────────────────────────────────

def demo_conditional_update():
    print("\n" + "="*60)
    print("  模式 2：Conditional Update — 只在有實質變更時才寫")
    print("="*60)

    dim_vendor = MiniDeltaTable("silver.dim_vendor", "vendor_id")
    initial_data = [
        {"vendor_id": "V001", "name": "Alpha Semi", "city": "Hsinchu", "lead_time": 12, "avl_status": "active"},
        {"vendor_id": "V002", "name": "Beta Memory", "city": "Seoul",  "lead_time": 21, "avl_status": "active"},
        {"vendor_id": "V003", "name": "Gamma PSU",   "city": "Dongguan","lead_time": 10, "avl_status": "active"},
    ]
    for row in initial_data:
        dim_vendor.data[row["vendor_id"]] = row
    dim_vendor._commit("INITIAL_LOAD", 3, 0, 0)

    bronze_dump = [
        {"vendor_id": "V001", "name": "Alpha Semi", "city": "Hsinchu", "lead_time": 12, "avl_status": "conditional"},  # 只有 avl_status 改了
        {"vendor_id": "V002", "name": "Beta Memory", "city": "Seoul",  "lead_time": 21, "avl_status": "active"},       # 完全沒變
        {"vendor_id": "V004", "name": "Delta NIC",   "city": "Shanghai","lead_time": 18, "avl_status": "active"},      # 新供應商
    ]

    def has_real_change(target, source):
        """只有當 city / lead_time / avl_status 其中之一有真實變更才更新"""
        return (
            target["city"]       != source["city"] or
            target["lead_time"]  != source["lead_time"] or
            target["avl_status"] != source["avl_status"]
        )

    dim_vendor.merge(
        source=bronze_dump,
        on="vendor_id",
        when_matched_update={
            "city":       lambda t, s: s["city"],
            "lead_time":  lambda t, s: s["lead_time"],
            "avl_status": lambda t, s: s["avl_status"],
        },
        when_matched_condition=has_real_change,
        when_not_matched_insert=lambda s: dict(s),
        label="MERGE conditional_update"
    )
    dim_vendor.show(title="Conditional Update 結果")
    print("  ✅ V002（無變更）不計入 UPDATE → 在真實 Delta Lake 中不觸發 CoW 重寫！")
    print("  ✅ V001 只有 avl_status 改了 → 只更新這一行")


# ─────────────────────────────────────────────
# 4. 模式三：Insert-Only Dedup（Kafka CDC 去重）
# ─────────────────────────────────────────────

def demo_insert_only_dedup():
    print("\n" + "="*60)
    print("  模式 3：Insert-Only Dedup — Kafka CDC 事件去重插入")
    print("="*60)

    event_logs = MiniDeltaTable("silver.po_events", "event_id")

    # 模擬已存在的事件
    existing = [
        {"event_id": "E001", "po_number": "PO-2026-001", "event_type": "PO_CREATED", "vendor_id": "V001"},
        {"event_id": "E002", "po_number": "PO-2026-001", "event_type": "PO_CONFIRMED", "vendor_id": "V001"},
    ]
    for row in existing:
        event_logs.data[row["event_id"]] = row
    event_logs._commit("INITIAL_LOAD", 2, 0, 0)

    # Kafka 推送（包含重複事件，E001 重發了）
    kafka_batch = [
        {"event_id": "E001", "po_number": "PO-2026-001", "event_type": "PO_CREATED",   "vendor_id": "V001"},  # 重複！
        {"event_id": "E003", "po_number": "PO-2026-001", "event_type": "GR_COMPLETED",  "vendor_id": "V001"},  # 新
        {"event_id": "E004", "po_number": "PO-2026-002", "event_type": "PO_CREATED",    "vendor_id": "V002"},  # 新
    ]
    print(f"\n  Kafka batch 中有 {len(kafka_batch)} 筆事件（含 1 筆重複）")

    event_logs.merge(
        source=kafka_batch,
        on="event_id",
        # ❌ WHEN MATCHED：什麼都不做（不更新已存在的 event）
        # ✅ WHEN NOT MATCHED：只插入新的
        when_not_matched_insert=lambda s: dict(s),
        label="MERGE insert_only_dedup"
    )
    event_logs.show(title="去重後 Event Log")
    print(f"  ✅ E001 重複事件被忽略，最終 {event_logs.count()} 筆（應為 4 筆）")


# ─────────────────────────────────────────────
# 5. 模式四：SCD Type 2（供應商 AVL 狀態歷史追蹤）
# ─────────────────────────────────────────────

class MiniDeltaTableSCD2:
    """SCD Type 2 專用版本，支援多 key（vendor_id + is_current）"""
    def __init__(self, name: str):
        self.name = name
        self.rows: list = []        # 所有歷史記錄（包含已關閉的）
        self.version: int = 0
        self.log: list = []

    def _commit(self, action: str, added: int, updated: int):
        entry = {"version": self.version, "action": action, "added": added, "updated": updated}
        self.log.append(entry)
        self.version += 1
        print(f"  📝 {action}: +{added} INSERT, ~{updated} UPDATE  (v{self.version-1})")

    def current_rows(self) -> list:
        return [r for r in self.rows if r["is_current"]]

    def get_current(self, vendor_id: str) -> Optional[dict]:
        found = [r for r in self.rows if r["vendor_id"] == vendor_id and r["is_current"]]
        return found[0] if found else None

    def scd2_merge(self, source: list, check_cols: list):
        """
        SCD Type 2 MERGE（UNION 展開法）
        - MATCHED + 有變更 → 關閉舊行（is_current=False）+ 插入新行
        - MATCHED + 無變更 → 不動
        - NOT MATCHED → 插入新行
        """
        added = updated = 0
        today = date.today().isoformat()

        for s in source:
            vendor_id = s["vendor_id"]
            current = self.get_current(vendor_id)

            if current is None:
                # 全新供應商
                new_row = {**s, "effective_from": today, "effective_to": "9999-12-31", "is_current": True}
                self.rows.append(new_row)
                added += 1

            else:
                # 檢查是否有變更
                has_change = any(current.get(col) != s.get(col) for col in check_cols)
                if has_change:
                    # ① 關閉舊行
                    current["effective_to"] = today
                    current["is_current"] = False
                    updated += 1
                    # ② 插入新行
                    new_row = {**s, "effective_from": today, "effective_to": "9999-12-31", "is_current": True}
                    self.rows.append(new_row)
                    added += 1

        self._commit("SCD2_MERGE", added, updated)

    def show_history(self, vendor_id: str):
        history = [r for r in self.rows if r["vendor_id"] == vendor_id]
        print(f"\n  📋 {vendor_id} 完整歷史（{len(history)} 筆）：")
        for r in history:
            status = "✅ 現行" if r["is_current"] else "⬜ 關閉"
            print(f"    {status} | {r['effective_from']} ~ {r['effective_to']} | avl={r['avl_status']} | LT={r['lead_time']}d")


def demo_scd_type_2():
    print("\n" + "="*60)
    print("  模式 4：SCD Type 2 — 供應商 AVL 狀態歷史追蹤")
    print("="*60)

    vendor_history = MiniDeltaTableSCD2("silver.dim_vendor_scd2")

    # Day 1：初始載入
    print("\n  [Day 1] 初始載入")
    initial = [
        {"vendor_id": "V001", "name": "Alpha Semi", "city": "Hsinchu",  "lead_time": 12, "avl_status": "active"},
        {"vendor_id": "V002", "name": "Beta Memory", "city": "Seoul",   "lead_time": 21, "avl_status": "active"},
    ]
    vendor_history.scd2_merge(initial, check_cols=["city", "lead_time", "avl_status"])

    # Day 2：Alpha Semi 被降為 conditional（品質問題），Beta 沒變，新增 V003
    print("\n  [Day 30] V001 降為 conditional，V003 新供應商入 AVL")
    update1 = [
        {"vendor_id": "V001", "name": "Alpha Semi", "city": "Hsinchu",  "lead_time": 12, "avl_status": "conditional"},  # 有變更
        {"vendor_id": "V002", "name": "Beta Memory", "city": "Seoul",   "lead_time": 21, "avl_status": "active"},       # 無變更
        {"vendor_id": "V003", "name": "Gamma NIC",   "city": "Shanghai","lead_time": 18, "avl_status": "active"},       # 新
    ]
    vendor_history.scd2_merge(update1, check_cols=["city", "lead_time", "avl_status"])

    # Day 3：Alpha Semi 改善後恢復 active
    print("\n  [Day 60] V001 恢復 active")
    update2 = [
        {"vendor_id": "V001", "name": "Alpha Semi", "city": "Hsinchu",  "lead_time": 10, "avl_status": "active"},  # LT 也改了
        {"vendor_id": "V002", "name": "Beta Memory", "city": "Seoul",   "lead_time": 21, "avl_status": "active"},
        {"vendor_id": "V003", "name": "Gamma NIC",   "city": "Shanghai","lead_time": 18, "avl_status": "active"},
    ]
    vendor_history.scd2_merge(update2, check_cols=["city", "lead_time", "avl_status"])

    print(f"\n  📊 總記錄數：{len(vendor_history.rows)} 筆（包含歷史版本）")
    print(f"  📊 現行版本：{len(vendor_history.current_rows())} 筆")

    # 查詢 V001 的完整歷史
    vendor_history.show_history("V001")

    print("\n  ✅ SCD Type 2 核心價值：")
    print("     - 可以查詢「去年這時候 V001 的 AVL 狀態是什麼？」")
    print("     - 可以追蹤供應商品質事件的歷史 → 供應商績效報告")
    print("     - 對應 dbt snapshot（check strategy on check_cols）")


# ─────────────────────────────────────────────
# 6. Bonus：Multi-Table Transaction 概念（Catalog Commits）
# ─────────────────────────────────────────────

class MiniTransaction:
    """
    模擬 Databricks Catalog Commits 的 Multi-Table Transaction

    傳統 Delta：每張表獨立 commit → 中間失敗 = 部分更新
    Catalog Commits：所有表的 MERGE 在同一 transaction boundary → 全成或全失敗
    """
    def __init__(self, tables: dict):
        self.tables = tables      # {name: MiniDeltaTable}
        self.operations = []      # 待執行操作
        self._snapshots = {}      # 事務開始前的快照

    def __enter__(self):
        # 快照所有表的當前狀態
        for name, table in self.tables.items():
            self._snapshots[name] = deepcopy(table.data)
        return self

    def add_merge(self, table_name: str, **merge_kwargs):
        self.operations.append((table_name, merge_kwargs))

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            # 成功：執行所有操作
            print("\n  🔐 [TRANSACTION START]")
            for table_name, kwargs in self.operations:
                table = self.tables[table_name]
                table.merge(**kwargs, label=f"TXN:{table_name}")
            print("  🔐 [TRANSACTION COMMIT] ✅ 所有 MERGE 成功，統一提交")
        else:
            # 失敗：回滾所有表
            print(f"\n  ❌ Transaction ROLLBACK：{exc_val}")
            for name, table in self.tables.items():
                table.data = self._snapshots[name]
            print("  🔄 所有表已回滾到事務開始前的狀態")
        return True  # 不重新拋出異常


def demo_multi_table_transaction():
    print("\n" + "="*60)
    print("  Bonus：Multi-Table Transaction（Catalog Commits 概念）")
    print("="*60)

    # 兩張表：供應商 + 物料（必須一起更新）
    dim_vendor  = MiniDeltaTable("silver.dim_vendor",   "vendor_id")
    dim_material = MiniDeltaTable("silver.dim_material", "material_id")

    initial_vendors = [
        {"vendor_id": "V001", "name": "Alpha Semi", "lead_time": 12, "avl_status": "active"},
    ]
    initial_materials = [
        {"material_id": "M001", "desc": "GPU A100", "primary_vendor": "V001", "std_lead_time": 12},
    ]
    for r in initial_vendors:  dim_vendor.data[r["vendor_id"]] = r
    for r in initial_materials: dim_material.data[r["material_id"]] = r
    dim_vendor._commit("INITIAL_LOAD", 1, 0, 0)
    dim_material._commit("INITIAL_LOAD", 1, 0, 0)

    print(f"\n  Before: dim_vendor V001 lead_time={dim_vendor.data['V001']['lead_time']}")
    print(f"  Before: dim_material M001 std_lead_time={dim_material.data['M001']['std_lead_time']}")

    # 場景：Alpha Semi 的 Lead Time 從 12 → 18 天（GPU 缺貨）
    # 必須同時更新 dim_vendor + dim_material，否則兩張表不一致
    vendor_cdc    = [{"vendor_id":   "V001", "name": "Alpha Semi", "lead_time": 18, "avl_status": "conditional"}]
    material_cdc  = [{"material_id": "M001", "desc": "GPU A100",   "primary_vendor": "V001", "std_lead_time": 18}]

    print("\n  [場景 1] 成功的 Multi-Table Transaction")
    with MiniTransaction({"dim_vendor": dim_vendor, "dim_material": dim_material}) as txn:
        txn.add_merge(
            "dim_vendor",
            source=vendor_cdc,
            on="vendor_id",
            when_matched_update={
                "lead_time":  lambda t, s: s["lead_time"],
                "avl_status": lambda t, s: s["avl_status"],
            },
        )
        txn.add_merge(
            "dim_material",
            source=material_cdc,
            on="material_id",
            when_matched_update={
                "std_lead_time": lambda t, s: s["std_lead_time"],
            },
        )

    print(f"\n  After: dim_vendor V001 lead_time={dim_vendor.data['V001']['lead_time']}")
    print(f"  After: dim_material M001 std_lead_time={dim_material.data['M001']['std_lead_time']}")
    print("  ✅ 兩張表的 lead_time 同步更新（一致性維持）")

    # 場景 2：模擬 ROLLBACK
    print("\n  [場景 2] 模擬錯誤 → ROLLBACK")
    dim_vendor2  = MiniDeltaTable("silver.dim_vendor2",   "vendor_id")
    dim_material2 = MiniDeltaTable("silver.dim_material2", "material_id")
    dim_vendor2.data  = deepcopy(dim_vendor.data)
    dim_material2.data = deepcopy(dim_material.data)

    try:
        with MiniTransaction({"dim_vendor": dim_vendor2, "dim_material": dim_material2}) as txn:
            txn.add_merge(
                "dim_vendor",
                source=[{"vendor_id": "V001", "name": "Alpha Semi", "lead_time": 99, "avl_status": "suspended"}],
                on="vendor_id",
                when_matched_update={
                    "lead_time":  lambda t, s: s["lead_time"],
                    "avl_status": lambda t, s: s["avl_status"],
                },
            )
            raise RuntimeError("downstream validation failed!")  # 模擬中途失敗
    except Exception:
        pass

    print(f"\n  After ROLLBACK: dim_vendor V001 lead_time={dim_vendor2.data['V001']['lead_time']}")
    print(f"  After ROLLBACK: dim_vendor V001 avl_status={dim_vendor2.data['V001']['avl_status']}")
    print("  ✅ 回滾成功，lead_time 和 avl_status 維持舊值（不是 99/suspended）")


# ─────────────────────────────────────────────
# 7. Main
# ─────────────────────────────────────────────

def main():
    print("\n" + "🔷"*30)
    print("  Delta Lake MERGE 操作練習")
    print("  場景：ODM 伺服器廠採購主數據 CDC 同步")
    print("🔷"*30)

    demo_basic_upsert()
    demo_conditional_update()
    demo_insert_only_dedup()
    demo_scd_type_2()
    demo_multi_table_transaction()

    print("\n" + "="*60)
    print("  📊 學習總結")
    print("="*60)
    print("""
  1. Basic Upsert      — WHEN MATCHED UPDATE + WHEN NOT MATCHED INSERT
  2. Conditional Update — 加 WHEN MATCHED AND (...changed...) → 避免 no-op CoW
  3. Insert-Only Dedup — 只用 WHEN NOT MATCHED → Kafka CDC 去重標準做法
  4. SCD Type 2        — UNION 展開法：close old row + insert new row
  5. Catalog Commits   — UC 協調 multi-table transaction → 跨表原子性

  ⚠️  最重要的 Production 注意事項：
  - source 必須先 dropDuplicates（MERGE 不處理 source 內部重複）
  - WHEN NOT MATCHED BY SOURCE 不加條件 = 全表重寫！
  - 高頻小批次 MERGE → 定期 OPTIMIZE 防止小檔案問題
  - ON 條件對應 Liquid Clustering key → 加速 data skipping

  🏭 ODM 供應鏈應用：
  - dim_vendor / dim_material SCD Type 2 → 供應商/物料歷史追蹤
  - Kafka CDC 事件 → Insert-Only Dedup 保證冪等性
  - ECO 工程變更 → Multi-Table Transaction（Catalog Commits）確保一致性
    """)


if __name__ == "__main__":
    main()
