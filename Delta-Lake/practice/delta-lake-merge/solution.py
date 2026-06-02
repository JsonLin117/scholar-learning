"""
Delta Lake MERGE 操作模擬器
===========================
dependency-free：不需要 PySpark 或 Delta Lake，純 Python 模擬 MERGE 五大模式。

場景：ODM 伺服器製造商的供應商主數據 CDC 同步
"""

from __future__ import annotations
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any
import hashlib
import json
import copy


# ============================================================
# Mini Delta Table（延續 time-travel 練習的簡化版）
# ============================================================

@dataclass
class MiniDeltaTable:
    """模擬 Delta Lake table，支援 MERGE、version tracking、CoW 計數"""
    name: str
    primary_key: str  # 用於 MERGE 的 ON 條件
    rows: list[dict] = field(default_factory=list)
    version: int = 0
    cow_rewrites: int = 0  # 追蹤 Copy-on-Write 重寫次數
    
    def snapshot(self) -> list[dict]:
        """返回當前快照（deep copy）"""
        return copy.deepcopy(self.rows)
    
    def _commit(self, new_rows: list[dict], metrics: dict):
        """提交新版本"""
        self.rows = new_rows
        self.version += 1
        return metrics


# ============================================================
# MiniMerge 引擎
# ============================================================

class MiniMerge:
    """
    模擬 Delta Lake MERGE INTO 的四個 clause：
    - WHEN MATCHED → UPDATE / DELETE
    - WHEN NOT MATCHED → INSERT
    - WHEN NOT MATCHED BY SOURCE → UPDATE / DELETE
    """
    
    def __init__(self, target: MiniDeltaTable, source: list[dict], on_key: str):
        self.target = target
        self.source = source
        self.on_key = on_key
        self._matched_update = None      # (condition_fn, set_fn)
        self._matched_delete = None      # condition_fn
        self._not_matched_insert = None  # values_fn
        self._not_matched_by_source_update = None  # (condition_fn, set_fn)
        self._not_matched_by_source_delete = None  # condition_fn
    
    def when_matched_update(self, condition=None, set_fn=None):
        """WHEN MATCHED [AND condition] THEN UPDATE SET ..."""
        self._matched_update = (condition or (lambda t, s: True), set_fn)
        return self
    
    def when_matched_update_all(self):
        """WHEN MATCHED THEN UPDATE SET *"""
        self._matched_update = (
            lambda t, s: True,
            lambda t, s: {**t, **s}  # source 覆蓋 target 所有欄位
        )
        return self
    
    def when_matched_delete(self, condition=None):
        """WHEN MATCHED [AND condition] THEN DELETE"""
        self._matched_delete = condition or (lambda t, s: True)
        return self
    
    def when_not_matched_insert(self, values_fn=None):
        """WHEN NOT MATCHED THEN INSERT ..."""
        self._not_matched_insert = values_fn or (lambda s: s)
        return self
    
    def when_not_matched_insert_all(self):
        """WHEN NOT MATCHED THEN INSERT *"""
        self._not_matched_insert = lambda s: s
        return self
    
    def when_not_matched_by_source_update(self, condition=None, set_fn=None):
        """WHEN NOT MATCHED BY SOURCE [AND condition] THEN UPDATE SET ..."""
        self._not_matched_by_source_update = (condition or (lambda t: True), set_fn)
        return self
    
    def when_not_matched_by_source_delete(self, condition=None):
        """WHEN NOT MATCHED BY SOURCE [AND condition] THEN DELETE"""
        self._not_matched_by_source_delete = condition or (lambda t: True)
        return self
    
    def execute(self) -> dict:
        """
        執行 MERGE，返回 metrics。
        模擬 Delta Lake 的 Copy-on-Write 語意。
        """
        # Step 0: 檢查 source 是否有重複 key
        source_keys = [s[self.on_key] for s in self.source]
        if len(source_keys) != len(set(source_keys)):
            dupes = [k for k in set(source_keys) if source_keys.count(k) > 1]
            raise ValueError(
                f"AnalysisException: Multiple source rows matched for key(s): {dupes}. "
                f"Preprocess source to eliminate duplicates before MERGE."
            )
        
        # 建立 source lookup
        source_map = {s[self.on_key]: s for s in self.source}
        target_keys = {r[self.on_key] for r in self.target.rows}
        
        new_rows = []
        metrics = {
            "rows_inserted": 0,
            "rows_updated": 0,
            "rows_deleted": 0,
            "rows_unchanged": 0,
            "cow_files_rewritten": 0,  # 被影響的行數（= 需要重寫的 file 中的行數）
        }
        
        # Step 1: 處理 target 中的每一行
        for t_row in self.target.rows:
            key = t_row[self.on_key]
            
            if key in source_map:
                # MATCHED
                s_row = source_map[key]
                
                # 檢查 MATCHED DELETE
                if self._matched_delete and self._matched_delete(t_row, s_row):
                    metrics["rows_deleted"] += 1
                    metrics["cow_files_rewritten"] += 1
                    continue  # 不加入 new_rows = 刪除
                
                # 檢查 MATCHED UPDATE
                if self._matched_update:
                    cond_fn, set_fn = self._matched_update
                    if cond_fn(t_row, s_row):
                        updated_row = set_fn(t_row, s_row)
                        new_rows.append(updated_row)
                        metrics["rows_updated"] += 1
                        metrics["cow_files_rewritten"] += 1
                    else:
                        new_rows.append(t_row)  # 條件不滿足，保持不變
                        metrics["rows_unchanged"] += 1
                else:
                    new_rows.append(t_row)
                    metrics["rows_unchanged"] += 1
            else:
                # NOT MATCHED BY SOURCE
                if self._not_matched_by_source_delete:
                    if self._not_matched_by_source_delete(t_row):
                        metrics["rows_deleted"] += 1
                        metrics["cow_files_rewritten"] += 1
                        continue
                    else:
                        new_rows.append(t_row)
                        metrics["rows_unchanged"] += 1
                elif self._not_matched_by_source_update:
                    cond_fn, set_fn = self._not_matched_by_source_update
                    if cond_fn(t_row):
                        updated_row = set_fn(t_row)
                        new_rows.append(updated_row)
                        metrics["rows_updated"] += 1
                        metrics["cow_files_rewritten"] += 1
                    else:
                        new_rows.append(t_row)
                        metrics["rows_unchanged"] += 1
                else:
                    new_rows.append(t_row)
                    metrics["rows_unchanged"] += 1
        
        # Step 2: 處理 NOT MATCHED（source 有但 target 沒有）
        if self._not_matched_insert:
            for s_row in self.source:
                key = s_row[self.on_key]
                if key not in target_keys:
                    new_row = self._not_matched_insert(s_row)
                    new_rows.append(new_row)
                    metrics["rows_inserted"] += 1
        
        # Step 3: 提交
        self.target.cow_rewrites += metrics["cow_files_rewritten"]
        self.target._commit(new_rows, metrics)
        
        return metrics


# ============================================================
# 測試場景
# ============================================================

def generate_vendors() -> list[dict]:
    """生成 ODM 供應商初始資料"""
    return [
        {"vendor_id": "V001", "name": "台積電", "city": "新竹", "lead_time": 14, "avl_status": "Active", "updated_at": "2026-01-01"},
        {"vendor_id": "V002", "name": "日月光", "city": "高雄", "lead_time": 10, "avl_status": "Active", "updated_at": "2026-01-01"},
        {"vendor_id": "V003", "name": "廣達", "city": "桃園", "lead_time": 7, "avl_status": "Active", "updated_at": "2026-01-01"},
        {"vendor_id": "V004", "name": "鴻準", "city": "深圳", "lead_time": 21, "avl_status": "Probation", "updated_at": "2026-01-01"},
        {"vendor_id": "V005", "name": "京東方", "city": "北京", "lead_time": 30, "avl_status": "Active", "updated_at": "2026-01-01"},
    ]


def print_table(title: str, rows: list[dict], cols: list[str] | None = None):
    """美化列印表格"""
    if not rows:
        print(f"\n📋 {title}: (empty)")
        return
    cols = cols or list(rows[0].keys())
    widths = {c: max(len(c), max(len(str(r.get(c, ""))) for r in rows)) for c in cols}
    header = " | ".join(c.ljust(widths[c]) for c in cols)
    print(f"\n📋 {title}")
    print("─" * len(header))
    print(header)
    print("─" * len(header))
    for r in rows:
        print(" | ".join(str(r.get(c, "")).ljust(widths[c]) for c in cols))


def print_metrics(title: str, metrics: dict):
    """列印 MERGE metrics"""
    print(f"\n📊 {title}")
    for k, v in metrics.items():
        icon = {"rows_inserted": "➕", "rows_updated": "✏️", "rows_deleted": "🗑️",
                "rows_unchanged": "⏸️", "cow_files_rewritten": "📄"}.get(k, "•")
        print(f"  {icon} {k}: {v}")


# ============================================================
# 模式 1：Basic Upsert（UPDATE SET * / INSERT *）
# ============================================================

def demo_basic_upsert():
    print("\n" + "=" * 70)
    print("🔹 模式 1：Basic Upsert（最常見）")
    print("=" * 70)
    
    table = MiniDeltaTable(name="dim_vendor", primary_key="vendor_id", rows=generate_vendors())
    
    # 模擬每日 CDC：V002 搬家、V003 lead time 變了、V006 是新供應商
    source = [
        {"vendor_id": "V002", "name": "日月光", "city": "台南", "lead_time": 10, "avl_status": "Active", "updated_at": "2026-06-02"},
        {"vendor_id": "V003", "name": "廣達", "city": "桃園", "lead_time": 5, "avl_status": "Active", "updated_at": "2026-06-02"},
        {"vendor_id": "V006", "name": "聯發科", "city": "新竹", "lead_time": 12, "avl_status": "Active", "updated_at": "2026-06-02"},
    ]
    
    merge = MiniMerge(table, source, "vendor_id")
    metrics = (merge
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )
    
    print_metrics("Basic Upsert Metrics", metrics)
    print_table("合併後的 dim_vendor", table.rows,
                ["vendor_id", "name", "city", "lead_time", "avl_status"])
    
    # 問題：V001, V004, V005 沒有變更，但 V002, V003 都被重寫了（即使只有一個欄位變）
    print(f"\n⚠️ CoW 重寫總計: {table.cow_rewrites} 行")
    return table.cow_rewrites


# ============================================================
# 模式 2：Conditional Update（避免無意義重寫）
# ============================================================

def demo_conditional_update():
    print("\n" + "=" * 70)
    print("🔹 模式 2：Conditional Update（只在真正有變更時才 UPDATE）")
    print("=" * 70)
    
    table = MiniDeltaTable(name="dim_vendor", primary_key="vendor_id", rows=generate_vendors())
    
    # 同樣的 source，但多加一個「沒變的」供應商
    source = [
        {"vendor_id": "V001", "name": "台積電", "city": "新竹", "lead_time": 14, "avl_status": "Active", "updated_at": "2026-06-02"},  # 完全沒變！
        {"vendor_id": "V002", "name": "日月光", "city": "台南", "lead_time": 10, "avl_status": "Active", "updated_at": "2026-06-02"},  # city 變了
        {"vendor_id": "V003", "name": "廣達", "city": "桃園", "lead_time": 5, "avl_status": "Active", "updated_at": "2026-06-02"},    # lead_time 變了
        {"vendor_id": "V006", "name": "聯發科", "city": "新竹", "lead_time": 12, "avl_status": "Active", "updated_at": "2026-06-02"},  # 新的
    ]
    
    def has_changed(target_row, source_row):
        """變更偵測：只有 city / lead_time / avl_status 真的不同才算變更"""
        return (target_row["city"] != source_row["city"] or
                target_row["lead_time"] != source_row["lead_time"] or
                target_row["avl_status"] != source_row["avl_status"])
    
    merge = MiniMerge(table, source, "vendor_id")
    metrics = (merge
        .when_matched_update(
            condition=has_changed,
            set_fn=lambda t, s: {**t, **s}  # 用 source 覆蓋
        )
        .when_not_matched_insert_all()
        .execute()
    )
    
    print_metrics("Conditional Update Metrics", metrics)
    print(f"\n✅ V001 沒有變更 → rows_unchanged 應為 3（V001 + V004 + V005）")
    print(f"⚠️ CoW 重寫總計: {table.cow_rewrites} 行（比 Basic Upsert 少！）")
    return table.cow_rewrites


# ============================================================
# 模式 3：Insert-Only Dedup（Log 去重）
# ============================================================

def demo_insert_only_dedup():
    print("\n" + "=" * 70)
    print("🔹 模式 3：Insert-Only Dedup（只插入新的，跳過已存在的）")
    print("=" * 70)
    
    # 模擬 event log 表
    existing_logs = [
        {"event_id": "E001", "type": "PO_CREATED", "po_number": "PO-2026-001", "timestamp": "2026-06-01 08:00"},
        {"event_id": "E002", "type": "GR_RECEIVED", "po_number": "PO-2026-001", "timestamp": "2026-06-01 09:00"},
        {"event_id": "E003", "type": "PO_CREATED", "po_number": "PO-2026-002", "timestamp": "2026-06-01 10:00"},
    ]
    
    table = MiniDeltaTable(name="event_logs", primary_key="event_id", rows=existing_logs)
    
    # Source 有重複：E002 已存在、E004/E005 是新的
    source = [
        {"event_id": "E002", "type": "GR_RECEIVED", "po_number": "PO-2026-001", "timestamp": "2026-06-01 09:00"},  # 重複！
        {"event_id": "E004", "type": "INVOICE_POSTED", "po_number": "PO-2026-001", "timestamp": "2026-06-01 11:00"},  # 新的
        {"event_id": "E005", "type": "PO_CREATED", "po_number": "PO-2026-003", "timestamp": "2026-06-01 12:00"},  # 新的
    ]
    
    merge = MiniMerge(table, source, "event_id")
    metrics = (merge
        # 不設 whenMatched → 匹配到的不做任何操作
        .when_not_matched_insert_all()
        .execute()
    )
    
    print_metrics("Insert-Only Dedup Metrics", metrics)
    print_table("去重後的 event_logs", table.rows, ["event_id", "type", "po_number", "timestamp"])
    print(f"\n✅ E002 已存在 → 跳過（rows_unchanged=3）")
    print(f"✅ E004, E005 是新的 → 插入（rows_inserted=2）")


# ============================================================
# 模式 4：Source 重複 Key 偵測（Anti-Pattern 演示）
# ============================================================

def demo_source_duplicate_detection():
    print("\n" + "=" * 70)
    print("🔹 模式 4：Source 重複 Key → 報錯（Anti-Pattern）")
    print("=" * 70)
    
    table = MiniDeltaTable(name="dim_vendor", primary_key="vendor_id", rows=generate_vendors())
    
    # Source 有重複 key！
    bad_source = [
        {"vendor_id": "V002", "name": "日月光", "city": "台南", "lead_time": 10, "avl_status": "Active"},
        {"vendor_id": "V002", "name": "日月光（高雄）", "city": "高雄", "lead_time": 8, "avl_status": "Active"},  # 重複！
    ]
    
    try:
        merge = MiniMerge(table, bad_source, "vendor_id")
        merge.when_matched_update_all().when_not_matched_insert_all().execute()
        print("❌ 應該報錯但沒有！")
    except ValueError as e:
        print(f"✅ 正確攔截：{e}")
    
    # 正確做法：先 dedup
    print("\n📝 正確做法：MERGE 前先 dropDuplicates")
    seen = {}
    for row in bad_source:
        seen[row["vendor_id"]] = row  # 保留最後一筆（模擬 orderBy desc + dropDuplicates）
    deduped = list(seen.values())
    print(f"  去重後：{len(deduped)} 行（原本 {len(bad_source)} 行）")


# ============================================================
# 模式 5：SCD Type 2 MERGE（UNION 展開法）
# ============================================================

def demo_scd_type2_merge():
    print("\n" + "=" * 70)
    print("🔹 模式 5：SCD Type 2（UNION 展開法）")
    print("=" * 70)
    
    TODAY = "2026-06-02"
    FAR_FUTURE = "9999-12-31"
    
    # 初始 dim_vendor 帶 SCD Type 2 欄位
    initial = [
        {"vendor_id": "V001", "name": "台積電", "city": "新竹", "lead_time": 14, "avl_status": "Active",
         "effective_from": "2026-01-01", "effective_to": FAR_FUTURE, "is_current": True,
         "sk": "sk-001"},
        {"vendor_id": "V002", "name": "日月光", "city": "高雄", "lead_time": 10, "avl_status": "Active",
         "effective_from": "2026-01-01", "effective_to": FAR_FUTURE, "is_current": True,
         "sk": "sk-002"},
        {"vendor_id": "V003", "name": "廣達", "city": "桃園", "lead_time": 7, "avl_status": "Active",
         "effective_from": "2026-01-01", "effective_to": FAR_FUTURE, "is_current": True,
         "sk": "sk-003"},
    ]
    
    table = MiniDeltaTable(name="dim_vendor_scd2", primary_key="sk", rows=initial)
    
    # CDC 來源：V002 搬家了（city 變化）、V004 是新供應商
    cdc_events = [
        {"vendor_id": "V002", "name": "日月光", "city": "台南", "lead_time": 10, "avl_status": "Active"},
        {"vendor_id": "V004", "name": "鴻準", "city": "深圳", "lead_time": 21, "avl_status": "Probation"},
    ]
    
    # SCD Type 2 的 UNION 展開法：
    # 組 1：close old（匹配到 + 有變更 → UPDATE effective_to, is_current）
    # 組 2：insert new version（匹配到 + 有變更 → INSERT 新行）
    # 組 3：insert truly new（沒匹配到 → INSERT）
    
    # Step 1：找出有變更的供應商
    current_map = {r["vendor_id"]: r for r in table.rows if r["is_current"]}
    
    changed = []
    new_vendors = []
    for event in cdc_events:
        vid = event["vendor_id"]
        if vid in current_map:
            old = current_map[vid]
            # 檢查 SCD Type 2 欄位是否有變更
            if old["city"] != event["city"] or old["lead_time"] != event["lead_time"] or old["avl_status"] != event["avl_status"]:
                changed.append((old, event))
        else:
            new_vendors.append(event)
    
    print(f"  變更偵測：{len(changed)} 個供應商有屬性變更，{len(new_vendors)} 個新供應商")
    
    # Step 2：手動執行 SCD Type 2（因為 MERGE 的 sk 是 surrogate key，不能用 vendor_id）
    new_rows = []
    close_count = 0
    insert_count = 0
    
    for row in table.rows:
        # 檢查是否是要 close 的行
        is_closing = False
        for old, event in changed:
            if row["sk"] == old["sk"]:
                # Close old version
                closed_row = {**row, "effective_to": TODAY, "is_current": False}
                new_rows.append(closed_row)
                is_closing = True
                close_count += 1
                break
        if not is_closing:
            new_rows.append(row)
    
    # 插入新版本
    for old, event in changed:
        sk = hashlib.md5(f"{event['vendor_id']}_{TODAY}".encode()).hexdigest()[:8]
        new_row = {
            "vendor_id": event["vendor_id"],
            "name": event["name"],
            "city": event["city"],
            "lead_time": event["lead_time"],
            "avl_status": event["avl_status"],
            "effective_from": TODAY,
            "effective_to": FAR_FUTURE,
            "is_current": True,
            "sk": f"sk-{sk}",
        }
        new_rows.append(new_row)
        insert_count += 1
    
    # 插入全新供應商
    for event in new_vendors:
        sk = hashlib.md5(f"{event['vendor_id']}_{TODAY}".encode()).hexdigest()[:8]
        new_row = {
            "vendor_id": event["vendor_id"],
            "name": event["name"],
            "city": event["city"],
            "lead_time": event["lead_time"],
            "avl_status": event["avl_status"],
            "effective_from": TODAY,
            "effective_to": FAR_FUTURE,
            "is_current": True,
            "sk": f"sk-{sk}",
        }
        new_rows.append(new_row)
        insert_count += 1
    
    table.rows = new_rows
    table.version += 1
    
    print(f"\n📊 SCD Type 2 Metrics:")
    print(f"  ✏️  Old versions closed: {close_count}")
    print(f"  ➕ New versions inserted: {insert_count}")
    print(f"  📋 Total rows: {len(table.rows)}（初始 {len(initial)} → 增長 {len(table.rows) - len(initial)}）")
    
    print_table("SCD Type 2 dim_vendor（全部行）", table.rows,
                ["sk", "vendor_id", "city", "lead_time", "is_current", "effective_from", "effective_to"])
    
    # Point-in-Time Query
    print("\n🔍 Point-in-Time Query：2026-01-15 時 V002 在哪個城市？")
    pit_date = "2026-01-15"
    for r in table.rows:
        if r["vendor_id"] == "V002" and r["effective_from"] <= pit_date <= r["effective_to"]:
            print(f"  → V002 在 {pit_date} 時的城市：{r['city']}（✅ 高雄，正確！）")
    
    print(f"\n🔍 Point-in-Time Query：{TODAY} 時 V002 在哪個城市？")
    for r in table.rows:
        if r["vendor_id"] == "V002" and r["effective_from"] <= TODAY <= r["effective_to"]:
            print(f"  → V002 在 {TODAY} 時的城市：{r['city']}（✅ 台南，正確！）")


# ============================================================
# 模式 6：Incremental Sync + Soft Delete
# ============================================================

def demo_incremental_sync():
    print("\n" + "=" * 70)
    print("🔹 模式 6：Incremental Sync（WHEN NOT MATCHED BY SOURCE → soft delete）")
    print("=" * 70)
    
    table = MiniDeltaTable(name="dim_vendor", primary_key="vendor_id", rows=[
        {"vendor_id": "V001", "name": "台積電", "status": "active", "last_seen": "2026-05-28"},
        {"vendor_id": "V002", "name": "日月光", "status": "active", "last_seen": "2026-05-28"},
        {"vendor_id": "V003", "name": "廣達", "status": "active", "last_seen": "2026-05-28"},
        {"vendor_id": "V004", "name": "鴻準", "status": "active", "last_seen": "2026-01-15"},  # 很久沒出現了
    ])
    
    # Source：最近 5 天的資料（V003 消失了、V005 是新的）
    source = [
        {"vendor_id": "V001", "name": "台積電", "status": "active", "last_seen": "2026-06-02"},
        {"vendor_id": "V002", "name": "日月光", "status": "active", "last_seen": "2026-06-02"},
        {"vendor_id": "V005", "name": "聯發科", "status": "active", "last_seen": "2026-06-02"},
    ]
    
    five_days_ago = "2026-05-28"  # current_date() - 5 days
    
    merge = MiniMerge(table, source, "vendor_id")
    metrics = (merge
        .when_matched_update(
            condition=lambda t, s: True,
            set_fn=lambda t, s: {**t, "last_seen": s["last_seen"]}
        )
        .when_not_matched_insert_all()
        .when_not_matched_by_source_update(
            condition=lambda t: t["last_seen"] >= five_days_ago,  # 只處理最近 5 天的
            set_fn=lambda t: {**t, "status": "inactive"}
        )
        .execute()
    )
    
    print_metrics("Incremental Sync Metrics", metrics)
    print_table("同步後", table.rows, ["vendor_id", "name", "status", "last_seen"])
    print(f"\n✅ V003 最近 5 天在 source 裡消失 → 標記 inactive")
    print(f"✅ V004 last_seen 是 1 月 → 不受 WHEN NOT MATCHED BY SOURCE 條件影響（未被標記）")
    print(f"✅ V005 是新的 → 直接 INSERT")


# ============================================================
# 綜合比較
# ============================================================

def demo_cow_comparison():
    print("\n" + "=" * 70)
    print("🔹 綜合比較：Conditional vs Unconditional Update 的 CoW 差異")
    print("=" * 70)
    
    # 模擬大表：100 個供應商，但 CDC 只有 10 個，其中 7 個沒變
    vendors_100 = [
        {"vendor_id": f"V{i:03d}", "name": f"Vendor_{i}", "city": f"City_{i % 5}",
         "lead_time": 10 + i % 20, "avl_status": "Active"}
        for i in range(100)
    ]
    
    # CDC：10 個供應商，其中只有 3 個真的有變更
    cdc = [
        {"vendor_id": "V000", "name": "Vendor_0", "city": "City_0", "lead_time": 10, "avl_status": "Active"},        # 沒變
        {"vendor_id": "V001", "name": "Vendor_1", "city": "City_1", "lead_time": 11, "avl_status": "Active"},        # 沒變
        {"vendor_id": "V002", "name": "Vendor_2", "city": "City_2", "lead_time": 12, "avl_status": "Active"},        # 沒變
        {"vendor_id": "V003", "name": "Vendor_3", "city": "City_NEW", "lead_time": 13, "avl_status": "Active"},      # city 變了！
        {"vendor_id": "V004", "name": "Vendor_4", "city": "City_4", "lead_time": 99, "avl_status": "Active"},        # lead_time 變了！
        {"vendor_id": "V005", "name": "Vendor_5", "city": "City_0", "lead_time": 15, "avl_status": "Active"},        # 沒變
        {"vendor_id": "V006", "name": "Vendor_6", "city": "City_1", "lead_time": 16, "avl_status": "Active"},        # 沒變
        {"vendor_id": "V007", "name": "Vendor_7", "city": "City_2", "lead_time": 17, "avl_status": "Active"},        # 沒變
        {"vendor_id": "V008", "name": "Vendor_8", "city": "City_3", "lead_time": 18, "avl_status": "Suspended"},     # avl_status 變了！
        {"vendor_id": "V009", "name": "Vendor_9", "city": "City_4", "lead_time": 19, "avl_status": "Active"},        # 沒變
    ]
    
    # Unconditional
    t1 = MiniDeltaTable(name="uncond", primary_key="vendor_id", rows=copy.deepcopy(vendors_100))
    m1 = MiniMerge(t1, cdc, "vendor_id")
    r1 = m1.when_matched_update_all().when_not_matched_insert_all().execute()
    
    # Conditional
    t2 = MiniDeltaTable(name="cond", primary_key="vendor_id", rows=copy.deepcopy(vendors_100))
    m2 = MiniMerge(t2, cdc, "vendor_id")
    r2 = (m2
        .when_matched_update(
            condition=lambda t, s: (t["city"] != s["city"] or t["lead_time"] != s["lead_time"] or t["avl_status"] != s["avl_status"]),
            set_fn=lambda t, s: {**t, **s}
        )
        .when_not_matched_insert_all()
        .execute()
    )
    
    print(f"\n{'':4}{'Unconditional':>20} {'Conditional':>20}")
    print(f"{'':4}{'─' * 20} {'─' * 20}")
    print(f"{'rows_updated':16} {r1['rows_updated']:>20} {r2['rows_updated']:>20}")
    print(f"{'rows_unchanged':16} {r1['rows_unchanged']:>20} {r2['rows_unchanged']:>20}")
    print(f"{'cow_rewritten':16} {r1['cow_files_rewritten']:>20} {r2['cow_files_rewritten']:>20}")
    
    saved = r1['cow_files_rewritten'] - r2['cow_files_rewritten']
    pct = saved / r1['cow_files_rewritten'] * 100 if r1['cow_files_rewritten'] else 0
    print(f"\n✅ Conditional Update 減少了 {saved} 次 CoW 重寫（節省 {pct:.0f}%）")
    print(f"  → 在 ODM 供應鏈場景（~90% 供應商主數據每天不變），這個優化效果極為顯著")


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":
    print("=" * 70)
    print("🏗️ Delta Lake MERGE 操作模擬器")
    print("  場景：ODM 供應商主數據 CDC 同步")
    print("=" * 70)
    
    demo_basic_upsert()
    demo_conditional_update()
    demo_insert_only_dedup()
    demo_source_duplicate_detection()
    demo_scd_type2_merge()
    demo_incremental_sync()
    demo_cow_comparison()
    
    print("\n" + "=" * 70)
    print("✅ 所有模式演示完成！")
    print("=" * 70)
    print("""
📝 關鍵學習：
  1. Basic Upsert 是最常見的模式，但要注意 Source 去重
  2. Conditional Update 能減少 70% 的不必要 CoW 重寫（供應鏈場景）
  3. Insert-Only Dedup 適合 log 類資料，source 要先自己去重
  4. SCD Type 2 用 UNION 展開法在一次 MERGE 中同時 close old + insert new
  5. WHEN NOT MATCHED BY SOURCE 一定要加條件，否則全表重寫
  6. MERGE + Liquid Clustering + 定期 OPTIMIZE = 最佳效能組合
""")
