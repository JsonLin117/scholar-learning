"""
dbt Incremental Model 練習
===========================
模擬 dbt 的 incremental materialization：5 種策略、lookback window、
incremental_predicates、on_schema_change、incremental vs snapshot 對比。

場景：ODM 工廠 SAP MATDOC 庫存異動表
"""

from datetime import date, timedelta
from dataclasses import dataclass, field
from typing import Optional
import hashlib
import json
import copy


# ============================================================
# Part 1: Mini dbt Incremental Engine
# ============================================================

@dataclass
class IncrementalConfig:
    """dbt incremental model 配置"""
    materialized: str = "incremental"
    incremental_strategy: str = "merge"          # append|merge|delete+insert|insert_overwrite|microbatch
    unique_key: list = field(default_factory=list)
    on_schema_change: str = "ignore"             # ignore|fail|append_new_columns|sync_all_columns
    incremental_predicates: list = field(default_factory=list)
    lookback_days: int = 3
    batch_size: str = "day"                      # microbatch only
    event_time: Optional[str] = None             # microbatch only


class MiniDbtTable:
    """模擬資料庫中的一張表"""
    def __init__(self, name: str, columns: list[str]):
        self.name = name
        self.columns = columns
        self.rows: list[dict] = []
        self.exists = False
        self._scan_count = 0   # 追蹤掃描行數

    def insert(self, rows: list[dict]):
        for r in rows:
            self.rows.append({c: r.get(c) for c in self.columns})
        self.exists = True

    def create_as(self, rows: list[dict]):
        """CREATE TABLE AS SELECT"""
        self.rows = []
        if rows:
            self.columns = list(rows[0].keys())
        for r in rows:
            self.rows.append(dict(r))
        self.exists = True

    def scan(self, predicate=None) -> list[dict]:
        """掃描表，可選 predicate 限縮範圍"""
        if predicate:
            result = [r for r in self.rows if predicate(r)]
        else:
            result = list(self.rows)
        self._scan_count += len(result)
        return result

    def max_value(self, col: str):
        if not self.rows:
            return None
        return max(r[col] for r in self.rows if r.get(col) is not None)

    def delete_where(self, predicate):
        before = len(self.rows)
        self.rows = [r for r in self.rows if not predicate(r)]
        return before - len(self.rows)

    def add_column(self, col_name: str, default=None):
        if col_name not in self.columns:
            self.columns.append(col_name)
            for r in self.rows:
                r[col_name] = default

    def drop_column(self, col_name: str):
        if col_name in self.columns:
            self.columns.remove(col_name)
            for r in self.rows:
                r.pop(col_name, None)

    def __len__(self):
        return len(self.rows)


class IncrementalRunner:
    """模擬 dbt 的 incremental materialization 邏輯"""

    def __init__(self, config: IncrementalConfig):
        self.config = config
        self.run_count = 0

    def is_incremental(self, target: MiniDbtTable, full_refresh: bool = False) -> bool:
        """is_incremental() macro 的 4 個條件"""
        return (
            target.exists and
            not full_refresh and
            self.config.materialized == "incremental" and
            self.run_count > 0
        )

    def run(self, target: MiniDbtTable, new_data: list[dict],
            full_refresh: bool = False) -> dict:
        """執行一次 incremental run"""
        stats = {
            "strategy": self.config.incremental_strategy,
            "is_incremental": False,
            "source_rows": len(new_data),
            "target_rows_before": len(target),
            "target_rows_after": 0,
            "rows_inserted": 0,
            "rows_updated": 0,
            "rows_deleted": 0,
            "target_scan_rows": 0,
        }

        target._scan_count = 0

        if not self.is_incremental(target, full_refresh):
            # 首跑：CREATE TABLE AS
            target.create_as(new_data)
            stats["is_incremental"] = False
            stats["rows_inserted"] = len(new_data)
        else:
            stats["is_incremental"] = True
            strategy = self.config.incremental_strategy

            if strategy == "append":
                self._run_append(target, new_data, stats)
            elif strategy == "merge":
                self._run_merge(target, new_data, stats)
            elif strategy == "delete+insert":
                self._run_delete_insert(target, new_data, stats)
            elif strategy == "insert_overwrite":
                self._run_insert_overwrite(target, new_data, stats)
            elif strategy == "microbatch":
                self._run_microbatch(target, new_data, stats)

        stats["target_rows_after"] = len(target)
        stats["target_scan_rows"] = target._scan_count
        self.run_count += 1
        return stats

    def _run_append(self, target, new_data, stats):
        """純插入，不檢查重複"""
        target.insert(new_data)
        stats["rows_inserted"] = len(new_data)

    def _run_merge(self, target, new_data, stats):
        """MERGE INTO（SCD Type 1 upsert）"""
        uk = self.config.unique_key
        if not uk:
            # 沒有 unique_key → 退化為 append
            return self._run_append(target, new_data, stats)

        # 建立 incremental_predicates filter
        pred = self._build_target_predicate()

        # 掃描目標表（可能被 predicate 限縮）
        existing = target.scan(pred)
        existing_keys = {}
        for i, r in enumerate(target.rows):
            if pred is None or pred(r):
                key = tuple(r.get(k) for k in uk)
                existing_keys[key] = i

        inserted, updated = 0, 0
        for row in new_data:
            key = tuple(row.get(k) for k in uk)
            if key in existing_keys:
                idx = existing_keys[key]
                target.rows[idx] = {c: row.get(c) for c in target.columns}
                updated += 1
            else:
                target.insert([row])
                inserted += 1

        stats["rows_inserted"] = inserted
        stats["rows_updated"] = updated

    def _run_delete_insert(self, target, new_data, stats):
        """先刪後插"""
        uk = self.config.unique_key
        new_keys = {tuple(r.get(k) for k in uk) for r in new_data}

        deleted = target.delete_where(
            lambda r: tuple(r.get(k) for k in uk) in new_keys
        )
        target.insert(new_data)
        stats["rows_deleted"] = deleted
        stats["rows_inserted"] = len(new_data)

    def _run_insert_overwrite(self, target, new_data, stats):
        """以分區為單位替換"""
        partition_col = self.config.event_time or "posting_date"
        partitions = {r.get(partition_col) for r in new_data}

        deleted = target.delete_where(
            lambda r: r.get(partition_col) in partitions
        )
        target.insert(new_data)
        stats["rows_deleted"] = deleted
        stats["rows_inserted"] = len(new_data)

    def _run_microbatch(self, target, new_data, stats):
        """按時間批次處理（每天一批）"""
        event_col = self.config.event_time or "posting_date"
        batches = {}
        for r in new_data:
            batch_key = r.get(event_col)
            batches.setdefault(batch_key, []).append(r)

        total_deleted, total_inserted = 0, 0
        for batch_date, batch_rows in sorted(batches.items()):
            deleted = target.delete_where(lambda r, d=batch_date: r.get(event_col) == d)
            target.insert(batch_rows)
            total_deleted += deleted
            total_inserted += len(batch_rows)

        stats["rows_deleted"] = total_deleted
        stats["rows_inserted"] = total_inserted
        stats["batches"] = len(batches)

    def _build_target_predicate(self):
        """把 incremental_predicates 轉成 Python predicate"""
        preds = self.config.incremental_predicates
        if not preds:
            return None
        # 簡化：只支援 "posting_date > N_DAYS_AGO" 語意
        return None  # 真實場景由 SQL 引擎處理


# ============================================================
# Part 2: 測試資料 — ODM MATDOC 庫存異動
# ============================================================

def generate_matdoc_data(start_date: date, days: int, rows_per_day: int = 5,
                         plant: str = "TW01") -> list[dict]:
    """生成模擬 MATDOC 資料"""
    materials = ["GPU-A100", "CPU-EPYC", "SSD-PM1733", "RAM-DDR5", "HBA-CARD",
                 "HEATSINK-01", "FAN-MODULE", "CABLE-PCIE", "RISER-CARD", "PSU-2400W"]
    mvt_types = ["101", "261", "601", "122", "311"]  # GR/GI/Ship/Return/Transfer

    rows = []
    doc_counter = 1000
    for d in range(days):
        current_date = start_date + timedelta(days=d)
        for i in range(rows_per_day):
            mat_idx = (d * rows_per_day + i) % len(materials)
            mvt_idx = (d + i) % len(mvt_types)
            doc_counter += 1
            rows.append({
                "plant": plant,
                "material": materials[mat_idx],
                "posting_date": current_date.isoformat(),
                "doc_num": f"MAT{doc_counter:06d}",
                "movement_type": mvt_types[mvt_idx],
                "quantity": (i + 1) * 10.0,
                "unit": "EA",
                "storage_location": f"SL{(i % 3) + 1:02d}",
                "vendor": f"VENDOR-{(i % 4) + 1:03d}",
            })
    return rows


def generate_late_arriving_data(original_date: date, count: int = 3) -> list[dict]:
    """生成 late-arriving data（回溯修正）"""
    rows = []
    for i in range(count):
        rows.append({
            "plant": "TW01",
            "material": "GPU-A100",
            "posting_date": original_date.isoformat(),
            "doc_num": f"LATE{9000 + i:06d}",
            "movement_type": "122",  # 退料
            "quantity": -(i + 1) * 5.0,
            "unit": "EA",
            "storage_location": "SL01",
            "vendor": "VENDOR-001",
        })
    return rows


# ============================================================
# Part 3: 5 種策略對比
# ============================================================

def demo_strategy_comparison():
    print("=" * 70)
    print("Part 1: 5 種 Incremental Strategy 對比")
    print("=" * 70)

    base_date = date(2026, 6, 20)
    initial_data = generate_matdoc_data(base_date, days=3, rows_per_day=5)
    new_data = generate_matdoc_data(base_date + timedelta(days=3), days=2, rows_per_day=5)

    # 製造一些 updated rows（同 key 但不同值）
    updated_data = []
    for r in initial_data[:3]:
        updated = dict(r)
        updated["quantity"] = r["quantity"] * 2  # 數量翻倍
        updated_data.append(updated)

    incremental_input = new_data + updated_data  # 混合新增 + 更新

    strategies = [
        ("append", IncrementalConfig(
            incremental_strategy="append",
            unique_key=[]
        )),
        ("merge", IncrementalConfig(
            incremental_strategy="merge",
            unique_key=["plant", "material", "posting_date", "doc_num"]
        )),
        ("delete+insert", IncrementalConfig(
            incremental_strategy="delete+insert",
            unique_key=["plant", "material", "posting_date", "doc_num"]
        )),
        ("insert_overwrite", IncrementalConfig(
            incremental_strategy="insert_overwrite",
            event_time="posting_date"
        )),
        ("microbatch", IncrementalConfig(
            incremental_strategy="microbatch",
            event_time="posting_date",
            batch_size="day"
        )),
    ]

    results = {}
    for name, config in strategies:
        table = MiniDbtTable("fct_matdoc", list(initial_data[0].keys()))
        runner = IncrementalRunner(config)

        # 首跑
        stats1 = runner.run(table, initial_data)

        # 增量跑
        stats2 = runner.run(table, incremental_input)

        results[name] = {
            "first_run": stats1,
            "incr_run": stats2,
            "final_rows": len(table),
        }

        print(f"\n📌 Strategy: {name}")
        print(f"  首跑：{stats1['source_rows']} rows → CREATE TABLE ({stats1['rows_inserted']} inserted)")
        print(f"  增量：{stats2['source_rows']} rows input")
        print(f"    → inserted: {stats2['rows_inserted']}, updated: {stats2.get('rows_updated', 0)}, "
              f"deleted: {stats2.get('rows_deleted', 0)}")
        if 'batches' in stats2:
            print(f"    → batches processed: {stats2['batches']}")
        print(f"  最終行數：{len(table)}")

    # 對比分析
    print(f"\n{'─' * 70}")
    print("📊 Strategy 對比總結：")
    print(f"{'Strategy':<20} {'Final Rows':>10} {'Updates':>8} {'Duplicates?':>12}")
    print(f"{'─' * 50}")
    for name, r in results.items():
        incr = r["incr_run"]
        has_dup = "YES ⚠️" if name == "append" else "NO"
        print(f"{name:<20} {r['final_rows']:>10} {incr.get('rows_updated', 0):>8} {has_dup:>12}")

    print(f"\n✅ 結論：")
    print(f"  - append: 最快但不去重（event log 適用）")
    print(f"  - merge: 最通用（有 PK 的表首選，Databricks 預設）")
    print(f"  - delete+insert: merge 替代方案（Redshift 用）")
    print(f"  - insert_overwrite: 分區替換（大型時間分區表）")
    print(f"  - microbatch: 按天批次，可平行+可重試（dbt 1.9+）")


# ============================================================
# Part 4: Lookback Window 模擬
# ============================================================

def demo_lookback_window():
    print("\n" + "=" * 70)
    print("Part 2: Lookback Window — Late-Arriving Data 捕捉")
    print("=" * 70)

    base_date = date(2026, 6, 20)
    initial_data = generate_matdoc_data(base_date, days=5, rows_per_day=5)  # 6/20-6/24

    config = IncrementalConfig(
        incremental_strategy="merge",
        unique_key=["plant", "material", "posting_date", "doc_num"],
        lookback_days=3,
    )
    table = MiniDbtTable("fct_matdoc", list(initial_data[0].keys()))
    runner = IncrementalRunner(config)

    # 首跑
    runner.run(table, initial_data)
    print(f"首跑完成：{len(table)} rows（6/20 ~ 6/24）")

    # 6/25 來了新資料 + 6/22 的 late-arriving 修正
    new_data = generate_matdoc_data(date(2026, 6, 25), days=1, rows_per_day=5)
    late_data = generate_late_arriving_data(date(2026, 6, 22), count=3)

    # 場景 A: 無 lookback (lookback=0)
    table_no_lb = MiniDbtTable("fct_matdoc_no_lb", list(initial_data[0].keys()))
    runner_no_lb = IncrementalRunner(IncrementalConfig(
        incremental_strategy="merge",
        unique_key=["plant", "material", "posting_date", "doc_num"],
        lookback_days=0,
    ))
    runner_no_lb.run(table_no_lb, initial_data)

    # 只取 > max(posting_date) 的資料（lookback=0）
    max_date_no_lb = table_no_lb.max_value("posting_date")
    filtered_no_lb = [r for r in (new_data + late_data) if r["posting_date"] >= max_date_no_lb]
    stats_no_lb = runner_no_lb.run(table_no_lb, filtered_no_lb)
    late_caught_no_lb = sum(1 for r in filtered_no_lb if r["doc_num"].startswith("LATE"))

    # 場景 B: 3 天 lookback
    table_lb3 = MiniDbtTable("fct_matdoc_lb3", list(initial_data[0].keys()))
    runner_lb3 = IncrementalRunner(IncrementalConfig(
        incremental_strategy="merge",
        unique_key=["plant", "material", "posting_date", "doc_num"],
        lookback_days=3,
    ))
    runner_lb3.run(table_lb3, initial_data)

    lb_cutoff = date(2026, 6, 25) - timedelta(days=3)  # 6/22
    filtered_lb3 = [r for r in (new_data + late_data)
                     if r["posting_date"] >= lb_cutoff.isoformat()]
    stats_lb3 = runner_lb3.run(table_lb3, filtered_lb3)
    late_caught_lb3 = sum(1 for r in filtered_lb3 if r["doc_num"].startswith("LATE"))

    print(f"\n場景：6/25 增量跑，同時有 6/22 的 late-arriving 修正（3 筆退料）")
    print(f"{'─' * 50}")
    print(f"{'Lookback':<15} {'Source Rows':>12} {'Late Caught':>12} {'結果':>15}")
    print(f"{'─' * 50}")
    print(f"{'0 天':<15} {len(filtered_no_lb):>12} {late_caught_no_lb:>12} "
          f"{'⚠️ 漏抓!' if late_caught_no_lb < 3 else '✅':>15}")
    print(f"{'3 天':<15} {len(filtered_lb3):>12} {late_caught_lb3:>12} "
          f"{'⚠️ 漏抓!' if late_caught_lb3 < 3 else '✅':>15}")

    print(f"\n✅ 結論：3 天 lookback 成功捕捉 6/22 的 late-arriving 修正")
    print(f"   ODM 建議：MATDOC 用 3 天（月底結帳期間臨時加到 7 天）")
    print(f"   Trade-off：lookback 越寬 → 重算越多（但保證不漏資料）")


# ============================================================
# Part 5: incremental_predicates 效果量化
# ============================================================

def demo_incremental_predicates():
    print("\n" + "=" * 70)
    print("Part 3: incremental_predicates — 目標表掃描縮減")
    print("=" * 70)

    base_date = date(2026, 1, 1)
    # 模擬半年資料（180 天 × 5 行 = 900 行）
    large_data = generate_matdoc_data(base_date, days=180, rows_per_day=5)
    new_data = generate_matdoc_data(date(2026, 6, 29), days=1, rows_per_day=5)

    # 場景 A: 無 predicate（全表掃描）
    table_no_pred = MiniDbtTable("fct_matdoc", list(large_data[0].keys()))
    runner_no_pred = IncrementalRunner(IncrementalConfig(
        incremental_strategy="merge",
        unique_key=["plant", "material", "posting_date", "doc_num"],
    ))
    runner_no_pred.run(table_no_pred, large_data)
    table_no_pred._scan_count = 0

    # 手動模擬全表掃描（merge 需要比對所有行）
    _ = table_no_pred.scan()  # 全表掃描
    scan_no_pred = table_no_pred._scan_count
    runner_no_pred.run(table_no_pred, new_data)

    # 場景 B: 有 predicate（只掃描最近 7 天）
    table_pred = MiniDbtTable("fct_matdoc", list(large_data[0].keys()))
    runner_pred = IncrementalRunner(IncrementalConfig(
        incremental_strategy="merge",
        unique_key=["plant", "material", "posting_date", "doc_num"],
        incremental_predicates=["posting_date > DATEADD(DAY, -7, CURRENT_DATE)"],
    ))
    runner_pred.run(table_pred, large_data)
    table_pred._scan_count = 0

    cutoff = (date(2026, 6, 29) - timedelta(days=7)).isoformat()
    _ = table_pred.scan(lambda r: r["posting_date"] > cutoff)
    scan_pred = table_pred._scan_count

    reduction = (1 - scan_pred / scan_no_pred) * 100 if scan_no_pred > 0 else 0

    print(f"\n目標表大小：{len(table_no_pred)} 行（180 天 × 5 行/天）")
    print(f"{'─' * 50}")
    print(f"{'配置':<30} {'Target 掃描行數':>15} {'掃描比例':>10}")
    print(f"{'─' * 50}")
    print(f"{'無 predicate（全表）':<30} {scan_no_pred:>15} {'100%':>10}")
    print(f"{'有 predicate（最近7天）':<30} {scan_pred:>15} {f'{100-reduction:.1f}%':>10}")
    print(f"\n🎯 掃描縮減：{reduction:.1f}%")
    print(f"   真實場景（億級行）：incremental_predicates 是效能關鍵")
    print(f"   Databricks：配合 Liquid Clustering 效果更好（file pruning）")


# ============================================================
# Part 6: on_schema_change 行為模擬
# ============================================================

def demo_on_schema_change():
    print("\n" + "=" * 70)
    print("Part 4: on_schema_change — ECO 新增欄位時的處理")
    print("=" * 70)

    base_date = date(2026, 6, 20)
    initial_data = generate_matdoc_data(base_date, days=3, rows_per_day=3)

    # ECO 後新增 eco_version 欄位
    new_data_with_eco = []
    for r in generate_matdoc_data(date(2026, 6, 23), days=1, rows_per_day=3):
        r["eco_version"] = "ECO-2026-042"
        new_data_with_eco.append(r)

    modes = ["ignore", "fail", "append_new_columns", "sync_all_columns"]

    for mode in modes:
        table = MiniDbtTable("fct_matdoc", list(initial_data[0].keys()))
        config = IncrementalConfig(
            incremental_strategy="merge",
            unique_key=["plant", "material", "posting_date", "doc_num"],
            on_schema_change=mode,
        )
        runner = IncrementalRunner(config)
        runner.run(table, initial_data)

        # 模擬 on_schema_change 行為
        new_columns = set()
        if new_data_with_eco:
            new_columns = set(new_data_with_eco[0].keys()) - set(table.columns)

        result_msg = ""
        error = False

        if mode == "ignore":
            # 新欄位直接丟棄
            stripped = [{k: v for k, v in r.items() if k in table.columns}
                        for r in new_data_with_eco]
            runner.run(table, stripped)
            result_msg = f"新欄位 {new_columns} 被丟棄，舊表結構不變"

        elif mode == "fail":
            if new_columns:
                error = True
                result_msg = f"❌ Schema mismatch detected: new columns {new_columns} → FAIL"
            else:
                runner.run(table, new_data_with_eco)
                result_msg = "Schema 沒變，正常執行"

        elif mode == "append_new_columns":
            for col in new_columns:
                table.add_column(col, default=None)
            runner.run(table, new_data_with_eco)
            old_rows_with_null = sum(1 for r in table.rows if r.get("eco_version") is None)
            result_msg = (f"新增欄位 {new_columns}，舊記錄填 NULL "
                         f"({old_rows_with_null} rows with NULL eco_version)")

        elif mode == "sync_all_columns":
            for col in new_columns:
                table.add_column(col, default=None)
            # sync 也會刪除 source 沒有的欄位（這裡 source 有所有舊欄位所以不刪）
            runner.run(table, new_data_with_eco)
            result_msg = f"完全同步：新增 {new_columns}，表結構 = source 結構"

        print(f"\n📌 on_schema_change='{mode}'")
        print(f"  新欄位偵測：{new_columns if new_columns else '無'}")
        print(f"  結果：{result_msg}")
        if not error:
            print(f"  最終行數：{len(table)}，欄位數：{len(table.columns)}")
            print(f"  欄位列表：{table.columns}")

    print(f"\n✅ ODM 建議：用 'append_new_columns'")
    print(f"   原因：ECO 常加新欄位（版本號、新規格），不該因此觸發 full-refresh")
    print(f"   舊記錄 eco_version=NULL 是可接受的（SQL 查詢用 COALESCE 處理）")


# ============================================================
# Part 7: Incremental vs Snapshot 對比
# ============================================================

def demo_incremental_vs_snapshot():
    print("\n" + "=" * 70)
    print("Part 5: Incremental (SCD1) vs Snapshot (SCD2)")
    print("=" * 70)

    # 供應商 Dimension 資料
    vendors_v1 = [
        {"vendor_id": "V001", "name": "Intel", "avl_status": "Active",
         "lead_time_days": 14, "city": "Santa Clara"},
        {"vendor_id": "V002", "name": "Samsung", "avl_status": "Active",
         "lead_time_days": 21, "city": "Hwaseong"},
        {"vendor_id": "V003", "name": "Micron", "avl_status": "Probation",
         "lead_time_days": 28, "city": "Boise"},
    ]

    # V002 lead time 變更、V003 升級為 Active
    vendors_v2 = [
        {"vendor_id": "V001", "name": "Intel", "avl_status": "Active",
         "lead_time_days": 14, "city": "Santa Clara"},
        {"vendor_id": "V002", "name": "Samsung", "avl_status": "Active",
         "lead_time_days": 14, "city": "Hwaseong"},  # lead time 改了
        {"vendor_id": "V003", "name": "Micron", "avl_status": "Active",
         "lead_time_days": 28, "city": "Boise"},       # status 改了
    ]

    # === Incremental (merge = SCD Type 1) ===
    incr_table = MiniDbtTable("dim_vendor_incr", list(vendors_v1[0].keys()))
    incr_runner = IncrementalRunner(IncrementalConfig(
        incremental_strategy="merge",
        unique_key=["vendor_id"],
    ))
    incr_runner.run(incr_table, vendors_v1)
    incr_runner.run(incr_table, vendors_v2)

    print("\n📌 Incremental (merge = SCD Type 1):")
    print(f"  最終行數：{len(incr_table)}")
    for r in incr_table.rows:
        print(f"    {r['vendor_id']}: avl={r['avl_status']}, lt={r['lead_time_days']}d")
    print(f"  ⚠️ 歷史被覆寫！V002 的舊 lead_time=21 已消失")

    # === Snapshot (SCD Type 2) ===
    snapshot_table = MiniDbtTable("dim_vendor_snapshot",
                                  list(vendors_v1[0].keys()) +
                                  ["valid_from", "valid_to", "is_current", "sk"])
    today = date(2026, 6, 20)
    tomorrow = date(2026, 6, 26)

    # 首跑
    for v in vendors_v1:
        sk = hashlib.sha256(f"{v['vendor_id']}|{today.isoformat()}".encode()).hexdigest()[:16]
        row = dict(v)
        row.update({
            "valid_from": today.isoformat(),
            "valid_to": "9999-12-31",
            "is_current": True,
            "sk": sk,
        })
        snapshot_table.rows.append(row)
    snapshot_table.exists = True

    # 偵測變更，建立新版本
    changes_found = 0
    for v2 in vendors_v2:
        current = next((r for r in snapshot_table.rows
                       if r["vendor_id"] == v2["vendor_id"] and r["is_current"]), None)
        if current:
            changed = any(current.get(k) != v2.get(k)
                         for k in ["avl_status", "lead_time_days", "city"])
            if changed:
                changes_found += 1
                # 關閉舊版本
                current["valid_to"] = tomorrow.isoformat()
                current["is_current"] = False
                # 新增新版本
                sk = hashlib.sha256(
                    f"{v2['vendor_id']}|{tomorrow.isoformat()}".encode()
                ).hexdigest()[:16]
                new_row = dict(v2)
                new_row.update({
                    "valid_from": tomorrow.isoformat(),
                    "valid_to": "9999-12-31",
                    "is_current": True,
                    "sk": sk,
                })
                snapshot_table.rows.append(new_row)

    print(f"\n📌 Snapshot (SCD Type 2):")
    print(f"  最終行數：{len(snapshot_table)}（原 3 + 變更 {changes_found} = {3 + changes_found}）")
    for r in snapshot_table.rows:
        marker = "🟢" if r["is_current"] else "⚪"
        print(f"    {marker} {r['vendor_id']}: avl={r['avl_status']}, "
              f"lt={r['lead_time_days']}d, "
              f"valid={r['valid_from']}~{r['valid_to']}")

    print(f"\n✅ 對比結論：")
    print(f"  Incremental (merge) = SCD1 → 覆寫，無歷史 → 適合 Fact table")
    print(f"  Snapshot = SCD2 → 保留歷史版本 → 適合 Dimension table")
    print(f"  ODM 決策：")
    print(f"    庫存異動 MATDOC → incremental（只要最新值）")
    print(f"    供應商 Dim → snapshot（追蹤 AVL 狀態 / Lead Time 歷史）")


# ============================================================
# Part 8: is_incremental() 行為驗證
# ============================================================

def demo_is_incremental_behavior():
    print("\n" + "=" * 70)
    print("Part 6: is_incremental() 四條件驗證")
    print("=" * 70)

    config = IncrementalConfig(
        incremental_strategy="merge",
        unique_key=["doc_num"],
    )
    table = MiniDbtTable("test", ["doc_num", "value"])
    runner = IncrementalRunner(config)

    scenarios = [
        ("首跑（表不存在）", False, False),
        ("第二次跑（表已存在）", False, True),
        ("--full-refresh", True, True),
        ("正常增量跑", False, True),
    ]

    for desc, full_refresh, table_exists in scenarios:
        table.exists = table_exists
        result = runner.is_incremental(table, full_refresh)
        print(f"  {desc:30s} → is_incremental() = {result}")
        if not result and not full_refresh:
            runner.run_count = 0  # Reset for next scenario
        else:
            runner.run_count = max(runner.run_count, 1)

    print(f"\n  ⚠️ CI 陷阱：CI 用臨時 schema → 表不存在 → 永遠 False → 測不到增量邏輯")
    print(f"  解法：dbt build --defer --state prod-manifest/")


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":
    print("🔧 dbt Incremental Model 練習")
    print("場景：ODM 工廠 SAP MATDOC 庫存異動 pipeline")
    print()

    demo_strategy_comparison()
    demo_lookback_window()
    demo_incremental_predicates()
    demo_on_schema_change()
    demo_incremental_vs_snapshot()
    demo_is_incremental_behavior()

    print("\n" + "=" * 70)
    print("✅ 全部練習完成！")
    print()
    print("📝 核心 Takeaways：")
    print("  1. merge 是最通用的策略（有 unique_key 的表首選）")
    print("  2. Lookback 3 天 + incremental_predicates = 安全又高效")
    print("  3. on_schema_change='append_new_columns' 最適合 ODM（ECO 常加欄位）")
    print("  4. Incremental ≠ Snapshot：前者覆寫(SCD1)，後者保留歷史(SCD2)")
    print("  5. CI 測不到增量邏輯 → 用 --defer --state 解決")
    print("  6. microbatch (dbt 1.9+) 按天批次，可平行+可重試，大型時間序列最佳")
    print("=" * 70)
