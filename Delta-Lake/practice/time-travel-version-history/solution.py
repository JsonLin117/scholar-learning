"""
Delta Lake Time Travel 實作練習
================================
模擬 Delta Lake 的 Transaction Log 版本管理機制，
展示 VERSION AS OF / TIMESTAMP AS OF / RESTORE / VACUUM 的核心邏輯。

場景：ODM 廠庫存管理表 (dim_inventory) 的多版本追蹤

依賴：無（dependency-free，純 Python 模擬）
"""

import json
import os
import copy
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any


# ─── Mini Delta Log 模擬器 ───────────────────────────────────────
class MiniDeltaLog:
    """
    模擬 Delta Lake 的 Transaction Log 核心機制：
    - 每次寫入產生一個新的 commit（version N.json）
    - 每個 commit 記錄 add/remove 的檔案變更
    - Time Travel 透過重播 commit log 重建歷史版本
    """

    def __init__(self, table_path: str):
        self.table_path = table_path
        self.log_path = os.path.join(table_path, "_delta_log")
        os.makedirs(self.log_path, exist_ok=True)
        self.current_version = -1
        # 模擬已刪除檔案的物理保留（VACUUM 前都還在）
        self.physical_files: Dict[str, List[dict]] = {}  # filename → rows
        self.vacuum_threshold_hours = 168  # 預設 7 天

    def _next_version(self) -> int:
        self.current_version += 1
        return self.current_version

    def _commit(self, actions: List[dict], operation: str,
                user_metadata: str = "", timestamp: Optional[datetime] = None):
        """寫入一個 commit file（模擬 _delta_log/NNNNN.json）"""
        version = self._next_version()
        ts = timestamp or datetime.now()

        commit = {
            "version": version,
            "timestamp": ts.isoformat(),
            "operation": operation,
            "userMetadata": user_metadata,
            "actions": actions,
            "commitInfo": {
                "operation": operation,
                "operationMetrics": {}
            }
        }

        # 計算 metrics
        adds = [a for a in actions if a["type"] == "add"]
        removes = [a for a in actions if a["type"] == "remove"]
        commit["commitInfo"]["operationMetrics"] = {
            "numAddedFiles": len(adds),
            "numRemovedFiles": len(removes),
            "numAddedRows": sum(len(a.get("rows", [])) for a in adds),
        }

        filename = f"{version:020d}.json"
        filepath = os.path.join(self.log_path, filename)
        with open(filepath, "w") as f:
            json.dump(commit, f, indent=2, default=str)

        return version

    # ─── 寫入操作 ──────────────────────────

    def write(self, rows: List[dict], mode: str = "append",
              timestamp: Optional[datetime] = None,
              user_metadata: str = "") -> int:
        """模擬 INSERT / OVERWRITE"""
        # 生成檔案名（模擬 Parquet 檔案）
        file_id = hashlib.md5(json.dumps(rows, sort_keys=True, default=str).encode()).hexdigest()[:8]
        filename = f"part-{file_id}.parquet"

        actions = []

        if mode == "overwrite":
            # 先 remove 所有現有檔案
            active_files = self._get_active_files(self.current_version)
            for f in active_files:
                actions.append({"type": "remove", "path": f})

        # 新增檔案
        actions.append({
            "type": "add",
            "path": filename,
            "rows": rows,
            "size": len(json.dumps(rows)),
            "modificationTime": int((timestamp or datetime.now()).timestamp() * 1000)
        })

        # 物理儲存（模擬 Parquet 檔案寫入）
        self.physical_files[filename] = rows

        return self._commit(actions, "WRITE" if mode == "append" else "OVERWRITE",
                           user_metadata, timestamp)

    def merge(self, source_rows: List[dict], match_key: str,
              timestamp: Optional[datetime] = None,
              user_metadata: str = "") -> int:
        """模擬 MERGE INTO（更新匹配行 + 插入不匹配行）"""
        # 取得當前活躍資料
        current_data = self._read_version(self.current_version)

        # 建立 lookup
        current_by_key = {r[match_key]: r for r in current_data}
        source_by_key = {r[match_key]: r for r in source_rows}

        # 計算變更
        updated_rows = []
        inserted_rows = []
        unchanged_rows = []

        for key, row in current_by_key.items():
            if key in source_by_key:
                updated_rows.append(source_by_key[key])  # 更新
            else:
                unchanged_rows.append(row)  # 不變

        for key, row in source_by_key.items():
            if key not in current_by_key:
                inserted_rows.append(row)  # 新增

        # 產生新檔案（包含 unchanged + updated + inserted）
        new_data = unchanged_rows + updated_rows + inserted_rows
        file_id = hashlib.md5(json.dumps(new_data, sort_keys=True, default=str).encode()).hexdigest()[:8]
        new_filename = f"part-{file_id}.parquet"

        actions = []
        # Remove 所有舊檔案
        active_files = self._get_active_files(self.current_version)
        for f in active_files:
            actions.append({"type": "remove", "path": f})
        # Add 新檔案
        actions.append({
            "type": "add",
            "path": new_filename,
            "rows": new_data,
            "size": len(json.dumps(new_data)),
            "modificationTime": int((timestamp or datetime.now()).timestamp() * 1000)
        })
        self.physical_files[new_filename] = new_data

        return self._commit(actions, "MERGE", user_metadata, timestamp)

    def delete(self, predicate_fn, timestamp: Optional[datetime] = None,
               user_metadata: str = "") -> int:
        """模擬 DELETE"""
        current_data = self._read_version(self.current_version)
        remaining = [r for r in current_data if not predicate_fn(r)]

        file_id = hashlib.md5(json.dumps(remaining, sort_keys=True, default=str).encode()).hexdigest()[:8]
        new_filename = f"part-{file_id}.parquet"

        actions = []
        active_files = self._get_active_files(self.current_version)
        for f in active_files:
            actions.append({"type": "remove", "path": f})
        actions.append({
            "type": "add", "path": new_filename, "rows": remaining,
            "size": len(json.dumps(remaining)),
            "modificationTime": int((timestamp or datetime.now()).timestamp() * 1000)
        })
        self.physical_files[new_filename] = remaining

        return self._commit(actions, "DELETE", user_metadata, timestamp)

    # ─── 讀取操作（Time Travel 核心）──────────────────

    def _get_active_files(self, version: int) -> List[str]:
        """重播 commit log 到指定版本，取得活躍檔案清單"""
        if version < 0:
            return []

        active = set()
        for v in range(version + 1):
            filepath = os.path.join(self.log_path, f"{v:020d}.json")
            if not os.path.exists(filepath):
                continue
            with open(filepath) as f:
                commit = json.load(f)
            for action in commit["actions"]:
                if action["type"] == "add":
                    active.add(action["path"])
                elif action["type"] == "remove":
                    active.discard(action["path"])

        return list(active)

    def _read_version(self, version: int) -> List[dict]:
        """讀取指定版本的完整資料（Time Travel 核心）"""
        active_files = self._get_active_files(version)
        all_rows = []
        for filename in active_files:
            if filename in self.physical_files:
                all_rows.extend(self.physical_files[filename])
            else:
                # 檔案已被 VACUUM 刪除！
                raise FileNotFoundError(
                    f"⚠️ Time Travel 失敗！檔案 {filename} 已被 VACUUM 刪除。"
                    f"\n   Version {version} 的資料不可恢復。"
                    f"\n   原因：VACUUM 已清除超過 deletedFileRetentionDuration 的舊檔案。"
                )
        return all_rows

    # ─── TIME TRAVEL API ──────────────────────────

    def read_version_as_of(self, version: int) -> List[dict]:
        """VERSION AS OF — 精確到版本號"""
        if version < 0 or version > self.current_version:
            raise ValueError(f"Version {version} 不存在（有效範圍：0-{self.current_version}）")
        return self._read_version(version)

    def read_timestamp_as_of(self, target_ts: datetime) -> List[dict]:
        """TIMESTAMP AS OF — 找到 ≤ target_ts 的最新版本"""
        best_version = None
        for v in range(self.current_version + 1):
            filepath = os.path.join(self.log_path, f"{v:020d}.json")
            with open(filepath) as f:
                commit = json.load(f)
            commit_ts = datetime.fromisoformat(commit["timestamp"])
            if commit_ts <= target_ts:
                best_version = v
            else:
                break

        if best_version is None:
            raise ValueError(f"沒有 ≤ {target_ts} 的版本（表的第一個版本更晚）")
        return self._read_version(best_version)

    def describe_history(self, limit: Optional[int] = None) -> List[dict]:
        """DESCRIBE HISTORY — 查看版本歷史"""
        history = []
        end = self.current_version
        start = max(0, end - limit + 1) if limit else 0

        for v in range(end, start - 1, -1):
            filepath = os.path.join(self.log_path, f"{v:020d}.json")
            if not os.path.exists(filepath):
                continue
            with open(filepath) as f:
                commit = json.load(f)
            history.append({
                "version": commit["version"],
                "timestamp": commit["timestamp"],
                "operation": commit["operation"],
                "userMetadata": commit.get("userMetadata", ""),
                "metrics": commit["commitInfo"]["operationMetrics"],
            })
        return history

    def restore_to_version(self, target_version: int,
                           timestamp: Optional[datetime] = None) -> int:
        """RESTORE TABLE — 回滾到歷史版本（產生新的 commit）"""
        # 讀取目標版本的資料
        target_data = self._read_version(target_version)

        # 找出當前版本的活躍檔案（要 remove）和目標版本的活躍檔案（要 restore）
        current_files = set(self._get_active_files(self.current_version))
        target_files = set(self._get_active_files(target_version))

        actions = []
        for f in current_files - target_files:
            actions.append({"type": "remove", "path": f})
        for f in target_files - current_files:
            actions.append({"type": "add", "path": f, "rows": self.physical_files.get(f, []),
                           "size": 0, "modificationTime": 0})

        # 如果檔案集完全一樣，就不需要動作
        if not actions:
            # 為了語意正確，還是產生一個 restore commit
            actions = [{"type": "add", "path": f, "rows": [], "size": 0, "modificationTime": 0}
                      for f in target_files]

        return self._commit(actions, f"RESTORE (to version {target_version})",
                           f"Restored to version {target_version}", timestamp)

    def vacuum(self, retain_hours: Optional[int] = None):
        """VACUUM — 刪除超過 retention 的已移除檔案"""
        retain = retain_hours if retain_hours is not None else self.vacuum_threshold_hours

        # 找出所有活躍檔案
        active_files = set(self._get_active_files(self.current_version))

        # 刪除不在活躍清單中的物理檔案
        deleted = []
        for filename in list(self.physical_files.keys()):
            if filename not in active_files:
                del self.physical_files[filename]
                deleted.append(filename)

        return deleted

    # ─── 版本 Diff（跨版本比較）──────────────────────

    def diff_versions(self, v1: int, v2: int, key_col: str) -> dict:
        """比較兩個版本的差異"""
        data_v1 = {r[key_col]: r for r in self._read_version(v1)}
        data_v2 = {r[key_col]: r for r in self._read_version(v2)}

        inserted = {k: v for k, v in data_v2.items() if k not in data_v1}
        deleted = {k: v for k, v in data_v1.items() if k not in data_v2}
        updated = {}
        for k in set(data_v1.keys()) & set(data_v2.keys()):
            if data_v1[k] != data_v2[k]:
                updated[k] = {"before": data_v1[k], "after": data_v2[k]}

        return {
            "from_version": v1, "to_version": v2,
            "inserted": inserted, "deleted": deleted, "updated": updated,
            "summary": f"+{len(inserted)} inserted, ~{len(updated)} updated, -{len(deleted)} deleted"
        }


# ─── ODM 庫存管理 Time Travel 練習 ──────────────────────────

def run_demo():
    """
    場景：ODM 庫存管理表 (dim_inventory)
    1. 建立初始庫存
    2. 每日 MRP Run 更新 Safety Stock
    3. 收貨 (GR) 增加庫存
    4. 發現 MERGE bug，用 Time Travel 診斷和回滾
    """

    import tempfile
    table_path = tempfile.mkdtemp(prefix="delta_time_travel_")
    dt = MiniDeltaLog(table_path)

    print("=" * 80)
    print("🏭 ODM 庫存管理 — Delta Lake Time Travel 練習")
    print("=" * 80)

    # ── Step 1：Version 0 — 建立初始庫存 ──────────
    base_time = datetime(2026, 5, 20, 8, 0, 0)

    initial_inventory = [
        {"part_no": "GPU-A100-80G", "plant": "TPE", "on_hand": 500, "safety_stock": 200,
         "unit_cost": 15000, "status": "active"},
        {"part_no": "GPU-H100-80G", "plant": "TPE", "on_hand": 300, "safety_stock": 150,
         "unit_cost": 25000, "status": "active"},
        {"part_no": "CPU-SPR-8480", "plant": "TPE", "on_hand": 1000, "safety_stock": 400,
         "unit_cost": 8000, "status": "active"},
        {"part_no": "DDR5-64G", "plant": "TPE", "on_hand": 5000, "safety_stock": 2000,
         "unit_cost": 300, "status": "active"},
        {"part_no": "SSD-NVMe-4T", "plant": "TPE", "on_hand": 2000, "safety_stock": 800,
         "unit_cost": 500, "status": "active"},
    ]

    v0 = dt.write(initial_inventory, "overwrite", base_time,
                  "Initial inventory load from SAP")
    print(f"\n📥 Version {v0}: 初始庫存載入（{len(initial_inventory)} 個 Part）")

    # ── Step 2：Version 1 — MRP Run 更新 Safety Stock ──────
    day1 = base_time + timedelta(days=1)
    mrp_update = [
        {"part_no": "GPU-A100-80G", "plant": "TPE", "on_hand": 500, "safety_stock": 250,
         "unit_cost": 15000, "status": "active"},  # SS 200→250（CSP 需求增加）
        {"part_no": "GPU-H100-80G", "plant": "TPE", "on_hand": 300, "safety_stock": 200,
         "unit_cost": 25000, "status": "active"},  # SS 150→200
    ]
    v1 = dt.merge(mrp_update, "part_no", day1, "MRP Run W21 - SS adjustment")
    print(f"📊 Version {v1}: MRP Run 更新 Safety Stock（A100: 200→250, H100: 150→200）")

    # ── Step 3：Version 2 — 收貨 (GR) 增加庫存 ──────
    day2 = base_time + timedelta(days=2)
    gr_update = [
        {"part_no": "GPU-A100-80G", "plant": "TPE", "on_hand": 650, "safety_stock": 250,
         "unit_cost": 15000, "status": "active"},  # +150 收貨
        {"part_no": "DDR5-64G", "plant": "TPE", "on_hand": 6000, "safety_stock": 2000,
         "unit_cost": 300, "status": "active"},  # +1000 收貨
    ]
    v2 = dt.merge(gr_update, "part_no", day2, "GR Batch #2026-W21-001")
    print(f"📦 Version {v2}: 收貨入庫（A100 +150, DDR5 +1000）")

    # ── Step 4：Version 3 — ❌ Bug! 錯誤的 MERGE 清零 SS ──────
    day3 = base_time + timedelta(days=3)
    # 模擬 bug：ETL 邏輯錯誤，把所有 GPU 的 Safety Stock 設為 0
    buggy_update = [
        {"part_no": "GPU-A100-80G", "plant": "TPE", "on_hand": 650, "safety_stock": 0,
         "unit_cost": 15000, "status": "active"},  # ❌ SS → 0!
        {"part_no": "GPU-H100-80G", "plant": "TPE", "on_hand": 300, "safety_stock": 0,
         "unit_cost": 25000, "status": "active"},  # ❌ SS → 0!
    ]
    v3 = dt.merge(buggy_update, "part_no", day3, "MRP Run W21 - BUGGY BATCH")
    print(f"🐛 Version {v3}: ❌ Bug! GPU Safety Stock 被錯誤清零！")

    # ── Step 5：Version 4 — 新增一個新料號 ──────
    day4 = base_time + timedelta(days=4)
    new_part = [
        {"part_no": "NIC-CX7-200G", "plant": "TPE", "on_hand": 100, "safety_stock": 50,
         "unit_cost": 1200, "status": "active"},
    ]
    v4 = dt.merge(new_part, "part_no", day4, "New Part - ConnectX-7")
    print(f"📥 Version {v4}: 新增 ConnectX-7 NIC")

    # ══════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("📖 DESCRIBE HISTORY — 查看完整版本歷史")
    print("=" * 80)

    for h in dt.describe_history():
        metrics = h["metrics"]
        print(f"  v{h['version']} | {h['timestamp'][:19]} | {h['operation']:8s} | "
              f"+{metrics.get('numAddedRows', '?')} rows | {h['userMetadata']}")

    # ══════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("🕐 VERSION AS OF — 查詢歷史版本")
    print("=" * 80)

    # 查 version 2（收貨後，bug 之前）
    v2_data = dt.read_version_as_of(2)
    print(f"\n  📸 Version 2（收貨後，Bug 之前）的 GPU 庫存：")
    for row in v2_data:
        if "GPU" in row["part_no"]:
            print(f"    {row['part_no']}: on_hand={row['on_hand']}, SS={row['safety_stock']}")

    # 查 version 3（Bug 版本）
    v3_data = dt.read_version_as_of(3)
    print(f"\n  🐛 Version 3（Bug 版本）的 GPU 庫存：")
    for row in v3_data:
        if "GPU" in row["part_no"]:
            print(f"    {row['part_no']}: on_hand={row['on_hand']}, SS={row['safety_stock']} ❌")

    # ══════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("⏰ TIMESTAMP AS OF — 按時間查詢")
    print("=" * 80)

    # 查 day2 早上 8 點（收貨之後、Bug 之前）
    target = base_time + timedelta(days=2, hours=1)
    ts_data = dt.read_timestamp_as_of(target)
    print(f"\n  📸 {target.isoformat()} 時的庫存（找 ≤ 此時間的最新版本）：")
    print(f"    共 {len(ts_data)} 個 Part Number")

    # ══════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("🔍 版本 DIFF — 找出 Bug 影響範圍")
    print("=" * 80)

    diff = dt.diff_versions(2, 3, "part_no")
    print(f"\n  Version {diff['from_version']} → Version {diff['to_version']}: {diff['summary']}")

    if diff["updated"]:
        print("\n  被修改的行（Bug 影響）：")
        for key, changes in diff["updated"].items():
            before = changes["before"]
            after = changes["after"]
            # 找出哪些欄位變了
            changed_cols = [c for c in before if before[c] != after[c]]
            for col in changed_cols:
                print(f"    {key}: {col} = {before[col]} → {after[col]}"
                      + (" ⚠️ Safety Stock 被清零！" if col == "safety_stock" and after[col] == 0 else ""))

    # ══════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("⏪ RESTORE TABLE — 回滾到 Bug 之前")
    print("=" * 80)

    # 但我們不能直接回到 v2，因為 v4 新增了 NIC 料號
    # 正確做法：RESTORE 到 v2，然後重新 MERGE v4 的新增
    print("\n  ⚠️ 注意：不能直接 RESTORE 到 v2，因為 v4 新增了 NIC-CX7-200G")
    print("  → 需要選擇性修復，而不是全表回滾")
    print("\n  方案 A：RESTORE 到 v2 + 重新 MERGE v4 的新增")
    print("  方案 B：直接 MERGE 修正資料（更安全）")
    print("\n  選擇方案 B（生產環境更安全）：")

    # 用 Time Travel 讀取 v2 的正確 SS 值，然後 MERGE 修正
    v2_correct = dt.read_version_as_of(2)
    fix_data = [r for r in v2_correct if "GPU" in r["part_no"]]
    # 但要保留 v4 的 on_hand（收貨後的數量不動）
    for row in fix_data:
        # 從當前版本取 on_hand
        current = [r for r in dt.read_version_as_of(dt.current_version) if r["part_no"] == row["part_no"]]
        if current:
            row["on_hand"] = current[0]["on_hand"]  # 保留最新的 on_hand

    v5 = dt.merge(fix_data, "part_no", base_time + timedelta(days=5),
                  "HOTFIX: Restore GPU Safety Stock from v2 (time travel)")
    print(f"\n  ✅ Version {v5}: GPU SS 已從 v2 恢復")

    # 驗證修復結果
    fixed_data = dt.read_version_as_of(v5)
    print("\n  修復後的 GPU 庫存：")
    for row in fixed_data:
        if "GPU" in row["part_no"]:
            print(f"    {row['part_no']}: on_hand={row['on_hand']}, SS={row['safety_stock']} ✅")

    # 確認 NIC 沒被影響
    nic = [r for r in fixed_data if r["part_no"] == "NIC-CX7-200G"]
    if nic:
        print(f"    NIC-CX7-200G: on_hand={nic[0]['on_hand']}, SS={nic[0]['safety_stock']} ✅ 未受影響")

    # ══════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("🗑️ VACUUM 模擬 — 展示 VACUUM 如何破壞 Time Travel")
    print("=" * 80)

    print(f"\n  VACUUM 前物理檔案數：{len(dt.physical_files)}")
    deleted = dt.vacuum(retain_hours=0)  # 模擬 VACUUM 0 HOURS
    print(f"  VACUUM 後刪除了 {len(deleted)} 個已移除的舊檔案")
    print(f"  剩餘物理檔案數：{len(dt.physical_files)}")

    # 嘗試 Time Travel 到舊版本 → 應該失敗
    print(f"\n  嘗試 Time Travel 到 Version 0（VACUUM 後）...")
    try:
        dt.read_version_as_of(0)
        print("  ❌ 不應該成功（模擬限制：我們的簡化實作中所有版本共享同一組檔案引用）")
    except FileNotFoundError as e:
        print(f"  ✅ 預期的錯誤：{e}")
    except Exception:
        print("  ⚠️ 注意：在我們的簡化模擬中，VACUUM 只刪除不在最新活躍集中的檔案")
        print("      真實 Delta Lake 中，Version 0 的舊檔案已被刪除，Time Travel 會失敗")

    # ══════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("📊 Retention 設定建議（SA 視角）")
    print("=" * 80)

    retention_guide = {
        "庫存表 (fact_inventory)": {
            "deletedFileRetentionDuration": "30 days",
            "logRetentionDuration": "90 days",
            "reason": "月結/季結報表再現需求，MRP bug 修復視窗"
        },
        "物料主數據 (dim_material)": {
            "deletedFileRetentionDuration": "90 days",
            "logRetentionDuration": "365 days",
            "reason": "SCD Type 1 更新的歷史追溯，年度審計需求"
        },
        "交易明細 (fact_po_line)": {
            "deletedFileRetentionDuration": "7 days",
            "logRetentionDuration": "30 days",
            "reason": "Append-only 為主，很少需要回滾"
        },
        "Staging Layer (bronze)": {
            "deletedFileRetentionDuration": "3 days",
            "logRetentionDuration": "7 days",
            "reason": "隨時可從 source 重新載入，節省儲存成本"
        },
    }

    for table, config in retention_guide.items():
        print(f"\n  📋 {table}")
        print(f"     deletedFileRetention: {config['deletedFileRetentionDuration']}")
        print(f"     logRetention: {config['logRetentionDuration']}")
        print(f"     原因：{config['reason']}")

    # ── 清理 ──
    import shutil
    shutil.rmtree(table_path)

    print("\n" + "=" * 80)
    print("✅ 練習完成！")
    print("=" * 80)
    print("""
    📝 學到的核心概念：
    1. Transaction Log 是 Time Travel 的基礎（每次操作 = 一個 commit）
    2. VERSION AS OF 精確到操作，TIMESTAMP AS OF 精確到時間
    3. DESCRIBE HISTORY 查版本歷史，每個版本都有完整 metrics
    4. RESTORE 是「前進的回滾」——產生新版本而不是刪除舊版本
    5. VACUUM 會永久破壞 Time Travel——兩個 retention 閾值要理解
    6. 版本 Diff 是診斷 bug 的利器，但 CDF 更適合持續追蹤變更
    7. 生產環境回滾要考慮「中間版本的有效變更」，不能無腦全表 RESTORE

    🏭 ODM 供應鏈啟示：
    - 物料主數據表建議 90 天 retention（年度審計 + SCD 追溯）
    - 庫存表建議 30 天 retention（月結報表再現）
    - Bronze layer 只需 3-7 天（隨時可重載）
    - VACUUM 排程要比 retention 頻率低，避免意外失去 Time Travel 能力
    """)


if __name__ == "__main__":
    run_demo()
