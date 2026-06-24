"""
Surrogate Key vs Natural Key — ODM 供應商維度實作練習
=====================================================
Scholar Day 57 | Data-Warehouse #6 | 2026-06-25

展示：
1. Natural Key 的 6 大問題（回收、格式變更、SCD 不支援）
2. 三種 SK 生成策略對比（自增、Hash、UUID）
3. SCD Type 2 + SK 完整 pipeline
4. JOIN 效能：Integer SK vs VARCHAR NK
5. 跨系統整合（SAP + MES → unified SK）
6. 特殊 SK（未知 / 不適用）
"""

import hashlib
import uuid
import time
from datetime import date, timedelta
from dataclasses import dataclass, field
from typing import Optional

# ============================================================
# Part 1: Natural Key 問題展示
# ============================================================

def demo_natural_key_problems():
    """展示 Natural Key 在 DW 中的 6 大問題"""
    print("=" * 70)
    print("Part 1: Natural Key 的問題")
    print("=" * 70)

    # 問題 1：Key 回收（SAP 供應商被刪後 ID 重用）
    print("\n🔴 問題 1：Key 回收")
    sap_vendors_2024 = {"V001": "鴻海精密", "V002": "台積電", "V003": "力積電"}
    # 2025 年 V003 被刪除，ID 被重新分配給新供應商
    sap_vendors_2025 = {"V001": "鴻海精密", "V002": "台積電", "V003": "英業達"}  # V003 被回收！
    
    print(f"  2024 年 V003 = {sap_vendors_2024['V003']}")
    print(f"  2025 年 V003 = {sap_vendors_2025['V003']} ← 同一個 key，不同供應商！")
    print(f"  ❌ 用 NK 做 FK：2024 的採購紀錄指向 V003 → 查詢結果會錯誤地顯示「英業達」")
    
    # 問題 2：格式變更
    print("\n🔴 問題 2：格式變更")
    old_format = "V001"          # 原本 4 字元
    new_format = "TW-V00001"     # 系統升級後 9 字元
    print(f"  舊格式: {old_format} (VARCHAR(4))")
    print(f"  新格式: {new_format} (VARCHAR(10))")
    print(f"  ❌ Fact Table 數十億行的 FK 都要更新！ALTER COLUMN + 全表重寫")
    
    # 問題 3：SCD Type 2 不支援
    print("\n🔴 問題 3：SCD Type 2 不支援")
    print("  供應商 V001 從深圳搬到鄭州（需要保留歷史）")
    print("  用 NK 做 PK：V001 只能有一行 → 歷史被覆寫 ❌")
    print("  用 SK 做 PK：SK=1001（深圳版）+ SK=1002（鄭州版）→ 兩行共存 ✅")
    
    # 問題 4：複合 Key JOIN 效能
    print("\n🔴 問題 4：複合 NK JOIN 效能差")
    print("  NK: JOIN ON fact.vendor_id = dim.vendor_id AND fact.plant_code = dim.plant_code")
    print("  SK: JOIN ON fact.vendor_sk = dim.vendor_sk")
    print("  ❌ 雙欄 JOIN = 2x 比較、更大索引、更慢")
    
    # 問題 5：跨系統 Key 衝突
    print("\n🔴 問題 5：跨系統整合")
    sap_vendor = {"LIFNR": "0000001234", "name": "鴻海"}
    mes_vendor = {"vendor_code": "FH-001", "name": "鴻海"}
    print(f"  SAP: {sap_vendor['LIFNR']}")
    print(f"  MES: {mes_vendor['vendor_code']}")
    print(f"  ❌ 同一家供應商，兩個不同 ID → 用哪個做 Fact FK？")
    
    # 問題 6：NULL / 未知處理
    print("\n🔴 問題 6：未知值處理")
    print("  某筆採購的 vendor_id 還沒對應到 → NULL FK")
    print("  ❌ NULL FK = 外鍵違規 + JOIN 丟失行 + 報表漏數")
    
    print("\n✅ 結論：Surrogate Key 解決以上所有問題")


# ============================================================
# Part 2: 三種 SK 生成策略
# ============================================================

class SKGeneratorAutoIncrement:
    """自增整數 SK 生成器"""
    def __init__(self):
        self._counter = 0
    
    def generate(self, natural_key: str = None) -> int:
        self._counter += 1
        return self._counter
    
    def __repr__(self):
        return f"AutoIncrement(next={self._counter + 1})"

class SKGeneratorHash:
    """Hash-based SK 生成器（冪等）"""
    @staticmethod
    def generate(natural_key: str, valid_from: str) -> str:
        """SHA-256 of NK + valid_from → 取前 16 hex chars"""
        payload = f"{natural_key}|{valid_from}"
        return hashlib.sha256(payload.encode()).hexdigest()[:16]

class SKGeneratorUUID:
    """UUID SK 生成器"""
    @staticmethod
    def generate(natural_key: str = None) -> str:
        return str(uuid.uuid4())


def demo_sk_strategies():
    """對比三種 SK 生成策略"""
    print("\n" + "=" * 70)
    print("Part 2: 三種 SK 生成策略對比")
    print("=" * 70)
    
    natural_key = "V001"
    valid_from = "2026-01-01"
    
    # Auto-increment
    auto = SKGeneratorAutoIncrement()
    sk1 = auto.generate()
    sk2 = auto.generate()
    print(f"\n📌 自增整數: {sk1}, {sk2}")
    print(f"   空間: 4-8 bytes | 索引效率: 最佳 | 冪等: ❌（重跑會變）")
    
    # Hash-based
    hash_sk = SKGeneratorHash.generate(natural_key, valid_from)
    hash_sk_retry = SKGeneratorHash.generate(natural_key, valid_from)
    print(f"\n📌 Hash-based: {hash_sk}")
    print(f"   重跑驗證: {hash_sk_retry} {'✅ 冪等' if hash_sk == hash_sk_retry else '❌ 不冪等'}")
    print(f"   空間: 16 bytes (hex) | 索引效率: 中 | 冪等: ✅")
    
    # UUID
    uuid_sk1 = SKGeneratorUUID.generate()
    uuid_sk2 = SKGeneratorUUID.generate()
    print(f"\n📌 UUID: {uuid_sk1}")
    print(f"   空間: 36 bytes (string) | 索引效率: 差（無序）| 冪等: ❌")
    
    # 效能比較（模擬 JOIN 成本）
    print("\n📊 JOIN 效能模擬（100 萬次比較）：")
    
    n = 1_000_000
    
    # Integer comparison
    start = time.perf_counter()
    for i in range(n):
        _ = (1001 == 1002)
    int_time = time.perf_counter() - start
    
    # String comparison (16 char hash)
    hash_a = "a1b2c3d4e5f6g7h8"
    hash_b = "a1b2c3d4e5f6g7h9"
    start = time.perf_counter()
    for i in range(n):
        _ = (hash_a == hash_b)
    hash_time = time.perf_counter() - start
    
    # String comparison (36 char UUID)
    uuid_a = "550e8400-e29b-41d4-a716-446655440000"
    uuid_b = "550e8400-e29b-41d4-a716-446655440001"
    start = time.perf_counter()
    for i in range(n):
        _ = (uuid_a == uuid_b)
    uuid_time = time.perf_counter() - start
    
    print(f"  Integer:  {int_time:.3f}s (baseline)")
    print(f"  Hash-16:  {hash_time:.3f}s ({hash_time/int_time:.1f}x)")
    print(f"  UUID-36:  {uuid_time:.3f}s ({uuid_time/int_time:.1f}x)")
    
    # 空間比較
    print("\n📊 10 億行 Fact Table 的 FK 空間成本：")
    rows = 1_000_000_000
    print(f"  Integer (4B):  {rows * 4 / 1e9:.1f} GB")
    print(f"  Hash-16 (16B): {rows * 16 / 1e9:.1f} GB")
    print(f"  UUID (36B):    {rows * 36 / 1e9:.1f} GB")
    print(f"  VARCHAR NK (18B avg): {rows * 18 / 1e9:.1f} GB")


# ============================================================
# Part 3: SCD Type 2 + Surrogate Key Pipeline
# ============================================================

@dataclass
class VendorDimRow:
    """供應商維度一行記錄"""
    vendor_sk: str          # Surrogate Key (hash-based)
    vendor_id: str          # Natural Key (SAP LIFNR)
    name: str
    city: str
    avl_status: str         # Active / Probation / Suspended
    lead_time_days: int
    valid_from: str
    valid_to: str = "9999-12-31"
    is_current: bool = True


class VendorDimension:
    """供應商維度表（模擬 SCD Type 2 + SK）"""
    
    UNKNOWN_SK = "0000000000000000"  # SK=0 → 未知
    NA_SK = "ffffffffffffffff"       # SK=-1 → 不適用
    
    def __init__(self):
        self.rows: list[VendorDimRow] = []
        # 預載入特殊行
        self.rows.append(VendorDimRow(
            vendor_sk=self.UNKNOWN_SK,
            vendor_id="UNKNOWN",
            name="未知供應商",
            city="N/A",
            avl_status="Unknown",
            lead_time_days=0,
            valid_from="1900-01-01",
        ))
    
    def _generate_sk(self, vendor_id: str, valid_from: str) -> str:
        return SKGeneratorHash.generate(vendor_id, valid_from)
    
    def get_current(self, vendor_id: str) -> Optional[VendorDimRow]:
        """找到 NK 對應的當前版本"""
        for row in self.rows:
            if row.vendor_id == vendor_id and row.is_current:
                return row
        return None
    
    def lookup_sk(self, vendor_id: str) -> str:
        """ETL 用：把 NK 翻譯成 SK"""
        current = self.get_current(vendor_id)
        if current:
            return current.vendor_sk
        return self.UNKNOWN_SK  # 找不到 → 返回「未知」SK
    
    def scd2_merge(self, vendor_id: str, name: str, city: str,
                   avl_status: str, lead_time_days: int,
                   effective_date: str,
                   type1_fields: set = None,
                   type2_fields: set = None):
        """SCD Type 2 MERGE 操作"""
        type1_fields = type1_fields or {"name"}  # Type 1: 直接覆寫
        type2_fields = type2_fields or {"city", "avl_status", "lead_time_days"}  # Type 2: 保留歷史
        
        current = self.get_current(vendor_id)
        incoming = {"name": name, "city": city, "avl_status": avl_status,
                     "lead_time_days": lead_time_days}
        
        if current is None:
            # INSERT: 全新供應商
            sk = self._generate_sk(vendor_id, effective_date)
            self.rows.append(VendorDimRow(
                vendor_sk=sk, vendor_id=vendor_id,
                name=name, city=city, avl_status=avl_status,
                lead_time_days=lead_time_days,
                valid_from=effective_date,
            ))
            return "INSERT", sk
        
        # 檢查有沒有變更
        current_vals = {"name": current.name, "city": current.city,
                        "avl_status": current.avl_status,
                        "lead_time_days": current.lead_time_days}
        
        type1_changed = any(incoming[f] != current_vals[f] for f in type1_fields)
        type2_changed = any(incoming[f] != current_vals[f] for f in type2_fields)
        
        if not type1_changed and not type2_changed:
            return "NO_CHANGE", current.vendor_sk
        
        if type1_changed and not type2_changed:
            # Type 1: 直接覆寫（不產生新行）
            for f in type1_fields:
                setattr(current, f, incoming[f])
            return "TYPE1_UPDATE", current.vendor_sk
        
        # Type 2: 關閉舊行 + 插入新行
        yesterday = str(date.fromisoformat(effective_date) - timedelta(days=1))
        current.valid_to = yesterday
        current.is_current = False
        
        new_sk = self._generate_sk(vendor_id, effective_date)
        
        # Type 1 欄位在新行也更新
        new_name = incoming["name"] if type1_changed else current.name
        
        self.rows.append(VendorDimRow(
            vendor_sk=new_sk, vendor_id=vendor_id,
            name=new_name, city=incoming["city"],
            avl_status=incoming["avl_status"],
            lead_time_days=incoming["lead_time_days"],
            valid_from=effective_date,
        ))
        return "TYPE2_INSERT", new_sk


def demo_scd2_with_sk():
    """SCD Type 2 + Surrogate Key 完整流程"""
    print("\n" + "=" * 70)
    print("Part 3: SCD Type 2 + Surrogate Key Pipeline")
    print("=" * 70)
    
    dim = VendorDimension()
    
    # 模擬供應商生命週期
    events = [
        ("2024-01-01", "V001", "鴻海精密", "深圳", "Active", 14),
        ("2024-01-01", "V002", "台積電", "新竹", "Active", 21),
        ("2024-06-15", "V001", "鴻海精密", "鄭州", "Active", 10),   # 搬家+LT改 → Type 2
        ("2024-09-01", "V001", "Foxconn", "鄭州", "Active", 10),    # 只改名 → Type 1
        ("2025-01-10", "V001", "Foxconn", "鄭州", "Probation", 10), # AVL 降級 → Type 2
        ("2025-03-01", "V002", "台積電", "新竹", "Active", 21),     # 無變更
    ]
    
    print("\n📋 供應商事件處理：")
    for eff_date, vid, name, city, avl, lt in events:
        action, sk = dim.scd2_merge(vid, name, city, avl, lt, eff_date)
        print(f"  [{eff_date}] {vid} {name:10s} → {action:15s} SK={sk[:8]}...")
    
    # 顯示最終維度表
    print("\n📊 最終 dim_vendor 表：")
    print(f"  {'SK':16s} {'NK':6s} {'Name':12s} {'City':6s} {'AVL':12s} {'LT':3s} {'Valid From':11s} {'Valid To':11s} {'Current'}")
    print("  " + "-" * 95)
    for r in dim.rows:
        print(f"  {r.vendor_sk:16s} {r.vendor_id:6s} {r.name:12s} {r.city:6s} "
              f"{r.avl_status:12s} {r.lead_time_days:3d} {r.valid_from:11s} "
              f"{r.valid_to:11s} {'✅' if r.is_current else '❌'}")
    
    # 展示 SK 查找
    print("\n🔍 ETL SK 查找：")
    test_lookups = ["V001", "V002", "V999"]
    for nk in test_lookups:
        sk = dim.lookup_sk(nk)
        label = "(未知 → 返回特殊 SK)" if sk == dim.UNKNOWN_SK else ""
        print(f"  vendor_id={nk} → vendor_sk={sk[:16]} {label}")
    
    # 冪等性驗證
    print("\n🔁 Hash-based SK 冪等性驗證：")
    sk_run1 = SKGeneratorHash.generate("V001", "2024-01-01")
    sk_run2 = SKGeneratorHash.generate("V001", "2024-01-01")
    print(f"  Run 1: {sk_run1}")
    print(f"  Run 2: {sk_run2}")
    print(f"  冪等: {'✅ 相同' if sk_run1 == sk_run2 else '❌ 不同'}")
    
    # Point-in-Time Query 展示
    print("\n⏰ Point-in-Time Query（2024-08-01 的 V001 是什麼狀態？）：")
    query_date = "2024-08-01"
    pit_results = [r for r in dim.rows
                   if r.vendor_id == "V001"
                   and r.valid_from <= query_date <= r.valid_to]
    if pit_results:
        r = pit_results[0]
        print(f"  → SK={r.vendor_sk[:8]}... Name={r.name} City={r.city} AVL={r.avl_status} LT={r.lead_time_days}d")
    
    return dim


# ============================================================
# Part 4: 跨系統整合（XREF 對照表）
# ============================================================

def demo_cross_system_integration(dim: VendorDimension):
    """跨系統 NK → SK 對照表"""
    print("\n" + "=" * 70)
    print("Part 4: 跨系統整合（XREF 對照表）")
    print("=" * 70)
    
    # SAP 和 MES 用不同的 vendor ID 指向同一家供應商
    xref_table = [
        {"source_system": "SAP", "source_key": "0000001234", "unified_vendor_id": "V001"},
        {"source_system": "MES", "source_key": "FH-001",     "unified_vendor_id": "V001"},
        {"source_system": "SAP", "source_key": "0000005678", "unified_vendor_id": "V002"},
        {"source_system": "MES", "source_key": "TSMC-01",    "unified_vendor_id": "V002"},
        {"source_system": "WMS", "source_key": "WH-V001",    "unified_vendor_id": "V001"},
    ]
    
    print("\n📋 XREF 對照表（Source System → Unified NK → SK）：")
    print(f"  {'Source':6s} {'Source Key':12s} → {'Unified NK':12s} → {'Surrogate Key':16s}")
    print("  " + "-" * 60)
    for xref in xref_table:
        sk = dim.lookup_sk(xref["unified_vendor_id"])
        print(f"  {xref['source_system']:6s} {xref['source_key']:12s} → "
              f"{xref['unified_vendor_id']:12s} → {sk[:16]}")
    
    print("\n💡 關鍵洞察：")
    print("  - SAP '0000001234', MES 'FH-001', WMS 'WH-V001' 都映射到同一個 SK")
    print("  - Fact Table 只存 SK → 不管資料來自哪個系統，JOIN 都正確")
    print("  - XREF 表是跨系統整合的橋樑（Entity Resolution 的簡化版）")


# ============================================================
# Part 5: ABC 分析模擬（SC Daily Bonus）
# ============================================================

def demo_abc_analysis():
    """ABC 分析 + 盤點/採購策略分配"""
    print("\n" + "=" * 70)
    print("Part 5: ABC 分析（SC Daily #37）")
    print("=" * 70)
    
    # ODM 伺服器 BOM 物料清單（模擬）
    materials = [
        # (material_id, name, annual_qty, unit_price)
        ("CPU-001", "Intel Xeon Gold 6548Y+", 5000, 2800.00),
        ("GPU-001", "NVIDIA H100 SXM", 2000, 28000.00),
        ("MEM-001", "DDR5 64GB RDIMM", 20000, 180.00),
        ("SSD-001", "Samsung PM9A3 3.84TB", 15000, 420.00),
        ("PCB-001", "Server Mainboard", 12000, 85.00),
        ("PSU-001", "2000W Titanium PSU", 10000, 65.00),
        ("HSK-001", "Vapor Chamber Heatsink", 12000, 28.00),
        ("FAN-001", "80mm Dual Rotor Fan", 48000, 8.50),
        ("CBL-001", "SATA Power Cable", 60000, 1.20),
        ("SCR-001", "M3 Torx Screw (bag/100)", 5000, 3.50),
        ("LBL-001", "Barcode Label Roll", 2000, 12.00),
        ("PKG-001", "Anti-static Bag", 80000, 0.35),
        ("PAD-001", "Thermal Pad 1mm", 40000, 0.85),
        ("NIC-001", "25GbE NIC Dual Port", 8000, 120.00),
        ("BMC-001", "BMC Controller IC", 12000, 15.00),
        ("CAP-001", "Capacitor Kit", 100000, 0.15),
        ("RES-001", "Resistor Kit", 120000, 0.08),
        ("CON-001", "PCIe x16 Connector", 24000, 2.80),
    ]
    
    # Step 1: 計算年消耗金額
    items = []
    for mid, name, qty, price in materials:
        annual_value = qty * price
        items.append({"id": mid, "name": name, "qty": qty,
                       "price": price, "annual_value": annual_value})
    
    # Step 2: 按金額降序排列
    items.sort(key=lambda x: x["annual_value"], reverse=True)
    total_value = sum(i["annual_value"] for i in items)
    
    # Step 3: 計算累積百分比，分配 ABC
    cumulative = 0
    for item in items:
        cumulative += item["annual_value"]
        item["cum_pct"] = cumulative / total_value * 100
        if item["cum_pct"] <= 80:
            item["abc"] = "A"
        elif item["cum_pct"] <= 95:
            item["abc"] = "B"
        else:
            item["abc"] = "C"
    
    # 顯示結果
    print(f"\n📊 ABC 分析結果（年消耗總金額: ${total_value:,.0f}）：")
    print(f"  {'ID':8s} {'Name':28s} {'Annual Value':>14s} {'Cum%':>6s} {'ABC'}")
    print("  " + "-" * 70)
    for i in items:
        marker = {"A": "🔴", "B": "🟡", "C": "🟢"}[i["abc"]]
        print(f"  {i['id']:8s} {i['name']:28s} ${i['annual_value']:>12,.0f} "
              f"{i['cum_pct']:5.1f}% {marker} {i['abc']}")
    
    # 統計
    for cls in ["A", "B", "C"]:
        cls_items = [i for i in items if i["abc"] == cls]
        cls_value = sum(i["annual_value"] for i in cls_items)
        print(f"\n  {cls} 類：{len(cls_items)} 品項 ({len(cls_items)/len(items)*100:.0f}%)，"
              f"金額 ${cls_value:,.0f} ({cls_value/total_value*100:.1f}%)")
    
    # 管理策略矩陣
    print("\n📋 管理策略矩陣：")
    strategy = {
        "A": {"cycle_count": "每週", "safety_stock": "精算（Z=2.33, SL 99%）",
               "procurement": "LTA 長約 / JIT / 雙源", "focus": "💰 省的每分都是大錢"},
        "B": {"cycle_count": "每月", "safety_stock": "適度（Z=1.65, SL 95%）",
               "procurement": "框架合約 / MRP 驅動", "focus": "🔄 確保流程順暢"},
        "C": {"cycle_count": "每季", "safety_stock": "寬鬆（Z=1.28, SL 90%）",
               "procurement": "大量採購 / VMI / ROP", "focus": "⚡ 最小管理成本"},
    }
    
    for cls, s in strategy.items():
        print(f"\n  {cls} 類策略：")
        print(f"    盤點頻率: {s['cycle_count']}")
        print(f"    安全庫存: {s['safety_stock']}")
        print(f"    採購方式: {s['procurement']}")
        print(f"    核心原則: {s['focus']}")
    
    # 盤點工作量比較
    a_count = len([i for i in items if i["abc"] == "A"])
    b_count = len([i for i in items if i["abc"] == "B"])
    c_count = len([i for i in items if i["abc"] == "C"])
    
    # 等權盤點（每品項每週盤 1 次）= 全部 * 52
    uniform_annual = len(items) * 52
    # ABC 加權盤點
    weighted_annual = a_count * 52 + b_count * 12 + c_count * 4
    
    print(f"\n📊 盤點工作量比較（年度）：")
    print(f"  等權（全部每週盤）: {uniform_annual:,} 次")
    print(f"  ABC 加權盤點:       {weighted_annual:,} 次")
    print(f"  節省: {(1 - weighted_annual/uniform_annual)*100:.0f}% 工作量，"
          f"但覆蓋 {sum(i['annual_value'] for i in items if i['abc']=='A')/total_value*100:.0f}% 價值的物料仍每週盤點")


# ============================================================
# Part 6: 關鍵對比總結
# ============================================================

def print_summary():
    """SK vs NK 決策矩陣"""
    print("\n" + "=" * 70)
    print("Part 6: 決策矩陣 — 何時用 SK、何時可以用 NK")
    print("=" * 70)
    
    decisions = [
        ("正常 Dimension (vendor/material)", "SK (必須)", "SCD + 跨系統 + 效能"),
        ("Date Dimension", "NK 可 (YYYYMMDD int)", "日期永不變、人類可讀"),
        ("Degenerate Dimension (PO Number)", "NK (留在 Fact)", "不需要獨立 Dim"),
        ("Junk Dimension (flag 組合)", "SK (必須)", "組合數太多"),
        ("Fact Table FK", "SK (必須)", "Integer JOIN 最快"),
        ("Fact Table measure/DD", "NK 值直接存", "不是 FK"),
        ("Early-arriving Fact (維度還沒到)", "SK=0 (未知)", "避免 NULL FK"),
        ("跨系統整合 Fact", "SK + XREF", "統一不同系統的 ID"),
    ]
    
    print(f"\n  {'場景':40s} {'推薦':20s} {'理由'}")
    print("  " + "-" * 90)
    for scene, rec, reason in decisions:
        print(f"  {scene:40s} {rec:20s} {reason}")
    
    print("\n🎯 SA 核心記憶：")
    print("  Surrogate Key = DW 的『防火牆』——隔離來源系統變更，讓 DW 獨立演進")
    print("  Hash-based SK = 冪等的『身份證』——重跑 ETL 不會改變 SK")
    print("  SK=0 = 優雅處理『還不知道』——避免 NULL FK 的連鎖問題")


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":
    print("🏭 Scholar Practice: Surrogate Key vs Natural Key + ABC 分析")
    print("   ODM 供應鏈場景 | Data-Warehouse #6 + SC Daily #37")
    print()
    
    demo_natural_key_problems()
    demo_sk_strategies()
    dim = demo_scd2_with_sk()
    demo_cross_system_integration(dim)
    demo_abc_analysis()
    print_summary()
    
    print("\n✅ 練習完成！")
