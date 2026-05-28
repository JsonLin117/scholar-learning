"""
供應鏈組織架構 — RACI 矩陣 + 資訊流延遲模擬 + 供應商 Scorecard 引擎
=======================================================================
ODM 伺服器廠供應鏈組織資料架構練習

三大模組：
1. RACI 矩陣引擎 — 用於 workflow routing 和 RBAC 設計
2. 資訊流延遲模擬 — 量化 batch vs streaming 對業務決策的影響
3. 供應商 Scorecard 引擎 — QCDS 四維度評分 + AVL 狀態機

dependency-free（純 Python stdlib）
"""

import random
import math
from datetime import datetime, timedelta
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional
from enum import Enum

# ============================================================
# Part 1: RACI 矩陣引擎
# ============================================================

class RACIRole(Enum):
    """RACI 角色定義"""
    RESPONSIBLE = "R"    # 執行者
    ACCOUNTABLE = "A"    # 負責人（最終決策）
    CONSULTED = "C"      # 被諮詢（雙向溝通）
    INFORMED = "I"       # 被知會（單向通知）
    NONE = "-"           # 不涉及

# ODM 供應鏈部門
DEPARTMENTS = ["CF", "PMC", "Procurement", "Warehouse", "Logistics", "Quality", "RD"]

# RACI 矩陣定義（ODM 實務）
RACI_MATRIX = {
    "交期承諾(CSD)": {
        "CF": "A", "PMC": "C", "Procurement": "C", "Warehouse": "I",
        "Logistics": "C", "Quality": "I", "RD": "I"
    },
    "生產排程": {
        "CF": "C", "PMC": "A", "Procurement": "C", "Warehouse": "I",
        "Logistics": "I", "Quality": "I", "RD": "I"
    },
    "供應商選擇": {
        "CF": "I", "PMC": "C", "Procurement": "A", "Warehouse": "I",
        "Logistics": "I", "Quality": "C", "RD": "C"
    },
    "MRP_Run/備料建議": {
        "CF": "I", "PMC": "A", "Procurement": "R", "Warehouse": "I",
        "Logistics": "I", "Quality": "I", "RD": "I"
    },
    "催料(Expediting)": {
        "CF": "C", "PMC": "C", "Procurement": "A", "Warehouse": "I",
        "Logistics": "I", "Quality": "I", "RD": "I"
    },
    "插單接受": {
        "CF": "A", "PMC": "C", "Procurement": "C", "Warehouse": "I",
        "Logistics": "C", "Quality": "I", "RD": "I"
    },
    "ECO/BOM變更": {
        "CF": "I", "PMC": "C", "Procurement": "C", "Warehouse": "C",
        "Logistics": "I", "Quality": "C", "RD": "A"
    },
    "出貨放行": {
        "CF": "C", "PMC": "I", "Procurement": "I", "Warehouse": "A",
        "Logistics": "C", "Quality": "C", "RD": "I"
    },
    "3PL選擇": {
        "CF": "I", "PMC": "I", "Procurement": "I", "Warehouse": "I",
        "Logistics": "A", "Quality": "I", "RD": "I"
    },
    "品質Hold": {
        "CF": "I", "PMC": "I", "Procurement": "I", "Warehouse": "C",
        "Logistics": "I", "Quality": "A", "RD": "C"
    },
}


def query_raci(decision: str) -> dict:
    """查詢某決策事項的 RACI 分配"""
    return RACI_MATRIX.get(decision, {})


def get_approvers(decision: str) -> list:
    """取得某決策的審核者（A 角色）— 用於 workflow 路由"""
    raci = query_raci(decision)
    return [dept for dept, role in raci.items() if role == "A"]


def get_notification_list(decision: str) -> dict:
    """取得通知清單（依角色分類）— 用於系統通知設計"""
    raci = query_raci(decision)
    result = {"must_approve": [], "must_consult": [], "must_inform": [], "must_execute": []}
    for dept, role in raci.items():
        if role == "A":
            result["must_approve"].append(dept)
        elif role == "C":
            result["must_consult"].append(dept)
        elif role == "I":
            result["must_inform"].append(dept)
        elif role == "R":
            result["must_execute"].append(dept)
    return result


def generate_rbac_from_raci() -> dict:
    """從 RACI 矩陣生成 RBAC 權限建議"""
    rbac = {dept: {"approve": [], "read_write": [], "read_only": [], "notify_only": []}
            for dept in DEPARTMENTS}
    
    for decision, roles in RACI_MATRIX.items():
        for dept, role in roles.items():
            if role == "A":
                rbac[dept]["approve"].append(decision)
            elif role == "R":
                rbac[dept]["read_write"].append(decision)
            elif role == "C":
                rbac[dept]["read_only"].append(decision)
            elif role == "I":
                rbac[dept]["notify_only"].append(decision)
    
    return rbac


# ============================================================
# Part 2: 資訊流延遲模擬
# ============================================================

@dataclass
class InfoFlowEvent:
    """資訊流事件"""
    event_type: str          # 事件類型
    source_dept: str         # 發送方
    target_dept: str         # 接收方
    event_time: datetime     # 事件發生時間
    batch_delivery: datetime  # batch 模式下的送達時間
    stream_delivery: datetime # streaming 模式下的送達時間
    business_impact: str     # 業務影響描述
    cost_of_delay: float     # 延遲的成本（USD）

# 資訊流定義（延遲特性）
INFO_FLOWS = [
    {"type": "缺料Alert", "source": "PMC", "target": "Procurement",
     "latency_tolerance": "T+0", "batch_delay_hrs": 14, "stream_delay_min": 2,
     "delay_cost_per_hour": 500, "impact": "每延遲 1 小時可能需要 Premium Freight"},
    {"type": "客戶PO", "source": "Customer", "target": "CF",
     "latency_tolerance": "T+0", "batch_delay_hrs": 8, "stream_delay_min": 1,
     "delay_cost_per_hour": 200, "impact": "延遲收到 PO 壓縮內部處理時間"},
    {"type": "GR收貨確認", "source": "Warehouse", "target": "PMC",
     "latency_tolerance": "T+0", "batch_delay_hrs": 16, "stream_delay_min": 3,
     "delay_cost_per_hour": 100, "impact": "PMC 看不到即時庫存→排程失準"},
    {"type": "S&OP數據包", "source": "All", "target": "Management",
     "latency_tolerance": "Monthly", "batch_delay_hrs": 0, "stream_delay_min": 0,
     "delay_cost_per_hour": 0, "impact": "月度批次，不需即時"},
    {"type": "MRP_Exception", "source": "SAP", "target": "PMC",
     "latency_tolerance": "T+0", "batch_delay_hrs": 12, "stream_delay_min": 5,
     "delay_cost_per_hour": 300, "impact": "Exception 延遲→採購決策延遲→交期風險"},
    {"type": "品質Hold通知", "source": "Quality", "target": "PMC+Warehouse",
     "latency_tolerance": "T+0", "batch_delay_hrs": 6, "stream_delay_min": 1,
     "delay_cost_per_hour": 1000, "impact": "延遲 Hold→不良品流入產線→客訴風險"},
]


def simulate_info_flow(num_days: int = 30, seed: int = 42) -> list:
    """模擬 N 天的資訊流事件，比較 batch vs streaming"""
    random.seed(seed)
    base_date = datetime(2026, 5, 1)
    events = []
    
    for day in range(num_days):
        for flow_def in INFO_FLOWS:
            if flow_def["latency_tolerance"] == "Monthly" and day % 30 != 0:
                continue
            
            # 每天隨機產生 1-5 個事件（依類型）
            num_events = random.randint(1, 5) if flow_def["latency_tolerance"] == "T+0" else 1
            
            for _ in range(num_events):
                event_time = base_date + timedelta(
                    days=day,
                    hours=random.randint(8, 17),
                    minutes=random.randint(0, 59)
                )
                
                batch_delay = timedelta(hours=flow_def["batch_delay_hrs"] + random.uniform(-2, 2))
                stream_delay = timedelta(minutes=flow_def["stream_delay_min"] + random.uniform(-1, 1))
                
                delay_hours = max(0, batch_delay.total_seconds() / 3600 - stream_delay.total_seconds() / 3600)
                cost = delay_hours * flow_def["delay_cost_per_hour"]
                
                events.append(InfoFlowEvent(
                    event_type=flow_def["type"],
                    source_dept=flow_def["source"],
                    target_dept=flow_def["target"],
                    event_time=event_time,
                    batch_delivery=event_time + batch_delay,
                    stream_delivery=event_time + stream_delay,
                    business_impact=flow_def["impact"],
                    cost_of_delay=cost
                ))
    
    return events


def analyze_delay_impact(events: list) -> dict:
    """分析延遲影響"""
    stats = defaultdict(lambda: {
        "count": 0,
        "total_batch_delay_hrs": 0,
        "total_stream_delay_min": 0,
        "total_cost_of_delay": 0,
        "max_batch_delay_hrs": 0
    })
    
    for e in events:
        s = stats[e.event_type]
        batch_hrs = (e.batch_delivery - e.event_time).total_seconds() / 3600
        stream_min = (e.stream_delivery - e.event_time).total_seconds() / 60
        
        s["count"] += 1
        s["total_batch_delay_hrs"] += batch_hrs
        s["total_stream_delay_min"] += stream_min
        s["total_cost_of_delay"] += e.cost_of_delay
        s["max_batch_delay_hrs"] = max(s["max_batch_delay_hrs"], batch_hrs)
    
    return dict(stats)


# ============================================================
# Part 3: 供應商 Scorecard 引擎
# ============================================================

class AVLStatus(Enum):
    ACTIVE = "Active"
    PROBATION = "Probation"
    SUSPENDED = "Suspended"
    REMOVED = "Removed"

class ScoreGrade(Enum):
    A = "A"
    A_MINUS = "A-"
    B_PLUS = "B+"
    B = "B"
    C = "C"
    D = "D"

@dataclass
class VendorScore:
    """供應商 Scorecard"""
    vendor_id: str
    vendor_name: str
    period: str  # e.g. "2026-Q1"
    
    # Quality (30%)
    ppm: float = 0.0
    quality_incidents: int = 0
    quality_score: float = 0.0
    
    # Delivery (30%)
    otif_pct: float = 0.0
    lt_std_dev_days: float = 0.0
    shortage_count: int = 0
    delivery_score: float = 0.0
    
    # Cost (25%)
    price_variance_pct: float = 0.0
    cost_down_achievement_pct: float = 0.0
    cost_score: float = 0.0
    
    # Service (15%)
    response_time_hrs: float = 0.0
    eco_compliance_pct: float = 0.0
    service_score: float = 0.0
    
    # Weighted total
    total_score: float = 0.0
    grade: str = ""
    avl_action: str = ""
    
    # Weights
    quality_weight: float = 0.30
    delivery_weight: float = 0.30
    cost_weight: float = 0.25
    service_weight: float = 0.15


def calculate_quality_score(ppm: float, incidents: int) -> float:
    """品質分數計算（PPM + 客訴事件）"""
    # PPM scoring: 0 PPM = 100, 200 PPM = 90, 500 PPM = 75, 1000 PPM = 50, >2000 = 0
    if ppm <= 0:
        ppm_score = 100
    elif ppm <= 100:
        ppm_score = 100 - (ppm / 100) * 5  # 0-100 PPM → 100-95
    elif ppm <= 200:
        ppm_score = 95 - (ppm - 100) / 100 * 5  # 100-200 → 95-90
    elif ppm <= 500:
        ppm_score = 90 - (ppm - 200) / 300 * 15  # 200-500 → 90-75
    elif ppm <= 1000:
        ppm_score = 75 - (ppm - 500) / 500 * 25  # 500-1000 → 75-50
    else:
        ppm_score = max(0, 50 - (ppm - 1000) / 1000 * 50)  # >1000 → 50-0
    
    # 客訴事件扣分（每件扣 5 分）
    incident_penalty = incidents * 5
    return max(0, min(100, ppm_score - incident_penalty))


def calculate_delivery_score(otif: float, lt_std: float, shortages: int) -> float:
    """交貨分數計算"""
    # OTIF scoring: 100% = 100, 95% = 90, 85% = 70, 75% = 50
    if otif >= 100:
        otif_score = 100
    elif otif >= 95:
        otif_score = 100 - (100 - otif) * 2  # 95-100 → 90-100
    elif otif >= 85:
        otif_score = 90 - (95 - otif) * 2  # 85-95 → 70-90
    elif otif >= 75:
        otif_score = 70 - (85 - otif) * 2  # 75-85 → 50-70
    else:
        otif_score = max(0, 50 - (75 - otif) * 3)
    
    # LT 穩定度扣分（σ > 3 天開始扣）
    lt_penalty = max(0, (lt_std - 3) * 5)
    # Shortage 扣分（每次扣 3 分）
    shortage_penalty = shortages * 3
    
    return max(0, min(100, otif_score - lt_penalty - shortage_penalty))


def calculate_cost_score(price_var: float, cost_down: float) -> float:
    """成本分數計算"""
    # Price variance: ≤0% = 100, 0-3% = 80-100, 3-5% = 60-80, >5% = <60
    if price_var <= 0:
        pv_score = 100
    elif price_var <= 3:
        pv_score = 100 - price_var * 6.67  # 0-3% → 100-80
    elif price_var <= 5:
        pv_score = 80 - (price_var - 3) * 10  # 3-5% → 80-60
    else:
        pv_score = max(0, 60 - (price_var - 5) * 10)
    
    # Cost down achievement: ≥100% = 100, 80% = 85, 50% = 60
    cd_score = min(100, cost_down * 0.8 + 20) if cost_down > 0 else 50
    
    return (pv_score * 0.6 + cd_score * 0.4)


def calculate_service_score(response_hrs: float, eco_compliance: float) -> float:
    """服務分數計算"""
    # Response time: <4h = 100, <8h = 90, <24h = 75, <48h = 50
    if response_hrs <= 4:
        resp_score = 100
    elif response_hrs <= 8:
        resp_score = 100 - (response_hrs - 4) * 2.5
    elif response_hrs <= 24:
        resp_score = 90 - (response_hrs - 8) * 0.94
    elif response_hrs <= 48:
        resp_score = 75 - (response_hrs - 24) * 1.04
    else:
        resp_score = max(0, 50 - (response_hrs - 48) * 0.5)
    
    # ECO compliance: direct mapping
    eco_score = eco_compliance
    
    return resp_score * 0.6 + eco_score * 0.4


def grade_from_score(score: float) -> tuple:
    """從分數判斷等級和 AVL 動作"""
    if score >= 95:
        return "A", "優先配額、LTA 續約"
    elif score >= 90:
        return "A-", "維持份額、考慮增加"
    elif score >= 85:
        return "B+", "維持現有份額"
    elif score >= 80:
        return "B", "維持、觀察"
    elif score >= 70:
        return "C", "發 CAP、減少份額、啟動替代認證"
    else:
        return "D", "轉 Probation、3個月改善期限"


def calculate_scorecard(vendor: VendorScore) -> VendorScore:
    """計算完整 Scorecard"""
    vendor.quality_score = calculate_quality_score(vendor.ppm, vendor.quality_incidents)
    vendor.delivery_score = calculate_delivery_score(vendor.otif_pct, vendor.lt_std_dev_days, vendor.shortage_count)
    vendor.cost_score = calculate_cost_score(vendor.price_variance_pct, vendor.cost_down_achievement_pct)
    vendor.service_score = calculate_service_score(vendor.response_time_hrs, vendor.eco_compliance_pct)
    
    vendor.total_score = (
        vendor.quality_score * vendor.quality_weight +
        vendor.delivery_score * vendor.delivery_weight +
        vendor.cost_score * vendor.cost_weight +
        vendor.service_score * vendor.service_weight
    )
    
    vendor.grade, vendor.avl_action = grade_from_score(vendor.total_score)
    return vendor


def simulate_avl_state_machine(vendor_id: str, quarterly_grades: list) -> list:
    """模擬 AVL 狀態機（根據連續季度 Scorecard 結果）"""
    state = AVLStatus.ACTIVE
    history = []
    
    for i, grade in enumerate(quarterly_grades):
        prev_state = state
        
        if grade in ("A", "A-", "B+", "B"):
            if state == AVLStatus.PROBATION:
                state = AVLStatus.ACTIVE  # 改善成功，恢復
            # Active 維持
        elif grade == "C":
            if state == AVLStatus.ACTIVE:
                state = AVLStatus.ACTIVE  # 第一次 C 不降級，但發 CAP
            elif state == AVLStatus.PROBATION:
                state = AVLStatus.SUSPENDED  # Probation + C → Suspended
        elif grade == "D":
            if state == AVLStatus.ACTIVE:
                state = AVLStatus.PROBATION  # D → Probation
            elif state == AVLStatus.PROBATION:
                state = AVLStatus.SUSPENDED  # 連續 D → Suspended
        
        action = ""
        if prev_state != state:
            action = f"狀態變更：{prev_state.value} → {state.value}"
        elif grade == "C":
            action = "發出 Corrective Action Plan (CAP)"
        elif grade == "D":
            action = "發出 Warning Letter"
        else:
            action = "正常維持"
        
        history.append({
            "quarter": f"Q{i+1}",
            "grade": grade,
            "prev_state": prev_state.value,
            "new_state": state.value,
            "action": action
        })
    
    return history


# ============================================================
# Main: 執行所有模擬
# ============================================================

def main():
    print("=" * 70)
    print("🏗️  ODM 供應鏈組織架構 — RACI + 資訊流延遲 + Scorecard 引擎")
    print("=" * 70)
    
    # ── Part 1: RACI 矩陣 ──
    print("\n" + "─" * 50)
    print("📋 Part 1: RACI 矩陣引擎")
    print("─" * 50)
    
    # 範例查詢
    for decision in ["插單接受", "催料(Expediting)", "ECO/BOM變更"]:
        approvers = get_approvers(decision)
        notif = get_notification_list(decision)
        print(f"\n  決策：{decision}")
        print(f"    審核者(A)：{approvers}")
        print(f"    需諮詢(C)：{notif['must_consult']}")
        print(f"    需通知(I)：{notif['must_inform']}")
        if notif['must_execute']:
            print(f"    需執行(R)：{notif['must_execute']}")
    
    # RBAC 生成
    rbac = generate_rbac_from_raci()
    print("\n  📊 RBAC 權限建議（從 RACI 自動生成）：")
    for dept in ["CF", "PMC", "Procurement"]:
        print(f"\n    {dept}:")
        print(f"      可審核：{rbac[dept]['approve']}")
        print(f"      可操作：{rbac[dept]['read_write']}")
        print(f"      可檢視：{rbac[dept]['read_only'][:3]}...")  # 只顯示前 3 個
    
    # ── Part 2: 資訊流延遲 ──
    print("\n" + "─" * 50)
    print("📡 Part 2: 資訊流延遲模擬（30 天）")
    print("─" * 50)
    
    events = simulate_info_flow(num_days=30)
    print(f"\n  總事件數：{len(events)}")
    
    stats = analyze_delay_impact(events)
    total_cost = 0
    
    print(f"\n  {'事件類型':<18} {'事件數':>6} {'平均Batch延遲':>12} {'平均Stream延遲':>14} {'延遲成本(USD)':>14}")
    print(f"  {'─'*18} {'─'*6} {'─'*12} {'─'*14} {'─'*14}")
    
    for event_type, s in sorted(stats.items(), key=lambda x: -x[1]["total_cost_of_delay"]):
        avg_batch = s["total_batch_delay_hrs"] / s["count"]
        avg_stream = s["total_stream_delay_min"] / s["count"]
        cost = s["total_cost_of_delay"]
        total_cost += cost
        
        print(f"  {event_type:<18} {s['count']:>6} {avg_batch:>10.1f}h {avg_stream:>12.1f}min ${cost:>12,.0f}")
    
    print(f"\n  🔴 Batch vs Streaming 的延遲成本差：${total_cost:,.0f} / 月")
    print(f"     → 年化成本：${total_cost * 12:,.0f}")
    print(f"     → 結論：對延遲敏感的資訊流（缺料/品質Hold），streaming 的 ROI 非常明顯")
    
    # ── Part 3: 供應商 Scorecard ──
    print("\n" + "─" * 50)
    print("📊 Part 3: 供應商 Scorecard 引擎")
    print("─" * 50)
    
    # 模擬 5 個供應商
    vendors_data = [
        VendorScore("V001", "Alpha Semiconductor", "2026-Q1",
                    ppm=80, quality_incidents=0, otif_pct=97.5, lt_std_dev_days=1.5,
                    shortage_count=0, price_variance_pct=-1.2, cost_down_achievement_pct=110,
                    response_time_hrs=6, eco_compliance_pct=98),
        VendorScore("V002", "Beta Connectors Ltd", "2026-Q1",
                    ppm=350, quality_incidents=1, otif_pct=88, lt_std_dev_days=4.2,
                    shortage_count=2, price_variance_pct=2.1, cost_down_achievement_pct=75,
                    response_time_hrs=18, eco_compliance_pct=90),
        VendorScore("V003", "Gamma Passive Corp", "2026-Q1",
                    ppm=152, quality_incidents=0, otif_pct=91, lt_std_dev_days=2.5,
                    shortage_count=1, price_variance_pct=-0.5, cost_down_achievement_pct=84,
                    response_time_hrs=12, eco_compliance_pct=95),
        VendorScore("V004", "Delta PCB Inc", "2026-Q1",
                    ppm=800, quality_incidents=3, otif_pct=72, lt_std_dev_days=6.8,
                    shortage_count=5, price_variance_pct=5.5, cost_down_achievement_pct=30,
                    response_time_hrs=48, eco_compliance_pct=70),
        VendorScore("V005", "Epsilon Memory Co", "2026-Q1",
                    ppm=25, quality_incidents=0, otif_pct=99, lt_std_dev_days=0.8,
                    shortage_count=0, price_variance_pct=-2.5, cost_down_achievement_pct=120,
                    response_time_hrs=3, eco_compliance_pct=100),
    ]
    
    scored_vendors = [calculate_scorecard(v) for v in vendors_data]
    scored_vendors.sort(key=lambda v: -v.total_score)
    
    print(f"\n  {'排名':>4} {'供應商':<22} {'品質':>5} {'交貨':>5} {'成本':>5} {'服務':>5} {'總分':>6} {'等級':>4} {'AVL動作'}")
    print(f"  {'─'*4} {'─'*22} {'─'*5} {'─'*5} {'─'*5} {'─'*5} {'─'*6} {'─'*4} {'─'*30}")
    
    for rank, v in enumerate(scored_vendors, 1):
        print(f"  {rank:>4} {v.vendor_name:<22} {v.quality_score:>5.1f} {v.delivery_score:>5.1f} "
              f"{v.cost_score:>5.1f} {v.service_score:>5.1f} {v.total_score:>6.1f} {v.grade:>4} {v.avl_action}")
    
    # Delta PCB 詳細 Scorecard
    delta = [v for v in scored_vendors if v.vendor_id == "V004"][0]
    print(f"\n  🔴 Red Alert — {delta.vendor_name} 詳細 Scorecard：")
    print(f"    品質 (30%): PPM={delta.ppm}, 客訴={delta.quality_incidents}件 → {delta.quality_score:.1f}/100")
    print(f"    交貨 (30%): OTIF={delta.otif_pct}%, LT σ={delta.lt_std_dev_days}天, Shortage={delta.shortage_count}次 → {delta.delivery_score:.1f}/100")
    print(f"    成本 (25%): Price Var={delta.price_variance_pct}%, Cost Down={delta.cost_down_achievement_pct}% → {delta.cost_score:.1f}/100")
    print(f"    服務 (15%): 回應={delta.response_time_hrs}h, ECO={delta.eco_compliance_pct}% → {delta.service_score:.1f}/100")
    print(f"    總分：{delta.total_score:.1f} → 等級 {delta.grade} → {delta.avl_action}")
    
    # ── Part 3b: AVL 狀態機模擬 ──
    print(f"\n  📈 AVL 狀態機模擬（Delta PCB — 4 季追蹤）")
    quarterly = ["D", "C", "D", "B+"]  # 模擬 4 季績效軌跡
    history = simulate_avl_state_machine("V004", quarterly)
    
    print(f"    {'季度':>4} {'等級':>4} {'原狀態':>12} → {'新狀態':>12}  {'動作'}")
    print(f"    {'─'*4} {'─'*4} {'─'*12}   {'─'*12}  {'─'*30}")
    for h in history:
        print(f"    {h['quarter']:>4} {h['grade']:>4} {h['prev_state']:>12} → {h['new_state']:>12}  {h['action']}")
    
    # ── Summary ──
    print("\n" + "=" * 70)
    print("📋 SA 洞察總結")
    print("=" * 70)
    print("""
  1. RACI 矩陣 → RBAC 權限設計的藍本
     Conway's Law：系統邊界 ≈ 組織邊界
     每個 A（Accountable）= 該 Bounded Context 的 System/Data Owner

  2. 資訊流延遲 → 技術選型的依據
     T+0 需求（缺料/品質Hold）→ Kafka + Flink/SSS
     T+1 需求（庫存報表）→ Airflow DAG + DWH
     月度需求（S&OP）→ 手動 + Excel/BI

  3. Scorecard 引擎 → Data Pipeline 設計
     Bronze: SAP EKPO/EKES/MKPF/QMEL
     Silver: fct_po_delivery / fct_quality_defect
     Gold: rpt_vendor_scorecard（月度 Batch）
     Action: AVL 狀態機自動觸發（SCD Type 2 追蹤）
    """)
    print("✅ 練習完成")


if __name__ == "__main__":
    main()
