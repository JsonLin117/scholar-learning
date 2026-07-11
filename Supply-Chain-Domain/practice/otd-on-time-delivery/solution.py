"""
ODM OTD（On-Time Delivery）計算與根因分析引擎
================================================
模擬緯穎規模 ODM 廠的 OTD Dashboard 後端邏輯：
- 多口徑 OTD 計算（訂單層級 vs 訂單行層級、CRD 基準 vs CSD 基準）
- 根因分類與風險評分（呼應 cf-core-pain-points 的三重壓力：短交期/插單/ECO）
- fact_otd_daily 每日聚合表模擬

依賴：純 Python，無外部套件
"""

from dataclasses import dataclass, field
from datetime import date, timedelta
from enum import Enum
from typing import Optional
import random
import statistics

# ─────────────────────────────────────────────
# 1. 資料模型
# ─────────────────────────────────────────────


class DelayReason(Enum):
    NONE = "none"
    SHORTAGE = "shortage"                  # 缺料
    CAPACITY_BOTTLENECK = "capacity"       # 產能瓶頸
    ECO_IMPACT = "eco_impact"              # 設計變更衝擊
    RUSH_ORDER_CONFLICT = "rush_conflict"  # 插單擠壓排程
    LOGISTICS_DELAY = "logistics"          # 3PL/國際物流延誤
    QUALITY_REWORK = "rework"              # 品質問題重工


@dataclass
class ShipmentLine:
    """訂單行層級的出貨紀錄"""
    po_id: str
    line_id: str
    customer: str
    crd: date               # Customer Request Date（客戶希望日）
    csd: date               # Confirmed Ship Date（供應商承諾日）
    actual_ship_date: Optional[date]
    delay_reason: DelayReason = DelayReason.NONE
    is_rush_order: bool = False
    active_eco_count: int = 0


@dataclass
class OTDResult:
    customer: str
    total_lines: int
    on_time_lines: int
    otd_pct: float
    basis: str  # "CSD" or "CRD"


# ─────────────────────────────────────────────
# 2. 模擬資料產生
# ─────────────────────────────────────────────

CUSTOMERS = ["CSP-MSFT", "CSP-GOOG", "CSP-META"]


def generate_shipments(n: int = 200, seed: int = 42) -> list[ShipmentLine]:
    rng = random.Random(seed)
    shipments = []
    base_date = date(2026, 7, 1)

    for i in range(n):
        customer = rng.choice(CUSTOMERS)
        crd = base_date + timedelta(days=rng.randint(0, 30))
        # CSD 通常比 CRD 晚幾天（供應商評估後的實際承諾）
        csd_gap = rng.choice([0, 0, 1, 2, 3])
        csd = crd + timedelta(days=csd_gap)

        is_rush = rng.random() < 0.15
        eco_count = rng.choice([0, 0, 0, 1, 2]) if rng.random() < 0.2 else 0

        # 根據風險因子決定延誤機率與原因
        risk_score = 0.05  # 基礎延誤率
        reason = DelayReason.NONE
        if is_rush:
            risk_score += 0.25
        if eco_count > 0:
            risk_score += 0.20 * eco_count

        delayed = rng.random() < min(risk_score, 0.85)
        if delayed:
            if eco_count > 0:
                reason = DelayReason.ECO_IMPACT
            elif is_rush:
                reason = DelayReason.RUSH_ORDER_CONFLICT
            else:
                reason = rng.choice(
                    [DelayReason.SHORTAGE, DelayReason.CAPACITY_BOTTLENECK,
                     DelayReason.LOGISTICS_DELAY, DelayReason.QUALITY_REWORK]
                )
            slip_days = rng.randint(1, 7)
            actual_ship_date = csd + timedelta(days=slip_days)
        else:
            actual_ship_date = csd - timedelta(days=rng.choice([0, 0, 1]))

        shipments.append(
            ShipmentLine(
                po_id=f"PO{1000+i//3}",
                line_id=f"L{i%3+1}",
                customer=customer,
                crd=crd,
                csd=csd,
                actual_ship_date=actual_ship_date,
                delay_reason=reason,
                is_rush_order=is_rush,
                active_eco_count=eco_count,
            )
        )
    return shipments


# ─────────────────────────────────────────────
# 3. OTD 計算（雙口徑：CSD 基準 vs CRD 基準）
# ─────────────────────────────────────────────


def calc_otd(shipments: list[ShipmentLine], basis: str = "CSD",
             tolerance_days: int = 0) -> list[OTDResult]:
    """
    basis: "CSD"（業界標準口徑，對供應商自己承諾的日期）或 "CRD"（客戶希望日，較嚴格）
    tolerance_days: 容忍窗口，0 = 嚴格不晚於基準日
    """
    by_customer: dict[str, list[ShipmentLine]] = {}
    for s in shipments:
        by_customer.setdefault(s.customer, []).append(s)

    results = []
    for customer, lines in sorted(by_customer.items()):
        total = len(lines)
        on_time = 0
        for s in lines:
            ref_date = s.csd if basis == "CSD" else s.crd
            gap = (s.actual_ship_date - ref_date).days
            if gap <= tolerance_days:
                on_time += 1
        pct = round(on_time / total * 100, 1) if total else 0.0
        results.append(OTDResult(customer, total, on_time, pct, basis))
    return results


def calc_otd_order_level(shipments: list[ShipmentLine], basis: str = "CSD") -> float:
    """
    訂單層級口徑：一張 PO 只要有任一行遲到，整張 PO 就算不準時。
    比行層級口徑更嚴格，示範「口徑決策點 4」對數字的巨大影響。
    """
    by_po: dict[str, list[ShipmentLine]] = {}
    for s in shipments:
        by_po.setdefault(s.po_id, []).append(s)

    on_time_pos = 0
    for po_id, lines in by_po.items():
        po_on_time = True
        for s in lines:
            ref_date = s.csd if basis == "CSD" else s.crd
            gap = (s.actual_ship_date - ref_date).days
            if gap > 0:
                po_on_time = False
                break
        if po_on_time:
            on_time_pos += 1
    return round(on_time_pos / len(by_po) * 100, 1) if by_po else 0.0


# ─────────────────────────────────────────────
# 4. 根因分析
# ─────────────────────────────────────────────


def root_cause_breakdown(shipments: list[ShipmentLine]) -> dict[str, int]:
    delayed = [s for s in shipments if s.actual_ship_date > s.csd]
    counter: dict[str, int] = {}
    for s in delayed:
        key = s.delay_reason.value
        counter[key] = counter.get(key, 0) + 1
    return dict(sorted(counter.items(), key=lambda kv: -kv[1]))


# ─────────────────────────────────────────────
# 5. 訂單風險評分（三重壓力綜合指標）
# ─────────────────────────────────────────────


def composite_risk_score(s: ShipmentLine) -> float:
    """
    模擬 cf-core-pain-points 提到的跨系統風險評分表：
    短交期壓力（CRD-CSD gap 越小越緊）+ 插單 + 活躍 ECO 數量
    """
    lead_time_pressure = 1.0 if (s.csd - s.crd).days <= 0 else 0.3
    rush_weight = 0.4 if s.is_rush_order else 0.0
    eco_weight = 0.15 * s.active_eco_count
    score = lead_time_pressure * 0.4 + rush_weight + eco_weight
    return round(min(score, 1.0), 2)


def high_risk_orders(shipments: list[ShipmentLine], threshold: float = 0.5) -> list[tuple[str, float]]:
    scored = [(s.po_id, composite_risk_score(s)) for s in shipments]
    # 同一張 PO 取最大風險分數
    max_by_po: dict[str, float] = {}
    for po_id, score in scored:
        max_by_po[po_id] = max(max_by_po.get(po_id, 0.0), score)
    high_risk = [(po, sc) for po, sc in max_by_po.items() if sc >= threshold]
    return sorted(high_risk, key=lambda kv: -kv[1])


# ─────────────────────────────────────────────
# 6. fact_otd_daily 每日聚合表模擬
# ─────────────────────────────────────────────


def build_fact_otd_daily(shipments: list[ShipmentLine]) -> list[dict]:
    """模擬每日批次跑出的預聚合表，Dashboard 只讀這張表，不重跑原始 JOIN"""
    by_key: dict[tuple[str, str], list[ShipmentLine]] = {}
    for s in shipments:
        key = (s.customer, s.delay_reason.value)
        by_key.setdefault(key, []).append(s)

    rows = []
    for (customer, reason), lines in sorted(by_key.items()):
        avg_slip = statistics.mean(
            [(s.actual_ship_date - s.csd).days for s in lines]
        )
        rows.append({
            "customer": customer,
            "delay_reason": reason,
            "line_count": len(lines),
            "avg_slip_days": round(avg_slip, 1),
        })
    return rows


# ─────────────────────────────────────────────
# 7. 主程式：驗證與展示
# ─────────────────────────────────────────────


def main():
    shipments = generate_shipments(n=200, seed=42)

    print("=" * 70)
    print("ODM OTD 計算與根因分析引擎 — 驗證輸出")
    print("=" * 70)

    print("\n[1] OTD by Customer — Line-level, CSD 基準（業界標準口徑）")
    for r in calc_otd(shipments, basis="CSD"):
        print(f"  {r.customer:12s} | lines={r.total_lines:3d} | on_time={r.on_time_lines:3d} | OTD={r.otd_pct}%")

    print("\n[2] OTD by Customer — Line-level, CRD 基準（客戶視角，通常更嚴格）")
    for r in calc_otd(shipments, basis="CRD"):
        print(f"  {r.customer:12s} | lines={r.total_lines:3d} | on_time={r.on_time_lines:3d} | OTD={r.otd_pct}%")

    otd_line_csd = calc_otd(shipments, basis="CSD")
    overall_line_otd = round(
        sum(r.on_time_lines for r in otd_line_csd) / sum(r.total_lines for r in otd_line_csd) * 100, 1
    )
    otd_order_csd = calc_otd_order_level(shipments, basis="CSD")
    print(f"\n[3] 口徑決策點 4 驗證：Line-level OTD={overall_line_otd}% vs Order-level OTD={otd_order_csd}%")
    print("    → Order-level 更嚴格（一行遲到全單算不準時），數字通常較低，符合預期")
    assert otd_order_csd <= overall_line_otd, "Order-level OTD 應該 <= Line-level OTD"

    print("\n[4] 根因分布（Root Cause Analysis）")
    for reason, count in root_cause_breakdown(shipments).items():
        print(f"  {reason:20s} : {count} 筆")

    print("\n[5] 高風險訂單（Composite Risk Score >= 0.5）— PMC 優先處理清單")
    high_risk = high_risk_orders(shipments, threshold=0.5)
    print(f"  共 {len(high_risk)} 張高風險 PO（前 5 筆）：")
    for po, score in high_risk[:5]:
        print(f"    {po}: risk_score={score}")

    print("\n[6] fact_otd_daily 預聚合表（示意，前 6 列）")
    fact_table = build_fact_otd_daily(shipments)
    for row in fact_table[:6]:
        print(f"  {row}")

    # ── 驗證區塊 ──
    print("\n" + "=" * 70)
    print("驗證檢查")
    print("=" * 70)
    total_lines = len(shipments)
    assert total_lines == 200, f"預期 200 筆出貨紀錄，實際 {total_lines}"
    assert all(0 <= r.otd_pct <= 100 for r in otd_line_csd), "OTD% 必須介於 0-100"
    assert len(fact_table) > 0, "fact_otd_daily 不應為空"
    print("✅ 所有驗證通過：資料量正確、OTD% 範圍合理、Order-level <= Line-level、fact table 非空")


if __name__ == "__main__":
    main()
