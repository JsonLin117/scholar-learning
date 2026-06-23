"""
ODM 訂單生命週期追蹤系統
========================
模擬 ODM 伺服器廠的完整訂單流程追蹤，從 RFQ 到收款。

核心功能：
1. Event Sourcing 訂單狀態追蹤
2. 多源資料整合（CRM / SAP / MES）
3. KPI 計算引擎（OTD / Win Rate / C2C / Quote Cycle）
4. 風險預警（CRD-CSD Gap / 齊料延遲 / 逾期應收）

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

class OrderStage(Enum):
    """訂單生命週期階段"""
    RFQ_RECEIVED = "01_RFQ_RECEIVED"
    QUOTE_SENT = "02_QUOTE_SENT"
    DESIGN_WIN = "03_DESIGN_WIN"
    DESIGN_LOSS = "03_DESIGN_LOSS"
    PO_RECEIVED = "04_PO_RECEIVED"
    MRP_PLANNED = "05_MRP_PLANNED"
    MATERIAL_READY = "06_MATERIAL_READY"
    PRODUCTION_START = "07_PRODUCTION_START"
    PRODUCTION_COMPLETE = "08_PRODUCTION_COMPLETE"
    SHIPPED = "09_SHIPPED"
    INVOICED = "10_INVOICED"
    PAYMENT_RECEIVED = "11_PAYMENT_RECEIVED"


@dataclass
class OrderEvent:
    """訂單事件（Event Sourcing 風格）"""
    order_id: str
    stage: OrderStage
    event_date: date
    source_system: str  # CRM / SAP / MES
    details: dict = field(default_factory=dict)


@dataclass
class OrderSnapshot:
    """訂單快照（從事件重建的最新狀態）"""
    order_id: str
    customer: str
    product_sku: str
    quantity: int
    current_stage: OrderStage
    
    # 時間戳
    rfq_date: Optional[date] = None
    quote_date: Optional[date] = None
    design_result: Optional[str] = None  # WIN / LOSS
    po_date: Optional[date] = None
    crd: Optional[date] = None
    csd: Optional[date] = None
    mrp_date: Optional[date] = None
    material_ready_date: Optional[date] = None
    production_start: Optional[date] = None
    production_complete: Optional[date] = None
    ship_date: Optional[date] = None
    invoice_date: Optional[date] = None
    payment_date: Optional[date] = None
    
    # 金額
    quote_amount: float = 0.0
    invoice_amount: float = 0.0


# ─────────────────────────────────────────────
# 2. 訂單生命週期引擎
# ─────────────────────────────────────────────

class OrderLifecycleEngine:
    """
    訂單生命週期管理引擎
    
    核心設計：
    - Event Sourcing：所有狀態變更都記錄為 Event
    - 從 Event 重建 Snapshot（最新狀態）
    - 支援跨系統事件合併
    """
    
    def __init__(self):
        self.events: list[OrderEvent] = []
        self.orders: dict[str, OrderSnapshot] = {}
    
    def register_order(self, order_id: str, customer: str, 
                       product_sku: str, quantity: int):
        """註冊新訂單"""
        self.orders[order_id] = OrderSnapshot(
            order_id=order_id,
            customer=customer,
            product_sku=product_sku,
            quantity=quantity,
            current_stage=OrderStage.RFQ_RECEIVED
        )
    
    def append_event(self, event: OrderEvent):
        """追加事件（Event Sourcing 核心）"""
        self.events.append(event)
        self._apply_event(event)
    
    def _apply_event(self, event: OrderEvent):
        """將事件應用到訂單快照"""
        order = self.orders.get(event.order_id)
        if not order:
            return
        
        order.current_stage = event.stage
        
        match event.stage:
            case OrderStage.RFQ_RECEIVED:
                order.rfq_date = event.event_date
            case OrderStage.QUOTE_SENT:
                order.quote_date = event.event_date
                order.quote_amount = event.details.get("amount", 0)
            case OrderStage.DESIGN_WIN:
                order.design_result = "WIN"
            case OrderStage.DESIGN_LOSS:
                order.design_result = "LOSS"
            case OrderStage.PO_RECEIVED:
                order.po_date = event.event_date
                order.crd = event.details.get("crd")
                order.csd = event.details.get("csd")
            case OrderStage.MRP_PLANNED:
                order.mrp_date = event.event_date
            case OrderStage.MATERIAL_READY:
                order.material_ready_date = event.event_date
            case OrderStage.PRODUCTION_START:
                order.production_start = event.event_date
            case OrderStage.PRODUCTION_COMPLETE:
                order.production_complete = event.event_date
            case OrderStage.SHIPPED:
                order.ship_date = event.event_date
            case OrderStage.INVOICED:
                order.invoice_date = event.event_date
                order.invoice_amount = event.details.get("amount", 0)
            case OrderStage.PAYMENT_RECEIVED:
                order.payment_date = event.event_date
    
    def get_event_history(self, order_id: str) -> list[OrderEvent]:
        """取得訂單的完整事件歷史"""
        return [e for e in self.events if e.order_id == order_id]


# ─────────────────────────────────────────────
# 3. KPI 計算引擎
# ─────────────────────────────────────────────

class KPIEngine:
    """訂單生命週期 KPI 計算"""
    
    def __init__(self, engine: OrderLifecycleEngine):
        self.engine = engine
    
    def quote_win_rate(self) -> dict:
        """Design Win Rate（報價勝率）"""
        quoted = [o for o in self.engine.orders.values() 
                  if o.design_result is not None]
        wins = [o for o in quoted if o.design_result == "WIN"]
        total = len(quoted)
        return {
            "total_quoted": total,
            "wins": len(wins),
            "win_rate": len(wins) / total * 100 if total > 0 else 0,
        }
    
    def quote_cycle_time(self) -> dict:
        """報價週期分析（RFQ → Quote）"""
        cycles = []
        for o in self.engine.orders.values():
            if o.rfq_date and o.quote_date:
                cycles.append((o.quote_date - o.rfq_date).days)
        
        if not cycles:
            return {"avg": 0, "p50": 0, "p90": 0, "count": 0}
        
        cycles.sort()
        return {
            "count": len(cycles),
            "avg": round(statistics.mean(cycles), 1),
            "p50": cycles[len(cycles) // 2],
            "p90": cycles[int(len(cycles) * 0.9)],
            "min": min(cycles),
            "max": max(cycles),
        }
    
    def on_time_delivery(self) -> dict:
        """OTD（On-Time Delivery）分析"""
        shipped = [o for o in self.engine.orders.values() 
                   if o.ship_date and o.crd]
        on_time = [o for o in shipped if o.ship_date <= o.crd]
        late = [o for o in shipped if o.ship_date > o.crd]
        
        late_days = [(o.ship_date - o.crd).days for o in late]
        
        return {
            "total_shipped": len(shipped),
            "on_time": len(on_time),
            "late": len(late),
            "otd_rate": len(on_time) / len(shipped) * 100 if shipped else 0,
            "avg_late_days": round(statistics.mean(late_days), 1) if late_days else 0,
            "max_late_days": max(late_days) if late_days else 0,
        }
    
    def crd_csd_gap_analysis(self) -> dict:
        """CRD vs CSD Gap 分析"""
        gaps = []
        for o in self.engine.orders.values():
            if o.crd and o.csd:
                gap = (o.csd - o.crd).days
                gaps.append({"order_id": o.order_id, "gap_days": gap,
                             "customer": o.customer})
        
        if not gaps:
            return {"count": 0, "at_risk": []}
        
        at_risk = [g for g in gaps if g["gap_days"] > 7]
        return {
            "count": len(gaps),
            "avg_gap": round(statistics.mean([g["gap_days"] for g in gaps]), 1),
            "at_risk_count": len(at_risk),
            "at_risk_orders": at_risk[:5],  # 前 5 筆高風險
        }
    
    def cash_to_cash(self) -> dict:
        """Cash-to-Cash Cycle Time"""
        c2c_days = []
        for o in self.engine.orders.values():
            if o.ship_date and o.payment_date:
                c2c = (o.payment_date - o.ship_date).days
                c2c_days.append(c2c)
        
        if not c2c_days:
            return {"avg": 0, "count": 0}
        
        return {
            "count": len(c2c_days),
            "avg_days": round(statistics.mean(c2c_days), 1),
            "p50": sorted(c2c_days)[len(c2c_days) // 2],
            "min": min(c2c_days),
            "max": max(c2c_days),
        }
    
    def material_readiness(self) -> dict:
        """齊料率分析（備料階段）"""
        planned = [o for o in self.engine.orders.values() 
                   if o.mrp_date and o.production_start]
        
        # 齊料延遲 = material_ready_date > production_start（應該在開工前齊料）
        delayed = []
        for o in planned:
            if o.material_ready_date and o.production_start:
                if o.material_ready_date > o.production_start:
                    delay = (o.material_ready_date - o.production_start).days
                    delayed.append({"order_id": o.order_id, "delay_days": delay})
        
        return {
            "total_planned": len(planned),
            "material_delayed": len(delayed),
            "readiness_rate": (1 - len(delayed) / len(planned)) * 100 if planned else 0,
            "delayed_orders": delayed[:5],
        }
    
    def order_to_ship_time(self) -> dict:
        """接單到出貨時間分析"""
        o2s = []
        for o in self.engine.orders.values():
            if o.po_date and o.ship_date:
                o2s.append((o.ship_date - o.po_date).days)
        
        if not o2s:
            return {"avg": 0, "count": 0}
        
        o2s.sort()
        return {
            "count": len(o2s),
            "avg_days": round(statistics.mean(o2s), 1),
            "p50": o2s[len(o2s) // 2],
            "p90": o2s[int(len(o2s) * 0.9)],
        }


# ─────────────────────────────────────────────
# 4. 風險預警引擎
# ─────────────────────────────────────────────

class RiskAlertEngine:
    """訂單風險預警"""
    
    def __init__(self, engine: OrderLifecycleEngine, today: date):
        self.engine = engine
        self.today = today
    
    def check_all(self) -> list[dict]:
        """執行所有風險檢查"""
        alerts = []
        alerts.extend(self._check_crd_approaching())
        alerts.extend(self._check_material_not_ready())
        alerts.extend(self._check_overdue_ar())
        alerts.extend(self._check_stalled_orders())
        return alerts
    
    def _check_crd_approaching(self) -> list[dict]:
        """CRD 即將到期但尚未出貨"""
        alerts = []
        for o in self.engine.orders.values():
            if o.crd and not o.ship_date:
                days_to_crd = (o.crd - self.today).days
                if days_to_crd <= 14 and days_to_crd >= 0:
                    alerts.append({
                        "type": "CRD_APPROACHING",
                        "severity": "RED" if days_to_crd <= 3 else "YELLOW",
                        "order_id": o.order_id,
                        "customer": o.customer,
                        "crd": o.crd.isoformat(),
                        "days_remaining": days_to_crd,
                        "current_stage": o.current_stage.value,
                    })
                elif days_to_crd < 0:
                    alerts.append({
                        "type": "CRD_OVERDUE",
                        "severity": "RED",
                        "order_id": o.order_id,
                        "customer": o.customer,
                        "crd": o.crd.isoformat(),
                        "days_overdue": abs(days_to_crd),
                        "current_stage": o.current_stage.value,
                    })
        return alerts
    
    def _check_material_not_ready(self) -> list[dict]:
        """已計劃生產但物料未齊"""
        alerts = []
        for o in self.engine.orders.values():
            if (o.mrp_date and not o.material_ready_date 
                and o.current_stage.value <= OrderStage.MRP_PLANNED.value):
                days_since_mrp = (self.today - o.mrp_date).days
                if days_since_mrp > 30:
                    alerts.append({
                        "type": "MATERIAL_NOT_READY",
                        "severity": "YELLOW",
                        "order_id": o.order_id,
                        "customer": o.customer,
                        "days_since_mrp": days_since_mrp,
                    })
        return alerts
    
    def _check_overdue_ar(self) -> list[dict]:
        """應收帳款逾期"""
        alerts = []
        for o in self.engine.orders.values():
            if o.invoice_date and not o.payment_date:
                days_outstanding = (self.today - o.invoice_date).days
                if days_outstanding > 60:
                    alerts.append({
                        "type": "AR_OVERDUE",
                        "severity": "RED" if days_outstanding > 90 else "YELLOW",
                        "order_id": o.order_id,
                        "customer": o.customer,
                        "invoice_date": o.invoice_date.isoformat(),
                        "days_outstanding": days_outstanding,
                        "amount": o.invoice_amount,
                    })
        return alerts
    
    def _check_stalled_orders(self) -> list[dict]:
        """卡住的訂單（超過 14 天沒有狀態變更）"""
        alerts = []
        for o in self.engine.orders.values():
            if o.current_stage in (OrderStage.DESIGN_LOSS, 
                                    OrderStage.PAYMENT_RECEIVED):
                continue  # 終態不檢查
            
            events = self.engine.get_event_history(o.order_id)
            if events:
                last_event_date = max(e.event_date for e in events)
                days_stalled = (self.today - last_event_date).days
                if days_stalled > 14:
                    alerts.append({
                        "type": "ORDER_STALLED",
                        "severity": "YELLOW",
                        "order_id": o.order_id,
                        "customer": o.customer,
                        "current_stage": o.current_stage.value,
                        "days_stalled": days_stalled,
                    })
        return alerts


# ─────────────────────────────────────────────
# 5. 模擬資料生成與執行
# ─────────────────────────────────────────────

def generate_simulation_data(engine: OrderLifecycleEngine, 
                              base_date: date, num_orders: int = 50):
    """生成模擬的 ODM 訂單資料"""
    
    customers = [
        ("Microsoft Azure", "SV7220G3-GPU-8x"),
        ("Google Cloud", "SV5360G3-TPU-4x"),
        ("Meta AI Research", "SV7220G3-GPU-8x"),
        ("Amazon AWS", "SV3220G3-CPU-2S"),
        ("Oracle Cloud", "SV3220G3-CPU-2S"),
    ]
    
    random.seed(42)
    
    for i in range(num_orders):
        order_id = f"SO-2026-{1000 + i:04d}"
        customer, sku = random.choice(customers)
        quantity = random.choice([50, 100, 200, 500, 1000])
        
        # 註冊訂單
        engine.register_order(order_id, customer, sku, quantity)
        
        # ── 階段 1：RFQ ──
        rfq_date = base_date + timedelta(days=random.randint(0, 60))
        engine.append_event(OrderEvent(
            order_id=order_id, stage=OrderStage.RFQ_RECEIVED,
            event_date=rfq_date, source_system="CRM"
        ))
        
        # ── 階段 2：報價（7-21 天後）──
        quote_days = random.randint(7, 28)
        quote_date = rfq_date + timedelta(days=quote_days)
        unit_price = random.uniform(5000, 80000)  # USD per unit
        engine.append_event(OrderEvent(
            order_id=order_id, stage=OrderStage.QUOTE_SENT,
            event_date=quote_date, source_system="CRM",
            details={"amount": round(unit_price * quantity, 2)}
        ))
        
        # ── 階段 3：Design Win/Loss（60% win rate）──
        if random.random() < 0.6:
            engine.append_event(OrderEvent(
                order_id=order_id, stage=OrderStage.DESIGN_WIN,
                event_date=quote_date + timedelta(days=random.randint(7, 30)),
                source_system="CRM"
            ))
        else:
            engine.append_event(OrderEvent(
                order_id=order_id, stage=OrderStage.DESIGN_LOSS,
                event_date=quote_date + timedelta(days=random.randint(7, 30)),
                source_system="CRM"
            ))
            continue  # Design Loss 就結束
        
        # ── 階段 4：PO（Win 後 14-45 天）──
        po_date = quote_date + timedelta(days=random.randint(21, 60))
        crd = po_date + timedelta(days=random.randint(45, 90))
        csd_gap = random.randint(-3, 14)  # CSD 可能比 CRD 早或晚
        csd = crd + timedelta(days=csd_gap)
        
        engine.append_event(OrderEvent(
            order_id=order_id, stage=OrderStage.PO_RECEIVED,
            event_date=po_date, source_system="SAP",
            details={"crd": crd, "csd": csd}
        ))
        
        # ── 階段 5：MRP（PO 後 1-3 天）──
        mrp_date = po_date + timedelta(days=random.randint(1, 3))
        engine.append_event(OrderEvent(
            order_id=order_id, stage=OrderStage.MRP_PLANNED,
            event_date=mrp_date, source_system="SAP"
        ))
        
        # 有些訂單還在備料中（模擬進行中的訂單）
        if random.random() < 0.15:
            continue
        
        # ── 階段 6：齊料（MRP 後 21-56 天，GPU 訂單更久）──
        is_gpu = "GPU" in sku
        mat_days = random.randint(35, 70) if is_gpu else random.randint(21, 42)
        mat_ready = mrp_date + timedelta(days=mat_days)
        engine.append_event(OrderEvent(
            order_id=order_id, stage=OrderStage.MATERIAL_READY,
            event_date=mat_ready, source_system="SAP"
        ))
        
        # ── 階段 7：生產（齊料後 2-5 天開始，7-21 天完成）──
        prod_start = mat_ready + timedelta(days=random.randint(1, 5))
        prod_days = random.randint(7, 21)
        prod_end = prod_start + timedelta(days=prod_days)
        
        engine.append_event(OrderEvent(
            order_id=order_id, stage=OrderStage.PRODUCTION_START,
            event_date=prod_start, source_system="MES"
        ))
        engine.append_event(OrderEvent(
            order_id=order_id, stage=OrderStage.PRODUCTION_COMPLETE,
            event_date=prod_end, source_system="MES"
        ))
        
        # 有些訂單還在生產中
        if random.random() < 0.1:
            continue
        
        # ── 階段 8：出貨（完成後 2-5 天）──
        ship_date = prod_end + timedelta(days=random.randint(2, 5))
        # 有機率延遲（模擬真實情況）
        if random.random() < 0.25:
            ship_date = ship_date + timedelta(days=random.randint(3, 15))
        
        engine.append_event(OrderEvent(
            order_id=order_id, stage=OrderStage.SHIPPED,
            event_date=ship_date, source_system="SAP"
        ))
        
        # ── 階段 9：開票（出貨當天）──
        engine.append_event(OrderEvent(
            order_id=order_id, stage=OrderStage.INVOICED,
            event_date=ship_date, source_system="SAP",
            details={"amount": round(unit_price * quantity, 2)}
        ))
        
        # 部分訂單已收款
        if random.random() < 0.6:
            payment_days = random.choice([30, 45, 60])
            payment_date = ship_date + timedelta(days=payment_days)
            engine.append_event(OrderEvent(
                order_id=order_id, stage=OrderStage.PAYMENT_RECEIVED,
                event_date=payment_date, source_system="SAP"
            ))


def print_section(title: str):
    """列印區段標題"""
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


def main():
    # 初始化引擎
    engine = OrderLifecycleEngine()
    base_date = date(2026, 1, 1)
    today = date(2026, 6, 24)
    
    # 生成模擬資料
    generate_simulation_data(engine, base_date, num_orders=50)
    
    print_section("ODM 訂單生命週期追蹤系統")
    print(f"模擬日期：{today}")
    print(f"訂單總數：{len(engine.orders)}")
    print(f"事件總數：{len(engine.events)}")
    
    # ─── KPI Dashboard ───
    kpi = KPIEngine(engine)
    
    # 1. Design Win Rate
    print_section("📊 KPI #1: Design Win Rate（報價勝率）")
    wr = kpi.quote_win_rate()
    print(f"  總報價數：{wr['total_quoted']}")
    print(f"  Design Win：{wr['wins']}")
    print(f"  Win Rate：{wr['win_rate']:.1f}%")
    
    # 2. Quote Cycle Time
    print_section("📊 KPI #2: Quote Cycle Time（報價週期）")
    qct = kpi.quote_cycle_time()
    print(f"  樣本數：{qct['count']}")
    print(f"  平均：{qct['avg']} 天")
    print(f"  P50：{qct['p50']} 天")
    print(f"  P90：{qct['p90']} 天")
    print(f"  範圍：{qct['min']}-{qct['max']} 天")
    
    # 3. OTD
    print_section("📊 KPI #3: On-Time Delivery（準時交貨率）")
    otd = kpi.on_time_delivery()
    print(f"  已出貨：{otd['total_shipped']}")
    print(f"  準時：{otd['on_time']}（{otd['otd_rate']:.1f}%）")
    print(f"  遲交：{otd['late']}")
    if otd['late'] > 0:
        print(f"  平均遲交天數：{otd['avg_late_days']} 天")
        print(f"  最大遲交天數：{otd['max_late_days']} 天")
    
    # 4. CRD-CSD Gap
    print_section("📊 KPI #4: CRD-CSD Gap（交期承諾差距）")
    gap = kpi.crd_csd_gap_analysis()
    print(f"  有 CRD/CSD 的訂單：{gap['count']}")
    print(f"  平均 Gap：{gap['avg_gap']} 天")
    print(f"  高風險（Gap > 7 天）：{gap['at_risk_count']}")
    if gap['at_risk_orders']:
        print("  高風險訂單：")
        for r in gap['at_risk_orders'][:3]:
            print(f"    {r['order_id']} | {r['customer']} | Gap: {r['gap_days']} 天")
    
    # 5. Order-to-Ship
    print_section("📊 KPI #5: Order-to-Ship Time（接單到出貨）")
    o2s = kpi.order_to_ship_time()
    print(f"  樣本數：{o2s['count']}")
    print(f"  平均：{o2s['avg_days']} 天")
    print(f"  P50：{o2s['p50']} 天")
    print(f"  P90：{o2s['p90']} 天")
    
    # 6. Cash-to-Cash
    print_section("📊 KPI #6: Cash-to-Cash Cycle（收款週期）")
    c2c = kpi.cash_to_cash()
    print(f"  已收款訂單：{c2c['count']}")
    print(f"  平均收款天數：{c2c['avg_days']} 天")
    print(f"  P50：{c2c['p50']} 天")
    
    # 7. Material Readiness
    print_section("📊 KPI #7: Material Readiness（齊料率）")
    mr = kpi.material_readiness()
    print(f"  已計劃訂單：{mr['total_planned']}")
    print(f"  齊料延遲：{mr['material_delayed']}")
    print(f"  齊料率：{mr['readiness_rate']:.1f}%")
    
    # ─── 風險預警 ───
    print_section("🚨 風險預警")
    risk = RiskAlertEngine(engine, today)
    alerts = risk.check_all()
    
    if not alerts:
        print("  ✅ 無風險警報")
    else:
        by_severity = {"RED": [], "YELLOW": []}
        for a in alerts:
            by_severity[a["severity"]].append(a)
        
        print(f"  🔴 RED 警報：{len(by_severity['RED'])}")
        for a in by_severity['RED'][:5]:
            print(f"    [{a['type']}] {a['order_id']} | {a['customer']}")
            if 'days_remaining' in a:
                print(f"      CRD 剩 {a['days_remaining']} 天，目前在 {a['current_stage']}")
            elif 'days_overdue' in a:
                print(f"      CRD 逾期 {a['days_overdue']} 天")
            elif 'days_outstanding' in a:
                print(f"      應收帳款逾期 {a['days_outstanding']} 天，金額 ${a['amount']:,.0f}")
        
        print(f"\n  🟡 YELLOW 警報：{len(by_severity['YELLOW'])}")
        for a in by_severity['YELLOW'][:5]:
            print(f"    [{a['type']}] {a['order_id']} | {a['customer']}")
    
    # ─── 訂單事件歷史範例 ───
    print_section("📜 訂單事件歷史（範例）")
    # 找一個完整的已付款訂單
    completed = [o for o in engine.orders.values() 
                 if o.current_stage == OrderStage.PAYMENT_RECEIVED]
    if completed:
        sample = completed[0]
        print(f"  訂單：{sample.order_id}")
        print(f"  客戶：{sample.customer}")
        print(f"  產品：{sample.product_sku} × {sample.quantity}")
        print(f"  金額：${sample.invoice_amount:,.0f}")
        print()
        events = engine.get_event_history(sample.order_id)
        for e in events:
            print(f"  {e.event_date} | [{e.source_system:3s}] {e.stage.value}")
        
        print(f"\n  --- 時間指標 ---")
        if sample.rfq_date and sample.quote_date:
            print(f"  報價週期：{(sample.quote_date - sample.rfq_date).days} 天")
        if sample.po_date and sample.ship_date:
            print(f"  接單到出貨：{(sample.ship_date - sample.po_date).days} 天")
        if sample.crd and sample.ship_date:
            otd_status = "✅ 準時" if sample.ship_date <= sample.crd else f"❌ 遲交 {(sample.ship_date - sample.crd).days} 天"
            print(f"  OTD：{otd_status}")
        if sample.ship_date and sample.payment_date:
            print(f"  收款天數：{(sample.payment_date - sample.ship_date).days} 天")
    
    # ─── 跨系統整合分析 ───
    print_section("🔗 跨系統資料整合概覽")
    source_counts = {}
    for e in engine.events:
        source_counts[e.source_system] = source_counts.get(e.source_system, 0) + 1
    
    print("  各系統事件分佈：")
    for src, cnt in sorted(source_counts.items()):
        pct = cnt / len(engine.events) * 100
        bar = "█" * int(pct / 2)
        print(f"    {src:5s} | {cnt:4d} ({pct:5.1f}%) {bar}")
    
    print("\n  資料整合點：")
    print("  ┌─────────┐     ┌─────────┐     ┌─────────┐")
    print("  │   CRM   │────▶│   SAP   │────▶│   MES   │")
    print("  │ RFQ     │     │ PO/MRP  │     │ 生產追蹤│")
    print("  │ Quote   │     │ GI/AR   │     │ WIP 狀態│")
    print("  │ Win/Loss│     │ Invoice │     │ 品質資料│")
    print("  └─────────┘     └─────────┘     └─────────┘")
    print(f"     {source_counts.get('CRM',0)} events     {source_counts.get('SAP',0)} events     {source_counts.get('MES',0)} events")
    print()
    print("  Join Key: order_id（統一訂單編號）")
    print("  整合方式：Event Sourcing → 時間序列合併 → 統一 Snapshot")
    
    print(f"\n{'='*60}")
    print("  ✅ ODM 訂單生命週期追蹤系統 — 模擬完成")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
