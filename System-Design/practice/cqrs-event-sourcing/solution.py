"""
CQRS & Event Sourcing — ODM 採購訂單追蹤 + 良率/OEE 計算
學習日期：2026-07-01

Demo 1: Event Store + Rehydration
Demo 2: CQRS Read Models (Projections)  
Demo 3: Compensating Events
Demo 4: Snapshot Optimization
Demo 5: OEE & FPY Calculator (SC Daily #43)
Demo 6: Integrated ODM Scenario (CQRS + Yield)
"""

from __future__ import annotations
import json
import hashlib
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from typing import Any
from collections import defaultdict
import math

# ═══════════════════════════════════════════════════
# Demo 1: Event Store + Rehydration
# ═══════════════════════════════════════════════════

@dataclass
class Event:
    aggregate_id: str
    event_type: str
    data: dict
    timestamp: str
    version: int  # stream version for optimistic concurrency

class EventStore:
    """Append-only event store with optimistic concurrency."""
    
    def __init__(self):
        self._store: dict[str, list[Event]] = defaultdict(list)
        self._snapshots: dict[str, tuple[int, dict]] = {}  # agg_id -> (version, state)
    
    def append(self, event: Event) -> None:
        stream = self._store[event.aggregate_id]
        # Optimistic concurrency: version must be sequential
        expected_version = len(stream) + 1
        if event.version != expected_version:
            raise ConcurrencyError(
                f"Expected version {expected_version}, got {event.version}. "
                f"Another process modified aggregate '{event.aggregate_id}'."
            )
        stream.append(event)
    
    def get_stream(self, aggregate_id: str, after_version: int = 0) -> list[Event]:
        return [e for e in self._store[aggregate_id] if e.version > after_version]
    
    def get_all_events(self) -> list[Event]:
        all_events = []
        for stream in self._store.values():
            all_events.extend(stream)
        all_events.sort(key=lambda e: e.timestamp)
        return all_events
    
    def save_snapshot(self, aggregate_id: str, version: int, state: dict) -> None:
        self._snapshots[aggregate_id] = (version, state)
    
    def get_snapshot(self, aggregate_id: str) -> tuple[int, dict] | None:
        return self._snapshots.get(aggregate_id)

class ConcurrencyError(Exception):
    pass

# --- PO Aggregate (Domain Model) ---

class PurchaseOrderAggregate:
    """Write-side: PO domain model with event sourcing."""
    
    VALID_TRANSITIONS = {
        None: {"POCreated"},
        "created": {"POApproved", "POCancelled"},
        "approved": {"GoodsReceived", "POCancelled"},
        "partially_received": {"GoodsReceived", "QualityInspected"},
        "received": {"QualityInspected", "InvoiceMatched"},  # InvoiceMatched w/o QC = anomaly
        "inspected": {"InvoiceMatched"},
        "invoice_matched": {"PaymentReleased"},
        "cancelled": set(),
        "paid": set(),
    }
    
    def __init__(self):
        self.po_id: str | None = None
        self.status: str | None = None
        self.vendor: str | None = None
        self.items: list = []
        self.total_amount: float = 0
        self.received_qty: int = 0
        self.ordered_qty: int = 0
        self.defect_rate: float | None = None
        self.version: int = 0
        self._pending_events: list[Event] = []
    
    @classmethod
    def rehydrate(cls, events: list[Event]) -> "PurchaseOrderAggregate":
        agg = cls()
        for event in events:
            agg._apply(event)
        return agg
    
    @classmethod
    def from_snapshot(cls, snapshot_state: dict, version: int) -> "PurchaseOrderAggregate":
        agg = cls()
        agg.po_id = snapshot_state["po_id"]
        agg.status = snapshot_state["status"]
        agg.vendor = snapshot_state["vendor"]
        agg.items = snapshot_state["items"]
        agg.total_amount = snapshot_state["total_amount"]
        agg.received_qty = snapshot_state["received_qty"]
        agg.ordered_qty = snapshot_state["ordered_qty"]
        agg.defect_rate = snapshot_state.get("defect_rate")
        agg.version = version
        return agg
    
    def to_snapshot(self) -> dict:
        return {
            "po_id": self.po_id, "status": self.status, "vendor": self.vendor,
            "items": self.items, "total_amount": self.total_amount,
            "received_qty": self.received_qty, "ordered_qty": self.ordered_qty,
            "defect_rate": self.defect_rate,
        }
    
    def _apply(self, event: Event) -> None:
        handler = getattr(self, f"_on_{event.event_type}", None)
        if handler:
            handler(event.data)
        self.version = event.version
    
    def _raise_event(self, event_type: str, data: dict, timestamp: str) -> None:
        allowed = self.VALID_TRANSITIONS.get(self.status, set())
        if event_type not in allowed:
            raise InvalidTransitionError(
                f"Cannot transition from '{self.status}' via '{event_type}'. "
                f"Allowed: {allowed}"
            )
        event = Event(
            aggregate_id=self.po_id or data.get("po_id", ""),
            event_type=event_type,
            data=data,
            timestamp=timestamp,
            version=self.version + 1,
        )
        self._apply(event)
        self._pending_events.append(event)
    
    def get_pending_events(self) -> list[Event]:
        events = self._pending_events[:]
        self._pending_events.clear()
        return events
    
    # --- Command handlers ---
    def create(self, po_id: str, vendor: str, items: list, total: float, ts: str):
        self._raise_event("POCreated", {
            "po_id": po_id, "vendor": vendor, "items": items, "total_amount": total
        }, ts)
    
    def approve(self, approver: str, ts: str):
        self._raise_event("POApproved", {"approver": approver}, ts)
    
    def receive_goods(self, qty: int, location: str, ts: str):
        self._raise_event("GoodsReceived", {"qty": qty, "location": location}, ts)
    
    def inspect_quality(self, result: str, defect_rate: float, ts: str):
        self._raise_event("QualityInspected", {
            "result": result, "defect_rate": defect_rate
        }, ts)
    
    def match_invoice(self, invoice_no: str, ts: str):
        self._raise_event("InvoiceMatched", {"invoice_no": invoice_no}, ts)
    
    def release_payment(self, amount: float, ts: str):
        self._raise_event("PaymentReleased", {"amount": amount}, ts)
    
    def cancel(self, reason: str, ts: str):
        self._raise_event("POCancelled", {"reason": reason}, ts)
    
    # --- Event handlers (state transitions) ---
    def _on_POCreated(self, data):
        self.po_id = data["po_id"]
        self.status = "created"
        self.vendor = data["vendor"]
        self.items = data["items"]
        self.total_amount = data["total_amount"]
        self.ordered_qty = sum(i.get("qty", 0) for i in data["items"])
    
    def _on_POApproved(self, data):
        self.status = "approved"
    
    def _on_GoodsReceived(self, data):
        self.received_qty += data["qty"]
        self.status = "received" if self.received_qty >= self.ordered_qty else "partially_received"
    
    def _on_QualityInspected(self, data):
        self.defect_rate = data["defect_rate"]
        self.status = "inspected"
    
    def _on_InvoiceMatched(self, data):
        self.status = "invoice_matched"
    
    def _on_PaymentReleased(self, data):
        self.status = "paid"
    
    def _on_POCancelled(self, data):
        self.status = "cancelled"

class InvalidTransitionError(Exception):
    pass


# ═══════════════════════════════════════════════════
# Demo 2: CQRS Read Models (Projections)
# ═══════════════════════════════════════════════════

class POListProjection:
    """Read Model 1: PO 概覽列表（反正規化的 flat view）"""
    
    def __init__(self):
        self.po_list: dict[str, dict] = {}
    
    def handle(self, event: Event) -> None:
        po_id = event.aggregate_id
        if event.event_type == "POCreated":
            self.po_list[po_id] = {
                "po_id": po_id,
                "vendor": event.data["vendor"],
                "total": event.data["total_amount"],
                "status": "created",
                "created_at": event.timestamp,
                "item_count": len(event.data["items"]),
            }
        elif po_id in self.po_list:
            if event.event_type == "POApproved":
                self.po_list[po_id]["status"] = "approved"
            elif event.event_type == "GoodsReceived":
                self.po_list[po_id]["status"] = "received"
            elif event.event_type == "POCancelled":
                self.po_list[po_id]["status"] = "cancelled"
            elif event.event_type == "PaymentReleased":
                self.po_list[po_id]["status"] = "paid"

class VendorScoreProjection:
    """Read Model 2: 供應商績效統計"""
    
    def __init__(self):
        self.vendors: dict[str, dict] = defaultdict(lambda: {
            "total_pos": 0, "cancelled_pos": 0, "total_amount": 0,
            "defect_rates": [], "avg_defect_rate": None,
            "gr_to_payment_days": [],
        })
        self._po_vendors: dict[str, str] = {}
        self._po_gr_dates: dict[str, str] = {}
    
    def handle(self, event: Event) -> None:
        po_id = event.aggregate_id
        if event.event_type == "POCreated":
            vendor = event.data["vendor"]
            self._po_vendors[po_id] = vendor
            self.vendors[vendor]["total_pos"] += 1
            self.vendors[vendor]["total_amount"] += event.data["total_amount"]
        elif event.event_type == "GoodsReceived":
            self._po_gr_dates[po_id] = event.timestamp
        elif event.event_type == "QualityInspected":
            vendor = self._po_vendors.get(po_id)
            if vendor:
                self.vendors[vendor]["defect_rates"].append(event.data["defect_rate"])
                rates = self.vendors[vendor]["defect_rates"]
                self.vendors[vendor]["avg_defect_rate"] = sum(rates) / len(rates)
        elif event.event_type == "PaymentReleased":
            vendor = self._po_vendors.get(po_id)
            gr_date = self._po_gr_dates.get(po_id)
            if vendor and gr_date:
                gr = datetime.fromisoformat(gr_date)
                pay = datetime.fromisoformat(event.timestamp)
                days = (pay - gr).days
                self.vendors[vendor]["gr_to_payment_days"].append(days)
        elif event.event_type == "POCancelled":
            vendor = self._po_vendors.get(po_id)
            if vendor:
                self.vendors[vendor]["cancelled_pos"] += 1

class AnomalyDetectionProjection:
    """Read Model 3: 異常偵測（跳過 QC 的 PO、高不良率供應商）"""
    
    def __init__(self):
        self.alerts: list[dict] = []
        self._po_events: dict[str, set] = defaultdict(set)
    
    def handle(self, event: Event) -> None:
        po_id = event.aggregate_id
        self._po_events[po_id].add(event.event_type)
        
        # Alert 1: Payment without QC
        if event.event_type == "PaymentReleased":
            if "QualityInspected" not in self._po_events[po_id]:
                self.alerts.append({
                    "type": "PAYMENT_WITHOUT_QC",
                    "po_id": po_id,
                    "severity": "HIGH",
                    "message": f"PO {po_id} paid without quality inspection!",
                })
        
        # Alert 2: High defect rate
        if event.event_type == "QualityInspected":
            if event.data["defect_rate"] > 0.05:
                self.alerts.append({
                    "type": "HIGH_DEFECT_RATE",
                    "po_id": po_id,
                    "severity": "MEDIUM",
                    "message": f"PO {po_id} defect rate {event.data['defect_rate']:.1%} exceeds 5% threshold",
                })


# ═══════════════════════════════════════════════════
# Demo 5: OEE & FPY Calculator
# ═══════════════════════════════════════════════════

@dataclass
class StationRecord:
    """SFCS 過站記錄"""
    station: str
    total_in: int
    good_out: int
    reworked: int
    scrapped: int

@dataclass
class ShiftRecord:
    """一個班次的設備運行記錄"""
    planned_minutes: float
    downtime_minutes: float  # 故障+換線+等待
    theoretical_cycle_time: float  # 分鐘/件
    total_produced: int
    good_produced: int

def calculate_fpy(record: StationRecord) -> float:
    """First Pass Yield = (良品 - rework) / 投入"""
    if record.total_in == 0:
        return 0
    return (record.good_out - record.reworked) / record.total_in

def calculate_fty(record: StationRecord) -> float:
    """First Time Yield = 良品 / 投入（含 rework 救回的）"""
    if record.total_in == 0:
        return 0
    return record.good_out / record.total_in

def calculate_rty(stations: list[StationRecord]) -> float:
    """Rolled Throughput Yield = FPY₁ × FPY₂ × ... × FPYₙ"""
    rty = 1.0
    for s in stations:
        rty *= calculate_fpy(s)
    return rty

def calculate_oee(shift: ShiftRecord) -> dict:
    """OEE = Availability × Performance × Quality"""
    operating_time = shift.planned_minutes - shift.downtime_minutes
    
    availability = operating_time / shift.planned_minutes if shift.planned_minutes > 0 else 0
    
    theoretical_output = operating_time / shift.theoretical_cycle_time if shift.theoretical_cycle_time > 0 else 0
    performance = shift.total_produced / theoretical_output if theoretical_output > 0 else 0
    
    quality = shift.good_produced / shift.total_produced if shift.total_produced > 0 else 0
    
    oee = availability * performance * quality
    
    return {
        "availability": availability,
        "performance": performance,
        "quality": quality,
        "oee": oee,
        "operating_minutes": operating_time,
        "theoretical_output": theoretical_output,
    }

def calculate_dpmo(defects: int, opportunities_per_unit: int, units: int) -> dict:
    """DPMO and Sigma level"""
    total_opportunities = opportunities_per_unit * units
    dpmo = (defects / total_opportunities) * 1_000_000 if total_opportunities > 0 else 0
    
    # Approximate sigma level using rational approximation of inverse normal CDF
    if dpmo <= 0:
        sigma = 6.0
    elif dpmo >= 1_000_000:
        sigma = 0.0
    else:
        from math import sqrt, log
        # p = probability of defect, we want inverse normal of (1-p)
        p_defect = dpmo / 1_000_000
        p_good = 1 - p_defect
        # Abramowitz & Stegun approximation for inverse normal (upper tail)
        # For p_good close to 1, use: t = sqrt(-2*ln(1-p_good)) = sqrt(-2*ln(p_defect))
        t = sqrt(-2 * log(p_defect))
        c0, c1, c2 = 2.515517, 0.802853, 0.010328
        d1, d2, d3 = 1.432788, 0.189269, 0.001308
        z = t - (c0 + c1*t + c2*t*t) / (1 + d1*t + d2*t*t + d3*t*t*t)
        sigma = z + 1.5  # 1.5 sigma shift (industry convention)
    
    return {"dpmo": dpmo, "sigma_level": round(sigma, 1), "yield_pct": (1 - dpmo/1_000_000) * 100}

def mrp_scrap_adjustment(ordered_qty: int, scrap_rate: float) -> dict:
    """MRP 良率調整：計算需要多少投入才能得到 ordered_qty 良品"""
    if scrap_rate >= 1:
        return {"error": "Scrap rate cannot be 100%+"}
    required_input = math.ceil(ordered_qty / (1 - scrap_rate))
    extra_units = required_input - ordered_qty
    return {
        "ordered_qty": ordered_qty,
        "scrap_rate": scrap_rate,
        "required_input": required_input,
        "extra_units": extra_units,
        "extra_pct": extra_units / ordered_qty * 100,
    }


# ═══════════════════════════════════════════════════
# Demo 6: Run All Tests
# ═══════════════════════════════════════════════════

def run_demo_1():
    """Event Store + Rehydration + State Machine"""
    print("=" * 60)
    print("Demo 1: Event Store + Rehydration")
    print("=" * 60)
    
    store = EventStore()
    
    # Create PO through full lifecycle
    po = PurchaseOrderAggregate()
    base_ts = datetime(2026, 7, 1, 9, 0)
    
    po.create("PO-2026-001", "Intel", 
              [{"sku": "CPU-XEON-8490H", "qty": 500, "unit_price": 2000}],
              1_000_000, (base_ts).isoformat())
    po.approve("SCM_Manager", (base_ts + timedelta(hours=2)).isoformat())
    po.receive_goods(500, "RM-A3", (base_ts + timedelta(days=14)).isoformat())
    po.inspect_quality("PASS", 0.02, (base_ts + timedelta(days=14, hours=4)).isoformat())
    po.match_invoice("INV-9876", (base_ts + timedelta(days=20)).isoformat())
    po.release_payment(1_000_000, (base_ts + timedelta(days=30)).isoformat())
    
    # Persist events
    for event in po.get_pending_events():
        store.append(event)
    
    # Rehydrate from scratch
    events = store.get_stream("PO-2026-001")
    rebuilt = PurchaseOrderAggregate.rehydrate(events)
    
    assert rebuilt.status == "paid", f"Expected 'paid', got '{rebuilt.status}'"
    assert rebuilt.vendor == "Intel"
    assert rebuilt.received_qty == 500
    assert rebuilt.defect_rate == 0.02
    assert rebuilt.version == 6
    print(f"  ✅ Rehydrated PO: status={rebuilt.status}, vendor={rebuilt.vendor}, "
          f"received={rebuilt.received_qty}, defect={rebuilt.defect_rate:.1%}")
    
    # Test invalid transition
    po2 = PurchaseOrderAggregate()
    po2.create("PO-2026-002", "Samsung", [{"sku": "DRAM-64G", "qty": 1000}], 
               500_000, base_ts.isoformat())
    try:
        po2.receive_goods(1000, "RM-B1", base_ts.isoformat())  # Can't receive before approve
        assert False, "Should have raised InvalidTransitionError"
    except InvalidTransitionError as e:
        print(f"  ✅ State machine guard: {e}")
    
    # Test optimistic concurrency
    events2 = po2.get_pending_events()
    for e in events2:
        store.append(e)
    
    try:
        bad_event = Event("PO-2026-002", "POApproved", {}, base_ts.isoformat(), version=1)
        store.append(bad_event)  # Version 1 already exists
        assert False, "Should have raised ConcurrencyError"
    except ConcurrencyError as e:
        print(f"  ✅ Optimistic concurrency: {e}")
    
    print(f"  Total events in store: {len(store.get_all_events())}")


def run_demo_2():
    """CQRS Projections"""
    print("\n" + "=" * 60)
    print("Demo 2: CQRS Read Models (Projections)")
    print("=" * 60)
    
    store = EventStore()
    po_list = POListProjection()
    vendor_score = VendorScoreProjection()
    anomaly = AnomalyDetectionProjection()
    
    base_ts = datetime(2026, 7, 1, 9, 0)
    
    # Create 3 POs from different vendors
    scenarios = [
        ("PO-001", "Intel", [{"sku": "CPU", "qty": 100}], 200_000, 0.02),
        ("PO-002", "Samsung", [{"sku": "DRAM", "qty": 500}], 150_000, 0.08),  # High defect
        ("PO-003", "Intel", [{"sku": "SSD", "qty": 200}], 100_000, None),  # Will skip QC!
    ]
    
    for i, (po_id, vendor, items, total, defect_rate) in enumerate(scenarios):
        po = PurchaseOrderAggregate()
        t = base_ts + timedelta(days=i)
        po.create(po_id, vendor, items, total, t.isoformat())
        po.approve("Manager", (t + timedelta(hours=1)).isoformat())
        po.receive_goods(items[0]["qty"], "RM-A1", (t + timedelta(days=7)).isoformat())
        
        if defect_rate is not None:
            po.inspect_quality("PASS" if defect_rate < 0.05 else "CONDITIONAL", 
                             defect_rate, (t + timedelta(days=8)).isoformat())
            po.match_invoice(f"INV-{i+1}", (t + timedelta(days=15)).isoformat())
            po.release_payment(total, (t + timedelta(days=25)).isoformat())
        else:
            # Skip QC → straight to payment (anomaly!)
            po.match_invoice(f"INV-{i+1}", (t + timedelta(days=15)).isoformat())
            po.release_payment(total, (t + timedelta(days=25)).isoformat())
        
        for event in po.get_pending_events():
            store.append(event)
    
    # Project all events to read models
    for event in store.get_all_events():
        po_list.handle(event)
        vendor_score.handle(event)
        anomaly.handle(event)
    
    # Query Read Model 1: PO List
    print("\n  📋 PO List (Read Model 1):")
    for po_id, po in sorted(po_list.po_list.items()):
        print(f"    {po_id}: {po['vendor']} | ${po['total']:,.0f} | {po['status']}")
    assert len(po_list.po_list) == 3
    
    # Query Read Model 2: Vendor Scores
    print("\n  📊 Vendor Scores (Read Model 2):")
    for vendor, stats in sorted(vendor_score.vendors.items()):
        avg_defect = f"{stats['avg_defect_rate']:.1%}" if stats['avg_defect_rate'] is not None else "N/A"
        print(f"    {vendor}: {stats['total_pos']} POs | ${stats['total_amount']:,.0f} | "
              f"Avg Defect: {avg_defect} | Cancelled: {stats['cancelled_pos']}")
    
    intel = vendor_score.vendors["Intel"]
    assert intel["total_pos"] == 2
    assert intel["avg_defect_rate"] == 0.02  # Only PO-001 had QC
    print("  ✅ Intel: 2 POs, defect rate from 1 QC event")
    
    # Query Read Model 3: Anomalies
    print(f"\n  🚨 Anomalies (Read Model 3): {len(anomaly.alerts)} alerts")
    for alert in anomaly.alerts:
        print(f"    [{alert['severity']}] {alert['type']}: {alert['message']}")
    
    assert any(a["type"] == "PAYMENT_WITHOUT_QC" for a in anomaly.alerts), "Should detect PO-003 skipped QC"
    assert any(a["type"] == "HIGH_DEFECT_RATE" for a in anomaly.alerts), "Should detect PO-002 high defect"
    print("  ✅ Detected: payment without QC + high defect rate")


def run_demo_3():
    """Compensating Events"""
    print("\n" + "=" * 60)
    print("Demo 3: Compensating Events (PO Cancellation)")
    print("=" * 60)
    
    store = EventStore()
    base_ts = datetime(2026, 7, 1, 9, 0)
    
    # Create and approve PO, then cancel
    po = PurchaseOrderAggregate()
    po.create("PO-CANCEL-001", "TSMC", [{"sku": "ASIC", "qty": 50}], 
              500_000, base_ts.isoformat())
    po.approve("VP_SCM", (base_ts + timedelta(hours=1)).isoformat())
    
    # Cancel with reason (compensating event)
    po.cancel("Customer cancelled order; excess inventory risk", 
              (base_ts + timedelta(hours=3)).isoformat())
    
    for event in po.get_pending_events():
        store.append(event)
    
    # Verify history is preserved
    events = store.get_stream("PO-CANCEL-001")
    assert len(events) == 3, f"Expected 3 events (create+approve+cancel), got {len(events)}"
    
    rebuilt = PurchaseOrderAggregate.rehydrate(events)
    assert rebuilt.status == "cancelled"
    
    print(f"  Event history ({len(events)} events):")
    for e in events:
        print(f"    v{e.version}: {e.event_type} @ {e.timestamp}")
    
    # Cannot do anything after cancel
    try:
        rebuilt2 = PurchaseOrderAggregate.rehydrate(events)
        rebuilt2.approve("Someone", base_ts.isoformat())
        assert False, "Should not allow transitions from cancelled"
    except InvalidTransitionError:
        print("  ✅ Cancelled PO is terminal - no further transitions allowed")
    
    print(f"  ✅ Full audit trail preserved: created → approved → cancelled")
    print(f"     Reason: '{events[-1].data['reason']}'")


def run_demo_4():
    """Snapshot Optimization"""
    print("\n" + "=" * 60)
    print("Demo 4: Snapshot Optimization")
    print("=" * 60)
    
    store = EventStore()
    base_ts = datetime(2026, 7, 1, 9, 0)
    
    # Create PO with many GR events (simulating partial deliveries)
    po = PurchaseOrderAggregate()
    po.create("PO-MANY-001", "Micron", [{"sku": "NAND-256G", "qty": 10000}],
              2_000_000, base_ts.isoformat())
    po.approve("Manager", (base_ts + timedelta(hours=1)).isoformat())
    
    # 20 partial deliveries
    for i in range(20):
        po.receive_goods(500, f"RM-{chr(65 + i % 4)}{i % 5}", 
                        (base_ts + timedelta(days=i+1)).isoformat())
    
    for event in po.get_pending_events():
        store.append(event)
    
    total_events = len(store.get_stream("PO-MANY-001"))
    print(f"  Total events in stream: {total_events}")
    
    # Full rehydration (replay all)
    full_events = store.get_stream("PO-MANY-001")
    full_agg = PurchaseOrderAggregate.rehydrate(full_events)
    print(f"  Full rehydration: {total_events} events replayed → "
          f"received={full_agg.received_qty}, status={full_agg.status}")
    
    # Save snapshot at event 12 (after 10 GRs)
    snapshot_version = 12
    snapshot_events = store.get_stream("PO-MANY-001")[:snapshot_version]
    snapshot_agg = PurchaseOrderAggregate.rehydrate(snapshot_events)
    store.save_snapshot("PO-MANY-001", snapshot_version, snapshot_agg.to_snapshot())
    
    # Rehydrate from snapshot + remaining events
    snap = store.get_snapshot("PO-MANY-001")
    assert snap is not None
    snap_version, snap_state = snap
    partial_agg = PurchaseOrderAggregate.from_snapshot(snap_state, snap_version)
    remaining = store.get_stream("PO-MANY-001", after_version=snap_version)
    for event in remaining:
        partial_agg._apply(event)
    
    replay_count = len(remaining)
    print(f"  Snapshot rehydration: snapshot@v{snap_version} + {replay_count} events replayed")
    print(f"  Savings: {total_events - replay_count} events skipped "
          f"({(total_events - replay_count) / total_events:.0%} reduction)")
    
    # Verify same result
    assert partial_agg.received_qty == full_agg.received_qty
    assert partial_agg.status == full_agg.status
    assert partial_agg.version == full_agg.version
    print(f"  ✅ Results match: received={partial_agg.received_qty}, "
          f"status={partial_agg.status}, version={partial_agg.version}")


def run_demo_5():
    """OEE & FPY Calculator"""
    print("\n" + "=" * 60)
    print("Demo 5: OEE & FPY Calculator (SC Daily #43)")
    print("=" * 60)
    
    # --- FPY / RTY ---
    print("\n  📊 FPY & RTY (4-Station Server Assembly Line):")
    stations = [
        StationRecord("SMT", total_in=1000, good_out=980, reworked=30, scrapped=20),
        StationRecord("ICT/FCT", total_in=980, good_out=950, reworked=10, scrapped=30),
        StationRecord("Assembly", total_in=950, good_out=940, reworked=15, scrapped=10),
        StationRecord("Burn-in", total_in=940, good_out=920, reworked=5, scrapped=20),
    ]
    
    for s in stations:
        fpy = calculate_fpy(s)
        fty = calculate_fty(s)
        print(f"    {s.station:12s}: FPY={fpy:.1%}  FTY={fty:.1%}  "
              f"(in={s.total_in}, good={s.good_out}, rework={s.reworked}, scrap={s.scrapped})")
        
    rty = calculate_rty(stations)
    final_yield = stations[-1].good_out / stations[0].total_in
    hidden_factory = final_yield - rty
    
    print(f"\n    RTY (Rolled Throughput Yield): {rty:.1%}")
    print(f"    Final Yield:                  {final_yield:.1%}")
    print(f"    Hidden Factory:               {hidden_factory:.1%} "
          f"({hidden_factory * stations[0].total_in:.0f} units reworked)")
    
    assert rty < final_yield, "RTY should be lower than Final Yield (hidden factory)"
    print("  ✅ Hidden Factory confirmed: RTY < Final Yield")
    
    # --- OEE ---
    print("\n  📊 OEE Calculation (SMT Line, Day Shift):")
    shift = ShiftRecord(
        planned_minutes=480,
        downtime_minutes=50,   # 30 min changeover + 20 min breakdown
        theoretical_cycle_time=0.5,  # 0.5 min/board = 120 boards/hr
        total_produced=720,
        good_produced=700,
    )
    
    oee = calculate_oee(shift)
    print(f"    Planned: {shift.planned_minutes} min | Downtime: {shift.downtime_minutes} min")
    print(f"    Operating: {oee['operating_minutes']:.0f} min")
    print(f"    Theoretical max output: {oee['theoretical_output']:.0f} boards")
    print(f"    Actual output: {shift.total_produced} boards (good: {shift.good_produced})")
    print(f"\n    Availability:  {oee['availability']:.1%}")
    print(f"    Performance:   {oee['performance']:.1%}")
    print(f"    Quality:       {oee['quality']:.1%}")
    print(f"    ───────────────────────")
    print(f"    OEE:           {oee['oee']:.1%}")
    
    world_class = 0.85
    gap = world_class - oee['oee']
    print(f"\n    vs World-Class (85%): {'GAP ' + f'{gap:.1%}' if gap > 0 else 'ACHIEVED ✅'}")
    
    assert 0 < oee['oee'] < 1, "OEE should be between 0 and 1"
    assert oee['availability'] > oee['performance'] or oee['availability'] > oee['quality'], \
        "At least one factor should be lower"
    print("  ✅ OEE calculation verified")
    
    # --- DPMO ---
    print("\n  📊 DPMO & Sigma Level:")
    dpmo_result = calculate_dpmo(defects=85, opportunities_per_unit=50, units=1000)
    print(f"    Defects: 85 | Opportunities/unit: 50 | Units: 1,000")
    print(f"    DPMO: {dpmo_result['dpmo']:,.0f}")
    print(f"    Sigma Level: {dpmo_result['sigma_level']}σ")
    print(f"    Yield: {dpmo_result['yield_pct']:.3f}%")
    
    assert dpmo_result['dpmo'] == 1700
    assert dpmo_result['sigma_level'] >= 4.0
    print("  ✅ DPMO = 1,700 → ~4.4σ")
    
    # --- MRP Scrap Adjustment ---
    print("\n  📊 MRP Scrap Rate Adjustment:")
    for rate in [0.05, 0.10, 0.15]:
        adj = mrp_scrap_adjustment(1000, rate)
        print(f"    Order 1,000 @ {rate:.0%} scrap → Need {adj['required_input']} input "
              f"(+{adj['extra_units']} extra, +{adj['extra_pct']:.1f}%)")
    
    adj_10 = mrp_scrap_adjustment(1000, 0.10)
    assert adj_10['required_input'] >= 1111, "Should need at least 1111 for 10% scrap"
    print("  ✅ MRP scrap adjustment verified")


def run_demo_6():
    """Integrated: CQRS + Yield in ODM scenario"""
    print("\n" + "=" * 60)
    print("Demo 6: Integrated ODM Scenario (CQRS + Yield)")
    print("=" * 60)
    
    # SA Decision Framework: When to use CQRS?
    print("\n  🏗️ SA Decision Framework: CQRS Applicability")
    
    scenarios = [
        {
            "name": "庫存查詢系統",
            "read_write_ratio": 100,  # 100:1
            "domain_complexity": "low",
            "audit_required": False,
            "independent_scaling": True,
            "recommendation": "CQRS Level 2",
            "reason": "讀寫比極端不對稱，用 Materialized View 優化讀取",
        },
        {
            "name": "採購訂單系統",
            "read_write_ratio": 5,
            "domain_complexity": "high",
            "audit_required": True,
            "independent_scaling": False,
            "recommendation": "Event Sourcing + CQRS",
            "reason": "需要完整 audit trail + 複雜業務邏輯 + 多個讀模型",
        },
        {
            "name": "員工出勤系統",
            "read_write_ratio": 2,
            "domain_complexity": "low",
            "audit_required": False,
            "independent_scaling": False,
            "recommendation": "CRUD",
            "reason": "簡單 CRUD 足夠，CQRS 會增加不必要的複雜性",
        },
        {
            "name": "產線 OEE 監控",
            "read_write_ratio": 50,
            "domain_complexity": "medium",
            "audit_required": True,
            "independent_scaling": True,
            "recommendation": "CQRS Level 2 + CDC",
            "reason": "高頻寫入（SFCS 事件）+ 多種 Dashboard 讀取 + Delta CDF 同步",
        },
    ]
    
    for s in scenarios:
        print(f"\n    📌 {s['name']}")
        print(f"       讀寫比: {s['read_write_ratio']}:1 | 複雜度: {s['domain_complexity']} | "
              f"Audit: {'✓' if s['audit_required'] else '✗'}")
        print(f"       → {s['recommendation']}")
        print(f"       原因: {s['reason']}")
    
    # Verify decision logic
    def decide_architecture(read_write_ratio, domain_complexity, audit_required, independent_scaling):
        if audit_required and domain_complexity == "high":
            return "Event Sourcing + CQRS"
        elif read_write_ratio >= 50 or independent_scaling:
            return "CQRS Level 2" + (" + CDC" if audit_required else "")
        elif read_write_ratio >= 10:
            return "CQRS Level 1"
        else:
            return "CRUD"
    
    for s in scenarios:
        result = decide_architecture(s['read_write_ratio'], s['domain_complexity'],
                                    s['audit_required'], s['independent_scaling'])
        assert result == s['recommendation'], f"Decision mismatch for {s['name']}: {result} vs {s['recommendation']}"
    
    print("\n  ✅ All 4 scenarios correctly classified by decision framework")
    
    # Cross-topic synthesis: Yield impact on PO evaluation
    print("\n  🔗 Cross-Topic: Yield Rate → Vendor Scorecard → PO Decision")
    vendors_yield = {
        "Intel": {"fpy": 0.97, "oee_impact": 0.02},
        "Samsung": {"fpy": 0.91, "oee_impact": 0.08},
        "Micron": {"fpy": 0.95, "oee_impact": 0.04},
    }
    
    order_qty = 1000
    unit_cost = 5000
    
    print(f"\n    Base order: {order_qty} units × ${unit_cost}/unit")
    for vendor, data in vendors_yield.items():
        adj = mrp_scrap_adjustment(order_qty, 1 - data["fpy"])
        actual_cost = adj["required_input"] * unit_cost
        effective_unit_cost = actual_cost / order_qty
        premium = effective_unit_cost - unit_cost
        print(f"    {vendor:10s}: FPY={data['fpy']:.0%} → Need {adj['required_input']} input → "
              f"${effective_unit_cost:,.0f}/unit (+${premium:,.0f} premium)")
    
    print("\n  ✅ Yield-adjusted cost comparison complete")
    print("     Samsung FPY 91% → 每台多付 ~$500 的隱性成本")


def main():
    passed = 0
    total = 6
    
    for i, demo_fn in enumerate([
        run_demo_1, run_demo_2, run_demo_3, 
        run_demo_4, run_demo_5, run_demo_6,
    ], 1):
        try:
            demo_fn()
            passed += 1
        except Exception as e:
            print(f"\n  ❌ Demo {i} FAILED: {e}")
            import traceback
            traceback.print_exc()
    
    print("\n" + "=" * 60)
    print(f"Results: {passed}/{total} demos passed")
    print("=" * 60)
    
    if passed == total:
        print("🎉 All tests passed!")
    else:
        print(f"⚠️  {total - passed} demo(s) failed")

if __name__ == "__main__":
    main()
