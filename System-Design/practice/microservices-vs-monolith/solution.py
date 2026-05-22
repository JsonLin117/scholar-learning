"""
Microservices vs Monolith：ODM 供應鏈架構選型 Demo

學習日期：2026-05-23

這份練習展示：
1. 架構選型不是口號，而是 team/domain/scale/consistency 的 trade-off
2. SAP Core / MRP 這類強一致核心適合 Monolith 或 Modular Monolith
3. 客戶 Portal、AI 推論、IoT/事件處理才是微服務更合理的候選
4. Strangler Fig 用 capability + traffic slicing 漸進遷移，避免 Big Bang rewrite
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from enum import Enum
from random import Random
from typing import Callable, Dict, List, Tuple


# ─────────────────────────────────────────────────────────────
# Part 0: Architecture decision framework
# ─────────────────────────────────────────────────────────────

class Architecture(str, Enum):
    MONOLITH = "Monolith"
    MODULAR_MONOLITH = "Modular Monolith"
    MICROSERVICES = "Microservices"


@dataclass(frozen=True)
class ArchitectureContext:
    capability: str
    team_size: int
    domain_maturity: int          # 1-5, 邊界是否清楚
    scaling_pressure: int         # 1-5, 是否需要獨立水平擴展
    deploy_independence_need: int # 1-5, 是否常常需要獨立發布
    consistency_need: int         # 1-5, 交易一致性要求
    operational_maturity: int     # 1-5, K8s/CI/CD/observability 是否成熟


@dataclass(frozen=True)
class ArchitectureDecision:
    recommendation: Architecture
    scorecard: Dict[Architecture, int]
    reason: str


def recommend_architecture(ctx: ArchitectureContext) -> ArchitectureDecision:
    """簡化版 SA 決策器：把課堂 trade-off 轉成可執行規則。"""
    scores = {
        Architecture.MONOLITH: 0,
        Architecture.MODULAR_MONOLITH: 0,
        Architecture.MICROSERVICES: 0,
    }

    if ctx.team_size < 20:
        scores[Architecture.MONOLITH] += 3
        scores[Architecture.MODULAR_MONOLITH] += 2
    elif ctx.team_size <= 50:
        scores[Architecture.MODULAR_MONOLITH] += 3
    else:
        scores[Architecture.MICROSERVICES] += 3

    if ctx.domain_maturity < 3:
        scores[Architecture.MODULAR_MONOLITH] += 3
        scores[Architecture.MONOLITH] += 1
    else:
        scores[Architecture.MICROSERVICES] += 2
        scores[Architecture.MODULAR_MONOLITH] += 1

    if ctx.scaling_pressure >= 4:
        scores[Architecture.MICROSERVICES] += 3
    else:
        scores[Architecture.MONOLITH] += 1
        scores[Architecture.MODULAR_MONOLITH] += 1

    if ctx.deploy_independence_need >= 4:
        scores[Architecture.MICROSERVICES] += 2
    else:
        scores[Architecture.MONOLITH] += 1
        scores[Architecture.MODULAR_MONOLITH] += 1

    if ctx.consistency_need >= 4:
        scores[Architecture.MONOLITH] += 3
        scores[Architecture.MODULAR_MONOLITH] += 2
        scores[Architecture.MICROSERVICES] -= 2

    if ctx.operational_maturity < 4:
        scores[Architecture.MICROSERVICES] -= 3
        scores[Architecture.MODULAR_MONOLITH] += 2

    winner = max(scores, key=scores.get)
    reason = (
        f"{ctx.capability}: team={ctx.team_size}, domain={ctx.domain_maturity}/5, "
        f"scale={ctx.scaling_pressure}/5, consistency={ctx.consistency_need}/5, "
        f"ops={ctx.operational_maturity}/5 → {winner.value}"
    )
    return ArchitectureDecision(winner, scores, reason)


# ─────────────────────────────────────────────────────────────
# Part 1: Monolith / SAP Core — one ACID boundary
# ─────────────────────────────────────────────────────────────

@dataclass
class SapCoreState:
    inventory: Dict[str, int]
    sales_orders: Dict[str, dict] = field(default_factory=dict)
    mrp_signals: List[dict] = field(default_factory=list)


class SapCoreMonolith:
    """SAP Core 類型單體：強一致優先，所有關鍵表同一交易提交。"""

    def __init__(self, state: SapCoreState):
        self.state = state

    def commit_csp_order(self, order_id: str, customer: str, part_id: str, qty: int) -> None:
        snapshot = deepcopy(self.state)
        try:
            available = self.state.inventory.get(part_id, 0)
            if available < qty:
                raise ValueError(f"stock shortage for {part_id}: need={qty}, available={available}")
            self.state.inventory[part_id] = available - qty
            self.state.sales_orders[order_id] = {
                "customer": customer,
                "part_id": part_id,
                "qty": qty,
                "status": "CONFIRMED_IN_SAP",
            }
            if self.state.inventory[part_id] < 20:
                self.state.mrp_signals.append({"part_id": part_id, "suggested_buy": 100})
        except Exception:
            self.state.inventory = snapshot.inventory
            self.state.sales_orders = snapshot.sales_orders
            self.state.mrp_signals = snapshot.mrp_signals
            raise


# ─────────────────────────────────────────────────────────────
# Part 2: Modular Monolith — one deployment, strict boundaries
# ─────────────────────────────────────────────────────────────

class BoundaryViolation(RuntimeError):
    pass


@dataclass
class ModularState:
    order_table: Dict[str, dict] = field(default_factory=dict)
    inventory_table: Dict[str, int] = field(default_factory=lambda: {"GPU-H100": 80, "NIC-400G": 60})
    shipment_table: Dict[str, dict] = field(default_factory=dict)


class InventoryModuleAPI:
    def __init__(self, state: ModularState):
        self._state = state

    def check_and_reserve(self, order_id: str, part_id: str, qty: int) -> None:
        available = self._state.inventory_table.get(part_id, 0)
        if available < qty:
            raise ValueError(f"InventoryModule rejected reservation: {part_id}")
        self._state.inventory_table[part_id] = available - qty


class ShippingModuleAPI:
    def __init__(self, state: ModularState):
        self._state = state

    def create_draft_shipment(self, order_id: str, customer: str) -> None:
        self._state.shipment_table[order_id] = {"customer": customer, "status": "DRAFT"}


class OrderModule:
    def __init__(self, state: ModularState, inventory: InventoryModuleAPI, shipping: ShippingModuleAPI):
        self._state = state
        self._inventory = inventory
        self._shipping = shipping

    def create_order(self, order_id: str, customer: str, part_id: str, qty: int) -> None:
        snapshot = deepcopy(self._state)
        try:
            self._inventory.check_and_reserve(order_id, part_id, qty)
            self._state.order_table[order_id] = {"customer": customer, "part_id": part_id, "qty": qty}
            self._shipping.create_draft_shipment(order_id, customer)
        except Exception:
            self._state.order_table = snapshot.order_table
            self._state.inventory_table = snapshot.inventory_table
            self._state.shipment_table = snapshot.shipment_table
            raise

    def illegal_direct_inventory_update(self) -> None:
        """示範反模式：Order module 不能直接碰 Inventory table。"""
        raise BoundaryViolation("OrderModule must call InventoryModuleAPI; direct table access is forbidden")


# ─────────────────────────────────────────────────────────────
# Part 3: Microservices — network, events, and saga compensation
# ─────────────────────────────────────────────────────────────

@dataclass
class EventBus:
    events: List[Tuple[str, dict]] = field(default_factory=list)

    def publish(self, event_type: str, payload: dict) -> None:
        self.events.append((event_type, payload))


class Network:
    def __init__(self, seed: int = 7):
        self.random = Random(seed)
        self.total_latency_ms = 0
        self.calls = 0

    def call(self, service: str, action: Callable[[], None], fail_rate: float = 0.0) -> None:
        latency = self.random.randint(8, 45)
        self.total_latency_ms += latency
        self.calls += 1
        if self.random.random() < fail_rate:
            raise TimeoutError(f"{service} timeout after {latency}ms")
        action()


class InventoryService:
    def __init__(self, stock: Dict[str, int], bus: EventBus):
        self.stock = stock
        self.reservations: Dict[str, Tuple[str, int]] = {}
        self.bus = bus

    def reserve(self, order_id: str, part_id: str, qty: int) -> None:
        if self.stock.get(part_id, 0) < qty:
            raise ValueError("InventoryService shortage")
        self.stock[part_id] -= qty
        self.reservations[order_id] = (part_id, qty)
        self.bus.publish("InventoryReserved", {"order_id": order_id, "part_id": part_id, "qty": qty})

    def release(self, order_id: str) -> None:
        part_id, qty = self.reservations.pop(order_id, (None, 0))
        if part_id:
            self.stock[part_id] = self.stock.get(part_id, 0) + qty
            self.bus.publish("InventoryReleased", {"order_id": order_id, "part_id": part_id, "qty": qty})


class PortalOrderService:
    def __init__(self, bus: EventBus):
        self.orders: Dict[str, str] = {}
        self.bus = bus

    def create_order(self, order_id: str) -> None:
        self.orders[order_id] = "CREATED"
        self.bus.publish("OrderCreated", {"order_id": order_id})

    def cancel_order(self, order_id: str) -> None:
        self.orders[order_id] = "CANCELED"
        self.bus.publish("OrderCanceled", {"order_id": order_id})


class LogisticsService:
    def __init__(self, bus: EventBus):
        self.shipments: Dict[str, str] = {}
        self.bus = bus

    def book_lane(self, order_id: str, country: str) -> None:
        if country == "BR" :
            raise ValueError("customs lane unavailable for BR")
        self.shipments[order_id] = f"LANE-{country}"
        self.bus.publish("LaneBooked", {"order_id": order_id, "country": country})

    def cancel_lane(self, order_id: str) -> None:
        self.shipments.pop(order_id, None)
        self.bus.publish("LaneCanceled", {"order_id": order_id})


class MicroserviceSaga:
    def __init__(self, network: Network, order: PortalOrderService, inv: InventoryService, logistics: LogisticsService):
        self.network = network
        self.order = order
        self.inv = inv
        self.logistics = logistics

    def fulfill(self, order_id: str, part_id: str, qty: int, country: str) -> str:
        completed: List[Tuple[str, Callable[[], None]]] = []
        try:
            self.network.call("OrderService", lambda: self.order.create_order(order_id))
            completed.append(("OrderService", lambda: self.order.cancel_order(order_id)))

            self.network.call("InventoryService", lambda: self.inv.reserve(order_id, part_id, qty))
            completed.append(("InventoryService", lambda: self.inv.release(order_id)))

            self.network.call("LogisticsService", lambda: self.logistics.book_lane(order_id, country))
            completed.append(("LogisticsService", lambda: self.logistics.cancel_lane(order_id)))
            return "COMPLETED"
        except Exception as exc:
            print(f"   ⚠ saga failed: {exc}")
            for service, compensate in reversed(completed):
                print(f"   ↩ compensate {service}")
                compensate()
            return "COMPENSATED"


# ─────────────────────────────────────────────────────────────
# Part 4: Strangler Fig — route by capability and traffic slice
# ─────────────────────────────────────────────────────────────

@dataclass
class RouteResult:
    capability: str
    target: str
    request_id: int


class StranglerRouter:
    def __init__(self, migrated_capabilities: Dict[str, int]):
        """value = percentage of traffic routed to new service."""
        self.migrated_capabilities = migrated_capabilities

    def route(self, capability: str, request_id: int) -> RouteResult:
        percent = self.migrated_capabilities.get(capability, 0)
        bucket = request_id % 100
        target = "new-service" if bucket < percent else "legacy-monolith"
        return RouteResult(capability, target, request_id)


# ─────────────────────────────────────────────────────────────
# Demo Runner
# ─────────────────────────────────────────────────────────────

def demo_decision_framework() -> None:
    print("\n" + "=" * 78)
    print("0) SA 決策器：同一家公司，不同 capability 可以是不同架構")
    print("=" * 78)
    contexts = [
        ArchitectureContext("SAP MRP Core", 30, 5, 2, 1, 5, 3),
        ArchitectureContext("Customer Portal", 45, 4, 5, 5, 2, 4),
        ArchitectureContext("New Control Tower MVP", 12, 2, 2, 2, 3, 2),
        ArchitectureContext("AI Forecast Inference", 8, 4, 5, 4, 1, 4),
    ]
    for ctx in contexts:
        decision = recommend_architecture(ctx)
        print(f"✅ {decision.reason}")
        print("   score:", {k.value: v for k, v in decision.scorecard.items()})


def demo_monolith() -> None:
    print("\n" + "=" * 78)
    print("1) Monolith / SAP Core：強一致交易，適合 MRP/財務/正式庫存承諾")
    print("=" * 78)
    sap = SapCoreMonolith(SapCoreState(inventory={"GPU-H100": 120}))
    sap.commit_csp_order("SO-1001", "Microsoft", "GPU-H100", 105)
    print(f"✅ commit 後 inventory={sap.state.inventory}, orders={list(sap.state.sales_orders)}, mrp={sap.state.mrp_signals}")
    try:
        sap.commit_csp_order("SO-1002", "Meta", "GPU-H100", 999)
    except ValueError as exc:
        print(f"✅ shortage rollback：{exc}")
    print(f"   rollback 後仍只有 orders={list(sap.state.sales_orders)}")


def demo_modular_monolith() -> None:
    print("\n" + "=" * 78)
    print("2) Modular Monolith：單部署 + module boundary，先建好邊界再拆")
    print("=" * 78)
    state = ModularState()
    order_module = OrderModule(state, InventoryModuleAPI(state), ShippingModuleAPI(state))
    order_module.create_order("SO-2001", "Google", "GPU-H100", 30)
    print(f"✅ in-process API 完成：orders={state.order_table}, inventory={state.inventory_table}, shipments={state.shipment_table}")
    try:
        order_module.illegal_direct_inventory_update()
    except BoundaryViolation as exc:
        print(f"✅ boundary guard 擋下反模式：{exc}")


def demo_microservices() -> None:
    print("\n" + "=" * 78)
    print("3) Microservices：獨立服務 + network latency + saga compensation")
    print("=" * 78)
    bus = EventBus()
    network = Network(seed=23)
    order = PortalOrderService(bus)
    inv = InventoryService({"GPU-H100": 50}, bus)
    logistics = LogisticsService(bus)
    saga = MicroserviceSaga(network, order, inv, logistics)

    ok = saga.fulfill("SO-3001", "GPU-H100", 10, "US")
    print(f"✅ success saga={ok}, latency={network.total_latency_ms}ms/{network.calls} calls")

    failed = saga.fulfill("SO-3002", "GPU-H100", 10, "BR")
    print(f"✅ failed saga={failed}, stock restored={inv.stock}, order status={order.orders['SO-3002']}")
    print("   events:")
    for event_type, payload in bus.events:
        print(f"   - {event_type}: {payload}")


def demo_strangler_fig() -> None:
    print("\n" + "=" * 78)
    print("4) Strangler Fig：按 capability 漸進切流，不 Big Bang rewrite")
    print("=" * 78)
    router = StranglerRouter({
        "customer-portal-order-tracking": 30,
        "sap-mrp-core": 0,
        "ai-forecast-inference": 100,
    })
    for capability in ["customer-portal-order-tracking", "sap-mrp-core", "ai-forecast-inference"]:
        targets = {"legacy-monolith": 0, "new-service": 0}
        for request_id in range(1000, 1100):
            targets[router.route(capability, request_id).target] += 1
        print(f"✅ {capability}: {targets}")


def main() -> None:
    demo_decision_framework()
    demo_monolith()
    demo_modular_monolith()
    demo_microservices()
    demo_strangler_fig()

    print("\n" + "=" * 78)
    print("SA takeaway")
    print("=" * 78)
    print("SAP Core 不拆；新周邊先 Modular Monolith；高流量/獨立演進 capability 再微服務化。")


if __name__ == "__main__":
    main()
