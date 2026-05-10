"""
ACID vs BASE：ODM 跨系統訂單履約一致性 Demo

學習日期：2026-05-11

這份練習展示：
1. 單一資料庫內：ACID transaction 要嘛全部 commit，要嘛全部 rollback
2. 跨 ERP / Inventory / Payment / Shipping：用 Saga Pattern 達到最終一致
3. BASE read model：dashboard 可能短暫讀到舊資料，靠事件投影最後收斂
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Tuple


# ─────────────────────────────────────────────────────────────
# Part 1: ACID — 單一 DB 交易
# ─────────────────────────────────────────────────────────────

@dataclass
class OrderDB:
    """極簡 in-memory DB，用 copy-on-write 模擬 transaction isolation。"""

    inventory: Dict[str, int]
    orders: List[dict] = field(default_factory=list)

    def transaction(self) -> "Transaction":
        return Transaction(self)


class Transaction:
    def __init__(self, db: OrderDB):
        self.db = db
        self._working_inventory = deepcopy(db.inventory)
        self._working_orders = deepcopy(db.orders)
        self._committed = False

    def reserve_inventory(self, part_id: str, qty: int) -> None:
        available = self._working_inventory.get(part_id, 0)
        if available < qty:
            raise ValueError(f"庫存不足：{part_id} need={qty}, available={available}")
        self._working_inventory[part_id] = available - qty

    def create_order(self, customer: str, part_id: str, qty: int) -> None:
        self._working_orders.append(
            {
                "order_id": f"SO-{len(self._working_orders) + 1:04d}",
                "customer": customer,
                "part_id": part_id,
                "qty": qty,
                "status": "CONFIRMED",
            }
        )

    def commit(self) -> None:
        self.db.inventory = self._working_inventory
        self.db.orders = self._working_orders
        self._committed = True

    def __enter__(self) -> "Transaction":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        # 有錯就什麼都不寫回：atomic rollback
        if exc_type is None and not self._committed:
            self.commit()
        return False


def create_order_acid(db: OrderDB, customer: str, part_id: str, qty: int) -> None:
    """扣庫存 + 建訂單必須在同一個 ACID 邊界內完成。"""
    with db.transaction() as tx:
        tx.reserve_inventory(part_id, qty)
        tx.create_order(customer, part_id, qty)
        tx.commit()


# ─────────────────────────────────────────────────────────────
# Part 2: BASE — Saga Orchestration + 補償交易
# ─────────────────────────────────────────────────────────────

@dataclass
class ServiceState:
    inventory_reserved: Dict[str, int] = field(default_factory=dict)
    invoices: Dict[str, str] = field(default_factory=dict)
    shipments: Dict[str, str] = field(default_factory=dict)
    events: List[Tuple[str, dict]] = field(default_factory=list)


def emit(state: ServiceState, event_type: str, payload: dict) -> None:
    state.events.append((event_type, payload))


class InventoryService:
    def __init__(self, stock: Dict[str, int], state: ServiceState):
        self.stock = stock
        self.state = state

    def reserve(self, order_id: str, part_id: str, qty: int) -> None:
        if self.stock.get(part_id, 0) < qty:
            raise ValueError("Inventory reserve failed")
        self.stock[part_id] -= qty
        self.state.inventory_reserved[order_id] = qty
        emit(self.state, "InventoryReserved", {"order_id": order_id, "part_id": part_id, "qty": qty})

    def release(self, order_id: str, part_id: str) -> None:
        qty = self.state.inventory_reserved.pop(order_id, 0)
        self.stock[part_id] = self.stock.get(part_id, 0) + qty
        emit(self.state, "InventoryReleased", {"order_id": order_id, "part_id": part_id, "qty": qty})


class FinanceService:
    def __init__(self, state: ServiceState):
        self.state = state

    def issue_invoice(self, order_id: str, amount: int) -> None:
        self.state.invoices[order_id] = f"INV-{order_id}"
        emit(self.state, "InvoiceIssued", {"order_id": order_id, "amount": amount})

    def void_invoice(self, order_id: str) -> None:
        self.state.invoices.pop(order_id, None)
        emit(self.state, "InvoiceVoided", {"order_id": order_id})


class ShippingService:
    def __init__(self, state: ServiceState, fail_for_country: str | None = None):
        self.state = state
        self.fail_for_country = fail_for_country

    def schedule(self, order_id: str, country: str) -> None:
        if country == self.fail_for_country:
            raise ValueError(f"Shipping lane unavailable: {country}")
        self.state.shipments[order_id] = f"SHIP-{order_id}"
        emit(self.state, "ShipmentScheduled", {"order_id": order_id, "country": country})

    def cancel(self, order_id: str) -> None:
        self.state.shipments.pop(order_id, None)
        emit(self.state, "ShipmentCanceled", {"order_id": order_id})


@dataclass
class SagaStep:
    name: str
    action: Callable[[], None]
    compensate: Callable[[], None]


class OrderFulfillmentSaga:
    """Orchestration Saga：中央協調器負責步驟與補償順序。"""

    def __init__(self, steps: List[SagaStep]):
        self.steps = steps

    def execute(self) -> str:
        completed: List[SagaStep] = []
        try:
            for step in self.steps:
                print(f"   ▶ action: {step.name}")
                step.action()
                completed.append(step)
            return "COMPLETED"
        except Exception as exc:
            print(f"   ⚠ failed at {step.name}: {exc}")
            for done in reversed(completed):
                print(f"   ↩ compensate: {done.name}")
                done.compensate()
            return "COMPENSATED"


# ─────────────────────────────────────────────────────────────
# Part 3: Eventual consistency — 事件投影 read model
# ─────────────────────────────────────────────────────────────

class ControlTowerReadModel:
    """BASE read model：不是即時強一致，而是消費事件後最終一致。"""

    def __init__(self):
        self.orders: Dict[str, str] = {}
        self._offset = 0

    def project(self, events: List[Tuple[str, dict]], max_events: int | None = None) -> None:
        new_events = events[self._offset :]
        if max_events is not None:
            new_events = new_events[:max_events]

        for event_type, payload in new_events:
            order_id = payload["order_id"]
            if event_type == "InventoryReserved":
                self.orders[order_id] = "RESERVED"
            elif event_type == "InvoiceIssued":
                self.orders[order_id] = "INVOICED"
            elif event_type == "ShipmentScheduled":
                self.orders[order_id] = "READY_TO_SHIP"
            elif event_type in {"InvoiceVoided", "InventoryReleased", "ShipmentCanceled"}:
                self.orders[order_id] = "ROLLED_BACK"
            self._offset += 1

    def status(self, order_id: str) -> str:
        return self.orders.get(order_id, "UNKNOWN")


# ─────────────────────────────────────────────────────────────
# Demo Runner
# ─────────────────────────────────────────────────────────────

def demo_acid() -> None:
    print("\n" + "=" * 72)
    print("1) ACID：SAP/ERP 單一交易內扣庫存 + 建訂單")
    print("=" * 72)

    db = OrderDB(inventory={"CPU-X": 100})
    create_order_acid(db, customer="Meta", part_id="CPU-X", qty=30)
    print(f"✅ 成功交易後：inventory={db.inventory}, orders={db.orders}")

    try:
        create_order_acid(db, customer="Google", part_id="CPU-X", qty=999)
    except ValueError as exc:
        print(f"✅ 失敗交易 rollback：{exc}")

    print(f"   rollback 後狀態不變：inventory={db.inventory}, orders={len(db.orders)}")
    assert db.inventory["CPU-X"] == 70
    assert len(db.orders) == 1


def build_saga(order_id: str, country: str, shipping_fail_country: str | None = None):
    state = ServiceState()
    inventory = InventoryService(stock={"GPU-A": 50}, state=state)
    finance = FinanceService(state=state)
    shipping = ShippingService(state=state, fail_for_country=shipping_fail_country)

    saga = OrderFulfillmentSaga(
        steps=[
            SagaStep(
                "Inventory.reserve",
                lambda: inventory.reserve(order_id, part_id="GPU-A", qty=20),
                lambda: inventory.release(order_id, part_id="GPU-A"),
            ),
            SagaStep(
                "Finance.issue_invoice",
                lambda: finance.issue_invoice(order_id, amount=80_000),
                lambda: finance.void_invoice(order_id),
            ),
            SagaStep(
                "Shipping.schedule",
                lambda: shipping.schedule(order_id, country=country),
                lambda: shipping.cancel(order_id),
            ),
        ]
    )
    return saga, state, inventory


def demo_saga_success() -> None:
    print("\n" + "=" * 72)
    print("2) BASE/Saga 成功：跨 Inventory / Finance / Shipping 最終一致")
    print("=" * 72)

    saga, state, inventory = build_saga(order_id="SO-BASE-001", country="Mexico")
    result = saga.execute()
    print(f"✅ Saga result: {result}")
    print(f"   service states: reserved={state.inventory_reserved}, invoices={state.invoices}, shipments={state.shipments}")

    # Dashboard read model 一開始還沒消費事件：讀到舊資料
    read_model = ControlTowerReadModel()
    print(f"   dashboard before projection: {read_model.status('SO-BASE-001')} (stale)")
    read_model.project(state.events, max_events=2)
    print(f"   dashboard partial projection: {read_model.status('SO-BASE-001')} (still catching up)")
    read_model.project(state.events)
    print(f"   dashboard after projection: {read_model.status('SO-BASE-001')} (eventually consistent)")

    assert result == "COMPLETED"
    assert inventory.stock["GPU-A"] == 30
    assert read_model.status("SO-BASE-001") == "READY_TO_SHIP"


def demo_saga_compensation() -> None:
    print("\n" + "=" * 72)
    print("3) BASE/Saga 失敗：物流不可用 → 反向補償已完成步驟")
    print("=" * 72)

    saga, state, inventory = build_saga(
        order_id="SO-BASE-002",
        country="Czechia",
        shipping_fail_country="Czechia",
    )
    result = saga.execute()
    print(f"✅ Saga result: {result}")
    print(f"   compensated states: reserved={state.inventory_reserved}, invoices={state.invoices}, shipments={state.shipments}")
    print(f"   stock restored: {inventory.stock}")

    read_model = ControlTowerReadModel()
    read_model.project(state.events)
    print(f"   dashboard final status: {read_model.status('SO-BASE-002')}")

    assert result == "COMPENSATED"
    assert state.inventory_reserved == {}
    assert state.invoices == {}
    assert state.shipments == {}
    assert inventory.stock["GPU-A"] == 50
    assert read_model.status("SO-BASE-002") == "ROLLED_BACK"


def main() -> None:
    print("ACID vs BASE — ODM 供應鏈一致性邊界 Demo")
    demo_acid()
    demo_saga_success()
    demo_saga_compensation()

    print("\n" + "=" * 72)
    print("📝 SA 結論")
    print("=" * 72)
    print(
        """
1. ACID 適合單一 bounded context 內的硬承諾：正式訂單、付款、庫存扣減。
2. BASE 適合跨系統流程：每個服務 local ACID，跨服務用事件與補償交易收斂。
3. Saga 不是免費午餐：需要冪等、補償動作、事件追蹤、重試與人工介入機制。
4. Control Tower / Dashboard 通常是 read model，可接受短暫 stale，但必須標示資料延遲。
""".strip()
    )


if __name__ == "__main__":
    main()
