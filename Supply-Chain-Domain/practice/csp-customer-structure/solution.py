"""
CSP Customer Structure practice (dependency-free simulation)

ODM scenario:
- Three hyperscaler customers send rolling forecasts, firm POs, and pull signals.
- Critical AI server components have long procurement lead time.
- The engine converts uncertain demand into component risk and recommended action.

This is not an optimizer. It is a teaching simulator for the core architecture:
Forecast versions + BOM explosion + lead-time gap + supply ledger = PMC risk view.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from enum import Enum
from typing import Iterable


class SignalType(str, Enum):
    ROLLING_FORECAST = "Rolling Forecast"
    FIRM_PO = "Firm PO"
    PULL_SIGNAL = "Pull Signal"


@dataclass(frozen=True)
class Customer:
    name: str
    revenue_share: float
    forecast_reliability: float  # historical forecast -> PO conversion rate
    payment_days: int


@dataclass(frozen=True)
class DemandSignal:
    customer: str
    product: str
    signal_type: SignalType
    units: int
    requested_date: date
    version_date: date


@dataclass(frozen=True)
class BomLine:
    product: str
    component: str
    qty_per_unit: int
    criticality: str


@dataclass(frozen=True)
class ComponentSupply:
    component: str
    on_hand: int
    procurement_lt_days: int
    open_po: tuple[tuple[date, int], ...]  # (eta, qty)


@dataclass(frozen=True)
class ComponentRequirement:
    component: str
    demand_units: float
    need_by: date
    source_signal: SignalType
    customer: str
    product: str


@dataclass(frozen=True)
class RiskAssessment:
    component: str
    customer: str
    product: str
    signal: SignalType
    need_by: date
    required_qty: float
    available_by_need: int
    shortage: float
    lt_gap_days: int
    risk_score: float
    action: str


def certainty_weight(signal_type: SignalType, customer: Customer) -> float:
    """Turn demand signal into planning weight."""
    if signal_type is SignalType.PULL_SIGNAL:
        return 1.0
    if signal_type is SignalType.FIRM_PO:
        return 0.95
    return customer.forecast_reliability


def explode_bom(
    customers: dict[str, Customer],
    signals: Iterable[DemandSignal],
    bom: list[BomLine],
) -> list[ComponentRequirement]:
    bom_by_product: dict[str, list[BomLine]] = {}
    for line in bom:
        bom_by_product.setdefault(line.product, []).append(line)

    requirements: list[ComponentRequirement] = []
    for signal in signals:
        weight = certainty_weight(signal.signal_type, customers[signal.customer])
        weighted_units = signal.units * weight
        for line in bom_by_product[signal.product]:
            requirements.append(
                ComponentRequirement(
                    component=line.component,
                    demand_units=weighted_units * line.qty_per_unit,
                    need_by=signal.requested_date,
                    source_signal=signal.signal_type,
                    customer=signal.customer,
                    product=signal.product,
                )
            )
    return requirements


def available_quantity(supply: ComponentSupply, need_by: date) -> int:
    arriving = sum(qty for eta, qty in supply.open_po if eta <= need_by)
    return supply.on_hand + arriving


def earliest_new_po_arrival(today: date, supply: ComponentSupply) -> date:
    return today + timedelta(days=supply.procurement_lt_days)


def choose_action(shortage: float, lt_gap_days: int, signal: SignalType, component: str) -> str:
    if shortage <= 0:
        return "coverable: allocate inventory/open PO"
    if signal is SignalType.PULL_SIGNAL:
        return "red alert: negotiate CSD or use premium freight / customer escalation"
    if lt_gap_days > 0:
        return "buy-ahead: place PO now, but mark working-capital/E&O exposure"
    if component.startswith("GPU"):
        return "expedite allocation: GPU shortage, escalate vendor allocation immediately"
    return "expedite supplier + review substitute/dual-source option"


def risk_score(shortage: float, required: float, lt_gap_days: int, signal: SignalType) -> float:
    shortage_ratio = shortage / required if required else 0.0
    signal_multiplier = {
        SignalType.ROLLING_FORECAST: 0.65,
        SignalType.FIRM_PO: 0.9,
        SignalType.PULL_SIGNAL: 1.15,
    }[signal]
    lt_penalty = max(0, min(1.0, lt_gap_days / 60))
    return round(min(100.0, 100 * (0.7 * shortage_ratio + 0.3 * lt_penalty) * signal_multiplier), 1)


def assess_component_risk(
    today: date,
    requirements: Iterable[ComponentRequirement],
    supplies: dict[str, ComponentSupply],
) -> list[RiskAssessment]:
    assessments: list[RiskAssessment] = []
    for req in requirements:
        supply = supplies[req.component]
        available = available_quantity(supply, req.need_by)
        shortage = max(0.0, req.demand_units - available)
        new_po_eta = earliest_new_po_arrival(today, supply)
        lt_gap = (new_po_eta - req.need_by).days
        assessments.append(
            RiskAssessment(
                component=req.component,
                customer=req.customer,
                product=req.product,
                signal=req.source_signal,
                need_by=req.need_by,
                required_qty=req.demand_units,
                available_by_need=available,
                shortage=shortage,
                lt_gap_days=lt_gap,
                risk_score=risk_score(shortage, req.demand_units, lt_gap, req.source_signal),
                action=choose_action(shortage, lt_gap, req.source_signal, req.component),
            )
        )
    return sorted(assessments, key=lambda r: (r.risk_score, r.shortage), reverse=True)


def aggregate_working_capital_exposure(
    assessments: Iterable[RiskAssessment],
    unit_cost: dict[str, int],
) -> dict[str, float]:
    """Estimate money tied up if we buy shortage quantities from forecast-driven signals."""
    exposure: dict[str, float] = {}
    for r in assessments:
        if r.signal is SignalType.ROLLING_FORECAST and r.shortage > 0:
            exposure[r.customer] = exposure.get(r.customer, 0.0) + r.shortage * unit_cost[r.component]
    return exposure


def render_table(assessments: list[RiskAssessment]) -> str:
    header = (
        f"{'Risk':>5} {'Customer':<10} {'Signal':<16} {'Product':<10} {'Component':<12} "
        f"{'Need by':<10} {'Req':>8} {'Avail':>8} {'Short':>8} {'LT gap':>7}  Action"
    )
    lines = [header, "-" * len(header)]
    for r in assessments:
        lines.append(
            f"{r.risk_score:>5.1f} {r.customer:<10} {r.signal.value:<16} {r.product:<10} {r.component:<12} "
            f"{r.need_by.isoformat():<10} {r.required_qty:>8.0f} {r.available_by_need:>8} "
            f"{r.shortage:>8.0f} {r.lt_gap_days:>7}  {r.action}"
        )
    return "\n".join(lines)


def demo() -> None:
    today = date(2026, 5, 17)

    customers = {
        "Microsoft": Customer("Microsoft", revenue_share=0.49, forecast_reliability=0.82, payment_days=75),
        "Meta": Customer("Meta", revenue_share=0.28, forecast_reliability=0.68, payment_days=60),
        "Amazon": Customer("Amazon", revenue_share=0.21, forecast_reliability=0.74, payment_days=70),
    }

    bom = [
        BomLine("AI-RACK", "GPU-GB200", 8, "allocation-controlled"),
        BomLine("AI-RACK", "CPU-EPYC", 2, "long-lead"),
        BomLine("AI-RACK", "NIC-400G", 4, "long-lead"),
        BomLine("AI-RACK", "COLD-PLATE", 8, "custom-liquid-cooling"),
        BomLine("STD-SERVER", "CPU-XEON", 2, "long-lead"),
        BomLine("STD-SERVER", "NIC-100G", 2, "standard"),
    ]

    signals = [
        DemandSignal("Microsoft", "AI-RACK", SignalType.ROLLING_FORECAST, 120, today + timedelta(days=84), today),
        DemandSignal("Meta", "AI-RACK", SignalType.FIRM_PO, 56, today + timedelta(days=42), today),
        DemandSignal("Amazon", "STD-SERVER", SignalType.PULL_SIGNAL, 300, today + timedelta(days=14), today),
        DemandSignal("Microsoft", "AI-RACK", SignalType.PULL_SIGNAL, 30, today + timedelta(days=10), today),
    ]

    supplies = {
        "GPU-GB200": ComponentSupply("GPU-GB200", on_hand=180, procurement_lt_days=126, open_po=((today + timedelta(days=50), 300),)),
        "CPU-EPYC": ComponentSupply("CPU-EPYC", on_hand=120, procurement_lt_days=70, open_po=((today + timedelta(days=35), 160),)),
        "NIC-400G": ComponentSupply("NIC-400G", on_hand=600, procurement_lt_days=42, open_po=((today + timedelta(days=30), 300),)),
        "COLD-PLATE": ComponentSupply("COLD-PLATE", on_hand=250, procurement_lt_days=56, open_po=((today + timedelta(days=28), 260),)),
        "CPU-XEON": ComponentSupply("CPU-XEON", on_hand=700, procurement_lt_days=56, open_po=()),
        "NIC-100G": ComponentSupply("NIC-100G", on_hand=900, procurement_lt_days=21, open_po=()),
    }

    unit_cost = {
        "GPU-GB200": 35_000,
        "CPU-EPYC": 5_500,
        "NIC-400G": 1_200,
        "COLD-PLATE": 650,
        "CPU-XEON": 900,
        "NIC-100G": 250,
    }

    requirements = explode_bom(customers, signals, bom)
    assessments = assess_component_risk(today, requirements, supplies)
    exposure = aggregate_working_capital_exposure(assessments, unit_cost)

    print("CSP Forecast-to-Commit Risk Engine — ODM AI Server Supply Chain")
    print(f"Today: {today.isoformat()} | Signals: {len(signals)} | Component requirements: {len(requirements)}")
    print()
    print(render_table(assessments))
    print()
    print("Working-capital exposure if buying ahead for Rolling Forecast shortages:")
    if exposure:
        for customer, amount in sorted(exposure.items(), key=lambda kv: kv[1], reverse=True):
            print(f"- {customer:<10}: ${amount:,.0f}")
    else:
        print("- none")

    # Lightweight validation: keep the lesson honest.
    top = assessments[0]
    assert top.component == "GPU-GB200"
    assert top.shortage > 0
    assert any(r.signal is SignalType.PULL_SIGNAL and "red alert" in r.action for r in assessments)
    assert exposure["Microsoft"] > 1_000_000
    assert any("buy-ahead" in r.action for r in assessments)

    print()
    print("Key observations:")
    print("1. Pull signals with GPU shortage become immediate red alerts because procurement LT is already too late.")
    print("2. Rolling Forecast is not free demand: buying ahead protects OTD but creates working-capital/E&O exposure.")
    print("3. The SA platform needs forecast versioning, customer reliability scores, BOM explosion, and PO ETA in one model.")
    print("4. This is why CSP customer structure directly changes data architecture: demand uncertainty must be first-class data.")


if __name__ == "__main__":
    demo()
