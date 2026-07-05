#!/usr/bin/env python3
"""
Kimball Bus Architecture practice: Conformed Dimension Registry + Bus Matrix.

Scenario: ODM supply chain (Wiwynn-scale). Multiple teams build Data Marts
independently (Procurement, Receiving, Vendor Scorecard, Inventory). Without
governance, each team can accidentally define a slightly different
`dim_vendor` -> Data Marts can no longer JOIN consistently -> silo.

This script simulates the governance mechanism at the heart of Kimball's
Enterprise Data Warehouse Bus Architecture:

1. A central Conformed Dimension Registry (single source of truth for
   dimension schemas) - this is where Kimball borrows Inmon's centralized
   governance spirit, just scoped to dimensions instead of the whole EDW.
2. Fact table registration must reference registered dimensions; if a team
   tries to register a Fact with a dimension whose schema doesn't match the
   registry, it is rejected (this is the automated version of "Bus Matrix
   discipline").
3. Bus Matrix generation: business process x conformed dimension matrix,
   used to plan incremental Data Mart rollout.

Run:
    JAVA_HOME=/opt/homebrew/opt/openjdk@17 \
      .venv/bin/python3 Data-Warehouse/practice/kimball-vs-inmon/solution.py
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Set, Tuple


# ---------------------------------------------------------------------------
# 1. Conformed Dimension Registry (Kimball Bus Architecture's governance core)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class DimensionSpec:
    """Schema contract for a Conformed Dimension."""
    name: str
    key_field: str                 # surrogate key field name
    key_type: str                  # e.g. "INT", "BIGINT"
    required_fields: Tuple[str, ...]  # fields every Fact-facing copy must expose


class ConformedDimensionRegistry:
    """Single source of truth for dimension schemas across all Data Marts.

    This is the piece of code that plays the "Inmon-style centralized
    governance" role inside an otherwise Kimball bottom-up architecture.
    """

    def __init__(self) -> None:
        self._registry: Dict[str, DimensionSpec] = {}

    def register(self, spec: DimensionSpec) -> None:
        if spec.name in self._registry:
            raise ValueError(f"Dimension '{spec.name}' already registered — use conform_check instead")
        self._registry[spec.name] = spec

    def get(self, name: str) -> DimensionSpec:
        if name not in self._registry:
            raise KeyError(f"Dimension '{name}' is not a registered Conformed Dimension")
        return self._registry[name]

    def conform_check(self, candidate: DimensionSpec) -> List[str]:
        """Compare a team's proposed dimension definition against the
        registered contract. Returns a list of violations (empty = OK).
        """
        golden = self.get(candidate.name)
        violations = []
        if candidate.key_field != golden.key_field:
            violations.append(
                f"key_field mismatch: proposed='{candidate.key_field}' vs golden='{golden.key_field}'"
            )
        if candidate.key_type != golden.key_type:
            violations.append(
                f"key_type mismatch: proposed='{candidate.key_type}' vs golden='{golden.key_type}'"
            )
        missing = set(golden.required_fields) - set(candidate.required_fields)
        if missing:
            violations.append(f"missing required fields: {sorted(missing)}")
        return violations


# ---------------------------------------------------------------------------
# 2. Fact Table registration (each Data Mart declares its dimension usage)
# ---------------------------------------------------------------------------

@dataclass
class FactTableDef:
    process_name: str              # business process, e.g. "Procurement"
    fact_name: str                 # e.g. "fact_purchase_order"
    dimension_refs: List[str] = field(default_factory=list)


class BusMatrixBuilder:
    def __init__(self, registry: ConformedDimensionRegistry) -> None:
        self.registry = registry
        self.facts: List[FactTableDef] = []

    def register_fact(self, fact: FactTableDef, proposed_dims: Dict[str, DimensionSpec] | None = None) -> None:
        """Register a Fact table. `proposed_dims` lets a team pass its own
        (possibly non-conforming) dimension definitions for validation.
        """
        proposed_dims = proposed_dims or {}
        for dim_name in fact.dimension_refs:
            if dim_name in proposed_dims:
                violations = self.registry.conform_check(proposed_dims[dim_name])
                if violations:
                    raise ValueError(
                        f"❌ Fact '{fact.fact_name}' ({fact.process_name}) uses a NON-CONFORMED "
                        f"'{dim_name}': {violations}"
                    )
            else:
                # Just make sure the dimension exists in the registry at all.
                self.registry.get(dim_name)
        self.facts.append(fact)
        print(f"✅ Registered fact '{fact.fact_name}' for process '{fact.process_name}' "
              f"(dims: {fact.dimension_refs})")

    def build_bus_matrix(self) -> Tuple[List[str], List[str], Dict[Tuple[str, str], bool]]:
        processes = sorted({f.process_name for f in self.facts})
        dim_set: Set[str] = set()
        for f in self.facts:
            dim_set.update(f.dimension_refs)
        dims = sorted(dim_set)

        matrix: Dict[Tuple[str, str], bool] = {}
        for f in self.facts:
            for d in f.dimension_refs:
                matrix[(f.process_name, d)] = True
        return processes, dims, matrix

    def print_bus_matrix(self) -> None:
        processes, dims, matrix = self.build_bus_matrix()
        header = "業務流程".ljust(14) + "".join(d.ljust(14) for d in dims)
        print(header)
        print("-" * len(header))
        for p in processes:
            row = p.ljust(14)
            for d in dims:
                row += ("✓".ljust(14) if matrix.get((p, d)) else "".ljust(14))
            print(row)


# ---------------------------------------------------------------------------
# 3. Demo: ODM supply chain rollout — Procurement -> Receiving -> Vendor
#    Scorecard -> Inventory, each adding new facts against shared dimensions.
# ---------------------------------------------------------------------------

def main() -> None:
    registry = ConformedDimensionRegistry()

    # Golden (Conformed) dimension definitions — the single source of truth.
    registry.register(DimensionSpec(
        name="dim_vendor", key_field="vendor_key", key_type="INT",
        required_fields=("vendor_id", "vendor_name", "country", "avl_status"),
    ))
    registry.register(DimensionSpec(
        name="dim_date", key_field="date_key", key_type="INT",
        required_fields=("full_date", "fiscal_year", "fiscal_quarter"),
    ))
    registry.register(DimensionSpec(
        name="dim_material", key_field="material_key", key_type="INT",
        required_fields=("material_id", "material_desc", "material_group"),
    ))
    registry.register(DimensionSpec(
        name="dim_plant", key_field="plant_key", key_type="INT",
        required_fields=("plant_code", "plant_name", "region"),
    ))

    matrix_builder = BusMatrixBuilder(registry)

    print("=== Step 1: 採購 (Procurement) Data Mart 上線 ===")
    matrix_builder.register_fact(FactTableDef(
        process_name="採購(P2P)",
        fact_name="fact_purchase_order",
        dimension_refs=["dim_vendor", "dim_date", "dim_material", "dim_plant"],
    ))

    print("\n=== Step 2: 收貨 (Receiving) Data Mart 上線，正確引用 Conformed Dim ===")
    matrix_builder.register_fact(FactTableDef(
        process_name="收貨(GR)",
        fact_name="fact_goods_receipt",
        dimension_refs=["dim_vendor", "dim_date", "dim_material", "dim_plant"],
    ))

    print("\n=== Step 3: 供應商績效 (Vendor Scorecard) Data Mart 上線 ===")
    matrix_builder.register_fact(FactTableDef(
        process_name="供應商績效",
        fact_name="fact_vendor_scorecard",
        dimension_refs=["dim_vendor", "dim_date"],
    ))

    print("\n=== Step 4: 庫存 (Inventory) team 誤建了不一致的 dim_vendor 版本 ===")
    rogue_dim_vendor = DimensionSpec(
        name="dim_vendor", key_field="vendor_id", key_type="VARCHAR",  # <- 用 natural key 當 surrogate key，型別也不對
        required_fields=("vendor_id", "vendor_name"),                  # <- 少了 country / avl_status
    )
    try:
        matrix_builder.register_fact(
            FactTableDef(
                process_name="庫存管理",
                fact_name="fact_inventory_snapshot",
                dimension_refs=["dim_vendor", "dim_date", "dim_material", "dim_plant"],
            ),
            proposed_dims={"dim_vendor": rogue_dim_vendor},
        )
    except ValueError as e:
        print(e)
        print("→ 團隊被要求改用 Conformed Dimension，不能自建不一致版本")

    print("\n=== Step 4b: 庫存團隊修正後，正確引用 Conformed dim_vendor ===")
    matrix_builder.register_fact(FactTableDef(
        process_name="庫存管理",
        fact_name="fact_inventory_snapshot",
        dimension_refs=["dim_vendor", "dim_date", "dim_material", "dim_plant"],
    ))

    print("\n=== Enterprise Data Warehouse Bus Matrix ===")
    matrix_builder.print_bus_matrix()

    print("\n✅ 驗證完成：4 個業務流程的 Data Mart 都只引用 Conformed Dimensions，")
    print("   Bus Matrix 顯示目前的維度覆蓋範圍，可作為下一個 Data Mart（如：客戶履約 O2C）的擴充藍圖。")


if __name__ == "__main__":
    main()
