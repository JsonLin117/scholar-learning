"""
Liquid Clustering practice (dependency-free simulation)

This is NOT a Delta Lake engine. It is a small simulator for the core idea:
file-level min/max statistics become more useful when records with similar
query dimensions are physically clustered together.

ODM scenario:
- inventory movement history table
- common predicates: date range, plant, part_number, supplier
- compare unoptimized, partition+zorder-like, and liquid-clustered layouts
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from random import Random
from typing import Callable, Iterable

Record = dict[str, object]


@dataclass(frozen=True)
class DataFile:
    file_id: int
    layout: str
    rows: list[Record]
    stats: dict[str, tuple[object, object]]

    @property
    def row_count(self) -> int:
        return len(self.rows)


@dataclass(frozen=True)
class Predicate:
    name: str
    columns: tuple[str, ...]
    might_match_stats: Callable[[dict[str, tuple[object, object]]], bool]
    row_matches: Callable[[Record], bool]


@dataclass(frozen=True)
class ScanResult:
    layout: str
    predicate: str
    files_total: int
    files_scanned: int
    rows_scanned: int
    rows_matched: int

    @property
    def files_skipped(self) -> int:
        return self.files_total - self.files_scanned

    @property
    def skip_ratio(self) -> float:
        return self.files_skipped / self.files_total if self.files_total else 0.0


def generate_inventory_movements(n: int = 25_000, seed: int = 20260516) -> list[Record]:
    """Generate skewed ODM inventory movement data."""
    rng = Random(seed)
    start = date(2026, 1, 1)
    plants = ["TPE", "TYO", "SZX", "KHH", "MEX", "CZ"]
    suppliers = [f"SUP-{i:03d}" for i in range(1, 121)]
    hot_parts = [f"CPU-{i:03d}" for i in range(1, 31)]
    normal_parts = [f"PART-{i:05d}" for i in range(1, 1201)]
    movement_types = ["GR", "ISSUE", "TRANSFER", "ADJUST", "RETURN"]

    rows: list[Record] = []
    for i in range(n):
        day_offset = int(rng.triangular(0, 120, 85))  # more recent days are hotter
        plant = rng.choices(plants, weights=[18, 10, 28, 12, 20, 12], k=1)[0]
        part = rng.choice(hot_parts if rng.random() < 0.28 else normal_parts)
        supplier = rng.choice(suppliers)
        movement_type = rng.choices(movement_types, weights=[28, 46, 12, 8, 6], k=1)[0]
        quantity = rng.randint(1, 400) * (1 if movement_type in {"GR", "RETURN"} else -1)
        rows.append(
            {
                "movement_id": f"MOV-{i:07d}",
                "movement_date": start + timedelta(days=day_offset),
                "plant_id": plant,
                "part_number": part,
                "supplier_id": supplier,
                "movement_type": movement_type,
                "quantity": quantity,
            }
        )
    return rows


def build_rank_maps(rows: Iterable[Record], columns: Iterable[str]) -> dict[str, dict[object, int]]:
    maps: dict[str, dict[object, int]] = {}
    for col in columns:
        values = sorted({r[col] for r in rows})
        maps[col] = {v: i for i, v in enumerate(values)}
    return maps


def bit_interleave(values: list[int], bits: int = 12) -> int:
    """Morton/Z-order style bit interleaving for ranked dimensions."""
    out = 0
    for b in range(bits):
        for dim, value in enumerate(values):
            out |= ((value >> b) & 1) << (b * len(values) + dim)
    return out


def gray_code(x: int) -> int:
    return x ^ (x >> 1)


def liquid_locality_key(record: Record, columns: tuple[str, ...], ranks: dict[str, dict[object, int]]) -> tuple[int, ...]:
    """
    Hilbert-like locality key.

    Real Liquid Clustering uses Hilbert curves inside Delta/Databricks.
    Here we approximate the teaching point with Gray-coded Morton order:
    it keeps nearby ranked values closer than plain lexical sorting and is
    sufficient to demonstrate tighter file min/max statistics.
    """
    ranked = [ranks[c][record[c]] for c in columns]
    return (gray_code(bit_interleave(ranked)), *ranked)


def chunked(rows: list[Record], file_size: int) -> list[list[Record]]:
    return [rows[i : i + file_size] for i in range(0, len(rows), file_size)]


def file_stats(rows: list[Record], columns: Iterable[str]) -> dict[str, tuple[object, object]]:
    stats = {}
    for col in columns:
        values = [r[col] for r in rows]
        stats[col] = (min(values), max(values))
    return stats


def write_files(layout: str, rows: list[Record], sort_key: Callable[[Record], object], stats_cols: tuple[str, ...], file_size: int = 500) -> list[DataFile]:
    ordered = sorted(rows, key=sort_key)
    return [
        DataFile(file_id=i, layout=layout, rows=part, stats=file_stats(part, stats_cols))
        for i, part in enumerate(chunked(ordered, file_size))
    ]


def scan(files: list[DataFile], predicate: Predicate) -> ScanResult:
    scanned_files = 0
    scanned_rows = 0
    matched_rows = 0
    for f in files:
        if not predicate.might_match_stats(f.stats):
            continue
        scanned_files += 1
        scanned_rows += f.row_count
        matched_rows += sum(1 for row in f.rows if predicate.row_matches(row))
    return ScanResult(files[0].layout, predicate.name, len(files), scanned_files, scanned_rows, matched_rows)


def between(col: str, low: object, high: object) -> Callable[[dict[str, tuple[object, object]]], bool]:
    def check(stats: dict[str, tuple[object, object]]) -> bool:
        mn, mx = stats[col]
        return not (mx < low or mn > high)

    return check


def equals(col: str, value: object) -> Callable[[dict[str, tuple[object, object]]], bool]:
    def check(stats: dict[str, tuple[object, object]]) -> bool:
        mn, mx = stats[col]
        return mn <= value <= mx

    return check


def and_stats(*checks: Callable[[dict[str, tuple[object, object]]], bool]) -> Callable[[dict[str, tuple[object, object]]], bool]:
    return lambda stats: all(check(stats) for check in checks)


def render_table(results: list[ScanResult]) -> str:
    header = f"{'Layout':<28} {'Predicate':<34} {'Files':>7} {'Scanned':>7} {'Skipped':>7} {'Skip%':>7} {'Rows scanned':>13} {'Rows matched':>13}"
    lines = [header, "-" * len(header)]
    for r in results:
        lines.append(
            f"{r.layout:<28} {r.predicate:<34} {r.files_total:>7} {r.files_scanned:>7} "
            f"{r.files_skipped:>7} {r.skip_ratio:>6.1%} {r.rows_scanned:>13,} {r.rows_matched:>13,}"
        )
    return "\n".join(lines)


def main() -> None:
    rows = generate_inventory_movements()
    stats_cols = ("movement_date", "plant_id", "part_number", "supplier_id")
    ranks = build_rank_maps(rows, stats_cols)

    # 1) Original write order: append-only ingestion, no layout optimization.
    unoptimized = [
        DataFile(i, "unoptimized append order", part, file_stats(part, stats_cols))
        for i, part in enumerate(chunked(rows, 500))
    ]

    # 2) Traditional layout: physical partition by date, Z-order-ish inside date partition.
    partition_zorder = write_files(
        "partition(date)+zorder(plant,part)",
        rows,
        sort_key=lambda r: (r["movement_date"], bit_interleave([ranks["plant_id"][r["plant_id"]], ranks["part_number"][r["part_number"]]])),
        stats_cols=stats_cols,
    )

    # 3) Liquid clustering: no physical date partition, global multi-column locality.
    liquid = write_files(
        "liquid(date,plant,part)",
        rows,
        sort_key=lambda r: liquid_locality_key(r, ("movement_date", "plant_id", "part_number"), ranks),
        stats_cols=stats_cols,
    )

    # 4) Business changes its dominant query from part analysis to supplier risk.
    liquid_supplier = write_files(
        "liquid(date,supplier) FULL",
        rows,
        sort_key=lambda r: liquid_locality_key(r, ("movement_date", "supplier_id"), ranks),
        stats_cols=stats_cols,
    )

    query_start = date(2026, 3, 15)
    query_end = date(2026, 3, 31)
    target_part = "CPU-007"
    target_plant = "SZX"
    target_supplier = "SUP-042"

    predicates = [
        Predicate(
            name="date range only",
            columns=("movement_date",),
            might_match_stats=between("movement_date", query_start, query_end),
            row_matches=lambda r: query_start <= r["movement_date"] <= query_end,
        ),
        Predicate(
            name="date + plant + hot part",
            columns=("movement_date", "plant_id", "part_number"),
            might_match_stats=and_stats(
                between("movement_date", query_start, query_end),
                equals("plant_id", target_plant),
                equals("part_number", target_part),
            ),
            row_matches=lambda r: query_start <= r["movement_date"] <= query_end and r["plant_id"] == target_plant and r["part_number"] == target_part,
        ),
        Predicate(
            name="supplier risk query",
            columns=("supplier_id",),
            might_match_stats=equals("supplier_id", target_supplier),
            row_matches=lambda r: r["supplier_id"] == target_supplier,
        ),
    ]

    layouts = [unoptimized, partition_zorder, liquid, liquid_supplier]
    results = [scan(files, pred) for pred in predicates for files in layouts]

    print("Liquid Clustering simulator — ODM inventory movement table")
    print(f"Rows: {len(rows):,} | Files per layout: {len(unoptimized)} | Rows/file: 500")
    print()
    print(render_table(results))
    print()
    print("Key observations:")
    print("1. Partition+ZORDER is strong for date predicates, but supplier-only queries still scan many files.")
    print("2. Liquid(date,plant,part) gives balanced pruning without a physical partition directory.")
    print("3. After ALTER CLUSTER BY (movement_date, supplier_id) + OPTIMIZE FULL, supplier risk queries prune much better.")
    print("4. This mirrors the SA decision: choose clustering keys from real query history, then change them when the business question changes.")


if __name__ == "__main__":
    main()
