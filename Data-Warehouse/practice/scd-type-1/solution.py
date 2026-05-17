#!/usr/bin/env python3
"""
SCD Type 1 practice: ODM vendor dimension merge.

This dependency-free script simulates the core logic behind a Delta Lake
MERGE INTO for a Gold-layer dim_vendor table:

1. Deduplicate source rows by business key (vendor_id), keeping the latest.
2. Update only fields classified as SCD Type 1 when values really changed.
3. Insert new vendors.
4. Audit history-sensitive fields that should be handled by Type 2 instead.

Run:
    JAVA_HOME=/opt/homebrew/opt/openjdk@17 \
      .venv/bin/python3 Data-Warehouse/practice/scd-type-1/solution.py
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pprint import pprint
from typing import Any

TYPE1_FIELDS = {
    "vendor_name",
    "contact_phone",
    "contact_email",
    "quality_contact",
    "portal_display_name",
}

# These fields look tempting to overwrite, but they answer historical questions:
# "Which AVL status / payment term / address was valid when this PO was placed?"
HISTORY_SENSITIVE_FIELDS = {
    "registered_address",
    "payment_terms",
    "avl_status",
    "lead_time_days",
}

BUSINESS_KEY = "vendor_id"
NULL_EQUIVALENT = "<NULL>"


@dataclass
class MergeStats:
    inserted: int = 0
    updated: int = 0
    unchanged: int = 0
    source_duplicates_removed: int = 0
    skipped_null_overwrites: int = 0
    avoided_noop_updates: int = 0
    history_sensitive_changes: list[dict[str, Any]] = field(default_factory=list)
    type1_change_log: list[dict[str, Any]] = field(default_factory=list)


def parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def null_safe(value: Any) -> Any:
    """Normalize null-like values so comparisons are deterministic."""
    return NULL_EQUIVALENT if value is None else value


def changed(before: Any, after: Any) -> bool:
    """NULL-safe comparison, equivalent to COALESCE-style SQL comparison."""
    return null_safe(before) != null_safe(after)


def deduplicate_latest(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    """
    Keep the latest source row per vendor_id.

    Equivalent SQL shape:
        ROW_NUMBER() OVER (
          PARTITION BY vendor_id
          ORDER BY source_update_ts DESC, ingest_seq DESC
        ) = 1
    """
    latest_by_key: dict[str, dict[str, Any]] = {}

    for row in rows:
        key = row[BUSINESS_KEY]
        current = latest_by_key.get(key)
        if current is None:
            latest_by_key[key] = row
            continue

        row_rank = (parse_ts(row["source_update_ts"]), row["ingest_seq"])
        current_rank = (parse_ts(current["source_update_ts"]), current["ingest_seq"])
        if row_rank > current_rank:
            latest_by_key[key] = row

    duplicates_removed = len(rows) - len(latest_by_key)
    return list(latest_by_key.values()), duplicates_removed


def insert_new_vendor(source: dict[str, Any], now: str) -> dict[str, Any]:
    """Create a new dim row from the incoming source row."""
    row = {BUSINESS_KEY: source[BUSINESS_KEY]}
    for field_name in sorted(TYPE1_FIELDS | HISTORY_SENSITIVE_FIELDS):
        row[field_name] = source.get(field_name)
    row["created_at"] = now
    row["updated_at"] = now
    return row


def merge_scd_type1(
    target_rows: list[dict[str, Any]], source_rows: list[dict[str, Any]], now: str
) -> tuple[list[dict[str, Any]], MergeStats]:
    """Simulate MERGE INTO dim_vendor USING deduped_source."""
    target_by_key = {row[BUSINESS_KEY]: deepcopy(row) for row in target_rows}
    deduped_source, duplicate_count = deduplicate_latest(source_rows)

    stats = MergeStats(source_duplicates_removed=duplicate_count)

    for source in deduped_source:
        key = source[BUSINESS_KEY]
        target = target_by_key.get(key)

        if target is None:
            target_by_key[key] = insert_new_vendor(source, now)
            stats.inserted += 1
            continue

        row_changed = False

        # Safe Type 1 overwrites.
        for field_name in sorted(TYPE1_FIELDS):
            incoming = source.get(field_name)
            existing = target.get(field_name)

            # Operational safety rule: a missing source value should not wipe a
            # useful current contact field unless the upstream system explicitly
            # sends a deletion event. This is a common Lakehouse guardrail.
            if incoming is None and existing is not None:
                stats.skipped_null_overwrites += 1
                continue

            if changed(existing, incoming):
                target[field_name] = incoming
                row_changed = True
                stats.type1_change_log.append(
                    {
                        "vendor_id": key,
                        "field": field_name,
                        "before": existing,
                        "after": incoming,
                    }
                )
            else:
                stats.avoided_noop_updates += 1

        # Detect values that should not be Type 1 overwritten.
        for field_name in sorted(HISTORY_SENSITIVE_FIELDS):
            incoming = source.get(field_name)
            existing = target.get(field_name)
            if incoming is not None and changed(existing, incoming):
                stats.history_sensitive_changes.append(
                    {
                        "vendor_id": key,
                        "field": field_name,
                        "current_gold_value": existing,
                        "incoming_value": incoming,
                        "recommended_action": "route_to_scd_type2_snapshot",
                    }
                )

        if row_changed:
            target["updated_at"] = now
            stats.updated += 1
        else:
            stats.unchanged += 1

    return sorted(target_by_key.values(), key=lambda row: row[BUSINESS_KEY]), stats


def sample_target_dim_vendor() -> list[dict[str, Any]]:
    return [
        {
            "vendor_id": "V100",
            "vendor_name": "Formosa Cable Co.",
            "portal_display_name": "Formosa Cable",
            "contact_phone": "+886-2-1111-0000",
            "contact_email": "sales@formosa-cable.example",
            "quality_contact": "Amy Chen",
            "registered_address": "No. 1, Taipei, TW",
            "payment_terms": "NET45",
            "avl_status": "APPROVED",
            "lead_time_days": 28,
            "created_at": "2026-04-01T00:00:00Z",
            "updated_at": "2026-05-01T00:00:00Z",
        },
        {
            "vendor_id": "V200",
            "vendor_name": "Pacific PSU Ltd.",
            "portal_display_name": "Pacific PSU",
            "contact_phone": "+886-3-2222-0000",
            "contact_email": "pm@pacific-psu.example",
            "quality_contact": "Ben Wu",
            "registered_address": "Hsinchu Science Park, TW",
            "payment_terms": "NET60",
            "avl_status": "CONDITIONAL",
            "lead_time_days": 42,
            "created_at": "2026-04-01T00:00:00Z",
            "updated_at": "2026-05-10T00:00:00Z",
        },
        {
            "vendor_id": "V300",
            "vendor_name": "NorthBridge Thermal",
            "portal_display_name": "NorthBridge Thermal",
            "contact_phone": "+1-408-333-0000",
            "contact_email": "ops@northbridge-thermal.example",
            "quality_contact": "Chris Lee",
            "registered_address": "San Jose, CA, US",
            "payment_terms": "NET30",
            "avl_status": "APPROVED",
            "lead_time_days": 56,
            "created_at": "2026-04-15T00:00:00Z",
            "updated_at": "2026-04-15T00:00:00Z",
        },
    ]


def sample_source_updates() -> list[dict[str, Any]]:
    return [
        # Duplicate V200 older row: should be removed by source deduplication.
        {
            "vendor_id": "V200",
            "vendor_name": "Pacific PSU Ltd.",
            "portal_display_name": "Pacific PSU",
            "contact_phone": "+886-3-2222-0000",
            "contact_email": "pm-old@pacific-psu.example",
            "quality_contact": "Ben Wu",
            "registered_address": "Hsinchu Science Park, TW",
            "payment_terms": "NET60",
            "avl_status": "CONDITIONAL",
            "lead_time_days": 42,
            "source_update_ts": "2026-05-18T01:00:00Z",
            "ingest_seq": 10,
        },
        # Latest V200 row: contact is Type 1, AVL/payment/lead time need history.
        {
            "vendor_id": "V200",
            "vendor_name": "Pacific Power Systems Ltd.",
            "portal_display_name": "Pacific Power Systems",
            "contact_phone": "+886-3-2222-9999",
            "contact_email": "pm-new@pacific-psu.example",
            "quality_contact": "Bella Wu",
            "registered_address": "Hsinchu Science Park, TW",
            "payment_terms": "NET75",
            "avl_status": "APPROVED",
            "lead_time_days": 49,
            "source_update_ts": "2026-05-18T02:00:00Z",
            "ingest_seq": 11,
        },
        # V100 only sends null phone: do not wipe existing phone accidentally.
        {
            "vendor_id": "V100",
            "vendor_name": "Formosa Cable Co.",
            "portal_display_name": "Formosa Cable",
            "contact_phone": None,
            "contact_email": "sales@formosa-cable.example",
            "quality_contact": "Amy Chen",
            "registered_address": "No. 1, Taipei, TW",
            "payment_terms": "NET45",
            "avl_status": "APPROVED",
            "lead_time_days": 28,
            "source_update_ts": "2026-05-18T02:30:00Z",
            "ingest_seq": 12,
        },
        # New vendor inserted.
        {
            "vendor_id": "V400",
            "vendor_name": "Atlas GPU Logistics",
            "portal_display_name": "Atlas GPU Logistics",
            "contact_phone": "+1-512-444-0000",
            "contact_email": "allocations@atlas-gpu-logistics.example",
            "quality_contact": "Dana Lin",
            "registered_address": "Austin, TX, US",
            "payment_terms": "NET30",
            "avl_status": "PENDING",
            "lead_time_days": 84,
            "source_update_ts": "2026-05-18T03:00:00Z",
            "ingest_seq": 13,
        },
        # V300 unchanged: should avoid no-op COW updates.
        {
            "vendor_id": "V300",
            "vendor_name": "NorthBridge Thermal",
            "portal_display_name": "NorthBridge Thermal",
            "contact_phone": "+1-408-333-0000",
            "contact_email": "ops@northbridge-thermal.example",
            "quality_contact": "Chris Lee",
            "registered_address": "San Jose, CA, US",
            "payment_terms": "NET30",
            "avl_status": "APPROVED",
            "lead_time_days": 56,
            "source_update_ts": "2026-05-18T03:10:00Z",
            "ingest_seq": 14,
        },
    ]


def print_report(rows: list[dict[str, Any]], stats: MergeStats) -> None:
    print("\n=== SCD Type 1 Merge Summary ===")
    print(f"Inserted vendors:              {stats.inserted}")
    print(f"Updated vendors:               {stats.updated}")
    print(f"Unchanged vendors:             {stats.unchanged}")
    print(f"Source duplicates removed:     {stats.source_duplicates_removed}")
    print(f"Skipped null overwrites:       {stats.skipped_null_overwrites}")
    print(f"Avoided no-op field updates:   {stats.avoided_noop_updates}")
    print(f"History-sensitive changes:     {len(stats.history_sensitive_changes)}")

    print("\n--- Type 1 change log ---")
    pprint(stats.type1_change_log, sort_dicts=False)

    print("\n--- Needs Type 2 / audit routing ---")
    pprint(stats.history_sensitive_changes, sort_dicts=False)

    print("\n--- Final dim_vendor_current ---")
    for row in rows:
        print(
            f"{row['vendor_id']} | {row['vendor_name']} | "
            f"{row['contact_email']} | AVL={row['avl_status']} | LT={row['lead_time_days']}"
        )


def run_assertions(rows: list[dict[str, Any]], stats: MergeStats) -> None:
    by_id = {row[BUSINESS_KEY]: row for row in rows}

    assert stats.inserted == 1
    assert stats.updated == 1
    assert stats.unchanged == 2
    assert stats.source_duplicates_removed == 1
    assert stats.skipped_null_overwrites == 1
    assert len(stats.history_sensitive_changes) == 3

    # V200 Type 1 fields changed.
    assert by_id["V200"]["vendor_name"] == "Pacific Power Systems Ltd."
    assert by_id["V200"]["contact_email"] == "pm-new@pacific-psu.example"

    # But history-sensitive fields were not overwritten in the current dimension.
    assert by_id["V200"]["payment_terms"] == "NET60"
    assert by_id["V200"]["avl_status"] == "CONDITIONAL"
    assert by_id["V200"]["lead_time_days"] == 42

    # Null incoming phone did not wipe the existing value.
    assert by_id["V100"]["contact_phone"] == "+886-2-1111-0000"

    # New vendor inserted.
    assert by_id["V400"]["vendor_name"] == "Atlas GPU Logistics"


if __name__ == "__main__":
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    final_rows, merge_stats = merge_scd_type1(
        target_rows=sample_target_dim_vendor(),
        source_rows=sample_source_updates(),
        now=now,
    )
    print_report(final_rows, merge_stats)
    run_assertions(final_rows, merge_stats)
    print("\n✅ Practice validation passed: SCD Type 1 merge rules behaved as expected.")
