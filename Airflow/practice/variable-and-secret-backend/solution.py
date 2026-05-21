"""
Airflow Variable & Secret Backend practice (dependency-free).

Scenario:
- ODM has one Airflow platform orchestrating ERP sync for TPE / KH / SZ plants.
- We simulate Airflow's Variable lookup order:
  Worker Secret Backend -> Global Secret Backend -> Environment Variables -> Metadata DB
- We also quantify two common pitfalls:
  1. top-level Variable.get() during DAG parse
  2. phantom write when Secret Backend shadows DB values
"""

from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass, field
from typing import Any, Optional


class MetadataDB:
    """Tiny stand-in for Airflow's metadata DB Variable table."""

    def __init__(self, initial: dict[str, str]):
        self.values = dict(initial)
        self.read_count = 0
        self.write_count = 0

    def get(self, key: str) -> Optional[str]:
        self.read_count += 1
        return self.values.get(key)

    def set(self, key: str, value: str) -> None:
        self.write_count += 1
        self.values[key] = value

    def reset_metrics(self) -> None:
        self.read_count = 0
        self.write_count = 0


@dataclass
class SecretBackend:
    """Simulates an Airflow secrets backend such as Azure Key Vault."""

    name: str
    prefix: str
    secrets: dict[str, str]
    lookup_pattern: str = r".*"
    cache_ttl_seconds: int = 60
    api_calls: int = 0
    cache_hits: int = 0
    _cache: dict[str, tuple[float, Optional[str]]] = field(default_factory=dict)

    def _secret_name(self, key: str) -> str:
        # Azure Key Vault cannot use underscores in secret names; Airflow providers commonly map _ -> -.
        normalized = key.lower().replace("_", "-")
        return f"{self.prefix}-{normalized}"

    def get_variable(self, key: str) -> Optional[str]:
        if not re.fullmatch(self.lookup_pattern, key):
            return None

        now = time.time()
        if key in self._cache:
            cached_at, cached_value = self._cache[key]
            if now - cached_at <= self.cache_ttl_seconds:
                self.cache_hits += 1
                return cached_value

        self.api_calls += 1
        value = self.secrets.get(self._secret_name(key))
        self._cache[key] = (now, value)
        return value

    def has_key(self, key: str) -> bool:
        return self._secret_name(key) in self.secrets

    def reset_metrics(self) -> None:
        self.api_calls = 0
        self.cache_hits = 0
        self._cache.clear()


@dataclass
class LookupResult:
    key: str
    value: Any
    source: str


class VariableResolver:
    """Variable.get / Variable.set model with Airflow-like precedence."""

    def __init__(
        self,
        metadata_db: MetadataDB,
        env: dict[str, str],
        global_backend: SecretBackend,
        worker_backend: Optional[SecretBackend] = None,
    ):
        self.metadata_db = metadata_db
        self.env = env
        self.global_backend = global_backend
        self.worker_backend = worker_backend

    def get(self, key: str, *, deserialize_json: bool = False, worker_context: bool = False) -> LookupResult:
        search_chain: list[tuple[str, Any]] = []
        if worker_context and self.worker_backend:
            search_chain.append(("worker_secret_backend", self.worker_backend.get_variable))
        search_chain.append(("global_secret_backend", self.global_backend.get_variable))
        search_chain.append(("environment", self._get_env_var))
        search_chain.append(("metadata_db", self.metadata_db.get))

        for source, getter in search_chain:
            raw = getter(key)
            if raw is not None:
                value: Any = json.loads(raw) if deserialize_json else raw
                return LookupResult(key=key, value=value, source=source)

        raise KeyError(f"Variable {key!r} not found")

    def set(self, key: str, value: str) -> str:
        """Airflow Variable.set() writes to DB, not to secret backends."""
        self.metadata_db.set(key, value)
        if (self.worker_backend and self.worker_backend.has_key(key)) or self.global_backend.has_key(key):
            return (
                f"WARNING: Variable.set({key!r}) wrote metadata DB, but a Secret Backend value "
                "still has higher precedence. Future get() calls will not return the DB value."
            )
        return f"OK: Variable {key!r} stored in metadata DB."

    def _get_env_var(self, key: str) -> Optional[str]:
        env_key = "AIRFLOW_VAR_" + key.upper()
        return self.env.get(env_key)


def build_resolver() -> VariableResolver:
    metadata_db = MetadataDB(
        {
            "dashboard_refresh_interval": "30",
            "feature_flag_new_pipeline": "true",
            "etl_config": json.dumps({"plants": ["DEV"], "batch_size": 1000, "lookback_days": 1}),
        }
    )

    env = {
        "AIRFLOW_VAR_ALERT_CHANNEL": "teams://odm-de-alerts",
    }

    global_kv = SecretBackend(
        name="azure-key-vault-global",
        prefix="airflow-variables",
        lookup_pattern=r"etl_config|alert_channel|.*_secret",  # avoid needless calls for UI-only vars
        cache_ttl_seconds=300,
        secrets={
            "airflow-variables-etl-config": json.dumps(
                {
                    "plants": ["TPE", "KH", "SZ"],
                    "batch_size": 10000,
                    "lookback_days": 3,
                    "target_schema": "bronze_erp",
                }
            ),
        },
    )

    worker_kv = SecretBackend(
        name="azure-key-vault-worker-only",
        prefix="airflow-variables-team-de",
        lookup_pattern=r"erp_extract_window|.*_secret",
        cache_ttl_seconds=120,
        secrets={
            "airflow-variables-team-de-erp-extract-window": json.dumps(
                {"TPE": "01:00-02:30", "KH": "02:30-04:00", "SZ": "04:00-05:30"}
            )
        },
    )

    return VariableResolver(metadata_db, env, global_kv, worker_kv)


def render_odm_runtime_config(resolver: VariableResolver) -> None:
    print("\n=== ODM ERP sync runtime config ===")
    config = resolver.get("etl_config", deserialize_json=True, worker_context=True)
    alert = resolver.get("alert_channel", worker_context=True)
    extract_window = resolver.get("erp_extract_window", deserialize_json=True, worker_context=True)
    refresh = resolver.get("dashboard_refresh_interval", worker_context=False)

    print(f"etl_config source        : {config.source}")
    print(f"plants                   : {', '.join(config.value['plants'])}")
    print(f"batch_size/lookback      : {config.value['batch_size']} / {config.value['lookback_days']} days")
    print(f"alert_channel source     : {alert.source}")
    print(f"worker-only window source: {extract_window.source}")
    print(f"UI config source         : {refresh.source}")

    for plant in config.value["plants"]:
        print(
            f"- {plant}: extract {extract_window.value[plant]}, "
            f"load into {config.value['target_schema']}, alert via {alert.value}"
        )


def simulate_scheduler_parse_cost(resolver: VariableResolver, dag_count: int = 120, parse_cycles: int = 20) -> None:
    print("\n=== Anti-pattern: top-level Variable.get() during DAG parse ===")

    resolver.metadata_db.reset_metrics()
    resolver.global_backend.reset_metrics()
    resolver.worker_backend.reset_metrics() if resolver.worker_backend else None

    # Bad: every DAG file reads a DB-backed Variable at import/parse time.
    for _ in range(parse_cycles):
        for _ in range(dag_count):
            resolver.get("dashboard_refresh_interval")

    bad_db_reads = resolver.metadata_db.read_count
    print(f"Bad pattern DB reads     : {bad_db_reads:,} ({dag_count} DAGs × {parse_cycles} parse cycles)")

    # Good: Jinja/template/runtime resolution, so parse stage does not query Variable.
    resolver.metadata_db.reset_metrics()
    task_runs = 5
    for _ in range(task_runs):
        resolver.get("dashboard_refresh_interval")

    print(f"Good pattern DB reads    : {resolver.metadata_db.read_count:,} ({task_runs} task runtime reads only)")
    print("Rule                     : never call Variable.get() at DAG top level; use Jinja or task body.")


def demonstrate_secret_backend_cache(resolver: VariableResolver) -> None:
    print("\n=== Secret Backend cache / lookup_pattern ===")
    resolver.global_backend.reset_metrics()

    for _ in range(5):
        resolver.get("etl_config", deserialize_json=True, worker_context=True)
    for _ in range(5):
        resolver.get("dashboard_refresh_interval", worker_context=True)  # lookup_pattern skips KV

    print(f"Global KV API calls      : {resolver.global_backend.api_calls}")
    print(f"Global KV cache hits     : {resolver.global_backend.cache_hits}")
    print("Insight                  : lookup_pattern avoids useless KV calls; cache_ttl reduces repeated reads.")


def demonstrate_phantom_write(resolver: VariableResolver) -> None:
    print("\n=== Phantom write: Secret Backend is read-only ===")
    before = resolver.get("etl_config", deserialize_json=True, worker_context=True)
    warning = resolver.set(
        "etl_config",
        json.dumps({"plants": ["ONLY_DB"], "batch_size": 1, "lookback_days": 99}),
    )
    after = resolver.get("etl_config", deserialize_json=True, worker_context=True)

    print(f"Before set() source      : {before.source}, plants={before.value['plants']}")
    print(warning)
    print(f"After set() source       : {after.source}, plants={after.value['plants']}")
    print("Fix                      : update Azure Key Vault / Vault directly, not Variable.set().")


def main() -> None:
    os.environ.setdefault("AIRFLOW__CORE__FERNET_KEY", "not-used-in-this-simulation")
    resolver = build_resolver()

    render_odm_runtime_config(resolver)
    simulate_scheduler_parse_cost(resolver)
    demonstrate_secret_backend_cache(resolver)
    demonstrate_phantom_write(resolver)

    print("\n✅ Practice complete: Variable vs Secret Backend behavior is observable and testable.")


if __name__ == "__main__":
    main()
