"""
Airflow Connection & Hook practice
Scenario: ODM daily material shortage check without hard-coded credentials.

This is dependency-free on purpose: it models Airflow's Connection/Hook ideas so
it can run in a plain Python environment while preserving the architecture:

    Connection Resolver -> Hook.get_conn() -> task business logic

Key lessons:
  - Connections store external-system credentials and endpoints.
  - Search order matters: worker secrets -> global secrets -> env vars -> metadata DB.
  - Hooks should be created/used inside task execution, not at DAG parse time.
  - Variables are for runtime config; Connections are for credentials/endpoints.
"""

from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Tuple


# -----------------------------------------------------------------------------
# Connection model and backends
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class Connection:
    conn_id: str
    conn_type: str
    host: str
    login: str = ""
    password: str = ""
    port: Optional[int] = None
    schema: str = ""
    extra: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_json(cls, conn_id: str, raw: str) -> "Connection":
        payload = json.loads(raw)
        return cls(conn_id=conn_id, **payload)

    def safe_summary(self) -> str:
        """Show useful metadata while never printing secrets."""
        port = f":{self.port}" if self.port else ""
        login = self.login or "<no-login>"
        return f"{self.conn_type}://{login}:***@{self.host}{port}/{self.schema}"


class ConnectionBackend:
    name: str

    def get(self, conn_id: str) -> Optional[Connection]:
        raise NotImplementedError


class DictBackend(ConnectionBackend):
    def __init__(self, name: str, connections: Dict[str, Connection]):
        self.name = name
        self._connections = connections

    def get(self, conn_id: str) -> Optional[Connection]:
        return self._connections.get(conn_id)


class EnvVarBackend(ConnectionBackend):
    """Mimics AIRFLOW_CONN_<CONN_ID> lookup using JSON for readability."""

    name = "env_var"

    def get(self, conn_id: str) -> Optional[Connection]:
        key = "AIRFLOW_CONN_" + conn_id.upper().replace("-", "_")
        raw = os.environ.get(key)
        return Connection.from_json(conn_id, raw) if raw else None


class ConnectionResolver:
    def __init__(self, backends: Iterable[ConnectionBackend]):
        self.backends = list(backends)
        self.audit_log: List[Tuple[str, str]] = []

    def resolve(self, conn_id: str) -> Connection:
        for backend in self.backends:
            conn = backend.get(conn_id)
            if conn:
                self.audit_log.append((conn_id, backend.name))
                return conn
        raise KeyError(f"Connection not found: {conn_id}")


class VariableStore:
    """Tiny stand-in for Airflow Variable.get()."""

    def __init__(self, values: Dict[str, str]):
        self.values = values

    def get(self, key: str, default_var: str) -> str:
        return self.values.get(key, default_var)


# -----------------------------------------------------------------------------
# Hooks: get Connection, return native clients
# -----------------------------------------------------------------------------

class BaseHook:
    conn_name_attr = "conn_id"
    default_conn_name = "default"
    conn_type = "generic"
    hook_name = "Base Hook"

    def __init__(self, resolver: ConnectionResolver, conn_id: Optional[str] = None):
        self.resolver = resolver
        self.conn_id = conn_id or self.default_conn_name

    def get_connection(self) -> Connection:
        return self.resolver.resolve(self.conn_id)


class ERPDatabaseHook(BaseHook):
    conn_type = "postgres"
    hook_name = "ODM ERP Database"

    def get_conn(self) -> sqlite3.Connection:
        """Return a native DB connection; sqlite simulates ERP DB for the demo."""
        conn_meta = self.get_connection()
        if not conn_meta.login.endswith("readonly"):
            raise PermissionError("ERP extraction must use a readonly account")

        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(
            """
            CREATE TABLE purchase_orders (
                po_number TEXT,
                material_id TEXT,
                required_qty INTEGER,
                required_date TEXT
            );
            CREATE TABLE inventory (
                material_id TEXT PRIMARY KEY,
                on_hand INTEGER
            );
            """
        )
        conn.executemany(
            "INSERT INTO purchase_orders VALUES (?, ?, ?, ?)",
            [
                ("PO-9001", "CPU-XEON-8468", 120, "2026-05-18"),
                ("PO-9002", "SSD-7T68", 500, "2026-05-18"),
                ("PO-9003", "NIC-200G", 80, "2026-05-18"),
            ],
        )
        conn.executemany(
            "INSERT INTO inventory VALUES (?, ?)",
            [
                ("CPU-XEON-8468", 70),
                ("SSD-7T68", 470),
                ("NIC-200G", 10),
            ],
        )
        return conn

    def get_shortage_candidates(self, required_date: str) -> List[Dict[str, Any]]:
        db = self.get_conn()
        rows = db.execute(
            """
            SELECT po.po_number,
                   po.material_id,
                   po.required_qty,
                   po.required_date,
                   COALESCE(inv.on_hand, 0) AS on_hand,
                   po.required_qty - COALESCE(inv.on_hand, 0) AS gap
            FROM purchase_orders po
            LEFT JOIN inventory inv USING (material_id)
            WHERE po.required_date = ?
            ORDER BY gap DESC
            """,
            (required_date,),
        ).fetchall()
        return [dict(row) for row in rows]


@dataclass
class FakeHTTPSession:
    base_url: str
    headers: Dict[str, str]
    payloads: Dict[str, Any]

    def get_json(self, endpoint: str) -> Any:
        if "Authorization" not in self.headers:
            raise PermissionError("Missing bearer token")
        return self.payloads[endpoint]

    def post_json(self, endpoint: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        self.payloads[endpoint] = payload
        return {"status": "sent", "endpoint": f"{self.base_url}{endpoint}", "payload": payload}


class MESAPIHook(BaseHook):
    conn_type = "http"
    hook_name = "ODM MES/SFCS API"

    def get_conn(self) -> FakeHTTPSession:
        conn = self.get_connection()
        return FakeHTTPSession(
            base_url=f"https://{conn.host}:{conn.port or 443}",
            headers={"Authorization": f"Bearer {conn.password}"},
            payloads={
                "/api/v1/plants/TW01/wip": [
                    {"material_id": "CPU-XEON-8468", "wip_reserved": 20},
                    {"material_id": "SSD-7T68", "wip_reserved": 15},
                    {"material_id": "NIC-200G", "wip_reserved": 5},
                ]
            },
        )

    def get_wip_status(self, plant_code: str) -> List[Dict[str, Any]]:
        return self.get_conn().get_json(f"/api/v1/plants/{plant_code}/wip")


class TeamsWebhookHook(BaseHook):
    conn_type = "http"
    hook_name = "Teams Webhook"

    def send_shortage_alert(self, shortages: List[Dict[str, Any]]) -> Dict[str, Any]:
        conn = self.get_connection()
        session = FakeHTTPSession(
            base_url=f"https://{conn.host}",
            headers={"Authorization": f"Bearer {conn.password}"},
            payloads={},
        )
        text = "\n".join(
            f"- {s['material_id']}: gap={s['net_gap']} for {s['po_number']}"
            for s in shortages
        )
        return session.post_json("/webhook/shortage", {"text": f"ODM shortage alert\n{text}"})


# -----------------------------------------------------------------------------
# DAG-like task functions. Notice hooks are created inside task execution.
# -----------------------------------------------------------------------------

def parse_dag_without_touching_external_systems() -> List[str]:
    """DAG parse should only declare conn_ids; no DB/API connection is opened."""
    return ["erp_prod_readonly", "odm_mes_api", "odm_teams_webhook"]


def extract_erp_orders(resolver: ConnectionResolver, required_date: str) -> List[Dict[str, Any]]:
    hook = ERPDatabaseHook(resolver, conn_id="erp_prod_readonly")
    return hook.get_shortage_candidates(required_date)


def extract_mes_wip(resolver: ConnectionResolver, plant_code: str) -> Dict[str, int]:
    hook = MESAPIHook(resolver, conn_id="odm_mes_api")
    rows = hook.get_wip_status(plant_code)
    return {row["material_id"]: row["wip_reserved"] for row in rows}


def calculate_shortages(
    orders: List[Dict[str, Any]],
    wip_reserved_by_material: Dict[str, int],
    variables: VariableStore,
) -> List[Dict[str, Any]]:
    threshold = int(variables.get("shortage_alert_threshold", default_var="50"))
    shortages = []
    for order in orders:
        reserved = wip_reserved_by_material.get(order["material_id"], 0)
        net_gap = order["required_qty"] - order["on_hand"] - reserved
        if net_gap >= threshold:
            shortages.append({**order, "wip_reserved": reserved, "net_gap": net_gap})
    return shortages


def alert_shortages(resolver: ConnectionResolver, shortages: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not shortages:
        return None
    hook = TeamsWebhookHook(resolver, conn_id="odm_teams_webhook")
    return hook.send_shortage_alert(shortages)


# -----------------------------------------------------------------------------
# Demo wiring
# -----------------------------------------------------------------------------

def build_resolver() -> ConnectionResolver:
    # Simulate env-var Connection. This can be rotated by deployment config without
    # changing DAG code.
    os.environ["AIRFLOW_CONN_ODM_MES_API"] = json.dumps(
        {
            "conn_type": "http",
            "host": "mes-api.tw01.odm.example",
            "login": "airflow_mes_reader",
            "password": "env-token-mes-reader",
            "port": 443,
            "schema": "",
            "extra": {"timeout": 10},
        }
    )

    worker_secrets = DictBackend(
        "worker_specific_secrets",
        {
            "erp_prod_readonly": Connection(
                conn_id="erp_prod_readonly",
                conn_type="postgres",
                host="erp-read-replica.worker.local",
                login="airflow_erp_readonly",
                password="worker-secret-rotated",
                port=5432,
                schema="sap_production",
                extra={"sslmode": "require"},
            )
        },
    )
    global_secrets = DictBackend(
        "global_secrets_backend",
        {
            # Would be used if no worker-specific secret exists.
            "erp_prod_readonly": Connection(
                conn_id="erp_prod_readonly",
                conn_type="postgres",
                host="erp-read-replica.global.local",
                login="airflow_erp_readonly",
                password="global-secret",
                port=5432,
                schema="sap_production",
            )
        },
    )
    metadata_db = DictBackend(
        "metadata_db_fernet_encrypted",
        {
            "odm_teams_webhook": Connection(
                conn_id="odm_teams_webhook",
                conn_type="http",
                host="teams.example",
                login="airflow_alert_bot",
                password="metadata-fernet-protected-token",
                extra={"channel": "supply-chain-war-room"},
            ),
            # A stale ERP value exists in DB, but it must not win over secrets.
            "erp_prod_readonly": Connection(
                conn_id="erp_prod_readonly",
                conn_type="postgres",
                host="stale-db-value.local",
                login="admin",  # intentionally wrong; search order prevents use
                password="stale-admin-password",
                port=5432,
                schema="sap_production",
            ),
        },
    )

    return ConnectionResolver([worker_secrets, global_secrets, EnvVarBackend(), metadata_db])


def main() -> None:
    print("=" * 72)
    print("Airflow Connection & Hook practice — ODM shortage check")
    print("=" * 72)

    resolver = build_resolver()
    variables = VariableStore({"shortage_alert_threshold": "25"})

    declared_conn_ids = parse_dag_without_touching_external_systems()
    print(f"\nDAG parse declared conn_ids only: {declared_conn_ids}")
    print(f"Connection lookups during parse: {len(resolver.audit_log)}")
    assert len(resolver.audit_log) == 0

    orders = extract_erp_orders(resolver, required_date="2026-05-18")
    wip = extract_mes_wip(resolver, plant_code="TW01")
    shortages = calculate_shortages(orders, wip, variables)
    alert_result = alert_shortages(resolver, shortages)

    print("\nResolved Connections:")
    task_resolution_audit = list(resolver.audit_log)
    for conn_id, source in task_resolution_audit:
        conn = resolver.resolve(conn_id)
        # The extra resolve adds audit entries, so this print is for human-readable demo only.
        print(f"- {conn_id:<18} <- {source:<28} {conn.safe_summary()}")
    # Remove print-only duplicate audit entries before assertions.
    resolver.audit_log = task_resolution_audit

    print("\nShortage candidates:")
    for row in orders:
        print(
            f"- {row['po_number']} {row['material_id']}: "
            f"required={row['required_qty']}, on_hand={row['on_hand']}, raw_gap={row['gap']}"
        )

    print("\nNet shortages after MES WIP reservation:")
    for row in shortages:
        print(
            f"- {row['material_id']}: raw_gap={row['gap']}, "
            f"wip_reserved={row['wip_reserved']}, net_gap={row['net_gap']}"
        )

    print("\nAlert payload:")
    print(json.dumps(alert_result, ensure_ascii=False, indent=2))

    # Assertions are the real practice verification gate.
    assert resolver.audit_log == [
        ("erp_prod_readonly", "worker_specific_secrets"),
        ("odm_mes_api", "env_var"),
        ("odm_teams_webhook", "metadata_db_fernet_encrypted"),
    ]
    assert [s["material_id"] for s in shortages] == ["NIC-200G", "CPU-XEON-8468"]
    assert alert_result and alert_result["status"] == "sent"
    assert "stale-db-value" not in json.dumps(alert_result)
    print("\n✅ Practice passed: Connections resolved safely, hooks ran inside tasks, alert sent.")


if __name__ == "__main__":
    main()
