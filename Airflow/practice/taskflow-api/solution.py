"""
TaskFlow API practice: a tiny dependency-free simulator.

This is NOT Airflow. It is a learning model that makes TaskFlow's core ideas
visible:
- @task wraps a Python function into a task definition.
- Calling a task during DAG build returns XComArg, not the real value.
- XComArg arguments infer upstream dependencies automatically.
- Runtime executes tasks in dependency order and resolves XCom values.
- multiple_outputs, override(), and expand() are simulated for the ODM scenario.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple


@dataclass(frozen=True)
class XComArg:
    """Reference to a future task output, optionally narrowed to a dict key."""

    task_id: str
    key: Optional[str] = None

    def __getitem__(self, key: str) -> "XComArg":
        return XComArg(self.task_id, key)

    def __repr__(self) -> str:
        suffix = f"[{self.key!r}]" if self.key else ""
        return f"XComArg({self.task_id}{suffix})"


@dataclass
class TaskInvocation:
    task_id: str
    func: Callable[..., Any]
    args: Tuple[Any, ...]
    kwargs: Dict[str, Any]
    upstream: Set[str] = field(default_factory=set)
    multiple_outputs: bool = False


class MiniTaskFlowDag:
    """Builds and runs a small TaskFlow-like DAG."""

    def __init__(self, dag_id: str):
        self.dag_id = dag_id
        self.tasks: Dict[str, TaskInvocation] = {}

    def task(self, _func: Optional[Callable[..., Any]] = None, *, multiple_outputs: bool = False):
        def decorator(func: Callable[..., Any]) -> "TaskDef":
            return TaskDef(self, func, multiple_outputs=multiple_outputs)

        if _func is not None:
            return decorator(_func)
        return decorator

    def register(self, invocation: TaskInvocation) -> XComArg:
        if invocation.task_id in self.tasks:
            raise ValueError(f"Duplicate task_id: {invocation.task_id}")
        self.tasks[invocation.task_id] = invocation
        return XComArg(invocation.task_id)

    def graph_edges(self) -> List[Tuple[str, str]]:
        edges: List[Tuple[str, str]] = []
        for task_id, inv in self.tasks.items():
            for upstream in sorted(inv.upstream):
                edges.append((upstream, task_id))
        return sorted(edges)

    def run(self) -> Dict[str, Any]:
        """Execute in topological order and return the XCom store."""
        completed: Set[str] = set()
        xcom_store: Dict[str, Any] = {}

        print(f"\n=== Running DAG: {self.dag_id} ===")
        while len(completed) < len(self.tasks):
            ready = [
                task_id
                for task_id, inv in self.tasks.items()
                if task_id not in completed and inv.upstream <= completed
            ]
            if not ready:
                remaining = set(self.tasks) - completed
                raise RuntimeError(f"Cycle or unresolved dependency among: {remaining}")

            for task_id in sorted(ready):
                inv = self.tasks[task_id]
                resolved_args = tuple(self._resolve(a, xcom_store) for a in inv.args)
                resolved_kwargs = {k: self._resolve(v, xcom_store) for k, v in inv.kwargs.items()}
                print(f"▶ {task_id}  upstream={sorted(inv.upstream) or ['<none>']}")
                result = inv.func(*resolved_args, **resolved_kwargs)
                xcom_store[task_id] = result
                completed.add(task_id)

        return xcom_store

    @staticmethod
    def _resolve(value: Any, xcom_store: Dict[str, Any]) -> Any:
        if isinstance(value, XComArg):
            task_value = xcom_store[value.task_id]
            if value.key is None:
                return task_value
            return task_value[value.key]
        if isinstance(value, list):
            return [MiniTaskFlowDag._resolve(v, xcom_store) for v in value]
        if isinstance(value, dict):
            return {k: MiniTaskFlowDag._resolve(v, xcom_store) for k, v in value.items()}
        return value


class TaskDef:
    def __init__(self, dag: MiniTaskFlowDag, func: Callable[..., Any], *, multiple_outputs: bool = False, task_id: Optional[str] = None):
        self.dag = dag
        self.func = func
        self.multiple_outputs = multiple_outputs
        self.task_id = task_id or func.__name__

    def __call__(self, *args: Any, **kwargs: Any) -> XComArg:
        upstream = self._collect_upstream(args) | self._collect_upstream(kwargs)
        invocation = TaskInvocation(
            task_id=self.task_id,
            func=self.func,
            args=args,
            kwargs=kwargs,
            upstream=upstream,
            multiple_outputs=self.multiple_outputs,
        )
        return self.dag.register(invocation)

    def override(self, **task_kwargs: Any) -> "TaskDef":
        return TaskDef(
            self.dag,
            self.func,
            multiple_outputs=self.multiple_outputs,
            task_id=task_kwargs.get("task_id", self.task_id),
        )

    def expand(self, **mapped_kwargs: Iterable[Any]) -> List[XComArg]:
        if len(mapped_kwargs) != 1:
            raise NotImplementedError("This teaching simulator supports one mapped argument.")
        (arg_name, values), = mapped_kwargs.items()
        invocations = []
        for index, value in enumerate(values):
            mapped_task = self.override(task_id=f"{self.task_id}__{index}")
            invocations.append(mapped_task(**{arg_name: value}))
        return invocations

    @classmethod
    def _collect_upstream(cls, value: Any) -> Set[str]:
        if isinstance(value, XComArg):
            return {value.task_id}
        if isinstance(value, (list, tuple, set)):
            return set().union(*(cls._collect_upstream(v) for v in value)) if value else set()
        if isinstance(value, dict):
            return set().union(*(cls._collect_upstream(v) for v in value.values())) if value else set()
        return set()


def build_odm_procurement_pipeline() -> MiniTaskFlowDag:
    dag = MiniTaskFlowDag("odm_procurement_taskflow_demo")

    @dag.task
    def extract_sap_po_changes() -> Dict[str, Any]:
        # Real Airflow should pass an ADLS/S3/Delta path via XCom, not the full file.
        records = [
            {"po": "4500001001", "vendor": "V-CPU", "amount": 320_000, "lead_time_days": 45, "material_type": "direct"},
            {"po": "4500001002", "vendor": "V-FAN", "amount": 18_000, "lead_time_days": 21, "material_type": "direct"},
            {"po": "4500001003", "vendor": "V-MRO", "amount": 4_200, "lead_time_days": 7, "material_type": "mro"},
            {"po": "4500001004", "vendor": "V-SSD", "amount": 180_000, "lead_time_days": 60, "material_type": "direct"},
        ]
        landing_uri = "abfss://bronze/sap_po_changes/dt=2026-05-24/batch-0730.json"
        print(f"   extracted {len(records)} SAP PO changes → {landing_uri}")
        return {"landing_uri": landing_uri, "records": records}

    @dag.task(multiple_outputs=True)
    def classify_procurement_batch(batch: Dict[str, Any]) -> Dict[str, Any]:
        records = batch["records"]
        direct_pos = [r for r in records if r["material_type"] == "direct"]
        mro_pos = [r for r in records if r["material_type"] == "mro"]
        risky_pos = [r for r in direct_pos if r["lead_time_days"] >= 45]
        delta_uri = batch["landing_uri"].replace("bronze", "silver").replace(".json", ".delta")
        print(f"   classified direct={len(direct_pos)}, mro={len(mro_pos)}, risky_lt={len(risky_pos)}")
        return {
            "direct_pos": direct_pos,
            "mro_pos": mro_pos,
            "risky_pos": risky_pos,
            "silver_delta_uri": delta_uri,
            "vendors": sorted({r["vendor"] for r in direct_pos}),
        }

    @dag.task
    def load_to_delta(silver_delta_uri: str, direct_pos: List[Dict[str, Any]], mro_pos: List[Dict[str, Any]]) -> Dict[str, Any]:
        print(f"   MERGE {len(direct_pos) + len(mro_pos)} records into {silver_delta_uri}")
        return {"table": "silver.procurement_po", "version": 1287, "uri": silver_delta_uri}

    @dag.task
    def alert_long_lead_time_pos(risky_pos: List[Dict[str, Any]]) -> List[str]:
        alerts = [f"{po['po']}:{po['vendor']} LT={po['lead_time_days']}d" for po in risky_pos]
        print(f"   created shortage-risk alerts: {alerts}")
        return alerts

    @dag.task
    def update_vendor_scorecard(vendor: str) -> Dict[str, Any]:
        # Dynamic task mapping: one task per vendor, same function reused.
        mock_scores = {"V-CPU": 82, "V-FAN": 91, "V-SSD": 76}
        score = mock_scores.get(vendor, 80)
        print(f"   vendor {vendor} scorecard recalculated → {score}")
        return {"vendor": vendor, "score": score}

    @dag.task
    def publish_control_tower_event(delta_commit: Dict[str, Any], alerts: List[str], scorecards: List[Dict[str, Any]]) -> Dict[str, Any]:
        payload = {
            "delta_table": delta_commit["table"],
            "delta_version": delta_commit["version"],
            "alert_count": len(alerts),
            "scorecard_count": len(scorecards),
        }
        print(f"   published control tower event: {payload}")
        return payload

    @dag.task
    def validate_xcom_boundary(batch: Dict[str, Any]) -> str:
        # This task demonstrates the anti-pattern guard: XCom should remain small.
        record_count = len(batch["records"])
        if record_count > 1_000:
            return "Too large for metadata DB XCom: pass only a storage URI."
        return f"Teaching sample is tiny ({record_count} records); production should pass URI only."

    raw = extract_sap_po_changes()
    classified = classify_procurement_batch(raw)
    delta_commit = load_to_delta(classified["silver_delta_uri"], classified["direct_pos"], classified["mro_pos"])
    alerts = alert_long_lead_time_pos(classified["risky_pos"])
    xcom_boundary_note = validate_xcom_boundary(raw)

    # In real Airflow, expand can map over an upstream XComArg at runtime. This tiny
    # simulator maps over a known vendor list to keep the execution dependency-free.
    scorecards = update_vendor_scorecard.expand(vendor=["V-CPU", "V-FAN", "V-SSD"])
    publish_control_tower_event.override(task_id="publish_procurement_control_tower_event")(
        delta_commit,
        alerts,
        scorecards,
    )

    # Keep this output visible so the practice explicitly links back to XCom best practice.
    @dag.task
    def print_architecture_note(note: str) -> None:
        print(f"   SA note: {note}")

    print_architecture_note(xcom_boundary_note)
    return dag


def main() -> None:
    dag = build_odm_procurement_pipeline()

    print("=== DAG build phase: TaskFlow inferred these edges ===")
    for upstream, downstream in dag.graph_edges():
        print(f"{upstream:35s} -> {downstream}")

    xcom = dag.run()

    print("\n=== Final XCom summary ===")
    for task_id, value in xcom.items():
        preview = repr(value)
        if len(preview) > 110:
            preview = preview[:107] + "..."
        print(f"{task_id:35s} = {preview}")

    event = xcom["publish_procurement_control_tower_event"]
    assert event["delta_version"] == 1287
    assert event["alert_count"] == 2
    assert event["scorecard_count"] == 3
    print("\n✅ Practice passed: TaskFlow-style dependencies, XComArg resolution, override(), and expand() all worked.")


if __name__ == "__main__":
    main()
