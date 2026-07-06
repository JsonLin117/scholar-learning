"""
Airflow DAG Params + Trigger UI 表單模擬器
==========================================
不需要安裝 Airflow，模擬 Param 的 JSON Schema 驗證邏輯與觸發流程。
用 ODM 供應鏈「庫存對帳 Ad-hoc DAG」場景展示 Params 機制的實際效果。

重點模擬：
1. Param 型別驗證（string/integer/boolean/enum/array/object）
2. DAG-level / Task-level / 使用者觸發覆寫的優先順序
3. JSON Schema 驗證失敗 → 擋在觸發前（ParamValidationError），不是 task 跑到一半才炸
4. dag_run_conf_overrides_params=False 時鎖定為唯讀常數的行為
5. 四種觸發來源（Web UI / CLI / REST API / TriggerDagRunOperator）的 conf 合併
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Callable, Optional
import re


# ============================================================
# 1. Param 定義：模擬 airflow.sdk.Param 的核心驗證邏輯
# ============================================================

class ParamValidationError(Exception):
    """對齊 Airflow 的 ParamValidationError：驗證失敗時擋在觸發前"""
    pass


@dataclass
class Param:
    """模擬 Airflow Param：只實作我們這個場景會用到的 JSON Schema 子集"""
    default: Any
    type: str | list[str] = "string"          # "string"/"integer"/"boolean"/"array"/"object"/["null","string"]
    enum: Optional[list[Any]] = None
    minLength: Optional[int] = None
    maxLength: Optional[int] = None
    minimum: Optional[float] = None
    maximum: Optional[float] = None
    title: str = ""
    description_md: str = ""

    def _type_matches(self, value: Any, type_name: str) -> bool:
        py_type_map = {
            "string": str,
            "integer": int,
            "number": (int, float),
            "boolean": bool,
            "array": list,
            "object": dict,
            "null": type(None),
        }
        expected = py_type_map.get(type_name)
        if expected is None:
            return True
        # bool 是 int 的子類別，Python 特例：要先排除，避免 True 被誤判成 integer
        if type_name == "integer" and isinstance(value, bool):
            return False
        return isinstance(value, expected)

    def validate(self, value: Any) -> None:
        """驗證單一值，不合法就丟 ParamValidationError（模擬觸發前擋下）"""
        allowed_types = self.type if isinstance(self.type, list) else [self.type]

        if not any(self._type_matches(value, t) for t in allowed_types):
            raise ParamValidationError(
                f"型別錯誤：期望 {allowed_types}，收到 {type(value).__name__}（值={value!r}）"
            )

        if self.enum is not None and value not in self.enum:
            raise ParamValidationError(
                f"enum 驗證失敗：{value!r} 不在允許清單 {self.enum} 內"
            )

        if isinstance(value, str):
            if self.minLength is not None and len(value) < self.minLength:
                raise ParamValidationError(
                    f"字串長度不足：minLength={self.minLength}，實際長度={len(value)}"
                )
            if self.maxLength is not None and len(value) > self.maxLength:
                raise ParamValidationError(
                    f"字串長度超限：maxLength={self.maxLength}，實際長度={len(value)}"
                )

        if isinstance(value, (int, float)) and not isinstance(value, bool):
            if self.minimum is not None and value < self.minimum:
                raise ParamValidationError(f"數值過小：minimum={self.minimum}，實際={value}")
            if self.maximum is not None and value > self.maximum:
                raise ParamValidationError(f"數值過大：maximum={self.maximum}，實際={value}")


# ============================================================
# 2. DAG 定義：庫存對帳 Ad-hoc DAG（對齊筆記中的 ODM 場景）
# ============================================================

@dataclass
class MiniDAG:
    dag_id: str
    params: dict[str, Param]
    dag_run_conf_overrides_params: bool = True  # 對齊 Airflow core config 預設值

    def resolve_run_params(self, user_conf: dict[str, Any]) -> dict[str, Any]:
        """
        模擬 Airflow 觸發一次 DagRun 時的 params 決議流程：
        1. 從 DAG-level 預設值出發
        2. 若 dag_run_conf_overrides_params=True，套用使用者提供的 conf 覆寫
        3. 對「最終生效值」逐一做 JSON Schema 驗證
        驗證失敗會擋下整次觸發（不會產生 DagRun）。
        """
        resolved = {key: p.default for key, p in self.params.items()}

        if self.dag_run_conf_overrides_params:
            for key, value in user_conf.items():
                if key in self.params:
                    resolved[key] = value
                else:
                    # Airflow 允許傳入未宣告的 key（存進 conf 但不受 Param 驗證管控）
                    resolved[key] = value
        else:
            if user_conf:
                raise ParamValidationError(
                    "dag_run_conf_overrides_params=False：此 DAG 的 params 為唯讀常數，"
                    "不允許觸發時覆寫任何值"
                )

        # 對有定義 Param 規則的 key 做驗證
        for key, p in self.params.items():
            p.validate(resolved[key])

        return resolved


def build_inventory_reconciliation_dag(read_only: bool = False) -> MiniDAG:
    """對齊筆記 dag-parameterization-params-ui-trigger.md 的 ODM 場景 DAG"""
    return MiniDAG(
        dag_id="inventory_reconciliation_adhoc",
        dag_run_conf_overrides_params=not read_only,
        params={
            "part_number": Param(
                "",
                type="string",
                title="料號 (Part Number)",
                description_md="要重新對帳的料號，例如 PN-2026-0731",
                minLength=1,
                maxLength=40,
            ),
            "warehouse_type": Param(
                "RM",
                type="string",
                enum=["RM", "WIP", "FG"],
                title="倉別",
                description_md="RM=原物料倉 / WIP=在製品暫存 / FG=成品倉",
            ),
            "recalculate_safety_stock": Param(
                False,
                type="boolean",
                title="是否同時重算安全庫存",
            ),
            "row_limit": Param(
                5000,
                type="integer",
                minimum=1,
                maximum=100000,
                title="單次對帳筆數上限",
            ),
        },
    )


# ============================================================
# 3. 模擬四種觸發來源的 conf
# ============================================================

def trigger_via_web_ui(dag: MiniDAG, form_values: dict[str, Any]) -> dict[str, Any]:
    """Web UI Trigger 表單：使用者在網頁上填的值"""
    return dag.resolve_run_params(form_values)


def trigger_via_cli(dag: MiniDAG, conf_json_str: str) -> dict[str, Any]:
    """CLI: airflow dags trigger inventory_reconciliation_adhoc --conf '{...}'"""
    import json
    conf = json.loads(conf_json_str)
    return dag.resolve_run_params(conf)


def trigger_via_rest_api(dag: MiniDAG, request_body: dict[str, Any]) -> dict[str, Any]:
    """REST API: POST /dags/{dag_id}/dagRuns { "conf": {...} }"""
    return dag.resolve_run_params(request_body.get("conf", {}))


def trigger_via_trigger_dagrun_operator(dag: MiniDAG, upstream_xcom_value: str) -> dict[str, Any]:
    """TriggerDagRunOperator：上游 DAG 用 XCom 值動態組 conf"""
    conf = {"part_number": upstream_xcom_value, "warehouse_type": "FG"}
    return dag.resolve_run_params(conf)


# ============================================================
# 4. 測試案例：驗證各種輸入場景
# ============================================================

@dataclass
class TestCase:
    name: str
    run: Callable[[], dict[str, Any]]
    expect_pass: bool


def run_all_test_cases() -> None:
    dag = build_inventory_reconciliation_dag()
    readonly_dag = build_inventory_reconciliation_dag(read_only=True)

    cases: list[TestCase] = [
        TestCase(
            "合法輸入（Web UI 表單）：正常料號 + RM 倉別",
            lambda: trigger_via_web_ui(dag, {"part_number": "PN-2026-0731", "warehouse_type": "RM"}),
            expect_pass=True,
        ),
        TestCase(
            "空料號 → 應被 minLength 擋下",
            lambda: trigger_via_web_ui(dag, {"part_number": "", "warehouse_type": "RM"}),
            expect_pass=False,
        ),
        TestCase(
            "不合法倉別代碼（誤植 'RAW'）→ 應被 enum 擋下",
            lambda: trigger_via_web_ui(dag, {"part_number": "PN-2026-0731", "warehouse_type": "RAW"}),
            expect_pass=False,
        ),
        TestCase(
            "布林值填成字串 'true' → 應被型別驗證擋下",
            lambda: trigger_via_web_ui(
                dag,
                {"part_number": "PN-2026-0731", "warehouse_type": "FG", "recalculate_safety_stock": "true"},
            ),
            expect_pass=False,
        ),
        TestCase(
            "row_limit 超過上限 200000 → 應被 maximum 擋下",
            lambda: trigger_via_web_ui(
                dag,
                {"part_number": "PN-2026-0731", "warehouse_type": "WIP", "row_limit": 200000},
            ),
            expect_pass=False,
        ),
        TestCase(
            "CLI 觸發：合法 JSON conf",
            lambda: trigger_via_cli(
                dag, '{"part_number": "PN-CLI-001", "warehouse_type": "FG", "row_limit": 200}'
            ),
            expect_pass=True,
        ),
        TestCase(
            "REST API 觸發：合法 conf",
            lambda: trigger_via_rest_api(
                dag, {"conf": {"part_number": "PN-API-002", "warehouse_type": "RM"}}
            ),
            expect_pass=True,
        ),
        TestCase(
            "TriggerDagRunOperator：上游 XCom 帶入合法料號",
            lambda: trigger_via_trigger_dagrun_operator(dag, "PN-UPSTREAM-777"),
            expect_pass=True,
        ),
        TestCase(
            "唯讀 DAG（dag_run_conf_overrides_params=False）+ 嘗試覆寫 → 應被擋下",
            lambda: trigger_via_web_ui(readonly_dag, {"part_number": "PN-2026-0731"}),
            expect_pass=False,
        ),
        TestCase(
            "唯讀 DAG + 空 conf → 用預設值驗證，但預設 part_number='' 本身不合法（minLength=1）"
            "，揭露一個真實的設計陷阱：required 欄位 + 唯讀覆寫 = 永遠無法觸發的死鎖 DAG",
            lambda: trigger_via_web_ui(readonly_dag, {}),
            expect_pass=False,
        ),
    ]

    print("=" * 70)
    print("ODM 庫存對帳 Ad-hoc DAG — Params 驗證測試")
    print("=" * 70)

    passed, failed = 0, 0
    for case in cases:
        try:
            result = case.run()
            actual_pass = True
            detail = f"resolved params = {result}"
        except ParamValidationError as e:
            actual_pass = False
            detail = f"REJECTED: {e}"

        ok = actual_pass == case.expect_pass
        status = "✅ PASS" if ok else "❌ 測試案例與預期不符"
        outcome_label = "通過驗證" if actual_pass else "被擋下"

        print(f"\n[{status}] {case.name}")
        print(f"  結果：{outcome_label}")
        print(f"  細節：{detail}")

        if ok:
            passed += 1
        else:
            failed += 1

    print("\n" + "=" * 70)
    print(f"總結：{passed}/{len(cases)} 案例符合預期，{failed} 案例不符")
    print("=" * 70)

    assert failed == 0, f"有 {failed} 個測試案例結果與預期不符，請檢查驗證邏輯"


if __name__ == "__main__":
    run_all_test_cases()
    print("\n🎯 核心收穫：JSON Schema 驗證在觸發前就攔截了所有錯誤輸入，")
    print("   倉管不需要懂 code，也不會因為填錯格式而產生半吊子的 DagRun。")
