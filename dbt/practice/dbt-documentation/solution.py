"""
dbt Documentation 練習 — ODM 供應鏈場景
=========================================
模擬 dbt 的文件系統三大機制（dependency-free，不需要真的 dbt CLI）：

1. DocBlockRegistry  — {% docs %} / {{ doc() }} 的註冊與解析
2. CoverageChecker   — marts 必寫 + PK/FK 必寫的分層文件覆蓋率規則引擎
3. PersistDocsEmitter — persist_docs 開啟時，把 description 轉成
                        COMMENT ON TABLE / COMMENT ON COLUMN DDL

場景：ODM 採購資料倉庫，staging(SAP 映射) → intermediate(業務邏輯) →
marts(採購事實表 + 供應商維度表)。CI 要求 marts 層 100% 文件覆蓋，
PK/FK 欄位無論哪層都必須有 description。
"""

from __future__ import annotations
import re
import sys
from dataclasses import dataclass, field
from typing import Optional


# ---------------------------------------------------------------------------
# 1. Doc Block Registry — 模擬 {% docs name %} ... {% enddocs %} + {{ doc() }}
# ---------------------------------------------------------------------------

class DocBlockRegistry:
    """模擬 dbt 的 docs block 機制：集中定義共用欄位說明，多處引用。"""

    DOC_REF_PATTERN = re.compile(r'\{\{\s*doc\(\s*["\']([a-zA-Z0-9_]+)["\']\s*\)\s*\}\}')

    def __init__(self):
        self._blocks: dict[str, str] = {}

    def register(self, name: str, content: str) -> None:
        if not re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', name):
            raise ValueError(f"doc block 名稱不合法（只能 [A-Za-z0-9_]，不能數字開頭）: {name!r}")
        if name in self._blocks:
            raise ValueError(f"doc block 名稱重複，全專案必須唯一: {name!r}")
        self._blocks[name] = content

    def resolve(self, description: str) -> str:
        """把 description 中所有 {{ doc("name") }} 替換成實際內容，模擬 dbt compile。"""
        def _sub(match: "re.Match[str]") -> str:
            name = match.group(1)
            if name not in self._blocks:
                raise KeyError(f"compile error: doc block '{name}' 不存在（{{{{ doc('{name}') }}}} 找不到定義）")
            return self._blocks[name]
        return self.DOC_REF_PATTERN.sub(_sub, description)

    def block_count(self) -> int:
        return len(self._blocks)


# ---------------------------------------------------------------------------
# 2. 資料模型：欄位 / model / 分層
# ---------------------------------------------------------------------------

@dataclass
class Column:
    name: str
    description: str = ""          # 可能含未 resolve 的 {{ doc() }} 語法
    is_pk: bool = False
    is_fk: bool = False

    def resolved_description(self, registry: DocBlockRegistry) -> str:
        return registry.resolve(self.description) if self.description else ""

    def has_description(self, registry: DocBlockRegistry) -> bool:
        return bool(self.resolved_description(registry).strip())


@dataclass
class Model:
    name: str
    layer: str                      # staging / intermediate / marts
    description: str = ""
    columns: list[Column] = field(default_factory=list)
    persist_docs: dict[str, bool] = field(default_factory=lambda: {"relation": False, "columns": False})
    materialization: str = "view"   # view / table / incremental

    def resolved_description(self, registry: DocBlockRegistry) -> str:
        return registry.resolve(self.description) if self.description else ""

    def has_description(self, registry: DocBlockRegistry) -> bool:
        return bool(self.resolved_description(registry).strip())


# ---------------------------------------------------------------------------
# 3. Coverage Checker — 分層必寫矩陣
# ---------------------------------------------------------------------------

@dataclass
class CoverageIssue:
    model: str
    target: str      # "model" 或 column name
    reason: str


class CoverageChecker:
    """
    必寫規則（CI 強制）：
      - marts model 本身必須有 description
      - marts 的每個 column 必須有 description
      - 任何層的 PK/FK column 必須有 description（無論哪層都算血緣關鍵欄位）
    選寫（不計入 fail，但仍算進 coverage % 分母，體現真實覆蓋率數字）：
      - staging / intermediate 的一般 column
      - staging / intermediate model 本身
    """

    def __init__(self, registry: DocBlockRegistry):
        self.registry = registry

    def _is_required(self, model: Model, target_is_model: bool, col: Optional[Column]) -> bool:
        if model.layer == "marts":
            return True
        if col is not None and (col.is_pk or col.is_fk):
            return True
        return False

    def check(self, models: list[Model]) -> tuple[list[CoverageIssue], dict]:
        issues: list[CoverageIssue] = []
        total_targets = 0
        documented_targets = 0

        for m in models:
            # model-level description
            total_targets += 1
            model_documented = m.has_description(self.registry)
            if model_documented:
                documented_targets += 1
            if self._is_required(m, True, None) and not model_documented:
                issues.append(CoverageIssue(m.name, "model", f"marts model 必須有 description（layer={m.layer}）"))

            # column-level description
            for c in m.columns:
                total_targets += 1
                col_documented = c.has_description(self.registry)
                if col_documented:
                    documented_targets += 1
                required = self._is_required(m, False, c)
                if required and not col_documented:
                    reason = "marts column 必須有 description" if m.layer == "marts" else "PK/FK column 必須有 description"
                    issues.append(CoverageIssue(m.name, c.name, reason))

        coverage_pct = round(100.0 * documented_targets / total_targets, 1) if total_targets else 100.0
        stats = {
            "total_targets": total_targets,
            "documented_targets": documented_targets,
            "coverage_pct": coverage_pct,
            "required_fail_count": len(issues),
        }
        return issues, stats


# ---------------------------------------------------------------------------
# 4. persist_docs → COMMENT ON TABLE/COLUMN DDL 產生器
# ---------------------------------------------------------------------------

class PersistDocsEmitter:
    """只對開啟 persist_docs 的 model 產生 COMMENT DDL，模擬倉庫 metadata 同步。"""

    def __init__(self, registry: DocBlockRegistry):
        self.registry = registry

    def emit(self, model: Model) -> list[str]:
        ddls: list[str] = []
        if model.persist_docs.get("relation") and model.has_description(self.registry):
            desc = model.resolved_description(self.registry).replace("'", "''").strip()
            ddls.append(f"COMMENT ON TABLE {model.name} IS '{desc}';")

        if model.persist_docs.get("columns"):
            for c in model.columns:
                if c.has_description(self.registry):
                    desc = c.resolved_description(self.registry).replace("'", "''").strip()
                    ddls.append(f"COMMENT ON COLUMN {model.name}.{c.name} IS '{desc}';")
        return ddls


# ---------------------------------------------------------------------------
# 5. 建構 ODM mini dbt project
# ---------------------------------------------------------------------------

def build_registry() -> DocBlockRegistry:
    registry = DocBlockRegistry()
    registry.register(
        "plant_code",
        "工廠代碼。格式：4 位數字。1000=台南廠, 2000=馬來西亞廠, 3000=墨西哥廠。來源：SAP T001W-WERKS。",
    )
    registry.register(
        "vendor_id",
        "供應商編號，FK -> dim_vendors.vendor_id。來源：SAP LFA1-LIFNR。",
    )
    registry.register(
        "po_status",
        "PO 狀態碼：OPEN/APPROVED/PARTIAL/CLOSED/CANCELLED，由 SAP EKKO-FRGKE + EKPO-ELIKZ 推導。",
    )
    return registry


def build_models_round1() -> list[Model]:
    """Round 1：vendor_id FK 故意漏寫 description，模擬 CI 第一次擋下。"""
    return [
        Model(
            name="stg_sap_po",
            layer="staging",
            description="SAP PO raw 映射，1:1 對應 EKKO/EKPO 表。",
            columns=[
                Column("po_number", "SAP 採購單號 raw 欄位", is_pk=True),
                Column("vendor_id_raw", ""),  # 選寫，staging 一般欄位可不寫
            ],
            persist_docs={"relation": False, "columns": False},
            materialization="view",
        ),
        Model(
            name="int_po_line_status",
            layer="intermediate",
            description="",  # intermediate model 本身選寫
            columns=[
                Column("po_number", "PO 單號", is_pk=True),
                Column(
                    "po_status",
                    '{{ doc("po_status") }}',
                ),
            ],
            persist_docs={"relation": False, "columns": False},
            materialization="ephemeral",
        ),
        Model(
            name="fct_purchase_orders",
            layer="marts",
            description="ODM 採購訂單事實表。每筆 PO line item 一條記錄。Grain: PO + Line Item。",
            columns=[
                Column("po_number", "SAP 採購單號（ME21N 產生）", is_pk=True),
                Column("vendor_id", "", is_fk=True),  # <-- 故意漏寫，觸發 CI fail
                Column("plant_code", '{{ doc("plant_code") }}'),
                Column("net_amount", "淨金額（未稅），幣別見 currency_code"),
            ],
            persist_docs={"relation": True, "columns": True},
            materialization="incremental",
        ),
        Model(
            name="dim_vendors",
            layer="marts",
            description="供應商維度表，SCD Type 2。",
            columns=[
                Column("vendor_id", '{{ doc("vendor_id") }}', is_pk=True),
                Column("vendor_name", "供應商名稱"),
            ],
            persist_docs={"relation": True, "columns": True},
            materialization="table",
        ),
    ]


def build_models_round2(models_round1: list[Model]) -> list[Model]:
    """Round 2：補齊 vendor_id FK 的 description，模擬開發者修正後 CI 轉綠。"""
    import copy
    models = copy.deepcopy(models_round1)
    for m in models:
        if m.name == "fct_purchase_orders":
            for c in m.columns:
                if c.name == "vendor_id":
                    c.description = '{{ doc("vendor_id") }}'
    return models


# ---------------------------------------------------------------------------
# 6. CI Gate 模擬
# ---------------------------------------------------------------------------

def run_ci_gate(round_name: str, models: list[Model], registry: DocBlockRegistry) -> bool:
    print(f"\n{'=' * 60}")
    print(f"  CI Gate — {round_name}")
    print(f"{'=' * 60}")

    checker = CoverageChecker(registry)
    issues, stats = checker.check(models)

    print(f"文件覆蓋率: {stats['coverage_pct']}%  "
          f"({stats['documented_targets']}/{stats['total_targets']} targets documented)")

    if issues:
        print(f"❌ FAIL — {len(issues)} 個必寫項目缺 description:")
        for issue in issues:
            print(f"   - [{issue.model}] {issue.target}: {issue.reason}")
        return False

    print("✅ PASS — 所有必寫項目（marts model/column + PK/FK column）都有 description")
    return True


def demo_persist_docs(models: list[Model], registry: DocBlockRegistry) -> None:
    print(f"\n{'=' * 60}")
    print("  persist_docs → COMMENT DDL 產生器")
    print(f"{'=' * 60}")

    emitter = PersistDocsEmitter(registry)
    for m in models:
        ddls = emitter.emit(m)
        persist_on = m.persist_docs.get("relation") or m.persist_docs.get("columns")
        tag = "persist_docs=ON " if persist_on else "persist_docs=OFF"
        print(f"\n[{m.layer}] {m.name}  ({tag}, materialization={m.materialization})")
        if not ddls:
            print("   (無 DDL 產生 — persist_docs 關閉或無 description)")
        for ddl in ddls:
            print(f"   {ddl}")


def demo_doc_ref_error(registry: DocBlockRegistry) -> None:
    print(f"\n{'=' * 60}")
    print("  doc() 引用錯誤模擬（compile-time 檢查）")
    print(f"{'=' * 60}")
    bad_col = Column("shipping_mode", '{{ doc("shipping_mode_undefined") }}')
    try:
        bad_col.resolved_description(registry)
    except KeyError as e:
        print(f"❌ 預期中的 compile error: {e}")
    else:
        print("⚠️ 應該要拋出錯誤但沒有，邏輯有誤！")


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main() -> int:
    registry = build_registry()
    print(f"DocBlockRegistry 已註冊 {registry.block_count()} 個共用 doc blocks: "
          f"plant_code, vendor_id, po_status")

    demo_doc_ref_error(registry)

    models_r1 = build_models_round1()
    passed_r1 = run_ci_gate("Round 1（初次 PR，vendor_id FK 漏寫）", models_r1, registry)

    models_r2 = build_models_round2(models_r1)
    passed_r2 = run_ci_gate("Round 2（補齊 vendor_id description 後）", models_r2, registry)

    demo_persist_docs(models_r2, registry)

    print(f"\n{'=' * 60}")
    print("  總結")
    print(f"{'=' * 60}")
    print(f"Round 1 CI: {'PASS' if passed_r1 else 'FAIL（預期行為，示範 gate 擋下不完整文件）'}")
    print(f"Round 2 CI: {'PASS' if passed_r2 else 'FAIL（不應該發生）'}")

    # 驗證邏輯正確性（assert，而非只是印出來）
    assert passed_r1 is False, "Round 1 應該要 FAIL（vendor_id FK 缺 description）"
    assert passed_r2 is True, "Round 2 補齊後應該要 PASS"

    # persist_docs 驗證：staging model 沒開 persist_docs，不應產生任何 DDL
    stg = next(m for m in models_r2 if m.name == "stg_sap_po")
    emitter = PersistDocsEmitter(registry)
    assert emitter.emit(stg) == [], "staging model 沒開 persist_docs，不該有 DDL"

    # marts model 開了 persist_docs 且有 description，必須產生 COMMENT ON TABLE
    fct = next(m for m in models_r2 if m.name == "fct_purchase_orders")
    fct_ddls = emitter.emit(fct)
    assert any(ddl.startswith("COMMENT ON TABLE fct_purchase_orders") for ddl in fct_ddls), \
        "marts model 開 persist_docs 應該產生 COMMENT ON TABLE"

    print("\n✅ 所有 assertion 通過：CI gate 邏輯 + persist_docs DDL 產生器行為正確。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
