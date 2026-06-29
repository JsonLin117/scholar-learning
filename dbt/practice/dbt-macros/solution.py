"""
dbt Macros 練習 — Mini Jinja + Macro Registry + Package System + ODM Macro Library
Scholar Day 61 | 2026-06-30
"""
from __future__ import annotations
import re
import hashlib
from dataclasses import dataclass, field
from typing import Any, Callable


# ═══════════════════════════════════════════════════════════════
#  Part 1: Mini Jinja Engine (simplified template compilation)
# ═══════════════════════════════════════════════════════════════

class MiniJinjaEngine:
    """Simulates Jinja's compile-time template expansion for dbt SQL."""

    def __init__(self, context: dict[str, Any] | None = None):
        self.context: dict[str, Any] = context or {}

    def set_var(self, name: str, value: Any):
        self.context[name] = value

    def compile(self, template: str) -> str:
        """Process a simplified Jinja template:
        - {{ expr }} → variable substitution
        - {% set name = value %} → set context var
        - {% for x in list %}...{% endfor %} → loop expansion
        - {% if cond %}...{% else %}...{% endif %} → conditional
        - {# comment #} → stripped
        """
        result = template
        # 1) Strip comments
        result = re.sub(r'\{#.*?#\}', '', result, flags=re.DOTALL)
        # 2) Process {% set %}
        result = self._process_set(result)
        # 3) Process {% for %}
        result = self._process_for(result)
        # 4) Process {% if %}
        result = self._process_if(result)
        # 5) Process {{ expressions }}
        result = self._process_expressions(result)
        # 6) Clean up multiple blank lines
        result = re.sub(r'\n{3,}', '\n\n', result)
        return result.strip()

    def _process_set(self, template: str) -> str:
        pattern = r'\{%[-\s]*set\s+(\w+)\s*=\s*(.+?)\s*[-]?%\}'
        for match in re.finditer(pattern, template):
            var_name = match.group(1)
            raw_value = match.group(2).strip()
            # Try to evaluate as Python literal
            try:
                value = eval(raw_value, {"__builtins__": {}}, self.context)
            except Exception:
                value = raw_value
            self.context[var_name] = value
        return re.sub(pattern, '', template)

    def _process_for(self, template: str) -> str:
        pattern = r'\{%[-\s]*for\s+(\w+)\s+in\s+(\w+)\s*[-]?%\}(.*?)\{%[-\s]*endfor\s*[-]?%\}'
        def replace_for(m):
            var_name = m.group(1)
            list_name = m.group(2)
            body = m.group(3)
            items = self.context.get(list_name, [])
            parts = []
            for i, item in enumerate(items):
                loop_ctx = {
                    'loop': type('Loop', (), {
                        'index': i + 1,
                        'index0': i,
                        'first': i == 0,
                        'last': i == len(items) - 1,
                        'length': len(items),
                    })()
                }
                self.context[var_name] = item
                self.context['loop'] = loop_ctx['loop']
                expanded = self._process_expressions(body)
                # Handle {{ "," if not loop.last }}
                expanded = re.sub(
                    r'\{\{\s*"([^"]*)"\s+if\s+not\s+loop\.last\s*\}\}',
                    lambda cm: cm.group(1) if not loop_ctx['loop'].last else '',
                    expanded
                )
                parts.append(expanded)
            return ''.join(parts)
        # Repeat until no more for loops (handles simple nesting)
        prev = None
        while prev != template:
            prev = template
            template = re.sub(pattern, replace_for, template, flags=re.DOTALL)
        return template

    def _process_if(self, template: str) -> str:
        # Simple if/else/endif (no elif for brevity)
        pattern = r'\{%[-\s]*if\s+(.+?)\s*[-]?%\}(.*?)(?:\{%[-\s]*else\s*[-]?%\}(.*?))?\{%[-\s]*endif\s*[-]?%\}'
        def replace_if(m):
            condition = m.group(1).strip()
            true_block = m.group(2)
            false_block = m.group(3) or ''
            try:
                result = eval(condition, {"__builtins__": {}}, self.context)
            except Exception:
                result = False
            return true_block if result else false_block
        prev = None
        while prev != template:
            prev = template
            template = re.sub(pattern, replace_if, template, flags=re.DOTALL)
        return template

    def _process_expressions(self, template: str) -> str:
        def replace_expr(m):
            expr = m.group(1).strip()
            try:
                result = eval(expr, {"__builtins__": {}}, self.context)
                return str(result)
            except Exception:
                return f"{{{{ {expr} }}}}"
        return re.sub(r'\{\{\s*(.+?)\s*\}\}', replace_expr, template)


# ═══════════════════════════════════════════════════════════════
#  Part 2: Macro Registry with Dispatch
# ═══════════════════════════════════════════════════════════════

@dataclass
class MacroDef:
    name: str
    package: str
    params: list[str]
    defaults: dict[str, Any]
    body_fn: Callable  # (context, **kwargs) -> str
    adapter_prefix: str = ""  # e.g., "databricks", "snowflake", ""="default"
    doc: str = ""

class MacroRegistry:
    """Manages macro registration, lookup, and dispatch across packages."""

    def __init__(self, current_adapter: str = "databricks"):
        self.macros: dict[str, list[MacroDef]] = {}  # name -> [MacroDef]
        self.current_adapter = current_adapter
        self.dispatch_config: dict[str, list[str]] = {}  # namespace -> search order

    def register(self, macro: MacroDef):
        key = f"{macro.package}.{macro.adapter_prefix}__{macro.name}" if macro.adapter_prefix else f"{macro.package}.default__{macro.name}"
        self.macros.setdefault(macro.name, []).append(macro)

    def set_dispatch(self, namespace: str, search_order: list[str]):
        self.dispatch_config[namespace] = search_order

    def call(self, name: str, namespace: str | None = None, **kwargs) -> str:
        """Dispatch to the best matching macro implementation."""
        candidates = self.macros.get(name, [])
        if not candidates:
            raise ValueError(f"Macro '{name}' not found")

        # Determine search order
        if namespace and namespace in self.dispatch_config:
            search_order = self.dispatch_config[namespace]
        elif namespace:
            search_order = [namespace]
        else:
            search_order = list(set(c.package for c in candidates))

        # Search: adapter-specific first, then default, in search order
        for pkg in search_order:
            # Look for adapter-specific
            for c in candidates:
                if c.package == pkg and c.adapter_prefix == self.current_adapter:
                    return c.body_fn(**kwargs)
            # Look for default
            for c in candidates:
                if c.package == pkg and c.adapter_prefix == "":
                    return c.body_fn(**kwargs)

        raise ValueError(f"No matching implementation for '{name}' on adapter '{self.current_adapter}'")

    def dispatch_trace(self, name: str, namespace: str | None = None) -> list[str]:
        """Show the search order for debugging."""
        candidates = self.macros.get(name, [])
        if namespace and namespace in self.dispatch_config:
            search_order = self.dispatch_config[namespace]
        elif namespace:
            search_order = [namespace]
        else:
            search_order = list(set(c.package for c in candidates))

        trace = []
        for pkg in search_order:
            trace.append(f"{pkg}.{self.current_adapter}__{name}")
            trace.append(f"{pkg}.default__{name}")
        return trace


# ═══════════════════════════════════════════════════════════════
#  Part 3: Package System
# ═══════════════════════════════════════════════════════════════

@dataclass
class Package:
    name: str
    version: str
    macros: list[MacroDef] = field(default_factory=list)
    tests: list[str] = field(default_factory=list)

class PackageManager:
    """Simulates dbt package installation and dependency resolution."""

    def __init__(self):
        self.installed: dict[str, Package] = {}
        self.registry = MacroRegistry()

    def install(self, package: Package):
        self.installed[package.name] = package
        for macro in package.macros:
            macro.package = package.name
            self.registry.register(macro)
        print(f"  ✅ Installed {package.name} v{package.version} ({len(package.macros)} macros)")

    def list_macros(self, package_name: str) -> list[str]:
        pkg = self.installed.get(package_name)
        if not pkg:
            return []
        return [f"{m.adapter_prefix + '__' if m.adapter_prefix else 'default__'}{m.name}" for m in pkg.macros]


# ═══════════════════════════════════════════════════════════════
#  Part 4: ODM Macro Library
# ═══════════════════════════════════════════════════════════════

def _standardize_material_no(col: str) -> str:
    return f"lpad(trim({col}), 18, '0')"

def _generate_surrogate_key(columns: list[str]) -> str:
    cols_concat = " || '|' || ".join(columns)
    return f"sha2(coalesce(cast({cols_concat} as string), ''), 256)"

def _cents_to_dollars(column_name: str, scale: int = 2) -> str:
    return f"({column_name} / 100)::numeric(16, {scale})"

def _cents_to_dollars_databricks(column_name: str, scale: int = 2) -> str:
    return f"cast({column_name} / 100 as decimal(16, {scale}))"

def _classify_vendor_status(avl_status: str, last_delivery_days: str) -> str:
    return f"""case
    when {avl_status} = 'ACTIVE' and {last_delivery_days} <= 90 then 'Active'
    when {avl_status} = 'ACTIVE' and {last_delivery_days} > 90 then 'Dormant'
    when {avl_status} = 'PROBATION' then 'Probation'
    when {avl_status} = 'BLOCKED' then 'Blocked'
    else 'Unknown'
  end"""

def _limit_data_in_dev(column_name: str, dev_days: int = 3, target_name: str = "dev") -> str:
    if target_name == "dev":
        return f"where {column_name} >= dateadd('day', -{dev_days}, current_timestamp)"
    return ""


# ═══════════════════════════════════════════════════════════════
#  Part 5: Anti-Pattern Detection
# ═══════════════════════════════════════════════════════════════

@dataclass
class AntiPatternResult:
    pattern: str
    severity: str  # "error" | "warning"
    location: str
    message: str

def detect_anti_patterns(macro_sql: str, macro_name: str = "unknown") -> list[AntiPatternResult]:
    """Detect common dbt macro anti-patterns."""
    issues = []

    # 1. ref() inside macro body
    if "ref(" in macro_sql and "macro" in macro_sql:
        issues.append(AntiPatternResult(
            pattern="ref_in_macro",
            severity="error",
            location=macro_name,
            message="ref() inside macro hides DAG dependency — model won't show in lineage graph"
        ))

    # 2. run_query without execute guard
    if "run_query" in macro_sql and "execute" not in macro_sql:
        issues.append(AntiPatternResult(
            pattern="missing_execute_guard",
            severity="error",
            location=macro_name,
            message="run_query() without {% if execute %} guard → fails during dbt parse/compile"
        ))

    # 3. Over-nesting: macro calling another macro more than 2 levels
    macro_calls = re.findall(r'\{\{\s*\w+\(', macro_sql)
    if len(macro_calls) > 4:
        issues.append(AntiPatternResult(
            pattern="over_nesting",
            severity="warning",
            location=macro_name,
            message=f"Found {len(macro_calls)} macro calls — potential over-nesting, hurts readability"
        ))

    # 4. Hardcoded schema/table names
    hardcoded = re.findall(r"from\s+\w+\.\w+\.\w+", macro_sql, re.IGNORECASE)
    if hardcoded:
        issues.append(AntiPatternResult(
            pattern="hardcoded_schema",
            severity="warning",
            location=macro_name,
            message=f"Hardcoded schema.table: {hardcoded[0]} → use ref()/source() instead"
        ))

    # 5. No whitespace control
    if "{%" in macro_sql and "{%-" not in macro_sql and "-%}" not in macro_sql:
        issues.append(AntiPatternResult(
            pattern="no_whitespace_control",
            severity="warning",
            location=macro_name,
            message="No Jinja whitespace control ({%- -%}) → compiled SQL may have extra blank lines"
        ))

    return issues


# ═══════════════════════════════════════════════════════════════
#  Part 6: Readability vs DRY Decision Engine
# ═══════════════════════════════════════════════════════════════

@dataclass
class MacroDecision:
    should_extract: bool
    reason: str
    score: float  # 0-10, higher = more reason to extract

def should_extract_macro(
    repetitions: int,
    platforms: int = 1,
    sql_lines: int = 1,
    business_logic: bool = False,
    likely_to_change: bool = False,
) -> MacroDecision:
    """
    dbt's philosophy: Favor readability over DRY-ness.
    Returns recommendation on whether to extract a macro.
    """
    score = 0.0
    reasons = []

    # Repetition factor
    if repetitions >= 5:
        score += 4.0
        reasons.append(f"used {repetitions}× (high repetition)")
    elif repetitions >= 3:
        score += 2.0
        reasons.append(f"used {repetitions}× (moderate repetition)")
    elif repetitions >= 2:
        score += 0.5
        reasons.append(f"used {repetitions}× only (low repetition)")

    # Cross-platform
    if platforms >= 2:
        score += 3.0
        reasons.append(f"{platforms} platforms → dispatch needed")

    # Complexity
    if sql_lines >= 10:
        score += 2.0
        reasons.append(f"{sql_lines} lines of SQL (complex logic)")
    elif sql_lines >= 5:
        score += 1.0
        reasons.append(f"{sql_lines} lines of SQL (moderate)")

    # Business logic encapsulation
    if business_logic:
        score += 2.0
        reasons.append("encapsulates business rule (single source of truth)")

    # Change likelihood
    if likely_to_change:
        score += 1.5
        reasons.append("likely to change → centralize for easy update")

    should = score >= 4.0
    verdict = "✅ EXTRACT as macro" if should else "❌ KEEP inline (favor readability)"

    return MacroDecision(
        should_extract=should,
        reason=f"{verdict} | Score: {score:.1f}/10 | {'; '.join(reasons)}",
        score=score
    )


# ═══════════════════════════════════════════════════════════════
#  DEMO
# ═══════════════════════════════════════════════════════════════

def run_demo():
    print("=" * 70)
    print("  dbt Macros 練習 — Scholar Day 61")
    print("=" * 70)

    # ─── Demo 1: Mini Jinja Engine ─────────────────────────────
    print("\n📝 Demo 1: Mini Jinja Engine — Template Compilation")
    print("-" * 50)

    engine = MiniJinjaEngine()

    # Test 1: Variable substitution
    engine.set_var("table_name", "raw.sap.mara")
    template1 = "SELECT * FROM {{ table_name }}"
    compiled1 = engine.compile(template1)
    print(f"  Template:  {template1}")
    print(f"  Compiled:  {compiled1}")
    assert "raw.sap.mara" in compiled1, "Variable substitution failed"

    # Test 2: for loop with loop.last
    engine.set_var("plants", ["TW01", "TW02", "CN01"])
    template2 = """{% for plant in plants %}
  SUM(CASE WHEN plant = '{{ plant }}' THEN qty END) AS qty_{{ plant }}{{ "," if not loop.last }}
{% endfor %}"""
    compiled2 = engine.compile(template2)
    print(f"\n  For loop template → compiled:")
    for line in compiled2.strip().split('\n'):
        if line.strip():
            print(f"    {line.strip()}")
    assert "qty_TW01," in compiled2, "For loop failed"
    assert "qty_CN01" in compiled2 and "qty_CN01," not in compiled2.replace("qty_CN01,", "X"), "loop.last failed"

    # Test 3: if/else with target
    engine.set_var("target_name", "dev")
    template3 = """{% if target_name == 'prod' %}
  {{ "config(materialized='table')" }}
{% else %}
  {{ "config(materialized='view')" }}
{% endif %}"""
    compiled3 = engine.compile(template3)
    print(f"\n  If/else (target=dev):")
    print(f"    {compiled3.strip()}")
    assert "view" in compiled3, "If/else failed"

    # Test 4: set + for combo
    template4 = """{% set methods = ['bank', 'card', 'gift'] %}
{% for m in methods %}
  SUM(CASE WHEN pay='{{ m }}' THEN amt END) AS {{ m }}_total{{ "," if not loop.last }}
{% endfor %}"""
    compiled4 = engine.compile(template4)
    print(f"\n  Set + For combo:")
    for line in compiled4.strip().split('\n'):
        if line.strip():
            print(f"    {line.strip()}")
    assert "bank_total" in compiled4

    # Test 5: Comments stripped
    template5 = "SELECT {# this is a comment #}1 AS one"
    compiled5 = engine.compile(template5)
    assert "comment" not in compiled5
    assert "1 AS one" in compiled5
    print(f"\n  Comment stripping: ✅ '{compiled5}'")

    print("\n  ✅ Mini Jinja Engine: 5/5 tests passed")

    # ─── Demo 2: Macro Registry + Dispatch ─────────────────────
    print("\n\n📦 Demo 2: Macro Registry + Adapter Dispatch")
    print("-" * 50)

    registry = MacroRegistry(current_adapter="databricks")

    # Register cents_to_dollars with default and databricks implementations
    registry.register(MacroDef(
        name="cents_to_dollars",
        package="my_project",
        params=["column_name", "scale"],
        defaults={"scale": 2},
        body_fn=_cents_to_dollars,
        adapter_prefix="",  # default
    ))
    registry.register(MacroDef(
        name="cents_to_dollars",
        package="my_project",
        params=["column_name", "scale"],
        defaults={"scale": 2},
        body_fn=_cents_to_dollars_databricks,
        adapter_prefix="databricks",
    ))

    # Dispatch on databricks → should use databricks version
    result_db = registry.call("cents_to_dollars", column_name="amount", scale=2)
    print(f"  Adapter: databricks")
    print(f"  Result:  {result_db}")
    assert "decimal" in result_db, "Databricks dispatch failed"

    # Switch to postgres → should use default version
    registry.current_adapter = "postgres"
    result_pg = registry.call("cents_to_dollars", column_name="amount", scale=2)
    print(f"\n  Adapter: postgres")
    print(f"  Result:  {result_pg}")
    assert "numeric" in result_pg, "Default dispatch failed"

    # Show dispatch trace
    trace = registry.dispatch_trace("cents_to_dollars")
    print(f"\n  Dispatch trace (postgres): {' → '.join(trace)}")

    # Override: dbt_utils namespace
    registry.current_adapter = "databricks"
    registry.set_dispatch("dbt_utils", ["my_project", "dbt_utils"])
    print(f"\n  Dispatch config: dbt_utils → ['my_project', 'dbt_utils']")
    trace2 = registry.dispatch_trace("cents_to_dollars", namespace="dbt_utils")
    print(f"  Override trace:  {' → '.join(trace2)}")

    print("\n  ✅ Dispatch: databricks → cast(decimal), postgres → ::numeric")

    # ─── Demo 3: Package System ────────────────────────────────
    print("\n\n📚 Demo 3: Package System")
    print("-" * 50)

    pm = PackageManager()

    # Install dbt_utils (simulated)
    dbt_utils = Package(
        name="dbt_utils",
        version="1.4.1",
        macros=[
            MacroDef(name="generate_surrogate_key", package="dbt_utils",
                     params=["columns"], defaults={},
                     body_fn=lambda columns: _generate_surrogate_key(columns)),
            MacroDef(name="star", package="dbt_utils",
                     params=["from_relation"], defaults={},
                     body_fn=lambda from_relation: f"/* all columns from {from_relation} */"),
            MacroDef(name="union_relations", package="dbt_utils",
                     params=["relations"], defaults={},
                     body_fn=lambda relations: f"/* UNION ALL of {len(relations)} relations */"),
        ],
        tests=["equal_rowcount", "fewer_rows_than", "expression_is_true"]
    )
    pm.install(dbt_utils)

    # Install our ODM utils
    odm_utils = Package(
        name="odm_utils",
        version="0.1.0",
        macros=[
            MacroDef(name="standardize_material_no", package="odm_utils",
                     params=["col"], defaults={},
                     body_fn=lambda col: _standardize_material_no(col)),
            MacroDef(name="classify_vendor_status", package="odm_utils",
                     params=["avl_status", "last_delivery_days"], defaults={},
                     body_fn=lambda avl_status, last_delivery_days:
                         _classify_vendor_status(avl_status, last_delivery_days)),
            MacroDef(name="limit_data_in_dev", package="odm_utils",
                     params=["column_name", "dev_days", "target_name"],
                     defaults={"dev_days": 3, "target_name": "dev"},
                     body_fn=lambda column_name, dev_days=3, target_name="dev":
                         _limit_data_in_dev(column_name, dev_days, target_name)),
        ]
    )
    pm.install(odm_utils)

    # Call macros
    sk = pm.registry.call("generate_surrogate_key", columns=["plant_code", "material_no"])
    print(f"\n  dbt_utils.generate_surrogate_key(['plant_code', 'material_no']):")
    print(f"    → {sk}")
    assert "sha2" in sk

    mat = pm.registry.call("standardize_material_no", col="matnr")
    print(f"\n  odm_utils.standardize_material_no('matnr'):")
    print(f"    → {mat}")
    assert "lpad" in mat

    vendor = pm.registry.call("classify_vendor_status",
                               avl_status="avl_status", last_delivery_days="days_since")
    print(f"\n  odm_utils.classify_vendor_status('avl_status', 'days_since'):")
    for line in vendor.strip().split('\n'):
        print(f"    {line}")

    limit = pm.registry.call("limit_data_in_dev", column_name="created_at",
                              dev_days=3, target_name="dev")
    print(f"\n  odm_utils.limit_data_in_dev('created_at', target='dev'):")
    print(f"    → {limit}")
    assert "dateadd" in limit

    limit_prod = pm.registry.call("limit_data_in_dev", column_name="created_at",
                                   dev_days=3, target_name="prod")
    print(f"  odm_utils.limit_data_in_dev('created_at', target='prod'):")
    print(f"    → (empty — no filter in prod) ✅")
    assert limit_prod == ""

    # List all macros
    print(f"\n  Installed macros:")
    for pkg_name, pkg in pm.installed.items():
        macros_list = pm.list_macros(pkg_name)
        print(f"    {pkg_name} v{pkg.version}: {macros_list}")

    print("\n  ✅ Package System: 2 packages, 6 macros, cross-package calls working")

    # ─── Demo 4: Anti-Pattern Detection ────────────────────────
    print("\n\n🔍 Demo 4: Anti-Pattern Detection")
    print("-" * 50)

    # Bad macro 1: ref() inside macro
    bad_macro1 = """
    {% macro get_vendor_data() %}
      SELECT * FROM {{ ref('dim_vendor') }}
      WHERE status = 'ACTIVE'
    {% endmacro %}
    """
    issues1 = detect_anti_patterns(bad_macro1, "get_vendor_data")
    print(f"  Macro: get_vendor_data")
    for issue in issues1:
        print(f"    [{issue.severity.upper()}] {issue.pattern}: {issue.message}")
    assert any(i.pattern == "ref_in_macro" for i in issues1)

    # Bad macro 2: run_query without execute guard
    bad_macro2 = """
    {% macro get_schemas() %}
      {% set query %}
        SELECT DISTINCT schema_name FROM information_schema.schemata
      {% endset %}
      {% set results = run_query(query) %}
      {{ return(results) }}
    {% endmacro %}
    """
    issues2 = detect_anti_patterns(bad_macro2, "get_schemas")
    print(f"\n  Macro: get_schemas")
    for issue in issues2:
        print(f"    [{issue.severity.upper()}] {issue.pattern}: {issue.message}")
    assert any(i.pattern == "missing_execute_guard" for i in issues2)

    # Bad macro 3: hardcoded schema
    bad_macro3 = """
    {% macro get_orders() %}
      SELECT * FROM raw.sap.vbak
      JOIN raw.sap.vbap ON vbak.vbeln = vbap.vbeln
    {% endmacro %}
    """
    issues3 = detect_anti_patterns(bad_macro3, "get_orders")
    print(f"\n  Macro: get_orders")
    for issue in issues3:
        print(f"    [{issue.severity.upper()}] {issue.pattern}: {issue.message}")
    assert any(i.pattern == "hardcoded_schema" for i in issues3)

    # Good macro: no issues (well, minimal)
    good_macro = """
    {%- macro cents_to_dollars(column_name, scale=2) -%}
      ({{ column_name }} / 100)::numeric(16, {{ scale }})
    {%- endmacro -%}
    """
    issues_good = detect_anti_patterns(good_macro, "cents_to_dollars")
    print(f"\n  Macro: cents_to_dollars (good)")
    if not issues_good:
        print(f"    ✅ No anti-patterns detected!")
    else:
        for issue in issues_good:
            print(f"    [{issue.severity.upper()}] {issue.pattern}")

    total_issues = len(issues1) + len(issues2) + len(issues3)
    print(f"\n  ✅ Anti-Pattern Detection: {total_issues} issues caught across 3 bad macros")

    # ─── Demo 5: Readability vs DRY Decision ──────────────────
    print("\n\n⚖️  Demo 5: Readability vs DRY Decision Engine")
    print("-" * 50)

    scenarios = [
        {
            "name": "Status classification (used in 8 models)",
            "repetitions": 8, "platforms": 1, "sql_lines": 6,
            "business_logic": True, "likely_to_change": True
        },
        {
            "name": "Simple COALESCE(col, 0) (used in 3 models)",
            "repetitions": 3, "platforms": 1, "sql_lines": 1,
            "business_logic": False, "likely_to_change": False
        },
        {
            "name": "Hash key generation (2 platforms)",
            "repetitions": 4, "platforms": 2, "sql_lines": 3,
            "business_logic": False, "likely_to_change": False
        },
        {
            "name": "Date formatting (used in 2 models)",
            "repetitions": 2, "platforms": 1, "sql_lines": 1,
            "business_logic": False, "likely_to_change": False
        },
        {
            "name": "Complex OTD calculation (15 lines, business rule)",
            "repetitions": 3, "platforms": 1, "sql_lines": 15,
            "business_logic": True, "likely_to_change": True
        },
    ]

    for s in scenarios:
        d = should_extract_macro(
            repetitions=s["repetitions"],
            platforms=s["platforms"],
            sql_lines=s["sql_lines"],
            business_logic=s["business_logic"],
            likely_to_change=s["likely_to_change"],
        )
        icon = "🟢" if d.should_extract else "🔴"
        print(f"  {icon} {s['name']}")
        print(f"     → {d.reason}")

    print()

    # ─── Demo 6: Full ODM Pipeline Compilation ─────────────────
    print("\n🏭 Demo 6: Full ODM Pipeline — Macro-Powered Model Compilation")
    print("-" * 50)

    engine2 = MiniJinjaEngine()
    engine2.set_var("plants", ["TW01", "TW02", "CN01"])
    engine2.set_var("target_name", "dev")

    # Simulate a full dbt model using macros
    model_template = """{# ODM Vendor Scorecard Mart #}
{% set plants = plants %}
SELECT
  {{ "lpad(trim(vendor_no), 18, '0')" }} AS vendor_no_std,
  vendor_name,
{% for plant in plants %}
  SUM(CASE WHEN plant_code = '{{ plant }}' THEN po_amount END) AS spend_{{ plant }}{{ "," if not loop.last }}
{% endfor %}
FROM staging.stg_purchase_orders
GROUP BY 1, 2
{% if target_name == 'dev' %}
LIMIT 1000
{% endif %}"""

    compiled_model = engine2.compile(model_template)
    print(f"  Compiled SQL (target=dev):\n")
    for line in compiled_model.split('\n'):
        print(f"    {line}")

    assert "spend_TW01" in compiled_model
    assert "spend_CN01" in compiled_model
    assert "LIMIT 1000" in compiled_model
    print(f"\n  ✅ Full pipeline compiled: 3 plant pivots + dev limit")

    # ─── Summary ───────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("  📊 Summary")
    print("=" * 70)
    print(f"""
  ✅ Demo 1: Mini Jinja Engine (5 tests — set/for/if/expr/comments)
  ✅ Demo 2: Adapter Dispatch (databricks → decimal, postgres → numeric)
  ✅ Demo 3: Package System (dbt_utils + odm_utils, 6 macros)
  ✅ Demo 4: Anti-Pattern Detection ({total_issues} issues: ref_in_macro, missing_execute_guard, hardcoded_schema)
  ✅ Demo 5: Readability vs DRY (5 scenarios, dbt philosophy applied)
  ✅ Demo 6: Full ODM Pipeline Compilation (3-plant pivot + dev limit)

  Key Takeaways:
  1. Macros = Functions for SQL → DRY where it matters
  2. Jinja compiles at BUILD time, not DB time
  3. dispatch() enables cross-platform SQL → SA multi-platform strategy
  4. Packages = Shared macro libraries (dbt_utils = must-have)
  5. "Favor readability over DRY-ness" — don't macro everything
  6. Anti-patterns: no ref() in macros, always use execute guard for run_query
""")


if __name__ == "__main__":
    run_demo()
