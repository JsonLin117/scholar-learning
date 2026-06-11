"""
CCA Practice: Tool Design 原則（D2 Task 2.1）
==============================================
ODM 供應鏈 Agent 工具設計的 5 個模擬器 + 5 題 CCA 模擬考

dependency-free, 不需要任何外部套件
"""

import json
import re
from dataclasses import dataclass, field
from typing import Any
from enum import Enum


# ============================================================
# Part 1: Tool Description Quality Scorer
# ============================================================

class DescriptionQuality(Enum):
    EXCELLENT = "excellent"
    GOOD = "good"
    NEEDS_IMPROVEMENT = "needs_improvement"
    POOR = "poor"


@dataclass
class DescriptionCheck:
    """Checks if a tool description meets CCA best practices."""
    has_purpose: bool = False          # What the tool does
    has_return_info: bool = False      # What it returns
    has_input_format: bool = False     # Input format/examples
    has_boundaries: bool = False       # What it does NOT do / limitations
    has_usage_context: bool = False    # When to use vs alternatives
    word_count: int = 0
    
    @property
    def score(self) -> int:
        checks = [self.has_purpose, self.has_return_info, self.has_input_format,
                  self.has_boundaries, self.has_usage_context]
        base = sum(checks)
        # Length bonus: at least 3 sentences (~40 words)
        if self.word_count >= 40:
            base += 1
        return base
    
    @property
    def quality(self) -> DescriptionQuality:
        s = self.score
        if s >= 5:
            return DescriptionQuality.EXCELLENT
        elif s >= 4:
            return DescriptionQuality.GOOD
        elif s >= 2:
            return DescriptionQuality.NEEDS_IMPROVEMENT
        return DescriptionQuality.POOR


def score_description(tool_def: dict) -> DescriptionCheck:
    """Analyze a tool description for CCA compliance."""
    desc = tool_def.get("description", "")
    words = desc.split()
    check = DescriptionCheck(word_count=len(words))
    
    desc_lower = desc.lower()
    
    # Purpose keywords
    purpose_kw = ["retrieves", "returns", "gets", "fetches", "creates", "updates",
                  "searches", "looks up", "checks", "validates", "processes"]
    check.has_purpose = any(kw in desc_lower for kw in purpose_kw)
    
    # Return info keywords
    return_kw = ["returns", "outputs", "provides", "response includes", "will return"]
    check.has_return_info = any(kw in desc_lower for kw in return_kw)
    
    # Input format indicators
    input_kw = ["format:", "e.g.", "example", "accepts", "expects", "parameter",
                "such as", "like"]
    check.has_input_format = any(kw in desc_lower for kw in input_kw)
    
    # Boundaries indicators
    boundary_kw = ["does not", "will not", "cannot", "don't", "instead use",
                   "not provide", "limitation", "only", "should not be used"]
    check.has_boundaries = any(kw in desc_lower for kw in boundary_kw)
    
    # Usage context indicators
    usage_kw = ["should be used when", "use this tool", "before calling",
                "after calling", "instead of", "prefer this over", "when the user"]
    check.has_usage_context = any(kw in desc_lower for kw in usage_kw)
    
    return check


def test_description_scorer():
    """Test with good and bad ODM tool descriptions."""
    print("=" * 70)
    print("Part 1: Tool Description Quality Scorer")
    print("=" * 70)
    
    tools = [
        {
            "name": "get_vendor",
            "description": "Gets vendor information.",
            "expected": DescriptionQuality.POOR
        },
        {
            "name": "get_vendor_context",
            "description": (
                "Retrieves comprehensive vendor context for supply chain analysis. "
                "Returns vendor profile including AVL status, lead time history, "
                "QCDS scorecard, recent PO history, and active shortage alerts. "
                "Accepts vendor_id (numeric SAP vendor code, e.g. 100234) or "
                "vendor_name (partial match supported). Use this tool when the user "
                "asks about vendor performance, shortage risk assessment, or before "
                "making procurement decisions. Does not return financial/payment "
                "information; use sap_mm_get_payment_terms for billing data."
            ),
            "expected": DescriptionQuality.EXCELLENT
        },
        {
            "name": "check_inventory",
            "description": (
                "Checks current inventory levels for a given material. "
                "Returns on-hand quantity, reserved quantity, and available-to-promise. "
                "Accepts SAP material number (e.g. MAT-001234)."
            ),
            "expected": DescriptionQuality.NEEDS_IMPROVEMENT  # missing boundaries + usage context
        },
        {
            "name": "search_po",
            "description": (
                "Searches purchase orders in SAP MM. Returns PO number, vendor, "
                "line items with quantities and prices, delivery dates, and GR status. "
                "Accepts filters: vendor_id, material_number, date_range "
                "(format: YYYY-MM-DD), po_status (open/closed/all). "
                "Use this tool when investigating procurement history or tracking "
                "deliveries. Does not include invoice/payment data; "
                "use sap_fi_get_invoice for financial documents."
            ),
            "expected": DescriptionQuality.EXCELLENT
        },
    ]
    
    passed = 0
    for tool in tools:
        check = score_description(tool)
        match = check.quality == tool["expected"]
        passed += int(match)
        status = "✅" if match else "❌"
        print(f"\n{status} Tool: {tool['name']}")
        print(f"   Score: {check.score}/6 → {check.quality.value}")
        print(f"   Expected: {tool['expected'].value}")
        print(f"   Checks: purpose={check.has_purpose} return={check.has_return_info} "
              f"input={check.has_input_format} boundary={check.has_boundaries} "
              f"usage={check.has_usage_context} words={check.word_count}")
    
    print(f"\nResult: {passed}/{len(tools)} passed")
    return passed, len(tools)


# ============================================================
# Part 2: tool_choice Strategy Selector
# ============================================================

@dataclass
class ToolChoiceScenario:
    description: str
    expected_choice: str  # "auto" | "any" | "specific:<name>"
    reasoning: str


def select_tool_choice(scenario: dict) -> str:
    """
    Given a scenario, determine the best tool_choice strategy.
    
    Rules:
    - If a specific tool MUST run first → forced specific
    - If structured output is required but multiple tools could apply → "any"
    - If text response is acceptable → "auto"
    - If deterministic first step is needed → forced specific
    """
    needs_structured = scenario.get("needs_structured_output", False)
    text_ok = scenario.get("text_response_acceptable", True)
    must_run_first = scenario.get("must_run_first", None)
    multiple_extractors = scenario.get("multiple_extractors", False)
    
    if must_run_first:
        return f"specific:{must_run_first}"
    elif needs_structured and multiple_extractors:
        return "any"
    elif needs_structured and not text_ok:
        return "any"
    else:
        return "auto"


def test_tool_choice_selector():
    """Test tool_choice selection across CCA-style scenarios."""
    print("\n" + "=" * 70)
    print("Part 2: tool_choice Strategy Selector")
    print("=" * 70)
    
    scenarios = [
        {
            "label": "探索型 Agent：查詢供應鏈問題，文字回答也可",
            "input": {
                "needs_structured_output": False,
                "text_response_acceptable": True,
                "must_run_first": None,
                "multiple_extractors": False
            },
            "expected": "auto",
        },
        {
            "label": "文件類型未知，多個 extraction schema",
            "input": {
                "needs_structured_output": True,
                "text_response_acceptable": False,
                "must_run_first": None,
                "multiple_extractors": True
            },
            "expected": "any",
        },
        {
            "label": "必須先執行 extract_metadata 再 enrich",
            "input": {
                "needs_structured_output": True,
                "text_response_acceptable": False,
                "must_run_first": "extract_metadata",
                "multiple_extractors": False
            },
            "expected": "specific:extract_metadata",
        },
        {
            "label": "客戶支持：先驗證客戶身份再處理退款",
            "input": {
                "needs_structured_output": False,
                "text_response_acceptable": True,
                "must_run_first": "get_customer",
                "multiple_extractors": False
            },
            "expected": "specific:get_customer",
        },
        {
            "label": "發票提取：PDF/圖片/掃描件，需結構化 JSON",
            "input": {
                "needs_structured_output": True,
                "text_response_acceptable": False,
                "must_run_first": None,
                "multiple_extractors": True
            },
            "expected": "any",
        },
        {
            "label": "ODM BOM 查詢：需要結構化但只有一個 bom_lookup 工具",
            "input": {
                "needs_structured_output": True,
                "text_response_acceptable": False,
                "must_run_first": "bom_lookup",
                "multiple_extractors": False
            },
            "expected": "specific:bom_lookup",
        },
    ]
    
    passed = 0
    for s in scenarios:
        result = select_tool_choice(s["input"])
        match = result == s["expected"]
        passed += int(match)
        status = "✅" if match else "❌"
        print(f"\n{status} {s['label']}")
        print(f"   Expected: {s['expected']} | Got: {result}")
    
    print(f"\nResult: {passed}/{len(scenarios)} passed")
    return passed, len(scenarios)


# ============================================================
# Part 3: Schema Design Validator
# ============================================================

@dataclass
class SchemaIssue:
    field: str
    issue_type: str  # "forced_required" | "missing_nullable" | "no_escape_enum" | "no_provenance"
    severity: str    # "high" | "medium" | "low"
    recommendation: str


def validate_schema(schema: dict, context: dict = None) -> list[SchemaIssue]:
    """
    Check a tool's input/output schema against CCA best practices.
    
    Rules:
    1. Fields that might be absent in source → should be nullable or optional
    2. Enum fields → should include "other" or "unclear" escape valve
    3. Extraction schemas → should have provenance fields
    4. Numeric comparisons → should have validation fields (calculated vs stated)
    """
    issues = []
    context = context or {}
    is_extraction = context.get("is_extraction_schema", False)
    
    properties = schema.get("properties", {})
    required = set(schema.get("required", []))
    
    for field_name, field_spec in properties.items():
        field_type = field_spec.get("type", "")
        
        # Check 1: Required field that might be absent
        might_be_absent = context.get("possibly_absent_fields", [])
        if field_name in required and field_name in might_be_absent:
            issues.append(SchemaIssue(
                field=field_name,
                issue_type="forced_required",
                severity="high",
                recommendation=f"Make '{field_name}' optional or nullable. "
                              f"Required + possibly absent → model will fabricate values."
            ))
        
        # Check 2: Enum without escape valve
        if "enum" in field_spec:
            enum_values = field_spec["enum"]
            has_escape = any(v in enum_values for v in ["other", "unclear", "unknown", "N/A"])
            if not has_escape:
                issues.append(SchemaIssue(
                    field=field_name,
                    issue_type="no_escape_enum",
                    severity="medium",
                    recommendation=f"Add 'other' or 'unclear' to enum for '{field_name}'. "
                                  f"Without it, data outside predefined categories is lost or forced."
                ))
        
        # Check 3: Non-nullable field that should be
        if field_name in might_be_absent and field_type == "string":
            if not isinstance(field_type, list) or "null" not in field_type:
                issues.append(SchemaIssue(
                    field=field_name,
                    issue_type="missing_nullable",
                    severity="medium",
                    recommendation=f"Use type: ['string', 'null'] for '{field_name}'. "
                                  f"Allows model to return null instead of hallucinating."
                ))
    
    # Check 4: Extraction schema without provenance
    if is_extraction:
        provenance_fields = {"source_url", "source_name", "page_number", "document_name",
                           "source", "reference", "citation"}
        has_provenance = bool(set(properties.keys()) & provenance_fields)
        if not has_provenance:
            issues.append(SchemaIssue(
                field="(schema-level)",
                issue_type="no_provenance",
                severity="medium",
                recommendation="Add provenance fields (source_url, page_number, etc.) "
                              "to preserve attribution in extraction schemas."
            ))
    
    return issues


def test_schema_validator():
    """Test schema validation with ODM scenarios."""
    print("\n" + "=" * 70)
    print("Part 3: Schema Design Validator")
    print("=" * 70)
    
    # Schema 1: Bad — vendor extraction with forced required + no escape
    bad_schema = {
        "type": "object",
        "properties": {
            "vendor_name": {"type": "string"},
            "certification": {
                "type": "string",
                "enum": ["ISO9001", "ISO14001", "IATF16949"]
            },
            "annual_revenue": {"type": "number"},
            "lead_time_days": {"type": "integer"},
        },
        "required": ["vendor_name", "certification", "annual_revenue", "lead_time_days"]
    }
    bad_context = {
        "is_extraction_schema": True,
        "possibly_absent_fields": ["annual_revenue", "lead_time_days", "certification"]
    }
    
    issues = validate_schema(bad_schema, bad_context)
    print(f"\n🔍 Bad schema issues found: {len(issues)}")
    for i in issues:
        print(f"   [{i.severity.upper()}] {i.field}: {i.issue_type}")
        print(f"         → {i.recommendation}")
    
    assert len(issues) >= 4, f"Expected ≥4 issues, got {len(issues)}"
    
    # Schema 2: Good — properly designed
    good_schema = {
        "type": "object",
        "properties": {
            "vendor_name": {"type": "string"},
            "certification": {
                "type": "string",
                "enum": ["ISO9001", "ISO14001", "IATF16949", "other", "unclear"]
            },
            "certification_detail": {
                "type": ["string", "null"],
                "description": "Details when certification = 'other' or 'unclear'"
            },
            "annual_revenue": {"type": ["number", "null"]},
            "lead_time_days": {"type": ["integer", "null"]},
            "source_url": {"type": "string"},
            "confidence": {"type": "number", "minimum": 0, "maximum": 1},
        },
        "required": ["vendor_name"]
    }
    good_context = {
        "is_extraction_schema": True,
        "possibly_absent_fields": ["annual_revenue", "lead_time_days"]
    }
    
    issues = validate_schema(good_schema, good_context)
    print(f"\n✅ Good schema issues found: {len(issues)}")
    for i in issues:
        print(f"   [{i.severity.upper()}] {i.field}: {i.issue_type}")
    
    assert len(issues) == 0, f"Expected 0 issues, got {len(issues)}"
    
    print(f"\nResult: Schema validator working correctly")
    return 2, 2  # Both test cases pass


# ============================================================
# Part 4: Tool Consolidation Analyzer
# ============================================================

@dataclass
class ConsolidationSuggestion:
    original_tools: list[str]
    consolidated_name: str
    rationale: str
    action_param_values: list[str]


def analyze_tool_consolidation(tools: list[dict]) -> list[ConsolidationSuggestion]:
    """
    Analyze a set of tools and suggest consolidations.
    
    Rules:
    1. Tools operating on the same resource → consolidate with action param
    2. Chained tools (A always followed by B) → combine into workflow tool
    3. Tools with same prefix/namespace → candidates for consolidation
    """
    suggestions = []
    
    # Group by namespace prefix
    prefix_groups: dict[str, list[dict]] = {}
    for tool in tools:
        name = tool["name"]
        parts = name.split("_", 1)
        if len(parts) > 1:
            prefix = parts[0]
            prefix_groups.setdefault(prefix, []).append(tool)
    
    # Detect CRUD-style patterns
    crud_verbs = {"get", "create", "update", "delete", "list", "search", "check"}
    
    for prefix, group in prefix_groups.items():
        if len(group) < 2:
            continue
        
        # Extract verb-resource patterns
        resources: dict[str, list[str]] = {}
        for tool in group:
            name = tool["name"]
            # Remove prefix
            remainder = name[len(prefix) + 1:] if name.startswith(prefix + "_") else name
            # Try to find verb
            parts = remainder.split("_", 1)
            if len(parts) == 2 and parts[0] in crud_verbs:
                verb, resource = parts
                resources.setdefault(resource, []).append(verb)
        
        # Suggest consolidation for resources with multiple verbs
        for resource, verbs in resources.items():
            if len(verbs) >= 2:
                original = [f"{prefix}_{v}_{resource}" for v in verbs]
                suggestions.append(ConsolidationSuggestion(
                    original_tools=original,
                    consolidated_name=f"{prefix}_{resource}",
                    rationale=f"Multiple CRUD operations on '{resource}' → "
                             f"consolidate with action parameter",
                    action_param_values=verbs
                ))
    
    # Detect chaining patterns (from tool metadata)
    chain_hints = {}
    for tool in tools:
        follows = tool.get("typically_follows", None)
        if follows:
            chain_hints.setdefault(follows, []).append(tool["name"])
    
    for leader, followers in chain_hints.items():
        if len(followers) >= 1:
            all_tools = [leader] + followers
            # Find common resource
            common = "_".join(set.intersection(*[set(t.split("_")) for t in all_tools]) - crud_verbs)
            suggestions.append(ConsolidationSuggestion(
                original_tools=all_tools,
                consolidated_name=f"workflow_{common or 'combined'}",
                rationale=f"'{leader}' is always followed by {followers} → "
                         f"combine into single workflow tool",
                action_param_values=["full_workflow"]
            ))
    
    return suggestions


def test_consolidation_analyzer():
    """Test with ODM tool proliferation scenario."""
    print("\n" + "=" * 70)
    print("Part 4: Tool Consolidation Analyzer")
    print("=" * 70)
    
    # Simulating a fragmented ODM tool set (18 tools → should be ~5)
    tools = [
        {"name": "sap_get_vendor", "description": "Get vendor by ID"},
        {"name": "sap_list_vendor", "description": "List all vendors"},
        {"name": "sap_update_vendor", "description": "Update vendor info"},
        {"name": "sap_create_vendor", "description": "Create new vendor"},
        {"name": "sap_get_po", "description": "Get PO by number"},
        {"name": "sap_create_po", "description": "Create new PO"},
        {"name": "sap_list_po", "description": "List POs"},
        {"name": "sap_get_material", "description": "Get material master"},
        {"name": "sap_search_material", "description": "Search materials"},
        {"name": "mes_get_wip", "description": "Get WIP status"},
        {"name": "mes_list_wip", "description": "List WIP items"},
        {"name": "mes_check_wip", "description": "Check WIP thresholds",
         "typically_follows": "mes_get_wip"},
    ]
    
    suggestions = analyze_tool_consolidation(tools)
    
    print(f"\n📊 Original tool count: {len(tools)}")
    print(f"🔄 Consolidation suggestions: {len(suggestions)}")
    
    for s in suggestions:
        print(f"\n   {', '.join(s.original_tools)}")
        print(f"   → {s.consolidated_name} (actions: {s.action_param_values})")
        print(f"   Rationale: {s.rationale}")
    
    # We expect at least 2 consolidation suggestions (vendor + po)
    assert len(suggestions) >= 2, f"Expected ≥2 suggestions, got {len(suggestions)}"
    
    # Calculate reduction
    tools_eliminated = sum(len(s.original_tools) - 1 for s in suggestions)
    new_count = len(tools) - tools_eliminated
    print(f"\n   Reduction: {len(tools)} → {new_count} tools "
          f"({tools_eliminated} eliminated)")
    
    print(f"\nResult: Consolidation analyzer working ✅")
    return 1, 1


# ============================================================
# Part 5: Error Response Quality Checker
# ============================================================

@dataclass
class ErrorQualityReport:
    has_is_error: bool = False
    has_category: bool = False
    has_retryable: bool = False
    has_message: bool = False
    has_attempted_action: bool = False
    has_partial_results: bool = False
    score: int = 0
    grade: str = "F"


def check_error_response(error_response: dict) -> ErrorQualityReport:
    """Check if an error response meets CCA structured error standards."""
    report = ErrorQualityReport()
    
    # Flatten: check both top-level and nested content
    content = error_response.get("content", error_response)
    if isinstance(content, str):
        # Generic string error = worst case
        report.has_message = len(content) > 10
        report.score = 1 if report.has_message else 0
        report.grade = "F"
        return report
    
    if isinstance(content, dict):
        check_dict = content
    else:
        check_dict = error_response
    
    report.has_is_error = "isError" in error_response or "is_error" in error_response
    report.has_category = any(k in check_dict for k in ["errorCategory", "error_category", "category"])
    report.has_retryable = any(k in check_dict for k in ["isRetryable", "is_retryable", "retryable"])
    report.has_message = any(k in check_dict for k in ["message", "human_readable_message", "description"])
    report.has_attempted_action = any(k in check_dict for k in 
        ["attempted_query", "attempted_action", "context", "attempted"])
    report.has_partial_results = any(k in check_dict for k in 
        ["partial_results", "partial_data", "partial"])
    
    checks = [report.has_is_error, report.has_category, report.has_retryable,
              report.has_message, report.has_attempted_action, report.has_partial_results]
    report.score = sum(checks)
    
    if report.score >= 5:
        report.grade = "A"
    elif report.score >= 4:
        report.grade = "B"
    elif report.score >= 3:
        report.grade = "C"
    elif report.score >= 2:
        report.grade = "D"
    else:
        report.grade = "F"
    
    return report


def test_error_response_checker():
    """Test with progressively better error responses."""
    print("\n" + "=" * 70)
    print("Part 5: Error Response Quality Checker")
    print("=" * 70)
    
    errors = [
        {
            "label": "反模式：通用字串錯誤",
            "response": {"isError": True, "content": "Operation failed"},
            "expected_grade": "F"
        },
        {
            "label": "稍好：有訊息但無結構",
            "response": {
                "isError": True,
                "content": {
                    "message": "SAP RFC call to BAPI_PO_GETDETAIL timed out after 30s"
                }
            },
            "expected_grade": "D"
        },
        {
            "label": "良好：有類別和重試標記",
            "response": {
                "isError": True,
                "content": {
                    "errorCategory": "transient",
                    "isRetryable": True,
                    "message": "SAP RFC timeout after 30s. Service may be under maintenance."
                }
            },
            "expected_grade": "C"
        },
        {
            "label": "優秀：完整結構化錯誤（CCA 標準）",
            "response": {
                "isError": True,
                "content": {
                    "errorCategory": "transient",
                    "isRetryable": True,
                    "message": "SAP RFC timeout. Orders API unavailable. Retry in 60s.",
                    "attempted_query": "BAPI_PO_GETDETAIL(PO_NUMBER='4500012345')",
                    "partial_results": {
                        "po_header": {"vendor": "100234", "status": "open"},
                        "po_items": None
                    }
                }
            },
            "expected_grade": "A"
        },
    ]
    
    passed = 0
    for e in errors:
        report = check_error_response(e["response"])
        match = report.grade == e["expected_grade"]
        passed += int(match)
        status = "✅" if match else "❌"
        print(f"\n{status} {e['label']}")
        print(f"   Grade: {report.grade} (expected: {e['expected_grade']}) "
              f"Score: {report.score}/6")
        print(f"   isError={report.has_is_error} category={report.has_category} "
              f"retryable={report.has_retryable}")
        print(f"   message={report.has_message} attempted={report.has_attempted_action} "
              f"partial={report.has_partial_results}")
    
    print(f"\nResult: {passed}/{len(errors)} passed")
    return passed, len(errors)


# ============================================================
# Part 6: CCA Mock Exam Questions (5 questions)
# ============================================================

@dataclass
class MockQuestion:
    question: str
    options: dict[str, str]
    correct: str
    explanation: str
    domain: str


def run_mock_exam():
    """5 CCA-style exam questions on Tool Design (D2)."""
    print("\n" + "=" * 70)
    print("Part 6: CCA Mock Exam — Tool Design (D2)")
    print("=" * 70)
    
    questions = [
        MockQuestion(
            question=(
                "Your customer support agent has 18 tools available and frequently "
                "selects the wrong tool when handling refund requests. The tools include "
                "get_customer, lookup_order, process_refund, and 15 others for various "
                "backend operations. What is the MOST effective improvement?"
            ),
            options={
                "A": "Add more detailed descriptions to all 18 tools",
                "B": "Reduce the agent's tool set to 4-5 role-specific tools",
                "C": "Use tool_choice: 'any' to force tool calls",
                "D": "Add few-shot examples in the system prompt for each tool"
            },
            correct="B",
            explanation=(
                "Too many tools degrade selection reliability. The recommended limit is "
                "4-5 tools per agent, scoped to the agent's role. While better descriptions "
                "help, reducing the number of tools is the most impactful change. "
                "tool_choice: 'any' forces a tool call but doesn't fix wrong selection."
            ),
            domain="D2"
        ),
        MockQuestion(
            question=(
                "You have two extraction tools: extract_invoice_pdf and extract_invoice_scan. "
                "Documents arrive in unknown formats. You need structured JSON output "
                "guaranteed. Which tool_choice configuration is best?"
            ),
            options={
                "A": "tool_choice: 'auto' — let the model decide",
                "B": "tool_choice: {'type': 'tool', 'name': 'extract_invoice_pdf'} — force PDF",
                "C": "tool_choice: 'any' — guarantee tool call, model picks which",
                "D": "No tool_choice — rely on system prompt instructions"
            },
            correct="C",
            explanation=(
                "tool_choice: 'any' guarantees structured output (tool call, not text) "
                "while letting the model choose the right extractor based on document format. "
                "'auto' might return text. Forced specific would always use PDF extractor "
                "even for scans. No tool_choice defaults to 'auto'."
            ),
            domain="D2"
        ),
        MockQuestion(
            question=(
                "An MCP tool returns: {\"isError\": true, \"content\": \"Operation failed\"}. "
                "The agent retries 5 times, wasting tokens. What is the root cause?"
            ),
            options={
                "A": "The agent's retry logic has no maximum attempt limit",
                "B": "The error response lacks errorCategory and isRetryable fields",
                "C": "The MCP server should use HTTP status codes instead",
                "D": "The agent should use tool_choice to avoid this tool"
            },
            correct="B",
            explanation=(
                "Without structured error information (errorCategory, isRetryable), the agent "
                "cannot distinguish transient failures (worth retrying) from permanent ones "
                "(validation, business rule violations). The generic 'Operation failed' gives "
                "no signal for the agent to make an informed decision. While retry limits help, "
                "the root cause is the uninformative error response."
            ),
            domain="D2"
        ),
        MockQuestion(
            question=(
                "Your extraction schema has a required 'annual_revenue' field for vendor "
                "documents. During testing, Claude sometimes fills in plausible-looking but "
                "incorrect revenue numbers. What schema change best addresses this?"
            ),
            options={
                "A": "Add a validation regex pattern for the revenue format",
                "B": "Make the field nullable: type: ['number', 'null']",
                "C": "Add a confidence score field next to revenue",
                "D": "Remove the field entirely and extract it in a second pass"
            },
            correct="B",
            explanation=(
                "Making the field nullable allows Claude to return null when revenue isn't "
                "found in the source document, rather than fabricating a value. Required fields "
                "for information that may be absent in source documents are a common cause of "
                "hallucination in extraction tasks. A confidence score helps but doesn't "
                "prevent the fabrication. Regex only validates format, not correctness."
            ),
            domain="D2"
        ),
        MockQuestion(
            question=(
                "You're building an ODM supply chain agent. Instead of implementing separate "
                "get_vendor_by_id, list_vendor_transactions, and get_vendor_avl_status tools, "
                "you implement a single get_vendor_context tool. This approach is recommended "
                "because:"
            ),
            options={
                "A": "It reduces the total number of API calls to SAP",
                "B": "It reduces tool selection ambiguity and consolidates chained operations",
                "C": "It makes the agent's system prompt shorter",
                "D": "It allows using tool_choice: 'auto' instead of 'any'"
            },
            correct="B",
            explanation=(
                "Tool consolidation reduces selection ambiguity (fewer tools to choose from) "
                "and handles frequently chained multi-step tasks in a single call. The key "
                "benefit is agent-side: clearer tool boundaries and less cognitive load on "
                "the model. While it may reduce API calls, that's a secondary implementation "
                "benefit, not the primary design principle."
            ),
            domain="D2"
        ),
    ]
    
    passed = 0
    for i, q in enumerate(questions, 1):
        print(f"\nQ{i}: {q.question}")
        for key, val in q.options.items():
            marker = "→" if key == q.correct else " "
            print(f"   {marker} {key}. {val}")
        
        # Auto-answer with correct (simulating "Scholar answers correctly")
        print(f"\n   ✅ Correct: {q.correct}")
        print(f"   💡 {q.explanation}")
        passed += 1
    
    print(f"\nMock Exam Result: {passed}/{len(questions)}")
    return passed, len(questions)


# ============================================================
# Main
# ============================================================

def main():
    print("🏆 CCA Practice: Tool Design 原則（D2 Task 2.1）")
    print("=" * 70)
    
    results = []
    results.append(("Description Scorer", *test_description_scorer()))
    results.append(("tool_choice Selector", *test_tool_choice_selector()))
    results.append(("Schema Validator", *test_schema_validator()))
    results.append(("Consolidation Analyzer", *test_consolidation_analyzer()))
    results.append(("Error Response Checker", *test_error_response_checker()))
    results.append(("CCA Mock Exam", *run_mock_exam()))
    
    print("\n" + "=" * 70)
    print("📊 Final Summary")
    print("=" * 70)
    
    total_passed = 0
    total_tests = 0
    for name, passed, total in results:
        total_passed += passed
        total_tests += total
        status = "✅" if passed == total else "⚠️"
        print(f"  {status} {name}: {passed}/{total}")
    
    print(f"\n  🎯 Total: {total_passed}/{total_tests}")
    print(f"  📈 Score: {total_passed/total_tests*100:.1f}%")


if __name__ == "__main__":
    main()
