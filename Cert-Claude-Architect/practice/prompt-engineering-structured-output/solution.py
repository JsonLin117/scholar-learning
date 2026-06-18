"""
Prompt Engineering & Structured Output Practice (D4 Task 4.1-4.3)
CCA 認證備考 — 2026-06-19

依賴：無（dependency-free 模擬）
場景：ODM 供應鏈文件提取系統
"""

import json
import re
from dataclasses import dataclass, field
from typing import Any, Optional
from datetime import date


# ============================================================
# Module 1: Explicit Criteria Engine (Task 4.1)
# ============================================================

class ExplicitCriteriaEngine:
    """
    模擬 Task 4.1 的核心概念：
    - 明確標準 vs 模糊指令
    - 誤報管理（暫時禁用高誤報類別）
    - 嚴重性分級（含代碼範例）
    """

    def __init__(self):
        self.categories_enabled = {
            "contradiction": True,    # 注釋與代碼行為矛盾
            "stale_reference": True,  # 引用不存在的函數/變數
            "resolved_todo": True,    # TODO 指向已修復的 bug
            "minor_style": False,     # 🔇 暫時禁用（高誤報）
            "missing_comment": False, # 🔇 暫時禁用（高誤報）
        }
        self.severity_definitions = {
            "critical": {
                "description": "用戶運行時會崩潰",
                "example": "NullPointerException during payment processing",
            },
            "high": {
                "description": "安全漏洞",
                "example": "SQL injection in username parameter",
            },
            "medium": {
                "description": "無即時影響的邏輯 bug",
                "example": "Off-by-one error in pagination",
            },
            "low": {
                "description": "代碼品質",
                "example": "Duplicate code block in utility function",
            },
        }

    def evaluate_finding(self, finding: dict) -> dict:
        """評估一個發現是否應該被報告"""
        category = finding.get("category", "")
        severity = finding.get("severity", "")

        # Check if category is enabled
        if category in self.categories_enabled:
            if not self.categories_enabled[category]:
                return {
                    "action": "suppressed",
                    "reason": f"Category '{category}' temporarily disabled (high false-positive rate)",
                    "recommendation": "Improve prompts for this category before re-enabling",
                }

        # Validate severity has concrete example
        if severity in self.severity_definitions:
            return {
                "action": "report",
                "severity": severity,
                "severity_definition": self.severity_definitions[severity]["description"],
                "severity_example": self.severity_definitions[severity]["example"],
            }

        return {"action": "reject", "reason": f"Unknown severity: {severity}"}

    def compare_vague_vs_explicit(self):
        """對比模糊指令 vs 明確標準的效果"""
        findings = [
            {"description": "Comment says 'returns user' but function returns None",
             "category": "contradiction", "severity": "high"},
            {"description": "Variable name could be more descriptive",
             "category": "minor_style", "severity": "low"},
            {"description": "TODO: fix null check — already fixed in commit abc123",
             "category": "resolved_todo", "severity": "medium"},
            {"description": "Missing docstring",
             "category": "missing_comment", "severity": "low"},
            {"description": "References helper_func() which was deleted",
             "category": "stale_reference", "severity": "high"},
        ]

        vague_results = []
        explicit_results = []

        for f in findings:
            # Vague approach: "be conservative, only report high-confidence"
            # → Model has no clear criteria, reports everything or nothing randomly
            vague_results.append({
                "finding": f["description"],
                "vague_action": "report (no filtering criteria)",
                "problem": "No way to distinguish real issues from noise",
            })

            # Explicit approach: categorized criteria with enable/disable
            result = self.evaluate_finding(f)
            explicit_results.append({
                "finding": f["description"],
                "explicit_action": result["action"],
                "reason": result.get("reason", f"Severity: {result.get('severity', 'N/A')}"),
            })

        return {
            "vague_approach": {
                "instruction": '"be conservative, only report high-confidence findings"',
                "total_reported": len(vague_results),
                "problem": "ALL findings reported — no actionable filtering",
            },
            "explicit_approach": {
                "instruction": "Categorized criteria with disabled high-FP categories",
                "total_reported": sum(1 for r in explicit_results if r["explicit_action"] == "report"),
                "suppressed": sum(1 for r in explicit_results if r["explicit_action"] == "suppressed"),
                "results": explicit_results,
            },
        }


# ============================================================
# Module 2: Few-Shot Prompt Builder (Task 4.2)
# ============================================================

class FewShotPromptBuilder:
    """
    模擬 Task 4.2 的核心概念：
    - 2-4 個 targeted few-shot examples
    - 歧義場景處理（展示推理過程）
    - 泛化能力驗證
    """

    def __init__(self):
        self.examples = []

    def add_example(self, input_text: str, output: dict, reasoning: str):
        """添加 few-shot 範例（含推理過程）"""
        self.examples.append({
            "input": input_text,
            "output": output,
            "reasoning": reasoning,
        })

    def build_prompt(self, task_description: str, new_input: str) -> dict:
        """建構包含 few-shot 的完整 prompt"""
        prompt_parts = [f"Task: {task_description}\n"]

        for i, ex in enumerate(self.examples, 1):
            prompt_parts.append(f"Example {i}:")
            prompt_parts.append(f"  Input: {ex['input']}")
            prompt_parts.append(f"  Output: {json.dumps(ex['output'], ensure_ascii=False)}")
            prompt_parts.append(f"  Reasoning: {ex['reasoning']}")
            prompt_parts.append("")

        prompt_parts.append(f"Now process this input:")
        prompt_parts.append(f"  Input: {new_input}")

        return {
            "prompt": "\n".join(prompt_parts),
            "example_count": len(self.examples),
            "within_range": 2 <= len(self.examples) <= 4,
        }

    def validate_generalization(self, examples: list, test_cases: list) -> dict:
        """驗證 few-shot 範例的泛化能力"""
        # Extract patterns from examples
        example_patterns = set()
        for ex in examples:
            for key in ex.get("output", {}):
                example_patterns.add(key)

        # Check test cases can be handled by the pattern
        results = []
        for tc in test_cases:
            # Simulated: check if test case structure matches example pattern
            tc_keys = set(tc.get("expected_output", {}).keys())
            coverage = len(tc_keys & example_patterns) / max(len(tc_keys), 1)
            results.append({
                "input": tc["input"],
                "pattern_coverage": coverage,
                "generalizable": coverage >= 0.5,  # At least 50% pattern overlap
                "novel_keys": list(tc_keys - example_patterns),
            })

        return {
            "total_test_cases": len(test_cases),
            "generalizable": sum(1 for r in results if r["generalizable"]),
            "failed": sum(1 for r in results if not r["generalizable"]),
            "details": results,
        }


# ============================================================
# Module 3: Tool Choice Simulator (Task 4.3)
# ============================================================

class ToolChoiceSimulator:
    """
    模擬 Task 4.3 的 tool_choice 三種模式：
    - auto: 模型可能返回文字或呼叫工具
    - any: 模型必須呼叫某個工具（自選）
    - tool(name): 模型必須呼叫指定工具
    """

    def __init__(self):
        self.tools = {}

    def register_tool(self, name: str, description: str, schema: dict):
        """註冊工具定義"""
        self.tools[name] = {
            "name": name,
            "description": description,
            "input_schema": schema,
        }

    def simulate_response(self, tool_choice: dict, user_message: str) -> dict:
        """模擬不同 tool_choice 下的模型行為"""
        choice_type = tool_choice.get("type", "auto")

        if choice_type == "auto":
            # Model decides: might return text OR call a tool
            if any(kw in user_message.lower() for kw in ["extract", "analyze", "process"]):
                # Likely calls a tool
                selected = list(self.tools.keys())[0] if self.tools else None
                return {
                    "tool_choice": "auto",
                    "behavior": "model_decided_tool",
                    "selected_tool": selected,
                    "risk": "Model might return text instead of calling tool — NO guarantee of structured output",
                    "stop_reason": "tool_use",
                }
            else:
                return {
                    "tool_choice": "auto",
                    "behavior": "model_decided_text",
                    "selected_tool": None,
                    "risk": "Returned text — no structured output",
                    "stop_reason": "end_turn",
                }

        elif choice_type == "any":
            # Model MUST call a tool (picks which one)
            # Simulated: pick most relevant based on message
            best_tool = None
            best_score = 0
            for name, tool in self.tools.items():
                # Simple keyword matching simulation
                desc_words = set(tool["description"].lower().split())
                msg_words = set(user_message.lower().split())
                score = len(desc_words & msg_words)
                if score > best_score:
                    best_score = score
                    best_tool = name
            if not best_tool:
                best_tool = list(self.tools.keys())[0]

            return {
                "tool_choice": "any",
                "behavior": "forced_tool_call",
                "selected_tool": best_tool,
                "guarantee": "✅ Structured output guaranteed — model MUST call a tool",
                "stop_reason": "tool_use",
            }

        elif choice_type == "tool":
            # Model MUST call the specified tool
            forced_name = tool_choice.get("name", "")
            if forced_name not in self.tools:
                return {
                    "tool_choice": f"tool({forced_name})",
                    "error": f"Tool '{forced_name}' not found",
                }
            return {
                "tool_choice": f"tool({forced_name})",
                "behavior": "forced_specific_tool",
                "selected_tool": forced_name,
                "guarantee": f"✅ Must call '{forced_name}' — use for enforcing execution order",
                "use_case": "Force extract_metadata BEFORE enrichment steps",
                "stop_reason": "tool_use",
            }

        return {"error": f"Unknown tool_choice type: {choice_type}"}


# ============================================================
# Module 4: Schema Design Validator (Task 4.3)
# ============================================================

class SchemaDesignValidator:
    """
    驗證 JSON Schema 設計是否符合 CCA D4 最佳實踐：
    1. required 只包含一定存在的欄位
    2. 可能缺失的欄位用 nullable
    3. enum 包含 "other" + detail string
    4. 包含 "unclear" 給模型誠實的出口
    5. 有 format normalization rules
    """

    RULES = [
        {
            "id": "R1_required_minimal",
            "name": "Required fields should be minimal",
            "check": "_check_required_minimal",
            "severity": "high",
        },
        {
            "id": "R2_nullable_for_optional",
            "name": "Optional fields should be nullable",
            "check": "_check_nullable",
            "severity": "high",
        },
        {
            "id": "R3_enum_has_other",
            "name": "Enum fields should include 'other' or 'unclear'",
            "check": "_check_enum_extensible",
            "severity": "medium",
        },
        {
            "id": "R4_detail_for_other",
            "name": "Enum with 'other' should have companion detail field",
            "check": "_check_other_detail",
            "severity": "medium",
        },
        {
            "id": "R5_no_fabrication_risk",
            "name": "Required fields should not include potentially missing data",
            "check": "_check_fabrication_risk",
            "severity": "critical",
        },
    ]

    # Fields that are commonly missing in source documents
    RISKY_REQUIRED = {"source", "author", "date", "url", "reference", "citation",
                      "phone", "email", "address", "fax", "website"}

    def validate(self, schema: dict) -> dict:
        """驗證 schema 設計"""
        results = []
        properties = schema.get("properties", {})
        required = set(schema.get("required", []))

        for rule in self.RULES:
            check_fn = getattr(self, rule["check"])
            issues = check_fn(properties, required, schema)
            results.append({
                "rule_id": rule["id"],
                "rule_name": rule["name"],
                "severity": rule["severity"],
                "passed": len(issues) == 0,
                "issues": issues,
            })

        passed = sum(1 for r in results if r["passed"])
        total = len(results)
        return {
            "score": f"{passed}/{total}",
            "all_passed": passed == total,
            "results": results,
        }

    def _check_required_minimal(self, properties, required, schema):
        issues = []
        if len(required) > len(properties) * 0.7:
            issues.append(
                f"Too many required fields ({len(required)}/{len(properties)}). "
                f"Consider making some nullable."
            )
        return issues

    def _check_nullable(self, properties, required, schema):
        issues = []
        for name, prop in properties.items():
            if name not in required:
                prop_type = prop.get("type", "")
                if isinstance(prop_type, str) and prop_type != "null":
                    # Not nullable but also not required → ambiguous
                    if "null" not in str(prop_type):
                        issues.append(
                            f"Field '{name}' is optional but not nullable. "
                            f"Use type: ['{prop_type}', 'null'] to allow null."
                        )
        return issues

    def _check_enum_extensible(self, properties, required, schema):
        issues = []
        for name, prop in properties.items():
            if "enum" in prop:
                enum_vals = prop["enum"]
                has_escape = any(v in enum_vals for v in ["other", "unclear", "unknown"])
                if not has_escape and len(enum_vals) < 10:
                    issues.append(
                        f"Enum field '{name}' has no escape value. "
                        f"Add 'other' or 'unclear' to prevent data loss."
                    )
        return issues

    def _check_other_detail(self, properties, required, schema):
        issues = []
        for name, prop in properties.items():
            if "enum" in prop and "other" in prop.get("enum", []):
                detail_name = f"{name}_detail"
                if detail_name not in properties:
                    issues.append(
                        f"Enum field '{name}' has 'other' but no companion "
                        f"'{detail_name}' field for additional context."
                    )
        return issues

    def _check_fabrication_risk(self, properties, required, schema):
        issues = []
        risky = required & self.RISKY_REQUIRED
        for field_name in risky:
            issues.append(
                f"⚠️ FABRICATION RISK: '{field_name}' is required but may not exist "
                f"in source document. Model will fabricate a value to satisfy this constraint. "
                f"Make it nullable instead."
            )
        return issues


# ============================================================
# Module 5: Mock Exam (5 Questions)
# ============================================================

MOCK_EXAM = [
    {
        "id": "D4-Q1",
        "scenario": "Structured Data Extraction",
        "question": (
            "你正在建構一個從非結構化供應商通知中提取結構化資料的系統。"
            "模型需要將供應商名稱分類為預定義的類別（原料、電子元件、包裝、物流），"
            "但有些供應商不屬於任何類別。你的 JSON schema 應該怎麼設計？"
        ),
        "options": {
            "A": '{"category": {"type": "string", "enum": ["raw_material", "electronics", "packaging", "logistics"]}}',
            "B": '{"category": {"type": "string", "enum": ["raw_material", "electronics", "packaging", "logistics", "other"]}, "category_detail": {"type": ["string", "null"]}}',
            "C": '{"category": {"type": "string"}} — 讓模型自由填寫',
            "D": '{"category": {"type": "string", "enum": ["raw_material", "electronics", "packaging", "logistics"]}, "is_valid_category": {"type": "boolean"}}',
        },
        "correct": "B",
        "explanation": (
            "B 正確：enum + 'other' + detail string 是 CCA 考試的標準答案。"
            "A 錯：沒有 'other'，不符合類別的供應商資料會被強制歸類（fabrication）。"
            "C 錯：沒有 enum 約束，輸出不一致。"
            "D 錯：boolean 無法捕捉真正的類別資訊。"
        ),
        "task_ref": "4.3",
    },
    {
        "id": "D4-Q2",
        "scenario": "CI/CD Code Review",
        "question": (
            "你的 CI/CD code review 系統誤報率太高，開發者不再信任任何類別的 findings。"
            "最有效的改善策略是什麼？"
        ),
        "options": {
            "A": "在 prompt 中加入 'be more conservative, only report high-confidence findings'",
            "B": "增加 extended thinking 讓模型更仔細思考",
            "C": "暫時禁用高誤報類別，改善該類別的 prompt 後重新啟用",
            "D": "降低所有類別的靈敏度",
        },
        "correct": "C",
        "explanation": (
            "C 正確：高誤報類別會污染所有類別的信任度。暫時禁用→改善→重新啟用。"
            "A 錯：'be conservative' 是模糊指令，不能改善精確度（Task 4.1 核心知識）。"
            "B 錯：extended thinking 不解決 criteria 不明確的問題。"
            "D 錯：降低靈敏度會漏掉真正的 bugs。"
        ),
        "task_ref": "4.1",
    },
    {
        "id": "D4-Q3",
        "scenario": "Structured Data Extraction",
        "question": (
            "你有三個不同的 extraction schemas（invoice、purchase_order、shipping_notice），"
            "文檔類型事先未知。你需要保證得到結構化輸出。"
            "tool_choice 應該怎麼設定？"
        ),
        "options": {
            "A": 'tool_choice: {"type": "auto"} — 讓模型自己判斷',
            "B": 'tool_choice: {"type": "any"} — 模型必須呼叫某個 tool，自選最佳 schema',
            "C": 'tool_choice: {"type": "tool", "name": "extract_invoice"} — 強制用 invoice schema',
            "D": "不設 tool_choice，在 prompt 中說「請輸出 JSON」",
        },
        "correct": "B",
        "explanation": (
            "B 正確：tool_choice: any 保證結構化輸出（must call a tool），"
            "同時讓模型根據文檔內容自選最佳 schema。"
            "A 錯：auto 模式下模型可能返回純文字，無法保證結構化輸出。"
            "C 錯：文檔類型未知，不應強制用 invoice schema。"
            "D 錯：沒有 tool_use 就沒有 JSON 語法保證。"
        ),
        "task_ref": "4.3",
    },
    {
        "id": "D4-Q4",
        "scenario": "Structured Data Extraction",
        "question": (
            "你的提取系統在處理非正式計量描述時產生大量空值或幻覺值。"
            "例如「大約兩把」「一小撮」等描述無法正確提取。"
            "最有效的改善方式是什麼？"
        ),
        "options": {
            "A": "在 schema 中加入更詳細的欄位定義和 description",
            "B": "加入 2-4 個 few-shot examples 展示非正式計量的正確轉換方式",
            "C": "增加 max_tokens 讓模型有更多空間思考",
            "D": "使用更大的模型（如 Opus）來提高理解能力",
        },
        "correct": "B",
        "explanation": (
            "B 正確：非正式計量種類繁多，規則無法覆蓋。"
            "Few-shot examples 能讓模型泛化到新的非正式表達（Task 4.2 核心知識）。"
            "A 錯：schema description 無法展示轉換的推理過程。"
            "C 錯：token 數量與理解能力無關。"
            "D 錯：模型大小不是問題，prompt 設計才是。"
        ),
        "task_ref": "4.2",
    },
    {
        "id": "D4-Q5",
        "scenario": "Structured Data Extraction",
        "question": (
            "你的 extraction schema 有一個 'source_url' 欄位設為 required。"
            "系統在處理某些文檔時，模型產生了看起來合理但實際不存在的 URL。"
            "根本原因是什麼？修復方式是什麼？"
        ),
        "options": {
            "A": "模型幻覺問題，使用 extended thinking 可以解決",
            "B": "source_url 是 required 但源文檔不一定有 URL → 模型被迫捏造值。改為 nullable",
            "C": "Schema syntax 錯誤，使用 strict: true 可以防止",
            "D": "需要加入驗證層在 tool 結果後檢查 URL 格式",
        },
        "correct": "B",
        "explanation": (
            "B 正確：required field + 資料不存在 = 模型被迫 fabricate。"
            "解法：改為 type: ['string', 'null']（Task 4.3 核心知識）。"
            "A 錯：extended thinking 不解決 schema 設計問題。"
            "C 錯：strict: true 消除語法錯誤，不消除語義錯誤/幻覺。"
            "D 錯：驗證層只能發現問題，不能防止根因。"
        ),
        "task_ref": "4.3",
    },
]


# ============================================================
# Main: Run All Modules
# ============================================================

def main():
    print("=" * 70)
    print("  CCA D4: Prompt Engineering & Structured Output Practice")
    print("  Date: 2026-06-19")
    print("=" * 70)

    passed = 0
    total = 0

    # ─────────────────────────────────────────────
    # Module 1: Explicit Criteria Engine
    # ─────────────────────────────────────────────
    print("\n" + "─" * 60)
    print("Module 1: Explicit Criteria Engine (Task 4.1)")
    print("─" * 60)

    engine = ExplicitCriteriaEngine()
    comparison = engine.compare_vague_vs_explicit()

    # Verify: vague reports everything, explicit filters
    vague_count = comparison["vague_approach"]["total_reported"]
    explicit_reported = comparison["explicit_approach"]["total_reported"]
    explicit_suppressed = comparison["explicit_approach"]["suppressed"]

    total += 3
    # Test 1: Vague reports all
    assert vague_count == 5, f"Vague should report all 5, got {vague_count}"
    passed += 1
    print(f"  ✅ Vague approach reports all {vague_count} findings (no filtering)")

    # Test 2: Explicit suppresses high-FP categories
    assert explicit_suppressed == 2, f"Expected 2 suppressed, got {explicit_suppressed}"
    passed += 1
    print(f"  ✅ Explicit approach suppresses {explicit_suppressed} high-FP categories")

    # Test 3: Explicit reports only actionable findings
    assert explicit_reported == 3, f"Expected 3 reported, got {explicit_reported}"
    passed += 1
    print(f"  ✅ Explicit approach reports {explicit_reported} actionable findings")

    print(f"\n  Key insight: '{comparison['vague_approach']['instruction']}'")
    print(f"  → Reports everything, no signal-to-noise improvement")
    print(f"  Explicit criteria: report {explicit_reported}, suppress {explicit_suppressed}")

    # ─────────────────────────────────────────────
    # Module 2: Few-Shot Prompt Builder
    # ─────────────────────────────────────────────
    print("\n" + "─" * 60)
    print("Module 2: Few-Shot Prompt Builder (Task 4.2)")
    print("─" * 60)

    builder = FewShotPromptBuilder()

    # Add ODM supplier notification examples (2-4 targeted)
    builder.add_example(
        input_text="供應商 ABC 通知：因晶片短缺，交期延後 2 週",
        output={
            "vendor": "ABC",
            "event_type": "delay",
            "impact": "2 weeks",
            "root_cause": "chip_shortage",
            "severity": "high",
        },
        reasoning="明確的延遲通知，有具體天數和原因。高嚴重性因為影響生產排程。"
    )

    builder.add_example(
        input_text="XYZ Corp: 新的 MOQ 要求，最低 500 件起訂",
        output={
            "vendor": "XYZ Corp",
            "event_type": "policy_change",
            "impact": "MOQ increased to 500",
            "root_cause": None,
            "severity": "medium",
        },
        reasoning="政策變更，不是緊急事件。root_cause 為 null 因為通知沒說明原因。"
    )

    builder.add_example(
        input_text="DEF 漲價通知：Q3 所有連接器漲 8%",
        output={
            "vendor": "DEF",
            "event_type": "price_increase",
            "impact": "8% increase on connectors",
            "root_cause": None,
            "severity": "high",
        },
        reasoning="直接影響 BOM cost。root_cause null 因為未說明漲價原因。"
    )

    # Build prompt
    result = builder.build_prompt(
        "Extract structured data from supplier notifications",
        "GHI 通知：被動元件工廠火災，預計停產 1 個月"
    )

    total += 2
    assert result["within_range"], f"Example count {result['example_count']} not in 2-4 range"
    passed += 1
    print(f"  ✅ Example count: {result['example_count']} (within 2-4 range)")

    # Verify all examples have reasoning
    for i, ex in enumerate(builder.examples):
        assert ex["reasoning"], f"Example {i+1} missing reasoning"
    passed += 1
    print(f"  ✅ All {len(builder.examples)} examples include reasoning (why A over B)")

    # Test generalization
    gen_result = builder.validate_generalization(
        examples=[
            {"output": {"vendor": "", "event_type": "", "impact": "", "root_cause": None, "severity": ""}},
        ],
        test_cases=[
            {"input": "JKL 降價 5%", "expected_output": {"vendor": "", "event_type": "", "impact": "", "severity": ""}},
            {"input": "MNO 倒閉", "expected_output": {"vendor": "", "event_type": "", "impact": "", "severity": ""}},
        ]
    )
    total += 1
    assert gen_result["generalizable"] == 2, f"Generalization failed"
    passed += 1
    print(f"  ✅ Generalization: {gen_result['generalizable']}/{gen_result['total_test_cases']} test cases covered")

    # ─────────────────────────────────────────────
    # Module 3: Tool Choice Simulator
    # ─────────────────────────────────────────────
    print("\n" + "─" * 60)
    print("Module 3: Tool Choice Simulator (Task 4.3)")
    print("─" * 60)

    sim = ToolChoiceSimulator()
    sim.register_tool("extract_invoice", "Extract data from invoice documents", {
        "type": "object",
        "properties": {"vendor": {"type": "string"}, "amount": {"type": "number"}},
    })
    sim.register_tool("extract_po", "Extract data from purchase order documents", {
        "type": "object",
        "properties": {"po_number": {"type": "string"}, "items": {"type": "array"}},
    })
    sim.register_tool("extract_shipping", "Extract data from shipping notice documents", {
        "type": "object",
        "properties": {"tracking": {"type": "string"}, "eta": {"type": "string"}},
    })

    # Test auto — might return text (no guarantee)
    auto_result = sim.simulate_response(
        {"type": "auto"}, "What is the weather today?"
    )
    total += 1
    assert auto_result["selected_tool"] is None, "Auto should return text for non-extraction query"
    passed += 1
    print(f"  ✅ auto + non-extraction query → text response (no structured output)")

    # Test auto — might call tool
    auto_result2 = sim.simulate_response(
        {"type": "auto"}, "Extract the vendor information"
    )
    total += 1
    assert auto_result2["selected_tool"] is not None, "Auto should call tool for extraction"
    passed += 1
    print(f"  ✅ auto + extraction query → tool call (but NOT guaranteed)")

    # Test any — must call tool
    any_result = sim.simulate_response(
        {"type": "any"}, "Process this document"
    )
    total += 1
    assert any_result["selected_tool"] is not None, "Any must call a tool"
    assert "guaranteed" in any_result.get("guarantee", "").lower()
    passed += 1
    print(f"  ✅ any → MUST call tool '{any_result['selected_tool']}' (structured output guaranteed)")

    # Test forced — must call specific tool
    forced_result = sim.simulate_response(
        {"type": "tool", "name": "extract_invoice"}, "Process this"
    )
    total += 1
    assert forced_result["selected_tool"] == "extract_invoice"
    passed += 1
    print(f"  ✅ tool(extract_invoice) → forced to call 'extract_invoice'")

    # ─────────────────────────────────────────────
    # Module 4: Schema Design Validator
    # ─────────────────────────────────────────────
    print("\n" + "─" * 60)
    print("Module 4: Schema Design Validator (Task 4.3)")
    print("─" * 60)

    validator = SchemaDesignValidator()

    # Bad schema: too many required, no nullable, no enum escape
    bad_schema = {
        "properties": {
            "vendor": {"type": "string"},
            "amount": {"type": "number"},
            "source": {"type": "string"},  # risky required
            "date": {"type": "string"},    # risky required
            "category": {"type": "string", "enum": ["A", "B", "C"]},  # no 'other'
        },
        "required": ["vendor", "amount", "source", "date", "category"],
    }
    bad_result = validator.validate(bad_schema)
    total += 1
    assert not bad_result["all_passed"], "Bad schema should fail validation"
    passed += 1
    failed_rules = [r["rule_id"] for r in bad_result["results"] if not r["passed"]]
    print(f"  ✅ Bad schema detected issues: {', '.join(failed_rules)}")

    # Good schema: minimal required, nullable, enum + other
    good_schema = {
        "properties": {
            "vendor": {"type": "string"},
            "amount": {"type": "number"},
            "source": {"type": ["string", "null"]},
            "date": {"type": ["string", "null"]},
            "category": {
                "type": "string",
                "enum": ["raw_material", "electronics", "packaging", "other", "unclear"],
            },
            "category_detail": {
                "type": ["string", "null"],
                "description": "Detail when category is 'other' or 'unclear'",
            },
        },
        "required": ["vendor", "amount", "category"],
    }
    good_result = validator.validate(good_schema)
    total += 1
    assert good_result["all_passed"], f"Good schema should pass: {good_result}"
    passed += 1
    print(f"  ✅ Good schema passes all {good_result['score']} rules")

    # Demonstrate specific fabrication risk
    fab_schema = {
        "properties": {
            "title": {"type": "string"},
            "url": {"type": "string"},
        },
        "required": ["title", "url"],
    }
    # url is in RISKY_REQUIRED but we need to add it
    validator.RISKY_REQUIRED.add("url")
    fab_result = validator.validate(fab_schema)
    fab_issues = [r for r in fab_result["results"] if r["rule_id"] == "R5_no_fabrication_risk"]
    total += 1
    if fab_issues and not fab_issues[0]["passed"]:
        passed += 1
        print(f"  ✅ Fabrication risk detected for required 'url' field")
    else:
        print(f"  ❌ Failed to detect fabrication risk")

    # ─────────────────────────────────────────────
    # Module 5: Mock Exam
    # ─────────────────────────────────────────────
    print("\n" + "─" * 60)
    print("Module 5: CCA D4 Mock Exam (5 Questions)")
    print("─" * 60)

    exam_passed = 0
    for q in MOCK_EXAM:
        total += 1
        print(f"\n  {q['id']} ({q['scenario']}, Task {q['task_ref']}):")
        print(f"    Q: {q['question'][:80]}...")
        for key, val in q["options"].items():
            marker = " ✅" if key == q["correct"] else ""
            print(f"    {key}: {val[:70]}...{marker}" if len(val) > 70 else f"    {key}: {val}{marker}")

        # Simulate answering correctly
        print(f"    → 正確：{q['correct']}")
        print(f"    → {q['explanation'][:100]}...")
        passed += 1
        exam_passed += 1

    print(f"\n  Mock Exam: {exam_passed}/{len(MOCK_EXAM)} correct")

    # ─────────────────────────────────────────────
    # Summary
    # ─────────────────────────────────────────────
    print("\n" + "=" * 70)
    print(f"  Total: {passed}/{total} validations passed")
    print("=" * 70)

    # Key concepts summary
    print("\n📋 D4 Key Concepts Checklist:")
    concepts = [
        ("4.1", "Explicit criteria > vague instructions ('be conservative' fails)"),
        ("4.1", "Temporarily disable high-FP categories, improve, re-enable"),
        ("4.1", "Severity definitions MUST include concrete code examples"),
        ("4.2", "2-4 few-shot examples (not 1, not 10)"),
        ("4.2", "Examples must show reasoning (why A over alternatives)"),
        ("4.2", "Few-shot enables generalization to novel patterns"),
        ("4.2", "Few-shot for informal measurements > rule-based instructions"),
        ("4.3", "tool_use + JSON schema eliminates SYNTAX errors"),
        ("4.3", "tool_use does NOT prevent SEMANTIC errors"),
        ("4.3", "auto=might text, any=must tool(self-pick), tool(X)=forced"),
        ("4.3", "nullable for potentially missing fields (prevents fabrication)"),
        ("4.3", "enum + 'other' + detail string for extensible categories"),
        ("4.3", "enum + 'unclear' for honest uncertainty"),
        ("4.3", "Format normalization rules alongside strict schemas"),
    ]
    for task, concept in concepts:
        print(f"  [{task}] {concept}")

    assert passed == total, f"Not all validations passed: {passed}/{total}"
    print(f"\n✅ All {total} validations passed!")


if __name__ == "__main__":
    main()
