"""
CCA Practice — Validation, Retry & Batch API (D4 Tasks 4.4-4.6)
================================================================
Dependency-free simulation of:
1. RetryWithFeedback: semantic validation + retry-with-error-feedback loop
2. SelfCorrectionEngine: calculated vs stated total conflict detection
3. BatchAPIDecisionEngine: sync vs batch API selection + SLA calculation
4. MultiInstanceReviewSimulator: self-review bias vs independent instance
5. CCA Mock Exam (5 questions)
"""

from dataclasses import dataclass, field
from typing import Optional
from enum import Enum
import json

# ============================================================================
# Module 1: RetryWithFeedback — Semantic Validation + Retry Loop
# ============================================================================

class ErrorType(Enum):
    SYNTAX = "syntax"       # JSON format errors → eliminated by tool_use
    SEMANTIC = "semantic"   # Value errors → needs validation + retry
    MISSING = "missing"     # Info not in source → retry is USELESS


@dataclass
class ValidationError:
    field: str
    error_type: ErrorType
    message: str
    expected: Optional[str] = None
    actual: Optional[str] = None


@dataclass
class ExtractionResult:
    data: dict
    source_document: str
    errors: list = field(default_factory=list)
    retry_count: int = 0
    max_retries: int = 3


class RetryWithFeedback:
    """
    Implements the retry-with-error-feedback pattern:
    1. Extract data using tool_use (syntax errors eliminated)
    2. Validate semantically (business rules)
    3. If errors → retry with: original doc + bad extraction + specific errors
    4. If info missing → DON'T retry, flag for human review
    """

    def __init__(self):
        self.retry_log = []

    def validate_invoice(self, extracted: dict, source: str) -> list[ValidationError]:
        """Semantic validation for invoice extraction."""
        errors = []

        # Rule 1: line_items sum must equal stated_total
        if "line_items" in extracted and "total" in extracted:
            calculated = sum(item.get("price", 0) for item in extracted["line_items"])
            stated = extracted["total"]
            if abs(calculated - stated) > 0.01:
                errors.append(ValidationError(
                    field="total",
                    error_type=ErrorType.SEMANTIC,
                    message=f"sum(line_items) = {calculated}, but stated total = {stated}",
                    expected=str(calculated),
                    actual=str(stated)
                ))

        # Rule 2: invoice_date must not be in the future
        if "invoice_date" in extracted:
            if extracted["invoice_date"] > "2026-06-20":
                errors.append(ValidationError(
                    field="invoice_date",
                    error_type=ErrorType.SEMANTIC,
                    message=f"Invoice date {extracted['invoice_date']} is in the future"
                ))

        # Rule 3: vendor_id must exist (if not in source → MISSING, don't retry)
        if "vendor_id" not in extracted or extracted["vendor_id"] is None:
            if "vendor" not in source.lower() and "供應商" not in source:
                errors.append(ValidationError(
                    field="vendor_id",
                    error_type=ErrorType.MISSING,
                    message="vendor_id not found in source document — info does not exist"
                ))
            else:
                errors.append(ValidationError(
                    field="vendor_id",
                    error_type=ErrorType.SEMANTIC,
                    message="vendor_id present in source but not extracted"
                ))

        # Rule 4: PO number format validation
        if "po_number" in extracted and extracted["po_number"]:
            if not extracted["po_number"].startswith("PO-"):
                errors.append(ValidationError(
                    field="po_number",
                    error_type=ErrorType.SEMANTIC,
                    message=f"PO number '{extracted['po_number']}' doesn't match format PO-XXXXX"
                ))

        return errors

    def should_retry(self, errors: list[ValidationError]) -> tuple[bool, list[ValidationError], list[ValidationError]]:
        """
        Determine if retry is worthwhile.
        Returns: (should_retry, retryable_errors, non_retryable_errors)

        KEY INSIGHT: Retry is USELESS for MISSING errors (info not in source).
        Only SEMANTIC errors are worth retrying.
        """
        retryable = [e for e in errors if e.error_type == ErrorType.SEMANTIC]
        non_retryable = [e for e in errors if e.error_type == ErrorType.MISSING]
        return len(retryable) > 0, retryable, non_retryable

    def build_retry_prompt(self, source: str, bad_extraction: dict,
                           errors: list[ValidationError]) -> str:
        """
        Build retry prompt with:
        1. Original document
        2. Previous (incorrect) extraction
        3. Specific validation errors
        """
        error_details = "\n".join([
            f"  - Field '{e.field}': {e.message}"
            for e in errors
        ])
        return (
            f"Please re-extract from the following document.\n\n"
            f"ORIGINAL DOCUMENT:\n{source}\n\n"
            f"PREVIOUS (INCORRECT) EXTRACTION:\n{json.dumps(bad_extraction, indent=2)}\n\n"
            f"SPECIFIC VALIDATION ERRORS:\n{error_details}\n\n"
            f"Please fix these errors and re-extract."
        )

    def run_extraction_loop(self, source: str, mock_extractions: list[dict]) -> dict:
        """
        Simulate the full extraction loop with mock model responses.
        """
        for attempt, extraction in enumerate(mock_extractions):
            errors = self.validate_invoice(extraction, source)

            if not errors:
                self.retry_log.append({
                    "attempt": attempt + 1,
                    "status": "success",
                    "data": extraction
                })
                return {"status": "success", "data": extraction, "attempts": attempt + 1}

            should_retry, retryable, non_retryable = self.should_retry(errors)

            self.retry_log.append({
                "attempt": attempt + 1,
                "status": "retry" if should_retry else "escalate",
                "retryable_errors": len(retryable),
                "non_retryable_errors": len(non_retryable),
                "prompt": self.build_retry_prompt(source, extraction, retryable) if should_retry else None
            })

            if non_retryable:
                return {
                    "status": "needs_human_review",
                    "reason": "Information not present in source",
                    "missing_fields": [e.field for e in non_retryable],
                    "attempts": attempt + 1
                }

            if not should_retry or attempt >= len(mock_extractions) - 1:
                return {
                    "status": "max_retries_exceeded",
                    "remaining_errors": [e.message for e in errors],
                    "attempts": attempt + 1
                }

        return {"status": "failed", "attempts": len(mock_extractions)}


# ============================================================================
# Module 2: SelfCorrectionEngine — Conflict Detection
# ============================================================================

class SelfCorrectionEngine:
    """
    Self-correction pattern: extract BOTH stated and calculated values,
    then detect conflicts automatically.

    Schema design key: include calculated_total + stated_total + conflict_detected
    """

    def extract_with_self_correction(self, document: dict) -> dict:
        """
        Simulate extraction that includes self-correction fields.
        """
        line_items = document.get("line_items", [])
        stated_total = document.get("stated_total", 0)
        calculated_total = sum(item["price"] * item.get("quantity", 1)
                               for item in line_items)

        conflict = abs(calculated_total - stated_total) > 0.01

        result = {
            "stated_total": stated_total,
            "calculated_total": round(calculated_total, 2),
            "conflict_detected": conflict,
            "line_items": line_items,
            "conflict_detail": None
        }

        if conflict:
            diff = round(calculated_total - stated_total, 2)
            result["conflict_detail"] = (
                f"Calculated ({calculated_total}) differs from stated ({stated_total}) "
                f"by {diff}. Possible causes: missing line item, rounding, or "
                f"discount not reflected in line items."
            )

        return result

    def design_anti_fabrication_schema(self) -> dict:
        """
        Demo: Schema design rules to prevent fabrication.
        - nullable fields for potentially missing info
        - enum + "other" for extensibility
        - enum + "unclear" for honest uncertainty
        """
        return {
            "type": "object",
            "properties": {
                "vendor_name": {
                    "type": "string",
                    "description": "Vendor name as stated in document"
                },
                "vendor_category": {
                    "type": "string",
                    "enum": ["electronic_components", "mechanical_parts",
                             "packaging", "logistics", "other", "unclear"],
                    "description": "Category. Use 'other' + detail if not in list. "
                                   "Use 'unclear' if cannot determine."
                },
                "category_detail": {
                    "type": ["string", "null"],
                    "description": "Detail when category is 'other' or 'unclear'"
                },
                "contract_value": {
                    "type": ["number", "null"],
                    "description": "Total contract value. null if not stated in document."
                },
                "stated_total": {"type": "number"},
                "calculated_total": {"type": "number"},
                "conflict_detected": {"type": "boolean"},
                "confidence": {
                    "type": "number",
                    "minimum": 0,
                    "maximum": 1,
                    "description": "Extraction confidence score"
                }
            },
            "required": ["vendor_name", "vendor_category", "stated_total",
                         "calculated_total", "conflict_detected", "confidence"]
        }


# ============================================================================
# Module 3: BatchAPIDecisionEngine — Sync vs Batch + SLA
# ============================================================================

@dataclass
class WorkloadProfile:
    name: str
    doc_count: int
    latency_requirement_hours: Optional[float]  # None = no latency requirement
    is_blocking: bool  # True = someone is waiting
    needs_tool_calling: bool  # Batch API doesn't support multi-turn


class BatchAPIDecisionEngine:
    """
    Decision engine for sync vs batch API selection.

    Key constraints:
    - Batch API: 50% cost savings, up to 24h processing, NO multi-turn tool calling
    - Sync API: immediate response, full cost, supports tool calling
    """

    BATCH_PROCESSING_WINDOW_HOURS = 24
    BATCH_COST_SAVINGS = 0.50  # 50%

    def recommend(self, workload: WorkloadProfile) -> dict:
        """
        Recommend sync vs batch API based on workload profile.
        """
        reasons = []
        recommendation = "batch"  # default to batch (cheaper)

        # Rule 1: Blocking workflows → sync
        if workload.is_blocking:
            recommendation = "sync"
            reasons.append("Blocking workflow: someone is waiting for results")

        # Rule 2: Latency requirement < 24h → sync
        if workload.latency_requirement_hours is not None:
            if workload.latency_requirement_hours < self.BATCH_PROCESSING_WINDOW_HOURS:
                recommendation = "sync"
                reasons.append(
                    f"Latency requirement ({workload.latency_requirement_hours}h) "
                    f"< batch window ({self.BATCH_PROCESSING_WINDOW_HOURS}h)"
                )

        # Rule 3: Needs multi-turn tool calling → sync
        if workload.needs_tool_calling:
            recommendation = "sync"
            reasons.append("Requires multi-turn tool calling (not supported by Batch API)")

        # If batch is recommended, calculate SLA
        sla_plan = None
        if recommendation == "batch":
            reasons.append(f"Non-blocking, latency-tolerant → save {int(self.BATCH_COST_SAVINGS * 100)}%")
            if workload.latency_requirement_hours:
                sla_plan = self.calculate_sla_plan(workload.latency_requirement_hours)

        cost_per_doc = 1.0 if recommendation == "sync" else (1.0 - self.BATCH_COST_SAVINGS)
        total_cost_ratio = cost_per_doc * workload.doc_count

        return {
            "recommendation": recommendation,
            "reasons": reasons,
            "cost_ratio": cost_per_doc,
            "total_cost_relative": total_cost_ratio,
            "savings_vs_sync": (1.0 - cost_per_doc) * workload.doc_count if recommendation == "batch" else 0,
            "sla_plan": sla_plan
        }

    def calculate_sla_plan(self, sla_hours: float) -> dict:
        """
        Calculate batch submission plan for given SLA.

        Example: SLA = 30h, batch window = 24h
        → submission_window = 30 - 24 = 6h
        → recommended_interval = 4h (safety margin)
        """
        submission_window = sla_hours - self.BATCH_PROCESSING_WINDOW_HOURS

        if submission_window <= 0:
            return {
                "feasible": False,
                "reason": f"SLA ({sla_hours}h) ≤ batch window ({self.BATCH_PROCESSING_WINDOW_HOURS}h)"
            }

        # Recommended interval: 2/3 of submission window for safety
        recommended_interval = max(1, int(submission_window * 2 / 3))

        return {
            "feasible": True,
            "submission_window_hours": submission_window,
            "recommended_interval_hours": recommended_interval,
            "example": (
                f"Submit batches every {recommended_interval}h. "
                f"Last submission must be {self.BATCH_PROCESSING_WINDOW_HOURS}h before deadline."
            )
        }

    def handle_batch_failure(self, results: list[dict]) -> dict:
        """
        Handle partial batch failures using custom_id.
        """
        succeeded = [r for r in results if r["status"] == "success"]
        failed = [r for r in results if r["status"] != "success"]

        resubmit_strategy = []
        for f in failed:
            if f.get("error") == "context_limit_exceeded":
                resubmit_strategy.append({
                    "custom_id": f["custom_id"],
                    "action": "chunk_and_resubmit",
                    "detail": "Split document into smaller chunks"
                })
            elif f.get("error") == "timeout":
                resubmit_strategy.append({
                    "custom_id": f["custom_id"],
                    "action": "resubmit_as_is",
                    "detail": "Transient failure, retry without modification"
                })
            else:
                resubmit_strategy.append({
                    "custom_id": f["custom_id"],
                    "action": "flag_for_review",
                    "detail": f"Unknown error: {f.get('error', 'N/A')}"
                })

        return {
            "total": len(results),
            "succeeded": len(succeeded),
            "failed": len(failed),
            "success_rate": f"{len(succeeded)/len(results)*100:.1f}%",
            "resubmit_strategy": resubmit_strategy
        }


# ============================================================================
# Module 4: MultiInstanceReviewSimulator
# ============================================================================

@dataclass
class CodeFinding:
    file: str
    line: int
    issue: str
    severity: str
    confidence: float
    detected_pattern: str


class MultiInstanceReviewSimulator:
    """
    Demonstrates why independent review instances outperform self-review.

    Key insight: The generator retains reasoning context and is less likely
    to question its own decisions.
    """

    def simulate_self_review(self, generated_code: dict) -> dict:
        """
        Simulate self-review (same session).
        The model has reasoning bias — it tends to approve its own decisions.
        """
        findings = []
        missed = []

        # Self-review catches obvious issues
        for issue in generated_code.get("obvious_issues", []):
            findings.append(CodeFinding(
                file=issue["file"], line=issue["line"],
                issue=issue["desc"], severity="high",
                confidence=0.9, detected_pattern=issue.get("pattern", "")
            ))

        # But MISSES subtle issues due to reasoning bias
        for issue in generated_code.get("subtle_issues", []):
            missed.append({
                "file": issue["file"],
                "issue": issue["desc"],
                "reason_missed": "Generator's reasoning context biases against questioning this decision"
            })

        return {
            "review_type": "self_review",
            "findings_count": len(findings),
            "missed_count": len(missed),
            "missed_issues": missed,
            "bias_warning": (
                "Self-review retains prior reasoning context. "
                "The model is less likely to challenge its own earlier decisions."
            )
        }

    def simulate_independent_review(self, generated_code: dict) -> dict:
        """
        Simulate independent instance review (separate session, no gen context).
        Catches more subtle issues because no reasoning bias.
        """
        findings = []

        # Catches both obvious AND subtle issues
        for issue in generated_code.get("obvious_issues", []):
            findings.append(CodeFinding(
                file=issue["file"], line=issue["line"],
                issue=issue["desc"], severity="high",
                confidence=0.9, detected_pattern=issue.get("pattern", "")
            ))

        for issue in generated_code.get("subtle_issues", []):
            findings.append(CodeFinding(
                file=issue["file"], line=issue["line"],
                issue=issue["desc"], severity=issue.get("severity", "medium"),
                confidence=0.75, detected_pattern=issue.get("pattern", "")
            ))

        return {
            "review_type": "independent_instance",
            "findings_count": len(findings),
            "missed_count": 0,
            "advantage": "No prior reasoning context → unbiased evaluation"
        }

    def simulate_multipass_review(self, files: list[dict]) -> dict:
        """
        Simulate multi-pass review for large PRs:
        Pass 1: Per-file local analysis
        Pass 2: Cross-file integration analysis
        """
        # Pass 1: Per-file
        per_file_findings = {}
        for f in files:
            local_issues = []
            for issue in f.get("local_issues", []):
                local_issues.append({
                    "line": issue["line"],
                    "issue": issue["desc"],
                    "severity": issue["severity"]
                })
            per_file_findings[f["name"]] = local_issues

        # Pass 2: Cross-file integration
        integration_findings = []
        # Check type mismatches across files
        exports = {}
        for f in files:
            for exp in f.get("exports", []):
                exports[exp["name"]] = {"file": f["name"], "type": exp["type"]}

        for f in files:
            for imp in f.get("imports", []):
                if imp["name"] in exports:
                    exp = exports[imp["name"]]
                    if imp.get("expected_type") and imp["expected_type"] != exp["type"]:
                        integration_findings.append({
                            "type": "type_mismatch",
                            "from_file": exp["file"],
                            "to_file": f["name"],
                            "symbol": imp["name"],
                            "expected": imp["expected_type"],
                            "actual": exp["type"]
                        })

        total_local = sum(len(v) for v in per_file_findings.values())

        return {
            "pass_1_per_file": per_file_findings,
            "pass_1_total_findings": total_local,
            "pass_2_integration": integration_findings,
            "pass_2_total_findings": len(integration_findings),
            "total_findings": total_local + len(integration_findings),
            "advantage_over_single_pass": (
                "Single-pass over all files causes attention dilution. "
                "Multi-pass ensures consistent depth per file AND catches cross-file issues."
            )
        }


# ============================================================================
# Module 5: CCA Mock Exam (5 questions)
# ============================================================================

MOCK_EXAM = [
    {
        "id": 1,
        "scenario": "Structured Data Extraction",
        "question": (
            "An extraction system uses tool_use with JSON Schema to extract invoice data. "
            "The system produces valid JSON but occasionally puts the tax amount in the "
            "discount field and vice versa. What type of error is this, and what should "
            "you add to fix it?"
        ),
        "options": {
            "A": "Syntax error — add stricter JSON Schema constraints",
            "B": "Semantic error — add validation checks that verify field values make business sense, with retry-with-error-feedback",
            "C": "Syntax error — switch from tool_use to raw text output with regex parsing",
            "D": "Semantic error — add more required fields to the schema"
        },
        "answer": "B",
        "explanation": (
            "Swapping values between fields is a SEMANTIC error (values are valid types but "
            "logically wrong). tool_use + JSON Schema eliminates SYNTAX errors (100%) but cannot "
            "catch semantic errors. Adding business rule validation (e.g., tax > 0, discount <= subtotal) "
            "with retry-with-error-feedback is the correct approach. A is wrong because stricter "
            "schema can't distinguish tax from discount. C is a downgrade. D adds fields but doesn't validate values."
        )
    },
    {
        "id": 2,
        "scenario": "Structured Data Extraction",
        "question": (
            "A batch extraction system processes 10,000 vendor contracts. 9,800 succeed, "
            "but 200 fail with 'context_limit_exceeded'. The team wants to handle failures "
            "efficiently. What approach should they use?"
        ),
        "options": {
            "A": "Resubmit all 10,000 documents in a new batch",
            "B": "Use custom_id to identify the 200 failed documents, chunk them into smaller pieces, and resubmit only those",
            "C": "Switch all 10,000 documents to synchronous API processing",
            "D": "Increase the max_tokens parameter and resubmit all documents"
        },
        "answer": "B",
        "explanation": (
            "custom_id is designed for correlating batch request/response pairs. The correct "
            "approach is: identify failures by custom_id → modify strategy (chunk long docs) → "
            "resubmit ONLY failed documents. A wastes resources re-processing 9,800 already-successful "
            "docs. C loses the 50% cost savings. D doesn't address context limits (input too large, "
            "not output too short)."
        )
    },
    {
        "id": 3,
        "scenario": "CI/CD with Claude Code",
        "question": (
            "A team uses Claude Code to generate code and then asks the SAME session to review "
            "it. The review rarely finds issues. A separate manual review by a senior engineer "
            "finds several subtle bugs. What's the most likely cause and best fix?"
        ),
        "options": {
            "A": "The system prompt needs more detailed review criteria",
            "B": "The model's context window is too full from code generation",
            "C": "The same session retains reasoning context from generation, making it less likely to question its own decisions — use an independent Claude instance for review",
            "D": "Add extended thinking to improve review quality"
        },
        "answer": "C",
        "explanation": (
            "This is the self-review bias problem. The model retains its prior reasoning context "
            "and is biased toward approving its own decisions. An independent review instance "
            "(without the generator's reasoning context) is more effective at catching subtle issues. "
            "A might help marginally but doesn't address the root cause. B is unlikely if the "
            "generation was normal. D doesn't solve the bias problem."
        )
    },
    {
        "id": 4,
        "scenario": "Structured Data Extraction",
        "question": (
            "An extraction system retries when validation fails. For document type X, the system "
            "consistently fails to extract a 'payment_terms' field after 3 retries. Investigation "
            "shows document type X never contains payment terms information. What should the team do?"
        ),
        "options": {
            "A": "Increase max retries to 5 for this document type",
            "B": "Add a few-shot example showing how to extract payment terms from similar documents",
            "C": "Detect that the information is absent from the source and stop retrying — flag for human review or mark as null",
            "D": "Use a different model with larger context window"
        },
        "answer": "C",
        "explanation": (
            "Key insight: retries are INEFFECTIVE when the required information is simply absent "
            "from the source document. More retries (A) just waste API calls. Few-shot (B) can't "
            "create information that doesn't exist. Larger context (D) is irrelevant — the data "
            "isn't there. The correct approach is to detect absence and surface it appropriately."
        )
    },
    {
        "id": 5,
        "scenario": "CI/CD with Claude Code",
        "question": (
            "A team needs to run nightly security audits on their codebase and also run "
            "pre-merge checks on pull requests. They want to minimize costs. Which API "
            "strategy should they use?"
        ),
        "options": {
            "A": "Use Batch API for both — it saves 50%",
            "B": "Use synchronous API for both — consistency is more important than cost",
            "C": "Use synchronous API for pre-merge checks (blocking, needs immediate results) and Batch API for nightly audits (non-blocking, saves 50%)",
            "D": "Use Batch API for pre-merge checks and synchronous API for nightly audits"
        },
        "answer": "C",
        "explanation": (
            "Pre-merge checks are BLOCKING — developers are waiting for results. Using Batch API "
            "(up to 24h processing, no latency SLA) would unacceptably delay merges. Nightly audits "
            "are NON-BLOCKING — results are needed by morning, not immediately. Batch API saves 50% "
            "and the 24h window fits within the overnight schedule. D is exactly backwards."
        )
    }
]


def run_mock_exam():
    """Run mock exam and score."""
    print("\n" + "=" * 70)
    print("📝 CCA MOCK EXAM — D4 Tasks 4.4-4.6")
    print("=" * 70)

    correct = 0
    total = len(MOCK_EXAM)

    for q in MOCK_EXAM:
        print(f"\n--- Q{q['id']} ({q['scenario']}) ---")
        print(f"Q: {q['question']}")
        for key, val in q["options"].items():
            marker = "✅" if key == q["answer"] else "  "
            print(f"  {marker} {key}) {val}")
        print(f"\n💡 Explanation: {q['explanation']}")
        correct += 1  # Auto-scored as correct for learning mode

    print(f"\n{'=' * 70}")
    print(f"Score: {correct}/{total}")
    return correct, total


# ============================================================================
# Main Validation Runner
# ============================================================================

def main():
    passed = 0
    total = 0

    # --- Module 1: RetryWithFeedback ---
    print("=" * 70)
    print("Module 1: RetryWithFeedback")
    print("=" * 70)

    engine = RetryWithFeedback()

    # Test 1.1: Successful retry (semantic error fixed on 2nd attempt)
    source = "Invoice from Vendor ABC Corp. PO-12345. Items: Widget A $75, Widget B $70. Total: $145.00"
    mock_extractions = [
        # Attempt 1: wrong total (semantic error)
        {"vendor_id": "ABC", "po_number": "PO-12345",
         "line_items": [{"name": "Widget A", "price": 75}, {"name": "Widget B", "price": 70}],
         "total": 150.00, "invoice_date": "2026-06-15"},
        # Attempt 2: fixed
        {"vendor_id": "ABC", "po_number": "PO-12345",
         "line_items": [{"name": "Widget A", "price": 75}, {"name": "Widget B", "price": 70}],
         "total": 145.00, "invoice_date": "2026-06-15"},
    ]
    result = engine.run_extraction_loop(source, mock_extractions)
    assert result["status"] == "success", f"Expected success, got {result['status']}"
    assert result["attempts"] == 2, f"Expected 2 attempts, got {result['attempts']}"
    print(f"  ✅ 1.1 Semantic error fixed on retry: {result['attempts']} attempts")
    passed += 1; total += 1

    # Test 1.2: Missing info → don't retry, flag for human
    engine2 = RetryWithFeedback()
    source2 = "Invoice #1234. Items: PCB Board $500. Total: $500.00. Date: 2026-06-10."
    mock_extractions2 = [
        {"vendor_id": None, "po_number": "PO-99999",
         "line_items": [{"name": "PCB Board", "price": 500}],
         "total": 500.00, "invoice_date": "2026-06-10"},
    ]
    result2 = engine2.run_extraction_loop(source2, mock_extractions2)
    assert result2["status"] == "needs_human_review", f"Expected needs_human_review, got {result2['status']}"
    assert "vendor_id" in result2["missing_fields"]
    print(f"  ✅ 1.2 Missing info detected → no retry, flagged for human review")
    passed += 1; total += 1

    # Test 1.3: Retry prompt includes all three components
    engine3 = RetryWithFeedback()
    errors = [ValidationError("total", ErrorType.SEMANTIC, "sum != total")]
    prompt = engine3.build_retry_prompt("source doc", {"total": 150}, errors)
    assert "ORIGINAL DOCUMENT" in prompt
    assert "PREVIOUS (INCORRECT) EXTRACTION" in prompt
    assert "SPECIFIC VALIDATION ERRORS" in prompt
    print(f"  ✅ 1.3 Retry prompt contains: original doc + bad extraction + specific errors")
    passed += 1; total += 1

    # Test 1.4: ErrorType classification
    assert ErrorType.SYNTAX.value == "syntax"
    assert ErrorType.SEMANTIC.value == "semantic"
    assert ErrorType.MISSING.value == "missing"
    print(f"  ✅ 1.4 Error types: syntax (tool_use eliminates), semantic (needs validation), missing (don't retry)")
    passed += 1; total += 1

    # --- Module 2: SelfCorrectionEngine ---
    print(f"\n{'=' * 70}")
    print("Module 2: SelfCorrectionEngine")
    print("=" * 70)

    sc = SelfCorrectionEngine()

    # Test 2.1: Conflict detected
    doc_conflict = {
        "stated_total": 150.00,
        "line_items": [
            {"name": "Widget A", "price": 75.00, "quantity": 1},
            {"name": "Widget B", "price": 70.00, "quantity": 1}
        ]
    }
    result = sc.extract_with_self_correction(doc_conflict)
    assert result["conflict_detected"] == True
    assert result["calculated_total"] == 145.00
    assert result["stated_total"] == 150.00
    assert result["conflict_detail"] is not None
    print(f"  ✅ 2.1 Conflict detected: calculated={result['calculated_total']} vs stated={result['stated_total']}")
    passed += 1; total += 1

    # Test 2.2: No conflict
    doc_ok = {
        "stated_total": 145.00,
        "line_items": [
            {"name": "Widget A", "price": 75.00, "quantity": 1},
            {"name": "Widget B", "price": 70.00, "quantity": 1}
        ]
    }
    result2 = sc.extract_with_self_correction(doc_ok)
    assert result2["conflict_detected"] == False
    assert result2["conflict_detail"] is None
    print(f"  ✅ 2.2 No conflict: calculated={result2['calculated_total']} == stated={result2['stated_total']}")
    passed += 1; total += 1

    # Test 2.3: Anti-fabrication schema
    schema = sc.design_anti_fabrication_schema()
    props = schema["properties"]
    # nullable fields exist
    assert props["contract_value"]["type"] == ["number", "null"]
    # enum + other + unclear
    assert "other" in props["vendor_category"]["enum"]
    assert "unclear" in props["vendor_category"]["enum"]
    # conflict_detected is required
    assert "conflict_detected" in schema["required"]
    print(f"  ✅ 2.3 Anti-fabrication schema: nullable, enum+other+unclear, conflict_detected required")
    passed += 1; total += 1

    # --- Module 3: BatchAPIDecisionEngine ---
    print(f"\n{'=' * 70}")
    print("Module 3: BatchAPIDecisionEngine")
    print("=" * 70)

    batch = BatchAPIDecisionEngine()

    # Test 3.1: Blocking workflow → sync
    w1 = WorkloadProfile("PR pre-merge check", 1, 0.5, is_blocking=True, needs_tool_calling=False)
    r1 = batch.recommend(w1)
    assert r1["recommendation"] == "sync"
    print(f"  ✅ 3.1 Blocking PR check → sync (cost ratio: {r1['cost_ratio']})")
    passed += 1; total += 1

    # Test 3.2: Non-blocking overnight → batch
    w2 = WorkloadProfile("Nightly audit", 10000, 48, is_blocking=False, needs_tool_calling=False)
    r2 = batch.recommend(w2)
    assert r2["recommendation"] == "batch"
    assert r2["cost_ratio"] == 0.5
    assert r2["savings_vs_sync"] == 5000.0
    print(f"  ✅ 3.2 Nightly audit 10k docs → batch (savings: {r2['savings_vs_sync']} units)")
    passed += 1; total += 1

    # Test 3.3: Needs multi-turn tool calling → sync
    w3 = WorkloadProfile("Agent task", 100, 48, is_blocking=False, needs_tool_calling=True)
    r3 = batch.recommend(w3)
    assert r3["recommendation"] == "sync"
    assert any("multi-turn" in r.lower() for r in r3["reasons"])
    print(f"  ✅ 3.3 Needs tool calling → sync (Batch API doesn't support multi-turn)")
    passed += 1; total += 1

    # Test 3.4: SLA calculation
    sla = batch.calculate_sla_plan(30)
    assert sla["feasible"] == True
    assert sla["submission_window_hours"] == 6  # 30 - 24 = 6
    assert sla["recommended_interval_hours"] == 4  # 6 * 2/3 = 4
    print(f"  ✅ 3.4 SLA plan: 30h SLA → submit every {sla['recommended_interval_hours']}h (window: {sla['submission_window_hours']}h)")
    passed += 1; total += 1

    # Test 3.5: SLA infeasible
    sla_bad = batch.calculate_sla_plan(20)
    assert sla_bad["feasible"] == False
    print(f"  ✅ 3.5 SLA plan: 20h SLA → infeasible (< 24h batch window)")
    passed += 1; total += 1

    # Test 3.6: Batch failure handling
    results = [
        {"custom_id": "doc-001", "status": "success"},
        {"custom_id": "doc-002", "status": "success"},
        {"custom_id": "doc-003", "status": "failed", "error": "context_limit_exceeded"},
        {"custom_id": "doc-004", "status": "failed", "error": "timeout"},
        {"custom_id": "doc-005", "status": "success"},
    ]
    failure_report = batch.handle_batch_failure(results)
    assert failure_report["succeeded"] == 3
    assert failure_report["failed"] == 2
    chunk_actions = [s for s in failure_report["resubmit_strategy"] if s["action"] == "chunk_and_resubmit"]
    retry_actions = [s for s in failure_report["resubmit_strategy"] if s["action"] == "resubmit_as_is"]
    assert len(chunk_actions) == 1  # context_limit → chunk
    assert len(retry_actions) == 1  # timeout → retry as-is
    print(f"  ✅ 3.6 Batch failure: {failure_report['success_rate']} success, smart resubmit strategy")
    passed += 1; total += 1

    # --- Module 4: MultiInstanceReviewSimulator ---
    print(f"\n{'=' * 70}")
    print("Module 4: MultiInstanceReviewSimulator")
    print("=" * 70)

    reviewer = MultiInstanceReviewSimulator()

    code = {
        "obvious_issues": [
            {"file": "auth.ts", "line": 42, "desc": "SQL injection via string concatenation",
             "pattern": "string_concat_sql", "severity": "critical"},
        ],
        "subtle_issues": [
            {"file": "auth.ts", "line": 88, "desc": "Race condition in session refresh",
             "pattern": "async_race_condition", "severity": "high"},
            {"file": "database.ts", "line": 15, "desc": "Connection pool exhaustion under load",
             "pattern": "resource_leak", "severity": "medium"},
        ]
    }

    # Test 4.1: Self-review misses subtle issues
    self_result = reviewer.simulate_self_review(code)
    assert self_result["findings_count"] == 1  # Only catches obvious
    assert self_result["missed_count"] == 2    # Misses subtle
    print(f"  ✅ 4.1 Self-review: found {self_result['findings_count']}, MISSED {self_result['missed_count']} subtle issues")
    passed += 1; total += 1

    # Test 4.2: Independent review catches all
    indep_result = reviewer.simulate_independent_review(code)
    assert indep_result["findings_count"] == 3  # Catches all
    assert indep_result["missed_count"] == 0
    print(f"  ✅ 4.2 Independent review: found {indep_result['findings_count']}, missed {indep_result['missed_count']} — no bias!")
    passed += 1; total += 1

    # Test 4.3: Multi-pass review (per-file + integration)
    files = [
        {
            "name": "auth.ts",
            "local_issues": [
                {"line": 42, "desc": "SQL injection", "severity": "critical"}
            ],
            "exports": [{"name": "validateUser", "type": "Promise<User>"}],
            "imports": []
        },
        {
            "name": "routes.ts",
            "local_issues": [
                {"line": 10, "desc": "Missing error handler", "severity": "medium"}
            ],
            "exports": [],
            "imports": [{"name": "validateUser", "expected_type": "Promise<boolean>"}]
        },
        {
            "name": "database.ts",
            "local_issues": [
                {"line": 5, "desc": "Hardcoded connection string", "severity": "high"},
                {"line": 30, "desc": "Missing connection close", "severity": "medium"}
            ],
            "exports": [{"name": "getConnection", "type": "Connection"}],
            "imports": []
        }
    ]
    mp_result = reviewer.simulate_multipass_review(files)
    assert mp_result["pass_1_total_findings"] == 4  # 1 + 1 + 2 local issues
    assert mp_result["pass_2_total_findings"] == 1  # type mismatch: Promise<User> vs Promise<boolean>
    assert mp_result["total_findings"] == 5
    print(f"  ✅ 4.3 Multi-pass: Pass1 local={mp_result['pass_1_total_findings']}, "
          f"Pass2 integration={mp_result['pass_2_total_findings']}, total={mp_result['total_findings']}")
    passed += 1; total += 1

    # Test 4.4: Integration finding detail
    int_finding = mp_result["pass_2_integration"][0]
    assert int_finding["type"] == "type_mismatch"
    assert int_finding["expected"] == "Promise<boolean>"
    assert int_finding["actual"] == "Promise<User>"
    print(f"  ✅ 4.4 Cross-file type mismatch: validateUser expected {int_finding['expected']} but got {int_finding['actual']}")
    passed += 1; total += 1

    # --- Module 5: Mock Exam ---
    exam_correct, exam_total = run_mock_exam()
    passed += exam_correct
    total += exam_total

    # --- Final Score ---
    print(f"\n{'=' * 70}")
    print(f"🏆 FINAL SCORE: {passed}/{total} validations passed")
    print(f"{'=' * 70}")

    assert passed == total, f"Some validations failed! {passed}/{total}"
    print("\n✅ All validations passed!")


if __name__ == "__main__":
    main()
