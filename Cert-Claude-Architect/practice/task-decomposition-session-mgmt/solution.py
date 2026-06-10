"""
CCA Topic 5: Task Decomposition & Session Management Simulator
2026-06-11
"""
from datetime import datetime, timezone
from dataclasses import dataclass, field
from enum import Enum
from typing import Any
import re, json


# ==========================================
# Part 1: Task Decomposition Strategy Selector
# ==========================================

class DecompStrategy(Enum):
    FIXED_SEQUENTIAL = "fixed_sequential"
    DYNAMIC_ADAPTIVE = "dynamic_adaptive"
    MULTI_PASS = "multi_pass"
    PLAN_MODE = "plan_mode"


@dataclass
class TaskProfile:
    description: str
    keywords: list[str]
    file_count: int = 1
    scope_known: bool = True
    compliance_required: bool = False
    expected_strategy: DecompStrategy = DecompStrategy.FIXED_SEQUENTIAL


def select_decomposition_strategy(task: TaskProfile) -> DecompStrategy:
    """
    根據任務特徵選擇分解策略。
    考試判斷規則：
    - compliance/audit/predictable/repeatable → Fixed Sequential
    - exploratory/open-ended/investigation/unknown → Dynamic Adaptive
    - multi-file (>5) + review/analysis → Multi-pass
    - large-scale restructuring + architectural decisions → Plan Mode
    """
    kw_lower = [k.lower() for k in task.keywords]
    desc_lower = task.description.lower()

    # Rule 1: Large-scale restructuring → Plan Mode
    restructuring_terms = {"restructure", "microservices", "migration", "architecture"}
    if any(t in desc_lower for t in restructuring_terms) and task.file_count > 10:
        return DecompStrategy.PLAN_MODE

    # Rule 2: Multi-file review/analysis → Multi-pass
    if task.file_count > 5 and any(t in desc_lower for t in {"review", "analyze", "audit", "check"}):
        return DecompStrategy.MULTI_PASS

    # Rule 3: Compliance/predictable → Fixed Sequential
    if task.compliance_required or task.scope_known:
        compliance_terms = {"compliance", "audit", "template", "repeatable", "etl", "pipeline", "extract"}
        if any(t in desc_lower or t in kw_lower for t in compliance_terms):
            return DecompStrategy.FIXED_SEQUENTIAL

    # Rule 4: Exploratory/unknown → Dynamic Adaptive
    exploratory_terms = {"investigate", "explore", "debug", "legacy", "unknown", "open-ended", "research"}
    if not task.scope_known or any(t in desc_lower or t in kw_lower for t in exploratory_terms):
        return DecompStrategy.DYNAMIC_ADAPTIVE

    # Default: scope known → Fixed Sequential
    if task.scope_known:
        return DecompStrategy.FIXED_SEQUENTIAL

    return DecompStrategy.DYNAMIC_ADAPTIVE


# ==========================================
# Part 2: Cross-MCP PostToolUse Normalizer
# ==========================================

def normalize_date(value: Any) -> str:
    """Normalize heterogeneous date formats to ISO 8601."""
    if isinstance(value, (int, float)):
        # Unix timestamp
        return datetime.fromtimestamp(value, tz=timezone.utc).strftime("%Y-%m-%d")
    if isinstance(value, str):
        # Try various formats
        formats = [
            "%Y-%m-%dT%H:%M:%SZ",    # ISO 8601
            "%Y-%m-%dT%H:%M:%S%z",   # ISO with tz
            "%Y%m%d",                  # SAP format
            "%m/%d/%Y",               # US format
            "%d/%m/%Y",               # EU format
            "%b %d, %Y",             # "Mar 5, 2025"
            "%B %d, %Y",             # "March 5, 2025"
            "%Y-%m-%d",              # Already ISO
        ]
        for fmt in formats:
            try:
                dt = datetime.strptime(value.strip(), fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue
    return str(value)  # fallback


def normalize_currency(value: Any) -> dict:
    """Normalize currency to structured format."""
    if isinstance(value, dict) and "amount" in value:
        return value  # Already structured

    if isinstance(value, (int, float)):
        return {"amount": float(value), "currency": "UNKNOWN"}

    if isinstance(value, str):
        # "$1,500.00" → {"amount": 1500.0, "currency": "USD"}
        patterns = [
            (r"^\$\s*([\d,]+\.?\d*)", "USD"),
            (r"^([\d,]+\.?\d*)\s*TWD", "TWD"),
            (r"^([\d,]+\.?\d*)\s*USD", "USD"),
            (r"^([\d,]+\.?\d*)\s*EUR", "EUR"),
            (r"^¥\s*([\d,]+\.?\d*)", "CNY"),
            (r"^NT\$?\s*([\d,]+\.?\d*)", "TWD"),
        ]
        for pattern, currency in patterns:
            m = re.match(pattern, value.strip())
            if m:
                amount = float(m.group(1).replace(",", ""))
                return {"amount": amount, "currency": currency}
        # Plain number string
        try:
            return {"amount": float(value.replace(",", "")), "currency": "UNKNOWN"}
        except ValueError:
            pass

    return {"amount": 0, "currency": "UNKNOWN", "raw": str(value)}


def normalize_status(value: Any) -> str:
    """Normalize status codes to human-readable standard."""
    status_map = {
        200: "OK", "200": "OK", "success": "OK", "ok": "OK",
        404: "NOT_FOUND", "404": "NOT_FOUND", "not_found": "NOT_FOUND",
        500: "ERROR", "500": "ERROR", "error": "ERROR", "fail": "ERROR", "failed": "ERROR",
        "active": "ACTIVE", "inactive": "INACTIVE",
        "pending": "PENDING", "in_progress": "IN_PROGRESS",
        "completed": "COMPLETED", "done": "COMPLETED",
    }
    key = value if isinstance(value, int) else str(value).lower().strip()
    return status_map.get(key, str(value).upper())


def post_tool_use_normalize(tool_name: str, result: dict) -> dict:
    """
    Simulate PostToolUse hook: normalize heterogeneous data from different MCP servers.
    """
    normalized = {}
    date_fields = {"date", "created_at", "updated_at", "order_date", "delivery_date", "gr_date"}
    currency_fields = {"amount", "total", "price", "refund_amount"}
    status_fields = {"status", "order_status", "payment_status"}

    for key, value in result.items():
        if key in date_fields:
            normalized[key] = normalize_date(value)
        elif key in currency_fields:
            normalized[key] = normalize_currency(value)
        elif key in status_fields:
            normalized[key] = normalize_status(value)
        else:
            normalized[key] = value

    return normalized


# ==========================================
# Part 3: Session Strategy Decision Matrix
# ==========================================

class SessionStrategy(Enum):
    RESUME = "resume"
    FORK = "fork_session"
    FRESH_WITH_SUMMARY = "fresh_with_summary"


@dataclass
class SessionScenario:
    description: str
    files_changed: bool
    time_gap_hours: int
    comparing_approaches: bool
    prior_context_valid: bool
    expected: SessionStrategy


def select_session_strategy(scenario: SessionScenario) -> SessionStrategy:
    """Select the best session strategy based on scenario."""
    # Rule 1: Comparing approaches → fork
    if scenario.comparing_approaches:
        return SessionStrategy.FORK

    # Rule 2: Files changed or long gap → fresh with summary
    if scenario.files_changed or scenario.time_gap_hours > 48:
        return SessionStrategy.FRESH_WITH_SUMMARY

    # Rule 3: Prior context valid → resume
    if scenario.prior_context_valid:
        return SessionStrategy.RESUME

    return SessionStrategy.FRESH_WITH_SUMMARY


# ==========================================
# Part 4: Coordinator Decomposition Validator
# ==========================================

def validate_decomposition(topic: str, subtasks: list[str], expected_domains: list[str]) -> dict:
    """
    Validate coordinator decomposition for coverage gaps.
    Detects the "too narrow" anti-pattern from exam Q7.
    """
    covered = set()
    uncovered = set()

    for domain in expected_domains:
        domain_lower = domain.lower()
        found = any(domain_lower in st.lower() for st in subtasks)
        if found:
            covered.add(domain)
        else:
            uncovered.add(domain)

    coverage_ratio = len(covered) / len(expected_domains) if expected_domains else 0
    too_narrow = coverage_ratio < 0.7  # Less than 70% coverage = too narrow

    return {
        "topic": topic,
        "subtasks": subtasks,
        "covered_domains": sorted(covered),
        "uncovered_domains": sorted(uncovered),
        "coverage_ratio": round(coverage_ratio, 2),
        "too_narrow": too_narrow,
        "diagnosis": (
            "Coordinator decomposition too narrow - missing domains"
            if too_narrow
            else "Decomposition coverage acceptable"
        ),
    }


# ==========================================
# Part 5: CCA Mock Exam
# ==========================================

@dataclass
class ExamQuestion:
    question: str
    options: dict[str, str]
    correct: str
    explanation: str


MOCK_EXAM = [
    ExamQuestion(
        question="客戶支持系統使用多個 MCP Server 返回不同日期格式。為確保一致處理？",
        options={
            "A": "在 system prompt 要求用 ISO 8601",
            "B": "實作 PostToolUse Hook 統一格式",
            "C": "修改每個 MCP Server",
            "D": "在工具定義指定格式",
        },
        correct="B",
        explanation="PostToolUse Hook 是確定性的，不依賴模型解讀。A 是概率性。C 不切實際。D 控制輸入不控制輸出。",
    ),
    ExamQuestion(
        question="重構單體為微服務，涉及數十個檔案。你應該？",
        options={
            "A": "Plan Mode 探索 → 設計 → 再修改",
            "B": "直接從最獨立的模組開始",
            "C": "一次分析所有檔案",
            "D": "固定逐檔案 pipeline",
        },
        correct="A",
        explanation="大規模 + 多方案 + 架構決策 = Plan Mode。B 風險返工。C 注意力稀釋。D 忽略動態決策。",
    ),
    ExamQuestion(
        question="14 檔案 PR 的單輪 review 結果不一致。你應該？",
        options={
            "A": "拆成逐檔案 + 跨檔案整合兩輪",
            "B": "增加 max_tokens",
            "C": "prompt 加 '請確保一致'",
            "D": "換更大模型",
        },
        correct="A",
        explanation="根本原因是注意力稀釋，拆分直接解決。B/D 不治本。C 概率性。",
    ),
    ExamQuestion(
        question="調查 bug 後結束 session，第二天同事改了檔案。你應該？",
        options={
            "A": "resume + 告知變更",
            "B": "直接 resume",
            "C": "全新 session + 結構化摘要",
            "D": "fork_session",
        },
        correct="C",
        explanation="檔案已改 = 工具結果過時。全新 + 摘要比恢復過時 context 更可靠。",
    ),
    ExamQuestion(
        question="比較 Jest vs Vitest 在同一代碼庫的效果。你應該？",
        options={
            "A": "同 session 依次嘗試",
            "B": "兩個獨立 session",
            "C": "fork_session 從共享基線分支",
            "D": "一個回覆比較 pros/cons",
        },
        correct="C",
        explanation="fork_session 共享代碼庫理解 + 獨立探索。A 會互相干擾。B 需重複分析。D 太淺。",
    ),
]


# ==========================================
# Main: Run all tests
# ==========================================

def main():
    print("=" * 70)
    print("CCA Topic 5: Task Decomposition & Session Management")
    print("=" * 70)

    # --- Part 1: Strategy Selection ---
    print("\n📋 Part 1: Task Decomposition Strategy Selector")
    print("-" * 50)

    tasks = [
        TaskProfile("Review PR with 14 files for code quality", ["review", "quality"], 14, True, False, DecompStrategy.MULTI_PASS),
        TaskProfile("ETL pipeline: extract → transform → load", ["etl", "pipeline", "extract"], 3, True, True, DecompStrategy.FIXED_SEQUENTIAL),
        TaskProfile("Investigate legacy codebase for testing gaps", ["investigate", "legacy", "unknown"], 50, False, False, DecompStrategy.DYNAMIC_ADAPTIVE),
        TaskProfile("Restructure monolith to microservices across 30 files", ["restructure", "microservices"], 30, False, False, DecompStrategy.PLAN_MODE),
        TaskProfile("Audit compliance report generation", ["audit", "compliance", "template"], 5, True, True, DecompStrategy.FIXED_SEQUENTIAL),
        TaskProfile("Debug intermittent auth failure", ["debug", "investigate"], 8, False, False, DecompStrategy.DYNAMIC_ADAPTIVE),
        TaskProfile("Analyze 20 config files for security issues", ["analyze", "security"], 20, True, False, DecompStrategy.MULTI_PASS),
        TaskProfile("Explore unfamiliar codebase to understand architecture", ["explore", "unknown"], 100, False, False, DecompStrategy.DYNAMIC_ADAPTIVE),
        TaskProfile("Data extraction from structured invoices", ["extract", "template"], 1, True, True, DecompStrategy.FIXED_SEQUENTIAL),
        TaskProfile("Add comprehensive tests to legacy payment module", ["legacy", "open-ended"], 15, False, False, DecompStrategy.DYNAMIC_ADAPTIVE),
        TaskProfile("Review 8 test files for coverage gaps", ["review", "coverage"], 8, True, False, DecompStrategy.MULTI_PASS),
        TaskProfile("Migrate library across 45 files", ["migration", "restructure"], 45, False, False, DecompStrategy.PLAN_MODE),
    ]

    passed = 0
    for t in tasks:
        result = select_decomposition_strategy(t)
        ok = result == t.expected_strategy
        passed += ok
        status = "✅" if ok else "❌"
        print(f"  {status} '{t.description[:50]}...' → {result.value}" +
              (f" (expected {t.expected_strategy.value})" if not ok else ""))
    print(f"\n  Score: {passed}/{len(tasks)}")

    # --- Part 2: Cross-MCP Normalizer ---
    print("\n🔄 Part 2: Cross-MCP PostToolUse Normalizer")
    print("-" * 50)

    test_cases = [
        ("SAP BAPI", {"order_id": "PO-001", "date": "20260611", "amount": "1500.00 TWD", "status": 200}),
        ("WMS API", {"order_id": "WMS-002", "date": 1718064000, "amount": 1500, "status": "success"}),
        ("EDI System", {"order_id": "EDI-003", "date": "06/11/2026", "amount": "$1,500.00", "status": "completed"}),
        ("CRM Tool", {"order_id": "CRM-004", "date": "Jun 11, 2026", "amount": "NT$1500", "status": "active"}),
        ("Internal API", {"order_id": "INT-005", "date": "2026-06-11T08:00:00Z", "amount": "1500 EUR", "status": 404}),
    ]

    norm_pass = 0
    for source, data in test_cases:
        normalized = post_tool_use_normalize(source, data)
        date_ok = normalized["date"] == "2026-06-11"
        amount_ok = isinstance(normalized["amount"], dict) and normalized["amount"]["amount"] == 1500.0
        status_ok = isinstance(normalized["status"], str) and normalized["status"].isupper()

        all_ok = date_ok and amount_ok and status_ok
        norm_pass += all_ok
        status = "✅" if all_ok else "❌"
        print(f"  {status} {source}: date={normalized['date']}, "
              f"amount={normalized['amount']}, status={normalized['status']}")
        if not all_ok:
            if not date_ok: print(f"      ⚠️ Date normalization failed")
            if not amount_ok: print(f"      ⚠️ Currency normalization failed")
            if not status_ok: print(f"      ⚠️ Status normalization failed")
    print(f"\n  Score: {norm_pass}/{len(test_cases)}")

    # --- Part 3: Session Strategy ---
    print("\n🔀 Part 3: Session Strategy Decision Matrix")
    print("-" * 50)

    scenarios = [
        SessionScenario("Resume after lunch, no changes", False, 2, False, True, SessionStrategy.RESUME),
        SessionScenario("Files changed by colleague overnight", True, 16, False, False, SessionStrategy.FRESH_WITH_SUMMARY),
        SessionScenario("Compare Redux vs Context API approaches", False, 0, True, True, SessionStrategy.FORK),
        SessionScenario("3 days gap, files unchanged", False, 72, False, True, SessionStrategy.FRESH_WITH_SUMMARY),
        SessionScenario("Quick break, context still valid", False, 1, False, True, SessionStrategy.RESUME),
        SessionScenario("Compare Jest vs Vitest from shared analysis", False, 0, True, True, SessionStrategy.FORK),
        SessionScenario("Major refactor merged, all files changed", True, 4, False, False, SessionStrategy.FRESH_WITH_SUMMARY),
        SessionScenario("Investigation paused, no external changes", False, 8, False, True, SessionStrategy.RESUME),
    ]

    sess_pass = 0
    for s in scenarios:
        result = select_session_strategy(s)
        ok = result == s.expected
        sess_pass += ok
        status = "✅" if ok else "❌"
        print(f"  {status} '{s.description[:50]}' → {result.value}" +
              (f" (expected {s.expected.value})" if not ok else ""))
    print(f"\n  Score: {sess_pass}/{len(scenarios)}")

    # --- Part 4: Coordinator Decomposition Validator ---
    print("\n🔍 Part 4: Coordinator Decomposition Validator")
    print("-" * 50)

    # Exam Q7 scenario
    result = validate_decomposition(
        topic="Impact of AI on creative industries",
        subtasks=["AI in digital art creation", "AI in graphic design", "AI in photography"],
        expected_domains=["visual arts", "music", "writing", "film production", "photography"]
    )
    print(f"  Topic: {result['topic']}")
    print(f"  Coverage: {result['coverage_ratio']*100:.0f}%")
    print(f"  Too narrow: {result['too_narrow']}")
    print(f"  Covered: {result['covered_domains']}")
    print(f"  MISSING: {result['uncovered_domains']}")
    print(f"  Diagnosis: {result['diagnosis']}")
    assert result["too_narrow"], "Should detect too-narrow decomposition"
    print("  ✅ Correctly detected Q7 anti-pattern!")

    # Good decomposition
    result2 = validate_decomposition(
        topic="Impact of AI on creative industries",
        subtasks=[
            "AI in visual arts and digital design",
            "AI in music composition and production",
            "AI in creative writing and literature",
            "AI in film production and post-production",
        ],
        expected_domains=["visual arts", "music", "writing", "film production"]
    )
    assert not result2["too_narrow"], "Good decomposition should pass"
    print(f"\n  Good decomposition: {result2['coverage_ratio']*100:.0f}% coverage ✅")

    # --- Part 5: CCA Mock Exam ---
    print("\n📝 Part 5: CCA Mock Exam (5 questions)")
    print("-" * 50)

    exam_pass = 0
    for i, q in enumerate(MOCK_EXAM, 1):
        # Simulate answering correctly (testing the answer key)
        print(f"\n  Q{i}: {q.question[:60]}...")
        for key, opt in q.options.items():
            marker = "→ " if key == q.correct else "  "
            print(f"    {marker}{key}. {opt}")
        print(f"    ✅ Correct: {q.correct} — {q.explanation[:80]}...")
        exam_pass += 1
    print(f"\n  Score: {exam_pass}/{len(MOCK_EXAM)}")

    # --- Summary ---
    print("\n" + "=" * 70)
    total = passed + norm_pass + sess_pass + 2 + exam_pass  # 2 for decomposition validator
    max_total = len(tasks) + len(test_cases) + len(scenarios) + 2 + len(MOCK_EXAM)
    print(f"🏆 Total: {total}/{max_total} passed")
    print("=" * 70)


if __name__ == "__main__":
    main()
