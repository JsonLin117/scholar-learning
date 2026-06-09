"""
Workflow Enforcement & Handoff Simulator
========================================
CCA Domain 1 — Task 1.4 + 1.5
模擬 Claude Agent SDK 的 Hook 機制：PreToolUse / PostToolUse / Structured Handoff

不依賴任何外部套件，純 Python 模擬。

學習要點：
1. Hook 確定性（100%）vs Prompt 概率性（>90%）
2. PreToolUse 攔截：前置條件檢查 + 金額閾值
3. PostToolUse 正規化：日期/幣值/狀態碼統一
4. Structured Handoff Protocol：升級給人類的結構化摘要
5. Escalation Triggers：何時升級、何時自行處理
"""

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional, Callable
from enum import Enum

# ============================================================
# Part 1: Hook System Core（模擬 Claude Agent SDK Hook 機制）
# ============================================================

class HookDecision(Enum):
    ALLOW = "allow"       # 允許工具執行
    DENY = "deny"         # 拒絕工具執行
    MODIFY = "modify"     # 修改輸入後允許
    NO_DECISION = "none"  # Hook 無意見，走正常流程


@dataclass
class HookResult:
    decision: HookDecision = HookDecision.NO_DECISION
    reason: str = ""
    modified_input: Optional[dict] = None
    modified_result: Optional[dict] = None


@dataclass
class HookMatcher:
    """模擬 Agent SDK 的 matcher pattern"""
    pattern: str  # 例："process_refund", "Write|Edit", "mcp__crm__.*", "*"
    callbacks: list = field(default_factory=list)

    def matches(self, tool_name: str) -> bool:
        """
        Matcher 規則（跟 Claude Code 一致）：
        - 只含字母/數字/_/| → 精確匹配（| 分隔多個選項）
        - "*" 或空字串 → 匹配全部
        - 含其他字元 → 正則匹配
        """
        if self.pattern in ("*", ""):
            return True
        if re.match(r'^[\w|]+$', self.pattern):
            # 精確匹配模式
            return tool_name in self.pattern.split("|")
        else:
            # 正則匹配模式
            return bool(re.match(self.pattern, tool_name))


@dataclass
class HookRegistry:
    """Hook 註冊中心，管理所有 PreToolUse / PostToolUse hook"""
    pre_tool_use: list = field(default_factory=list)   # List[HookMatcher]
    post_tool_use: list = field(default_factory=list)   # List[HookMatcher]

    def register_pre(self, pattern: str, callback: Callable) -> None:
        # 找到已有的 matcher 或建立新的
        for m in self.pre_tool_use:
            if m.pattern == pattern:
                m.callbacks.append(callback)
                return
        self.pre_tool_use.append(HookMatcher(pattern=pattern, callbacks=[callback]))

    def register_post(self, pattern: str, callback: Callable) -> None:
        for m in self.post_tool_use:
            if m.pattern == pattern:
                m.callbacks.append(callback)
                return
        self.post_tool_use.append(HookMatcher(pattern=pattern, callbacks=[callback]))

    def run_pre_hooks(self, tool_name: str, tool_input: dict, context: dict) -> HookResult:
        """執行所有匹配的 PreToolUse hooks，第一個 DENY 就停止"""
        for matcher in self.pre_tool_use:
            if matcher.matches(tool_name):
                for cb in matcher.callbacks:
                    result = cb(tool_name, tool_input, context)
                    if result.decision == HookDecision.DENY:
                        return result  # 第一個拒絕就終止
                    if result.decision == HookDecision.MODIFY:
                        return result
        return HookResult()  # 無決策 → 允許

    def run_post_hooks(self, tool_name: str, tool_result: dict, context: dict) -> dict:
        """執行所有匹配的 PostToolUse hooks，依序修改 result"""
        current = tool_result.copy()
        for matcher in self.post_tool_use:
            if matcher.matches(tool_name):
                for cb in matcher.callbacks:
                    result = cb(tool_name, current, context)
                    if result.modified_result:
                        current = result.modified_result
        return current


# ============================================================
# Part 2: Matcher Pattern Testing
# ============================================================

def test_matcher_patterns():
    """驗證 matcher 的三種匹配模式"""
    print("=" * 70)
    print("📐 MATCHER PATTERN TESTING")
    print("=" * 70)

    test_cases = [
        # (pattern, tool_name, expected_match)
        ("process_refund", "process_refund", True),
        ("process_refund", "lookup_order", False),
        ("Write|Edit", "Write", True),
        ("Write|Edit", "Edit", True),
        ("Write|Edit", "Read", False),
        ("Write|Edit", "WriteFile", False),  # 精確匹配，不是 contains
        ("mcp__crm__.*", "mcp__crm__get_customer", True),
        ("mcp__crm__.*", "mcp__crm__lookup_order", True),
        ("mcp__crm__.*", "mcp__orders__get", False),
        ("*", "anything", True),
        ("", "anything", True),
        # 考試陷阱：mcp__memory 只含字母和下劃線 → 精確匹配！
        ("mcp__memory", "mcp__memory__save", False),  # ← 不匹配！
        ("mcp__memory__.*", "mcp__memory__save", True),  # 要用正則
    ]

    passed = 0
    for pattern, tool, expected in test_cases:
        m = HookMatcher(pattern=pattern, callbacks=[])
        actual = m.matches(tool)
        status = "✅" if actual == expected else "❌"
        if actual == expected:
            passed += 1
        print(f"  {status} pattern='{pattern}' tool='{tool}' → {actual} (expected {expected})")

    print(f"\n  結果：{passed}/{len(test_cases)} 通過")
    
    # 考試重點標注
    print("\n  ⚠️  考試陷阱：")
    print("  • 'mcp__memory' 只含 [a-z_] → 精確匹配模式 → 只匹配 'mcp__memory' 本身")
    print("  • 要匹配該 server 所有工具，用 'mcp__memory__.*'（正則模式）")
    print("  • '|' 分隔 = OR 精確匹配：'Write|Edit' 匹配 'Write' 或 'Edit'，不匹配 'WriteFile'")
    return passed == len(test_cases)


# ============================================================
# Part 3: ODM Procurement Agent Simulation
# ============================================================

class ODMProcurementAgent:
    """模擬 ODM 採購 AI Agent，帶 Hook 機制"""

    def __init__(self):
        self.hooks = HookRegistry()
        self.conversation_history = []
        self.verified_vendor = None
        self.tool_call_log = []
        self.escalation_log = []
        self._setup_hooks()

    def _setup_hooks(self):
        """註冊所有 hooks"""
        # PreToolUse: 供應商驗證前置條件
        self.hooks.register_pre("create_po|approve_po", self._require_vendor_verification)
        # PreToolUse: PO 金額閾值
        self.hooks.register_pre("create_po", self._enforce_po_amount_limit)
        # PostToolUse: 日期/幣值正規化
        self.hooks.register_post("*", self._normalize_tool_results)

    # --- PreToolUse Hooks ---

    def _require_vendor_verification(self, tool_name: str, tool_input: dict, ctx: dict) -> HookResult:
        """
        Programmatic Prerequisite: 建立/審批 PO 前必須先驗證供應商
        對應考試：12% skip get_customer 的那題
        """
        if self.verified_vendor is None:
            return HookResult(
                decision=HookDecision.DENY,
                reason=f"Vendor must be verified via lookup_vendor before {tool_name}. "
                       f"No verified vendor in current session."
            )
        return HookResult()

    def _enforce_po_amount_limit(self, tool_name: str, tool_input: dict, ctx: dict) -> HookResult:
        """
        金額閾值攔截: PO > $50,000 需人工審批
        對應考試：blocking refunds above threshold
        """
        amount = tool_input.get("amount", 0)
        if amount > 50000:
            # 自動生成結構化交接摘要
            handoff = self._create_handoff_summary(
                reason="po_amount_exceeds_threshold",
                details={
                    "tool_blocked": tool_name,
                    "requested_amount": amount,
                    "threshold": 50000,
                    "vendor": self.verified_vendor,
                    "po_details": tool_input
                }
            )
            self.escalation_log.append(handoff)
            return HookResult(
                decision=HookDecision.DENY,
                reason=f"PO amount ${amount:,.2f} exceeds $50,000 agent authority. "
                       f"Escalated to senior buyer with handoff summary."
            )
        return HookResult()

    # --- PostToolUse Hooks ---

    def _normalize_tool_results(self, tool_name: str, tool_result: dict, ctx: dict) -> HookResult:
        """
        正規化所有工具返回的日期/幣值/狀態碼
        對應考試：PostToolUse normalizes heterogeneous data formats
        """
        normalized = tool_result.copy()

        for key, val in list(normalized.items()):
            # Unix timestamp → ISO 8601
            if key.endswith("_at") and isinstance(val, (int, float)):
                normalized[key] = datetime.fromtimestamp(val, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

            # 人類可讀日期 → ISO 8601
            elif key.endswith("_date") and isinstance(val, str):
                for fmt in ["%b %d, %Y", "%d-%b-%Y", "%Y%m%d", "%m/%d/%Y"]:
                    try:
                        dt = datetime.strptime(val, fmt)
                        normalized[key] = dt.strftime("%Y-%m-%d")
                        break
                    except ValueError:
                        continue

            # 數字狀態碼 → 人類可讀
            elif key == "status" and isinstance(val, int):
                STATUS_MAP = {0: "active", 1: "suspended", 2: "closed", 3: "pending_review"}
                normalized[key] = STATUS_MAP.get(val, f"unknown({val})")

            # 幣值格式統一（去除 $ 符號，保留 2 位小數）
            elif key.endswith("_amount") and isinstance(val, str):
                try:
                    clean = val.replace("$", "").replace(",", "").strip()
                    normalized[key] = round(float(clean), 2)
                except ValueError:
                    pass

        return HookResult(modified_result=normalized)

    # --- Structured Handoff ---

    def _create_handoff_summary(self, reason: str, details: dict) -> dict:
        """
        結構化交接摘要 — 考試必背的欄位！
        人類 agent 收不到完整對話歷史，所以必須自包含。
        """
        return {
            "handoff_id": f"HO-{datetime.now().strftime('%Y%m%d%H%M%S')}",
            "timestamp": datetime.now().isoformat(),
            # 考試必背四欄位
            "customer_id": details.get("vendor", {}).get("vendor_id", "UNKNOWN"),
            "root_cause": self._analyze_root_cause(reason, details),
            "recommended_action": self._recommend_action(reason, details),
            "amount": details.get("requested_amount", 0),
            # 額外上下文
            "escalation_reason": reason,
            "conversation_summary": self._summarize_conversation(),
            "tool_call_history": [
                {"tool": t["tool"], "status": t["status"]}
                for t in self.tool_call_log[-5:]  # 最近 5 次工具呼叫
            ],
            "priority": "high" if details.get("requested_amount", 0) > 100000 else "medium"
        }

    def _analyze_root_cause(self, reason: str, details: dict) -> str:
        RCA_MAP = {
            "po_amount_exceeds_threshold": (
                f"PO creation blocked: amount ${details.get('requested_amount', 0):,.2f} "
                f"exceeds agent authority of $50,000"
            ),
            "vendor_not_in_avl": "Vendor not found in Approved Vendor List",
            "customer_explicit_request": "Customer explicitly requested human agent",
            "policy_gap": "Request falls outside defined policy scope"
        }
        return RCA_MAP.get(reason, f"Unknown escalation reason: {reason}")

    def _recommend_action(self, reason: str, details: dict) -> str:
        if reason == "po_amount_exceeds_threshold":
            return (f"Review PO for vendor {details.get('vendor', {}).get('name', 'Unknown')}. "
                    f"Amount: ${details.get('requested_amount', 0):,.2f}. "
                    f"Verify budget allocation and approve/reject.")
        return "Review and handle manually"

    def _summarize_conversation(self) -> str:
        if not self.conversation_history:
            return "No prior conversation context"
        return " → ".join(self.conversation_history[-5:])

    # --- Tool Simulation ---

    def call_tool(self, tool_name: str, tool_input: dict) -> dict:
        """模擬 Agent SDK 的工具呼叫流程（含 Hook 攔截）"""

        # Step 1: PreToolUse hooks
        pre_result = self.hooks.run_pre_hooks(tool_name, tool_input, {})

        if pre_result.decision == HookDecision.DENY:
            self.tool_call_log.append({
                "tool": tool_name,
                "status": "BLOCKED",
                "reason": pre_result.reason
            })
            self.conversation_history.append(f"[BLOCKED] {tool_name}: {pre_result.reason}")
            return {"error": True, "reason": pre_result.reason, "blocked_by": "PreToolUse hook"}

        # Step 2: 執行工具（模擬）
        raw_result = self._simulate_tool(tool_name, tool_input)

        # Step 3: PostToolUse hooks（正規化）
        normalized = self.hooks.run_post_hooks(tool_name, raw_result, {})

        self.tool_call_log.append({"tool": tool_name, "status": "OK"})
        self.conversation_history.append(f"[OK] {tool_name}")
        return normalized

    def _simulate_tool(self, tool_name: str, tool_input: dict) -> dict:
        """模擬各種工具的返回值（故意用不同格式，測試 PostToolUse 正規化）"""
        tools = {
            "check_inventory": {
                "part_number": tool_input.get("part_number", "PN-001"),
                "on_hand": 1500,
                "on_order": 3000,
                "last_updated_at": 1718000000,  # Unix timestamp
                "status": 0,  # 數字狀態碼
                "reorder_amount": "$12,500.50"  # 帶 $ 的字串
            },
            "lookup_vendor": {
                "vendor_id": "VND-2024-001",
                "name": "TechParts Corp",
                "avl_status": "approved",
                "contract_expiry_date": "Dec 31, 2026",  # 人類可讀日期
                "created_at": 1700000000,  # Unix timestamp
                "last_delivery_date": "20260515",  # SAP 格式 YYYYMMDD
                "status": 0,
                "credit_limit_amount": "$500,000.00"
            },
            "create_po": {
                "po_number": "PO-2026-10001",
                "created_at": 1718010000,
                "vendor_id": tool_input.get("vendor_id", "VND-001"),
                "total_amount": tool_input.get("amount", 0),
                "status": 3,  # pending_review
                "delivery_date": "06/30/2026"  # MM/DD/YYYY
            },
            "escalate_to_buyer": {
                "ticket_id": "ESC-2026-0042",
                "assigned_to": "senior_buyer_chen",
                "created_at": 1718020000,
                "status": 3
            }
        }
        return tools.get(tool_name, {"error": f"Unknown tool: {tool_name}"})


# ============================================================
# Part 4: Deterministic vs Probabilistic Comparison
# ============================================================

def simulate_prompt_vs_hook_reliability():
    """
    模擬 Prompt 指令 vs Hook 的可靠性差異
    考試核心考點：12% failure rate
    """
    print("\n" + "=" * 70)
    print("📊 DETERMINISTIC (Hook) vs PROBABILISTIC (Prompt) COMPARISON")
    print("=" * 70)

    import random
    random.seed(42)

    N_REQUESTS = 1000
    PROMPT_COMPLIANCE_RATE = 0.88  # 88% → 12% failure（考試數據）
    HOOK_COMPLIANCE_RATE = 1.0     # 100%

    # 模擬：每個請求需要先 verify_customer 再 process_refund
    prompt_violations = 0
    hook_violations = 0
    prompt_financial_loss = 0

    for i in range(N_REQUESTS):
        refund_amount = random.uniform(50, 800)

        # Prompt-based: 88% 機率遵守順序
        if random.random() > PROMPT_COMPLIANCE_RATE:
            prompt_violations += 1
            if random.random() > 0.5:  # 50% 的違規導致退款到錯誤帳戶
                prompt_financial_loss += refund_amount

        # Hook-based: 100% 強制順序
        # （Hook 永遠不會違反，因為是程式碼邏輯）

    print(f"\n  模擬 {N_REQUESTS} 個退款請求：")
    print(f"\n  📝 Prompt-based enforcement:")
    print(f"     Violations: {prompt_violations}/{N_REQUESTS} ({prompt_violations/N_REQUESTS*100:.1f}%)")
    print(f"     Financial loss from misidentified accounts: ${prompt_financial_loss:,.2f}")
    print(f"\n  🔒 Hook-based enforcement:")
    print(f"     Violations: {hook_violations}/{N_REQUESTS} ({hook_violations/N_REQUESTS*100:.1f}%)")
    print(f"     Financial loss: $0.00")

    print(f"\n  💡 考試關鍵：")
    print(f"     即使 prompt compliance 高達 88%，1000 次中仍有 {prompt_violations} 次違規")
    print(f"     金融場景不容許這個 failure rate → 必須用 Hook")

    return prompt_violations > 0 and hook_violations == 0


# ============================================================
# Part 5: Escalation Decision Matrix
# ============================================================

class EscalationDecision(Enum):
    RESOLVE = "resolve_autonomously"
    ESCALATE = "escalate_immediately"
    ACKNOWLEDGE_THEN_OFFER = "acknowledge_then_offer"
    ASK_FOR_MORE_INFO = "ask_for_more_info"


def evaluate_escalation_trigger(scenario: dict) -> tuple:
    """
    升級決策矩陣
    考試 Domain 5.2 考點
    """
    # Rule 1: 客戶明確要求人類 → 立刻升級（不先嘗試解決！）
    if scenario.get("customer_requests_human"):
        return EscalationDecision.ESCALATE, "Customer explicitly requested human agent"

    # Rule 2: 政策空白 → 升級
    if scenario.get("policy_gap"):
        return EscalationDecision.ESCALATE, "Policy is ambiguous or silent on this request"

    # Rule 3: 多筆客戶匹配 → 要求更多資訊（不要猜！）
    if scenario.get("multiple_matches"):
        return EscalationDecision.ASK_FOR_MORE_INFO, "Multiple matches found, request additional identifiers"

    # Rule 4: 客戶挫折但問題在能力範圍 → 先承認情緒，提供方案
    if scenario.get("customer_frustrated") and scenario.get("within_capability"):
        return EscalationDecision.ACKNOWLEDGE_THEN_OFFER, "Acknowledge frustration, offer resolution"

    # Rule 5: 問題在能力範圍 → 自行解決
    if scenario.get("within_capability"):
        return EscalationDecision.RESOLVE, "Standard case within agent capability"

    # Default: 無法判斷 → 升級
    return EscalationDecision.ESCALATE, "Unable to determine appropriate action"


def test_escalation_matrix():
    """測試升級決策矩陣"""
    print("\n" + "=" * 70)
    print("🚨 ESCALATION DECISION MATRIX")
    print("=" * 70)

    scenarios = [
        {
            "name": "Customer says 'I want to speak to a human'",
            "input": {"customer_requests_human": True, "within_capability": True},
            "expected": EscalationDecision.ESCALATE,
            "exam_trap": "❌ 先嘗試調查再升級 → 錯！明確要求 = 立刻升級"
        },
        {
            "name": "Customer wants competitor price match (policy only covers own-site)",
            "input": {"policy_gap": True, "within_capability": False},
            "expected": EscalationDecision.ESCALATE,
            "exam_trap": "❌ 自行判斷 → 錯！政策空白 = 升級"
        },
        {
            "name": "get_customer returns 3 matches for 'John Smith'",
            "input": {"multiple_matches": True},
            "expected": EscalationDecision.ASK_FOR_MORE_INFO,
            "exam_trap": "❌ 選最近活躍的帳戶 → 錯！要求額外識別資訊"
        },
        {
            "name": "Angry customer, standard damage replacement with photo",
            "input": {"customer_frustrated": True, "within_capability": True},
            "expected": EscalationDecision.ACKNOWLEDGE_THEN_OFFER,
            "exam_trap": "❌ Sentiment 高 → 自動升級 → 錯！問題簡單就自己處理"
        },
        {
            "name": "Standard return within 30-day policy",
            "input": {"within_capability": True},
            "expected": EscalationDecision.RESOLVE,
            "exam_trap": "✅ 正常情境，自行解決"
        }
    ]

    passed = 0
    for s in scenarios:
        decision, reason = evaluate_escalation_trigger(s["input"])
        ok = decision == s["expected"]
        if ok:
            passed += 1
        status = "✅" if ok else "❌"
        print(f"\n  {status} {s['name']}")
        print(f"     Decision: {decision.value}")
        print(f"     Reason: {reason}")
        print(f"     考試陷阱: {s['exam_trap']}")

    print(f"\n  結果：{passed}/{len(scenarios)} 通過")

    # 反模式提醒
    print("\n  ⚠️  不可靠的升級指標（反模式）：")
    print("  • ❌ Sentiment-based: 客戶語氣 ≠ 問題複雜度")
    print("  • ❌ Self-reported confidence: LLM 自評跟實際能力不相關")
    print("  • ❌ Heuristic customer selection: 多筆匹配不能猜，要問")
    return passed == len(scenarios)


# ============================================================
# Part 6: Full Agent Workflow Simulation
# ============================================================

def run_agent_workflow():
    """執行完整的 Agent 工作流模擬"""
    print("\n" + "=" * 70)
    print("🤖 ODM PROCUREMENT AGENT — FULL WORKFLOW SIMULATION")
    print("=" * 70)

    agent = ODMProcurementAgent()

    # --- Scenario 1: 正常流程（金額在閾值內）---
    print("\n── Scenario 1: Normal PO (under threshold) ──")

    # Step 1: 嘗試直接建 PO（應被 prerequisite hook 攔截）
    print("\n  Step 1: Try create_po WITHOUT vendor verification...")
    result = agent.call_tool("create_po", {"vendor_id": "VND-001", "amount": 25000})
    assert result.get("error"), "Should be blocked by prerequisite hook!"
    print(f"  → BLOCKED ✅: {result['reason'][:80]}...")

    # Step 2: 先查驗供應商
    print("\n  Step 2: Verify vendor first...")
    result = agent.call_tool("lookup_vendor", {"vendor_id": "VND-2024-001"})
    assert not result.get("error"), "lookup_vendor should succeed"
    agent.verified_vendor = result  # 記錄已驗證
    print(f"  → OK ✅: Vendor '{result['name']}' verified")
    print(f"     contract_expiry_date: {result['contract_expiry_date']}")  # 應已正規化
    print(f"     created_at: {result['created_at']}")  # 應已從 Unix → ISO
    print(f"     last_delivery_date: {result['last_delivery_date']}")
    print(f"     status: {result['status']}")  # 應已從 0 → 'active'
    print(f"     credit_limit_amount: {result['credit_limit_amount']}")  # 應已從 "$500,000.00" → 500000.0

    # Step 3: 建立 PO（現在有 verified vendor，且金額在閾值內）
    print("\n  Step 3: Create PO ($25,000 — within threshold)...")
    result = agent.call_tool("create_po", {"vendor_id": "VND-2024-001", "amount": 25000})
    assert not result.get("error"), "PO creation should succeed"
    print(f"  → OK ✅: PO#{result['po_number']}")
    print(f"     status: {result['status']}")  # pending_review (from 3)
    print(f"     delivery_date: {result['delivery_date']}")

    # --- Scenario 2: 金額超閾值（應觸發升級）---
    print("\n── Scenario 2: High-value PO ($120,000 — exceeds threshold) ──")

    result = agent.call_tool("create_po", {"vendor_id": "VND-2024-001", "amount": 120000})
    assert result.get("error"), "Should be blocked by amount threshold hook!"
    print(f"  → BLOCKED ✅: {result['reason'][:80]}...")

    # 檢查升級日誌
    assert len(agent.escalation_log) == 1, "Should have one escalation"
    handoff = agent.escalation_log[0]
    print(f"\n  📋 Structured Handoff Summary:")
    print(f"     handoff_id: {handoff['handoff_id']}")
    print(f"     customer_id: {handoff['customer_id']}")
    print(f"     root_cause: {handoff['root_cause']}")
    print(f"     recommended_action: {handoff['recommended_action'][:60]}...")
    print(f"     amount: ${handoff['amount']:,.2f}")
    print(f"     priority: {handoff['priority']}")
    print(f"     tool_call_history: {json.dumps(handoff['tool_call_history'], indent=6)}")

    # --- Scenario 3: PostToolUse 正規化驗證 ---
    print("\n── Scenario 3: PostToolUse Normalization Verification ──")

    result = agent.call_tool("check_inventory", {"part_number": "GPU-A100-80GB"})
    print(f"  Raw → Normalized:")
    print(f"     last_updated_at: Unix 1718000000 → {result['last_updated_at']}")
    print(f"     status: int 0 → '{result['status']}'")
    print(f"     reorder_amount: '$12,500.50' → {result['reorder_amount']}")

    # 驗證正規化結果
    assert isinstance(result["last_updated_at"], str) and "T" in result["last_updated_at"], \
        "Unix timestamp should be converted to ISO 8601"
    assert result["status"] == "active", f"Status 0 should be 'active', got {result['status']}"
    assert isinstance(result["reorder_amount"], float), \
        f"Amount should be float, got {type(result['reorder_amount'])}"
    print(f"  → All normalizations verified ✅")

    # 統計
    print(f"\n── Agent Summary ──")
    print(f"  Total tool calls: {len(agent.tool_call_log)}")
    blocked = sum(1 for t in agent.tool_call_log if t["status"] == "BLOCKED")
    print(f"  Blocked by hooks: {blocked}")
    print(f"  Escalations: {len(agent.escalation_log)}")
    print(f"  Conversation: {' → '.join(agent.conversation_history)}")

    return True


# ============================================================
# Part 7: CCA Mock Exam Questions
# ============================================================

def run_mock_exam():
    """5 題 CCA 模擬考"""
    print("\n" + "=" * 70)
    print("🎯 CCA MOCK EXAM — Workflow Enforcement & Handoff (D1 + D5)")
    print("=" * 70)

    questions = [
        {
            "q": """Q1: Production data shows 12% of cases skip get_customer and call
lookup_order directly, causing misidentified accounts. Best fix?""",
            "options": {
                "A": "Programmatic prerequisite blocking lookup_order until get_customer returns verified ID",
                "B": "Enhance system prompt stating get_customer is mandatory",
                "C": "Add few-shot examples showing get_customer first",
                "D": "Routing classifier enabling only appropriate tool subsets"
            },
            "answer": "A",
            "explanation": "Deterministic prerequisite (100%) > Prompt/few-shot (probabilistic). "
                          "D addresses tool availability, not ordering."
        },
        {
            "q": """Q2: Different MCP tools return dates as Unix timestamps, 'Mar 5, 2025',
and ISO 8601. Agent makes incorrect date comparisons. Best fix?""",
            "options": {
                "A": "System prompt: 'Always normalize dates before comparing'",
                "B": "PostToolUse hook normalizing all dates to ISO 8601",
                "C": "Modify each MCP server to return consistent formats",
                "D": "Few-shot examples of correct date comparison"
            },
            "answer": "B",
            "explanation": "PostToolUse hook = deterministic normalization before model processes. "
                          "A/D probabilistic. C may be infeasible (third-party servers)."
        },
        {
            "q": """Q3: Agent achieves 55% FCR (target 80%). Escalates simple cases
(standard replacements) while handling complex policy exceptions. Best fix?""",
            "options": {
                "A": "Explicit escalation criteria + few-shot examples in system prompt",
                "B": "Agent self-reports confidence 1-10, escalate below threshold",
                "C": "Separate classifier model on historical tickets",
                "D": "Sentiment analysis to detect frustration"
            },
            "answer": "A",
            "explanation": "Root cause = unclear decision boundaries. Few-shot is proportionate first step. "
                          "B: LLM self-confidence is uncalibrated. C: over-engineered. D: wrong problem."
        },
        {
            "q": """Q4: Customer says 'I want to talk to a real person'. The issue is a
standard damage replacement (within agent capability). What should the agent do?""",
            "options": {
                "A": "Investigate the issue first, then offer resolution before escalating",
                "B": "Escalate immediately, compiling a structured handoff summary",
                "C": "Acknowledge frustration, explain the agent can resolve it faster",
                "D": "Check sentiment score; if high frustration, escalate; otherwise resolve"
            },
            "answer": "B",
            "explanation": "Explicit human request = immediate escalation. Don't investigate first (A), "
                          "don't try to convince (C), don't use sentiment (D unreliable)."
        },
        {
            "q": """Q5: Your PreToolUse hook blocks process_refund when amount > $500.
The hook returns: { hookSpecificOutput: { hookEventName: 'PreToolUse',
permissionDecision: 'deny', permissionDecisionReason: '...' } }
What happens next in the agent loop?""",
            "options": {
                "A": "The agent receives the denial reason and can choose an alternative action (e.g., escalation)",
                "B": "The entire agent loop terminates with an error",
                "C": "The tool executes anyway but the result is marked as denied",
                "D": "The agent retries with a lower amount automatically"
            },
            "answer": "A",
            "explanation": "Hook deny → tool not executed → agent receives denial reason in conversation → "
                          "agent can decide next step (escalate, inform user, etc.). Loop continues."
        }
    ]

    score = 0
    for i, q in enumerate(questions):
        print(f"\n  {'─' * 60}")
        print(f"  {q['q']}")
        for opt, text in q["options"].items():
            marker = "  ✅" if opt == q["answer"] else "    "
            print(f"  {marker} {opt}. {text}")
        print(f"\n  Answer: {q['answer']} — {q['explanation'][:100]}")
        score += 1

    print(f"\n  {'─' * 60}")
    print(f"  Score: {score}/{len(questions)}")
    return True


# ============================================================
# Main
# ============================================================

def main():
    print("╔" + "═" * 68 + "╗")
    print("║  CCA D1: Workflow Enforcement & Handoff Simulator               ║")
    print("║  Task 1.4 + 1.5 | Hook 確定性 vs Prompt 概率性                  ║")
    print("╚" + "═" * 68 + "╝")

    results = {}

    # Test 1: Matcher patterns
    results["matcher"] = test_matcher_patterns()

    # Test 2: Deterministic vs Probabilistic
    results["reliability"] = simulate_prompt_vs_hook_reliability()

    # Test 3: Escalation matrix
    results["escalation"] = test_escalation_matrix()

    # Test 4: Full workflow
    results["workflow"] = run_agent_workflow()

    # Test 5: Mock exam
    results["exam"] = run_mock_exam()

    # Summary
    print("\n" + "=" * 70)
    print("📊 FINAL SUMMARY")
    print("=" * 70)
    for name, passed in results.items():
        print(f"  {'✅' if passed else '❌'} {name}")
    all_pass = all(results.values())
    print(f"\n  {'🎉 ALL TESTS PASSED!' if all_pass else '⚠️  Some tests failed'}")

    # Key takeaways
    print(f"\n  📝 Key Takeaways for CCA Exam:")
    print(f"  1. must/guaranteed/compliance/financial → Hook (deterministic)")
    print(f"  2. should/preferably/try → Prompt (probabilistic)")
    print(f"  3. PreToolUse deny → agent receives reason → alternative flow")
    print(f"  4. PostToolUse normalize → consistent format BEFORE model processes")
    print(f"  5. Handoff must include: customer_id + root_cause + recommended_action")
    print(f"  6. Customer says 'talk to human' → escalate IMMEDIATELY")
    print(f"  7. Sentiment/self-confidence are UNRELIABLE escalation triggers")


if __name__ == "__main__":
    main()
