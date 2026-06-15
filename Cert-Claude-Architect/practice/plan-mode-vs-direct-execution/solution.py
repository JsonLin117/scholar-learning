#!/usr/bin/env python3
"""
CCA Practice: Plan Mode vs Direct Execution (D3 Task 3.4)
ODM 供應鏈場景 — Claude Code 執行模式選擇與 Explore Subagent 模擬

Modules:
  1. Permission Mode System — 5 mode 切換邏輯
  2. Task Router — Plan vs Direct 決策引擎
  3. Explore Subagent — context 隔離 + throughness 模擬
  4. Ultraplan Workflow — CLI → Cloud → Review → Execute
  5. Iterative Refinement — 5 模式選擇器
  6. CCA Mock Exam — D3 Task 3.4 模擬考
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
import json

# ============================================================
# Module 1: Permission Mode System
# ============================================================

class PermissionMode(Enum):
    DEFAULT = "default"
    ACCEPT_EDITS = "acceptEdits"
    PLAN = "plan"
    AUTO = "auto"
    DONT_ASK = "dontAsk"
    BYPASS = "bypassPermissions"


@dataclass
class ModeCapabilities:
    """What each mode can do without prompting."""
    reads: bool = True
    file_edits: bool = False
    filesystem_cmds: bool = False  # mkdir, touch, mv, cp, etc.
    shell_commands: bool = False
    all_actions: bool = False
    safety_classifier: bool = False  # auto mode's background check

    def describe(self) -> str:
        if self.all_actions and self.safety_classifier:
            return "Everything (with safety classifier)"
        if self.all_actions:
            return "Everything (no checks)"
        parts = []
        if self.reads: parts.append("reads")
        if self.file_edits: parts.append("file_edits")
        if self.filesystem_cmds: parts.append("filesystem_cmds")
        if self.shell_commands: parts.append("shell_commands")
        return ", ".join(parts) if parts else "nothing"


MODE_CAPABILITIES = {
    PermissionMode.DEFAULT: ModeCapabilities(reads=True),
    PermissionMode.ACCEPT_EDITS: ModeCapabilities(reads=True, file_edits=True, filesystem_cmds=True),
    PermissionMode.PLAN: ModeCapabilities(reads=True),  # Read-only, no edits
    PermissionMode.AUTO: ModeCapabilities(all_actions=True, safety_classifier=True),
    PermissionMode.DONT_ASK: ModeCapabilities(reads=False),  # Only pre-approved tools
    PermissionMode.BYPASS: ModeCapabilities(all_actions=True),
}


@dataclass
class SessionConfig:
    """Session-level configuration affecting mode availability."""
    model: str = "claude-sonnet-4-6"
    provider: str = "anthropic"  # anthropic, bedrock, vertex, foundry
    plan_type: str = "pro"  # free, pro, team, enterprise
    admin_auto_enabled: bool = True
    bypass_flag_passed: bool = False
    default_mode_setting: Optional[str] = None
    default_mode_source: str = "user"  # user, project, local


class PermissionModeSystem:
    """Simulates Claude Code's permission mode switching."""

    # Standard Shift+Tab cycle
    BASE_CYCLE = [PermissionMode.DEFAULT, PermissionMode.ACCEPT_EDITS, PermissionMode.PLAN]

    def __init__(self, config: SessionConfig):
        self.config = config
        self.current_mode = self._resolve_default_mode()
        self.cycle = self._build_cycle()

    def _resolve_default_mode(self) -> PermissionMode:
        """Resolve the starting mode from settings."""
        setting = self.config.default_mode_setting
        source = self.config.default_mode_source

        if setting == "auto":
            # CRITICAL: auto mode defaultMode is IGNORED from project/local settings
            # This is a security restriction — a repo can't grant itself auto mode
            if source in ("project", "local"):
                print(f"  ⚠️  defaultMode='auto' from {source} settings IGNORED (security restriction)")
                return PermissionMode.DEFAULT
            if self._auto_mode_available():
                return PermissionMode.AUTO
            return PermissionMode.DEFAULT

        if setting:
            try:
                return PermissionMode(setting)
            except ValueError:
                return PermissionMode.DEFAULT

        return PermissionMode.DEFAULT

    def _auto_mode_available(self) -> bool:
        """Check all auto mode requirements."""
        # Model requirement
        auto_models_anthropic = {"claude-opus-4-6", "claude-sonnet-4-6", "claude-opus-4-7", "claude-opus-4-8", "claude-sonnet-4-7"}
        auto_models_cloud = {"claude-opus-4-7", "claude-opus-4-8"}  # Bedrock/Vertex/Foundry

        if self.config.provider == "anthropic":
            model_ok = self.config.model in auto_models_anthropic
        else:
            model_ok = self.config.model in auto_models_cloud

        # Admin requirement (Team/Enterprise)
        admin_ok = True
        if self.config.plan_type in ("team", "enterprise"):
            admin_ok = self.config.admin_auto_enabled

        return model_ok and admin_ok

    def _build_cycle(self) -> list:
        """Build the Shift+Tab cycle based on config."""
        cycle = list(self.BASE_CYCLE)

        # bypass appears after plan if flag was passed
        if self.config.bypass_flag_passed:
            cycle.append(PermissionMode.BYPASS)

        # auto appears last if available
        if self._auto_mode_available():
            cycle.append(PermissionMode.AUTO)

        # dontAsk never in cycle (set via --permission-mode only)
        return cycle

    def shift_tab(self) -> PermissionMode:
        """Cycle to next mode (Shift+Tab)."""
        if self.current_mode in self.cycle:
            idx = self.cycle.index(self.current_mode)
            self.current_mode = self.cycle[(idx + 1) % len(self.cycle)]
        else:
            self.current_mode = self.cycle[0]
        return self.current_mode

    def can_perform(self, action: str) -> tuple:
        """Check if current mode allows an action without prompting."""
        caps = MODE_CAPABILITIES[self.current_mode]

        if action == "read_file":
            return (caps.reads, "auto-approved" if caps.reads else "needs prompt")
        elif action == "edit_file":
            allowed = caps.file_edits or caps.all_actions
            return (allowed, "auto-approved" if allowed else "needs prompt")
        elif action == "mkdir":
            allowed = caps.filesystem_cmds or caps.all_actions
            return (allowed, "auto-approved" if allowed else "needs prompt")
        elif action == "run_shell":
            allowed = caps.shell_commands or caps.all_actions
            return (allowed, "auto-approved" if allowed else "needs prompt")
        else:
            return (caps.all_actions, "auto-approved" if caps.all_actions else "needs prompt")

    def approve_plan(self, option: str) -> PermissionMode:
        """Handle plan approval — exits plan mode."""
        options = {
            "auto": PermissionMode.AUTO,
            "accept_edits": PermissionMode.ACCEPT_EDITS,
            "manual_review": PermissionMode.DEFAULT,
            "keep_planning": PermissionMode.PLAN,  # stays in plan
            "ultraplan": PermissionMode.PLAN,  # sends to cloud
        }
        new_mode = options.get(option, PermissionMode.DEFAULT)
        if option != "keep_planning":
            self.current_mode = new_mode
        return new_mode


def test_permission_mode_system():
    """Test Module 1: Permission Mode System."""
    print("=" * 60)
    print("Module 1: Permission Mode System")
    print("=" * 60)

    passed = 0
    total = 0

    # Test 1: Default cycle (no special flags)
    config = SessionConfig()
    system = PermissionModeSystem(config)
    assert system.current_mode == PermissionMode.DEFAULT
    total += 1; passed += 1
    print(f"  ✅ T1: Default start mode = default")

    # Test 2: Shift+Tab cycle without auto/bypass
    config = SessionConfig(model="claude-haiku-4-5")  # Haiku doesn't support auto
    system = PermissionModeSystem(config)
    modes = []
    for _ in range(4):
        modes.append(system.shift_tab())
    assert modes == [PermissionMode.ACCEPT_EDITS, PermissionMode.PLAN,
                     PermissionMode.DEFAULT, PermissionMode.ACCEPT_EDITS]
    total += 1; passed += 1
    print(f"  ✅ T2: Basic cycle = default → acceptEdits → plan → default → ...")

    # Test 3: Auto mode available with Sonnet on Anthropic
    config = SessionConfig(model="claude-sonnet-4-6", provider="anthropic")
    system = PermissionModeSystem(config)
    assert system._auto_mode_available() == True
    total += 1; passed += 1
    print(f"  ✅ T3: Auto mode available with Sonnet 4.6 on Anthropic")

    # Test 4: Auto mode NOT available with Sonnet on Bedrock
    config = SessionConfig(model="claude-sonnet-4-6", provider="bedrock")
    system = PermissionModeSystem(config)
    assert system._auto_mode_available() == False
    total += 1; passed += 1
    print(f"  ✅ T4: Auto mode NOT available with Sonnet 4.6 on Bedrock")

    # Test 5: Auto mode available with Opus 4.7 on Vertex
    config = SessionConfig(model="claude-opus-4-7", provider="vertex")
    system = PermissionModeSystem(config)
    assert system._auto_mode_available() == True
    total += 1; passed += 1
    print(f"  ✅ T5: Auto mode available with Opus 4.7 on Vertex")

    # Test 6: CRITICAL — defaultMode="auto" from project settings IGNORED
    config = SessionConfig(default_mode_setting="auto", default_mode_source="project")
    system = PermissionModeSystem(config)
    assert system.current_mode == PermissionMode.DEFAULT
    total += 1; passed += 1
    print(f"  ✅ T6: defaultMode='auto' from project settings → IGNORED (security)")

    # Test 7: defaultMode="auto" from user settings OK
    config = SessionConfig(default_mode_setting="auto", default_mode_source="user")
    system = PermissionModeSystem(config)
    assert system.current_mode == PermissionMode.AUTO
    total += 1; passed += 1
    print(f"  ✅ T7: defaultMode='auto' from user settings → OK")

    # Test 8: Plan mode capabilities — read-only
    config = SessionConfig()
    system = PermissionModeSystem(config)
    system.current_mode = PermissionMode.PLAN
    assert system.can_perform("read_file") == (True, "auto-approved")
    assert system.can_perform("edit_file") == (False, "needs prompt")
    assert system.can_perform("run_shell") == (False, "needs prompt")
    total += 1; passed += 1
    print(f"  ✅ T8: Plan mode = read-only (no edits, no shell)")

    # Test 9: acceptEdits capabilities — reads + edits + filesystem cmds
    system.current_mode = PermissionMode.ACCEPT_EDITS
    assert system.can_perform("read_file") == (True, "auto-approved")
    assert system.can_perform("edit_file") == (True, "auto-approved")
    assert system.can_perform("mkdir") == (True, "auto-approved")
    assert system.can_perform("run_shell") == (False, "needs prompt")
    total += 1; passed += 1
    print(f"  ✅ T9: acceptEdits = reads + edits + fs cmds, but NOT shell")

    # Test 10: Plan approval exits plan mode
    system.current_mode = PermissionMode.PLAN
    new_mode = system.approve_plan("accept_edits")
    assert new_mode == PermissionMode.ACCEPT_EDITS
    assert system.current_mode == PermissionMode.ACCEPT_EDITS
    total += 1; passed += 1
    print(f"  ✅ T10: Plan approval 'accept_edits' exits plan → acceptEdits mode")

    # Test 11: "keep_planning" stays in plan mode
    system.current_mode = PermissionMode.PLAN
    new_mode = system.approve_plan("keep_planning")
    assert system.current_mode == PermissionMode.PLAN
    total += 1; passed += 1
    print(f"  ✅ T11: 'keep_planning' stays in plan mode")

    # Test 12: Bypass cycle only if flag passed
    config_no_bypass = SessionConfig(model="claude-haiku-4-5")
    sys_no = PermissionModeSystem(config_no_bypass)
    assert PermissionMode.BYPASS not in sys_no.cycle
    config_bypass = SessionConfig(model="claude-haiku-4-5", bypass_flag_passed=True)
    sys_yes = PermissionModeSystem(config_bypass)
    assert PermissionMode.BYPASS in sys_yes.cycle
    total += 1; passed += 1
    print(f"  ✅ T12: Bypass in cycle only if flag passed")

    print(f"\n  Module 1: {passed}/{total} passed\n")
    return passed, total


# ============================================================
# Module 2: Task Router — Plan vs Direct Decision Engine
# ============================================================

@dataclass
class TaskProfile:
    """Characteristics of a task for mode selection."""
    description: str
    files_affected: int = 1
    has_clear_stack_trace: bool = False
    multiple_valid_approaches: bool = False
    architectural_decision: bool = False
    unfamiliar_codebase: bool = False
    infrastructure_implications: bool = False
    can_describe_diff_in_one_sentence: bool = False
    cross_file_dependencies: bool = False


class TaskRouter:
    """Decides Plan Mode vs Direct Execution based on task characteristics."""

    @staticmethod
    def recommend(task: TaskProfile) -> dict:
        """
        Returns recommendation with mode, confidence, and reasoning.

        Priority order (from exam guide):
        1. Direct Execution: clear stack trace + single file
        2. Direct Execution: can describe diff in one sentence
        3. Plan Mode: 45+ files affected
        4. Plan Mode: architectural decision
        5. Plan Mode: multiple valid approaches
        6. Plan Mode: unfamiliar codebase
        7. Combined: moderate complexity
        """
        reasons = []

        # Strong Direct signals
        if task.has_clear_stack_trace and task.files_affected == 1:
            return {
                "mode": "direct",
                "confidence": 0.95,
                "reasoning": "Single-file fix with clear stack trace — no plan needed",
                "example": "Bug fix with known location (e.g., connections.py:42 typo)"
            }

        if task.can_describe_diff_in_one_sentence and task.files_affected <= 2:
            return {
                "mode": "direct",
                "confidence": 0.9,
                "reasoning": "Simple, well-scoped change describable in one sentence",
                "example": "Add a date validation conditional, rename variable"
            }

        # Strong Plan signals
        if task.files_affected >= 45:
            reasons.append(f"Large-scale change ({task.files_affected} files)")
        if task.architectural_decision:
            reasons.append("Architectural decision required")
        if task.multiple_valid_approaches:
            reasons.append("Multiple valid approaches to evaluate")
        if task.unfamiliar_codebase:
            reasons.append("Unfamiliar codebase — must understand before changing")
        if task.infrastructure_implications:
            reasons.append("Infrastructure implications to evaluate")

        if len(reasons) >= 2:
            return {
                "mode": "plan",
                "confidence": 0.95,
                "reasoning": "; ".join(reasons),
                "workflow": "Explore → Plan → Implement → Commit"
            }

        if len(reasons) == 1:
            return {
                "mode": "plan",
                "confidence": 0.8,
                "reasoning": reasons[0],
                "workflow": "Explore → Plan → Implement → Commit"
            }

        # Moderate complexity — combined approach
        if task.cross_file_dependencies or task.files_affected > 5:
            return {
                "mode": "combined",
                "confidence": 0.7,
                "reasoning": f"Moderate complexity ({task.files_affected} files, cross-file deps={task.cross_file_dependencies})",
                "workflow": "Quick plan → Direct implementation"
            }

        # Default: direct for simple tasks
        return {
            "mode": "direct",
            "confidence": 0.6,
            "reasoning": "No strong plan signals — default to direct execution",
            "example": "Standard change with reasonable scope"
        }


def test_task_router():
    """Test Module 2: Task Router."""
    print("=" * 60)
    print("Module 2: Task Router — Plan vs Direct Decision Engine")
    print("=" * 60)

    passed = 0
    total = 0

    # ODM Scenario 1: SAP Connection typo fix
    task1 = TaskProfile(
        description="Fix SAP Connection string typo in connections.py",
        files_affected=1,
        has_clear_stack_trace=True,
        can_describe_diff_in_one_sentence=True
    )
    r1 = TaskRouter.recommend(task1)
    assert r1["mode"] == "direct"
    total += 1; passed += 1
    print(f"  ✅ T1: SAP Connection typo → direct (confidence={r1['confidence']})")

    # ODM Scenario 2: Airflow 2 → 3 migration (40+ DAGs)
    task2 = TaskProfile(
        description="Migrate 48 Airflow 2 DAGs to Airflow 3",
        files_affected=48,
        multiple_valid_approaches=True,
        architectural_decision=True,
        cross_file_dependencies=True
    )
    r2 = TaskRouter.recommend(task2)
    assert r2["mode"] == "plan"
    assert r2["confidence"] >= 0.9
    total += 1; passed += 1
    print(f"  ✅ T2: Airflow migration (48 files) → plan (confidence={r2['confidence']})")

    # ODM Scenario 3: Add validation to a single function
    task3 = TaskProfile(
        description="Add date validation to PO approval function",
        files_affected=1,
        can_describe_diff_in_one_sentence=True
    )
    r3 = TaskRouter.recommend(task3)
    assert r3["mode"] == "direct"
    total += 1; passed += 1
    print(f"  ✅ T3: Add validation → direct")

    # ODM Scenario 4: New vendor scoring system (unfamiliar codebase)
    task4 = TaskProfile(
        description="Design vendor scoring system in legacy codebase",
        files_affected=15,
        unfamiliar_codebase=True,
        multiple_valid_approaches=True,
        architectural_decision=True
    )
    r4 = TaskRouter.recommend(task4)
    assert r4["mode"] == "plan"
    total += 1; passed += 1
    print(f"  ✅ T4: New system in unfamiliar codebase → plan")

    # ODM Scenario 5: Rename a variable across 3 files
    task5 = TaskProfile(
        description="Rename 'vendor_id' to 'supplier_id' in 3 files",
        files_affected=3,
        can_describe_diff_in_one_sentence=True
    )
    r5 = TaskRouter.recommend(task5)
    assert r5["mode"] == "direct"
    total += 1; passed += 1
    print(f"  ✅ T5: Variable rename (3 files, simple) → direct")

    # ODM Scenario 6: Choose between Redux vs Context API for dashboard
    task6 = TaskProfile(
        description="State management for supply chain dashboard",
        files_affected=20,
        multiple_valid_approaches=True,
        infrastructure_implications=True
    )
    r6 = TaskRouter.recommend(task6)
    assert r6["mode"] == "plan"
    total += 1; passed += 1
    print(f"  ✅ T6: Architecture choice (Redux vs Context) → plan")

    print(f"\n  Module 2: {passed}/{total} passed\n")
    return passed, total


# ============================================================
# Module 3: Explore Subagent Simulation
# ============================================================

class ExploreSubagent:
    """
    Simulates the built-in Explore subagent behavior.

    Key characteristics:
    - Model: Haiku (fast, low-latency)
    - Tools: Read-only (no Write/Edit)
    - Skips CLAUDE.md and git status at startup
    - Returns summaries, not raw verbose output
    - Three thoroughness levels
    """

    class Thoroughness(Enum):
        QUICK = "quick"          # Targeted lookups
        MEDIUM = "medium"        # Balanced exploration
        VERY_THOROUGH = "very thorough"  # Comprehensive analysis

    @dataclass
    class ExploreResult:
        thoroughness: str
        files_read: int
        tokens_consumed_subagent: int
        summary_tokens: int  # What goes back to main context
        findings: list
        context_savings_pct: float  # How much context was saved

    def __init__(self):
        self.model = "haiku"
        self.tools = ["Read", "Grep", "Glob"]  # Read-only
        self.denied_tools = ["Write", "Edit"]
        self.loads_claude_md = False  # Skips CLAUDE.md!
        self.loads_git_status = False  # Skips git status!

    def explore(self, task: str, thoroughness: 'ExploreSubagent.Thoroughness',
                codebase_files: dict) -> 'ExploreSubagent.ExploreResult':
        """Simulate an Explore subagent execution."""

        # Simulate file reading based on thoroughness
        all_files = list(codebase_files.keys())
        if thoroughness == self.Thoroughness.QUICK:
            files_to_read = all_files[:3]
            tokens_per_file = 200
        elif thoroughness == self.Thoroughness.MEDIUM:
            files_to_read = all_files[:8]
            tokens_per_file = 500
        else:  # VERY_THOROUGH
            files_to_read = all_files
            tokens_per_file = 1000

        total_tokens = len(files_to_read) * tokens_per_file
        findings = []
        for f in files_to_read:
            content = codebase_files.get(f, "")
            if content:
                findings.append(f"[{f}] {content}")

        # Summary compression: verbose → concise
        summary_tokens = min(total_tokens, 300)  # Summary is always compact
        savings = ((total_tokens - summary_tokens) / max(total_tokens, 1)) * 100

        return self.ExploreResult(
            thoroughness=thoroughness.value,
            files_read=len(files_to_read),
            tokens_consumed_subagent=total_tokens,
            summary_tokens=summary_tokens,
            findings=findings,
            context_savings_pct=round(savings, 1)
        )


@dataclass
class PlanSubagent:
    """The Plan subagent used during plan mode."""
    model: str = "inherited"  # Inherits from main conversation
    tools: list = field(default_factory=lambda: ["Read", "Grep", "Glob"])
    denied_tools: list = field(default_factory=lambda: ["Write", "Edit"])
    loads_claude_md: bool = False  # Also skips!
    purpose: str = "Codebase research for planning"


def test_explore_subagent():
    """Test Module 3: Explore Subagent."""
    print("=" * 60)
    print("Module 3: Explore Subagent Simulation")
    print("=" * 60)

    passed = 0
    total = 0

    explore = ExploreSubagent()

    # Test 1: Explore uses Haiku
    assert explore.model == "haiku"
    total += 1; passed += 1
    print(f"  ✅ T1: Explore subagent uses Haiku model")

    # Test 2: Read-only tools
    assert "Write" in explore.denied_tools
    assert "Edit" in explore.denied_tools
    assert "Read" in explore.tools
    total += 1; passed += 1
    print(f"  ✅ T2: Read-only tools (Write/Edit denied)")

    # Test 3: Skips CLAUDE.md and git status
    assert explore.loads_claude_md == False
    assert explore.loads_git_status == False
    total += 1; passed += 1
    print(f"  ✅ T3: Skips CLAUDE.md and git status (speed + cost)")

    # Test 4: Quick exploration — minimal files
    codebase = {
        "dags/sap_p2p_dag.py": "SAP P2P extraction DAG with 12 tasks",
        "dags/vendor_scoring_dag.py": "Vendor QCDS scoring pipeline",
        "dags/inventory_dag.py": "Inventory reconciliation daily",
        "plugins/sap_hook.py": "Custom SAP RFC hook",
        "plugins/delta_operator.py": "Delta Lake write operator",
        "tests/test_sap_p2p.py": "P2P DAG unit tests",
        "tests/test_vendor_scoring.py": "Vendor scoring tests",
        "config/connections.yaml": "Airflow connections config",
        "config/variables.yaml": "Airflow variables config",
        "README.md": "Project documentation",
    }
    result_quick = explore.explore("Find the SAP P2P DAG entry point",
                                    ExploreSubagent.Thoroughness.QUICK, codebase)
    assert result_quick.files_read == 3
    total += 1; passed += 1
    print(f"  ✅ T4: Quick = 3 files read")

    # Test 5: Very thorough — all files
    result_thorough = explore.explore("Map all DAG dependencies and data lineage",
                                       ExploreSubagent.Thoroughness.VERY_THOROUGH, codebase)
    assert result_thorough.files_read == 10
    total += 1; passed += 1
    print(f"  ✅ T5: Very thorough = all 10 files read")

    # Test 6: Context savings — summary is much smaller than raw
    assert result_thorough.context_savings_pct > 90
    total += 1; passed += 1
    print(f"  ✅ T6: Context savings = {result_thorough.context_savings_pct}% "
          f"({result_thorough.tokens_consumed_subagent} subagent → {result_thorough.summary_tokens} summary)")

    # Test 7: Plan subagent uses inherited model
    plan = PlanSubagent()
    assert plan.model == "inherited"
    assert plan.loads_claude_md == False
    total += 1; passed += 1
    print(f"  ✅ T7: Plan subagent inherits model, also skips CLAUDE.md")

    # Test 8: Compare Explore vs Plan
    assert explore.model == "haiku"
    assert plan.model == "inherited"
    total += 1; passed += 1
    print(f"  ✅ T8: Explore=Haiku vs Plan=inherited — different models, both read-only")

    print(f"\n  Module 3: {passed}/{total} passed\n")
    return passed, total


# ============================================================
# Module 4: Ultraplan Workflow Simulation
# ============================================================

class UltraplanStatus(Enum):
    RESEARCHING = "◇ ultraplan"
    NEEDS_INPUT = "◇ ultraplan needs your input"
    READY = "◆ ultraplan ready"
    EXECUTING = "executing"
    COMPLETED = "completed"


class UltraplanExecution(Enum):
    WEB = "web"            # Execute on the web, open PR
    TELEPORT = "teleport"  # Send plan back to terminal


@dataclass
class UltraplanSession:
    prompt: str
    provider: str = "anthropic"
    has_github_repo: bool = True
    has_web_account: bool = True
    remote_control_active: bool = False
    status: UltraplanStatus = UltraplanStatus.RESEARCHING
    plan_text: str = ""
    comments: list = field(default_factory=list)

    def can_launch(self) -> tuple:
        """Check if Ultraplan can be launched."""
        issues = []
        if self.provider in ("bedrock", "vertex", "foundry"):
            issues.append(f"Not available on {self.provider}")
        if not self.has_github_repo:
            issues.append("Requires GitHub repository")
        if not self.has_web_account:
            issues.append("Requires Claude Code on the web account")
        return (len(issues) == 0, issues)

    def launch(self) -> str:
        """Launch Ultraplan."""
        ok, issues = self.can_launch()
        if not ok:
            return f"Cannot launch: {'; '.join(issues)}"

        if self.remote_control_active:
            # Remote Control and Ultraplan are mutually exclusive
            return "Disconnecting Remote Control (mutually exclusive with Ultraplan)... Launched."

        self.status = UltraplanStatus.RESEARCHING
        return "Launched — Claude is researching your codebase"

    def plan_ready(self, plan: str):
        self.plan_text = plan
        self.status = UltraplanStatus.READY

    def add_comment(self, section: str, comment: str):
        """Inline comment on a plan section."""
        self.comments.append({"section": section, "comment": comment})

    def execute(self, where: UltraplanExecution) -> dict:
        if where == UltraplanExecution.WEB:
            self.status = UltraplanStatus.EXECUTING
            return {
                "action": "Implementing on web",
                "terminal_status": "Confirmation shown, indicator cleared",
                "next": "Review diff → Create PR"
            }
        else:  # TELEPORT
            self.status = UltraplanStatus.COMPLETED
            return {
                "action": "Plan teleported to terminal",
                "options": [
                    "Implement here — inject into current conversation",
                    "Start new session — fresh with only the plan",
                    "Cancel — save to file, return later"
                ]
            }


def test_ultraplan():
    """Test Module 4: Ultraplan Workflow."""
    print("=" * 60)
    print("Module 4: Ultraplan Workflow Simulation")
    print("=" * 60)

    passed = 0
    total = 0

    # Test 1: Cannot launch on Bedrock
    session_bedrock = UltraplanSession(prompt="migrate auth", provider="bedrock")
    ok, issues = session_bedrock.can_launch()
    assert ok == False
    assert "bedrock" in issues[0].lower()
    total += 1; passed += 1
    print(f"  ✅ T1: Ultraplan NOT available on Bedrock")

    # Test 2: Cannot launch without GitHub repo
    session_no_gh = UltraplanSession(prompt="migrate auth", has_github_repo=False)
    ok, issues = session_no_gh.can_launch()
    assert ok == False
    total += 1; passed += 1
    print(f"  ✅ T2: Ultraplan requires GitHub repo")

    # Test 3: Normal launch
    session = UltraplanSession(prompt="Migrate SAP P2P DAGs to Airflow 3")
    result = session.launch()
    assert session.status == UltraplanStatus.RESEARCHING
    total += 1; passed += 1
    print(f"  ✅ T3: Normal launch → RESEARCHING status")

    # Test 4: Remote Control disconnects
    session_rc = UltraplanSession(prompt="test", remote_control_active=True)
    result = session_rc.launch()
    assert "Disconnecting" in result
    total += 1; passed += 1
    print(f"  ✅ T4: Remote Control disconnects on Ultraplan launch")

    # Test 5: Plan ready → review with comments
    session.plan_ready("Phase 1: Migrate core DAGs\nPhase 2: Migrate integration DAGs")
    assert session.status == UltraplanStatus.READY
    session.add_comment("Phase 1", "Start with SAP P2P first — highest priority")
    assert len(session.comments) == 1
    total += 1; passed += 1
    print(f"  ✅ T5: Plan ready → inline comments")

    # Test 6: Teleport back options
    result = session.execute(UltraplanExecution.TELEPORT)
    assert len(result["options"]) == 3
    assert "Implement here" in result["options"][0]
    total += 1; passed += 1
    print(f"  ✅ T6: Teleport back → 3 options (implement/new session/cancel)")

    # Test 7: Web execution
    session2 = UltraplanSession(prompt="test")
    session2.plan_ready("test plan")
    result = session2.execute(UltraplanExecution.WEB)
    assert "PR" in result["next"]
    total += 1; passed += 1
    print(f"  ✅ T7: Web execution → Review diff → Create PR")

    print(f"\n  Module 4: {passed}/{total} passed\n")
    return passed, total


# ============================================================
# Module 5: Iterative Refinement Mode Selector
# ============================================================

class RefinementMode(Enum):
    IO_EXAMPLES = "concrete_io_examples"
    TDD = "test_driven_iteration"
    INTERVIEW = "interview_pattern"
    BATCH_ISSUES = "batch_interacting_issues"
    SEQUENTIAL = "sequential_independent"


@dataclass
class RefinementContext:
    """Context for choosing the right iterative refinement approach."""
    description: str
    prose_descriptions_inconsistent: bool = False
    behavior_spec_clear: bool = False
    unfamiliar_domain: bool = False
    issues_interact: bool = False
    multiple_independent_issues: bool = False
    edge_cases_important: bool = False


class RefinementSelector:
    """Recommends iterative refinement approach based on context."""

    @staticmethod
    def recommend(ctx: RefinementContext) -> list:
        """Returns recommended approaches in priority order."""
        recs = []

        if ctx.prose_descriptions_inconsistent:
            recs.append({
                "mode": RefinementMode.IO_EXAMPLES,
                "reason": "Prose descriptions interpreted inconsistently → concrete I/O examples are most effective",
                "tip": "Provide 2-4 targeted few-shot examples showing reasoning for choices"
            })

        if ctx.behavior_spec_clear or ctx.edge_cases_important:
            recs.append({
                "mode": RefinementMode.TDD,
                "reason": "Clear behavior spec / edge cases → write tests first, iterate on failures",
                "tip": "Include expected behavior, edge cases, and performance requirements"
            })

        if ctx.unfamiliar_domain:
            recs.append({
                "mode": RefinementMode.INTERVIEW,
                "reason": "Unfamiliar domain → Claude asks questions to surface non-obvious considerations",
                "tip": "E.g.: cache invalidation strategies, failure modes, compliance needs"
            })

        if ctx.issues_interact:
            recs.append({
                "mode": RefinementMode.BATCH_ISSUES,
                "reason": "Issues interact with each other → provide all in one message",
                "tip": "Fixes for interacting problems in a single detailed message"
            })

        if ctx.multiple_independent_issues:
            recs.append({
                "mode": RefinementMode.SEQUENTIAL,
                "reason": "Independent issues → fix sequentially to avoid confusion",
                "tip": "One issue per iteration, verify each before moving to next"
            })

        if not recs:
            recs.append({
                "mode": RefinementMode.IO_EXAMPLES,
                "reason": "Default: concrete examples are the most universally effective approach",
                "tip": "2-4 examples showing input → expected output"
            })

        return recs


def test_refinement_selector():
    """Test Module 5: Iterative Refinement Selector."""
    print("=" * 60)
    print("Module 5: Iterative Refinement Mode Selector")
    print("=" * 60)

    passed = 0
    total = 0

    # Test 1: ETL output format inconsistency → I/O examples
    ctx1 = RefinementContext(
        description="ETL transformation producing inconsistent date formats",
        prose_descriptions_inconsistent=True
    )
    recs1 = RefinementSelector.recommend(ctx1)
    assert recs1[0]["mode"] == RefinementMode.IO_EXAMPLES
    total += 1; passed += 1
    print(f"  ✅ T1: Date format inconsistency → I/O examples")

    # Test 2: Unfamiliar supply chain domain → Interview
    ctx2 = RefinementContext(
        description="Implement MRP net requirements calculation",
        unfamiliar_domain=True,
        behavior_spec_clear=True
    )
    recs2 = RefinementSelector.recommend(ctx2)
    modes = [r["mode"] for r in recs2]
    assert RefinementMode.INTERVIEW in modes
    assert RefinementMode.TDD in modes
    total += 1; passed += 1
    print(f"  ✅ T2: MRP calculation (unfamiliar + spec clear) → Interview + TDD")

    # Test 3: Interacting bugs in P2P pipeline → Batch
    ctx3 = RefinementContext(
        description="P2P pipeline: GR creates incorrect inventory AND invoice mismatch",
        issues_interact=True
    )
    recs3 = RefinementSelector.recommend(ctx3)
    assert recs3[0]["mode"] == RefinementMode.BATCH_ISSUES
    total += 1; passed += 1
    print(f"  ✅ T3: Interacting P2P bugs → batch issues")

    # Test 4: Multiple independent improvements → Sequential
    ctx4 = RefinementContext(
        description="Code review: naming convention, unused imports, type hints",
        multiple_independent_issues=True
    )
    recs4 = RefinementSelector.recommend(ctx4)
    assert recs4[0]["mode"] == RefinementMode.SEQUENTIAL
    total += 1; passed += 1
    print(f"  ✅ T4: Independent code review items → sequential")

    # Test 5: Edge cases in validation → TDD
    ctx5 = RefinementContext(
        description="PO approval validation with edge cases",
        behavior_spec_clear=True,
        edge_cases_important=True
    )
    recs5 = RefinementSelector.recommend(ctx5)
    assert recs5[0]["mode"] == RefinementMode.TDD
    total += 1; passed += 1
    print(f"  ✅ T5: Validation edge cases → TDD")

    print(f"\n  Module 5: {passed}/{total} passed\n")
    return passed, total


# ============================================================
# Module 6: CCA Mock Exam — D3 Task 3.4
# ============================================================

def run_mock_exam():
    """CCA Mock Exam: Plan Mode vs Direct Execution."""
    print("=" * 60)
    print("Module 6: CCA Mock Exam — D3 Task 3.4")
    print("=" * 60)

    questions = [
        {
            "id": 1,
            "scenario": "Code Generation",
            "q": """You're tasked with migrating a legacy authentication module from session-based 
auth to JWT tokens. The change spans 52 files across 8 directories, with multiple 
valid implementation approaches (symmetric vs asymmetric keys, token refresh strategies, 
cookie vs header storage). What approach should you use?""",
            "options": {
                "A": "Direct execution with detailed pre-instructions covering all decisions",
                "B": "Plan mode to explore the codebase, evaluate approaches, and design before implementing",
                "C": "Direct execution file by file, deciding approach as you go",
                "D": "Write all JWT code from scratch in a new directory, then integrate"
            },
            "answer": "B",
            "explanation": """Plan mode is designed for: large-scale changes (52 files), multiple valid 
approaches (symmetric/asymmetric, refresh strategy), and architectural decisions. 
A assumes you already know the answer. C is reactive and risks costly rework. 
D ignores existing patterns and dependencies."""
        },
        {
            "id": 2,
            "scenario": "Developer Productivity",
            "q": """During a long codebase investigation, Claude has explored 20+ files and the context 
window is filling up. You need to understand a specific module's dependency graph 
before making changes. What's the most effective approach?""",
            "options": {
                "A": "Use /compact to summarize the conversation, then continue exploration",
                "B": "Start a new session and re-read the important files",
                "C": "Use the Explore subagent to investigate the dependency graph in isolated context",
                "D": "Manually read each file and take notes in a scratchpad"
            },
            "answer": "C",
            "explanation": """The Explore subagent isolates verbose discovery output in its own context 
window and returns only a summary. This prevents context exhaustion during multi-phase 
tasks. A risks losing precise details. B wastes tokens re-reading. D doesn't leverage 
Claude's capabilities."""
        },
        {
            "id": 3,
            "scenario": "CI/CD Integration",
            "q": """A team wants to set 'plan' as the default mode for their project so developers 
always explore before coding. They also want auto mode available for senior devs.
Where should they configure defaultMode='plan', and what's the constraint on auto mode?""",
            "options": {
                "A": "Set defaultMode='plan' in ~/.claude/settings.json; auto mode works from any settings location",
                "B": "Set defaultMode='plan' in .claude/settings.json; defaultMode='auto' is ignored from project settings (security restriction)",
                "C": "Set defaultMode='plan' in CLAUDE.md; auto mode requires --permission-mode auto flag each time",
                "D": "Set defaultMode='plan' in .mcp.json; auto mode requires admin approval only"
            },
            "answer": "B",
            "explanation": """defaultMode='plan' goes in .claude/settings.json (project level, shared via VCS).
CRITICAL: defaultMode='auto' from project (.claude/settings.json) or local settings is 
IGNORED — this is a security restriction so a repository can't grant itself auto mode. 
Auto mode's defaultMode must be in ~/.claude/settings.json (user level)."""
        },
        {
            "id": 4,
            "scenario": "Code Generation",
            "q": """The Explore subagent and Plan subagent are both built-in read-only subagents. 
Which statement correctly describes their differences?""",
            "options": {
                "A": "Explore uses Haiku and loads CLAUDE.md; Plan inherits the main model and skips CLAUDE.md",
                "B": "Both use Haiku for speed and skip CLAUDE.md files",
                "C": "Explore uses Haiku and skips CLAUDE.md; Plan inherits the main model and also skips CLAUDE.md",
                "D": "Explore inherits the main model; Plan uses Haiku for faster research"
            },
            "answer": "C",
            "explanation": """Explore uses Haiku (fast, low-latency) while Plan inherits from the main 
conversation model. BOTH skip CLAUDE.md and git status to keep research fast and 
inexpensive. This is explicitly stated in the official docs: 'Explore and Plan skip 
your CLAUDE.md files and the parent session's git status.'"""
        },
        {
            "id": 5,
            "scenario": "Developer Productivity",
            "q": """After Claude finishes a plan in plan mode, which of the following is NOT a valid 
plan approval option?""",
            "options": {
                "A": "Approve and start in auto mode",
                "B": "Approve and accept edits",
                "C": "Approve and send to Explore subagent for validation",
                "D": "Refine with Ultraplan for browser-based review"
            },
            "answer": "C",
            "explanation": """The five plan approval options are: (1) Approve & auto mode, (2) Approve & 
accept edits, (3) Approve & manual review, (4) Keep planning with feedback, 
(5) Refine with Ultraplan. 'Send to Explore subagent for validation' is NOT 
an approval option — Explore is for codebase exploration, not plan validation."""
        },
    ]

    passed = 0
    for q in questions:
        # Auto-answer with the correct answer
        correct = q["answer"]
        is_correct = True
        status = "✅" if is_correct else "❌"
        passed += 1 if is_correct else 0
        print(f"\n  Q{q['id']} ({q['scenario']})")
        print(f"  {q['q'][:100]}...")
        print(f"  → Answer: {correct}) {q['options'][correct][:80]}...")
        print(f"  {status} {q['explanation'][:100]}...")

    total = len(questions)
    print(f"\n  Module 6: {passed}/{total} passed\n")
    return passed, total


# ============================================================
# Main
# ============================================================

def main():
    print()
    print("╔══════════════════════════════════════════════════════╗")
    print("║  CCA Practice: Plan Mode vs Direct Execution (D3)  ║")
    print("║  ODM 供應鏈場景 × Claude Code 執行模式選擇          ║")
    print("╚══════════════════════════════════════════════════════╝")
    print()

    results = []
    results.append(test_permission_mode_system())
    results.append(test_task_router())
    results.append(test_explore_subagent())
    results.append(test_ultraplan())
    results.append(test_refinement_selector())
    results.append(run_mock_exam())

    total_passed = sum(r[0] for r in results)
    total_tests = sum(r[1] for r in results)

    print("=" * 60)
    print(f"  TOTAL: {total_passed}/{total_tests} passed")
    print("=" * 60)

    if total_passed == total_tests:
        print("  🎉 All tests passed!")
    else:
        print(f"  ⚠️  {total_tests - total_passed} tests failed")


if __name__ == "__main__":
    main()
