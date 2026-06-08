"""
CCA Practice: Subagent Configuration & Context Passing
======================================================
模擬 Claude Agent SDK 的 AgentDefinition 配置、Context 隔離、
顯式 Context 傳遞、allowedTools 行為、巢狀限制。

ODM 場景：PMC Coordinator 分解客戶急單，
委派 Inventory/Procurement/MRP subagents 並聚合結果。
"""

from __future__ import annotations
import json
import time
import copy
from dataclasses import dataclass, field
from typing import Any
from datetime import datetime, timedelta


# ============================================================
# Part 1: AgentDefinition 模擬
# ============================================================

@dataclass
class AgentDefinition:
    """
    模擬 Claude Agent SDK 的 AgentDefinition。
    
    必填欄位：description, prompt
    可選欄位：tools, disallowedTools, model, skills, memory,
              mcpServers, maxTurns, background, effort, permissionMode
    """
    description: str          # Required: coordinator 據此決定何時 invoke
    prompt: str               # Required: subagent system prompt
    tools: list[str] | None = None          # Optional: 省略 = 繼承 parent 所有工具
    disallowed_tools: list[str] | None = None  # Optional: 黑名單
    model: str | None = None                # Optional: 模型覆寫
    skills: list[str] | None = None
    memory: str | None = None               # 'user' | 'project' | 'local'
    max_turns: int | None = None
    background: bool = False
    effort: str | None = None

    def __post_init__(self):
        if not self.description:
            raise ValueError("AgentDefinition.description is REQUIRED")
        if not self.prompt:
            raise ValueError("AgentDefinition.prompt is REQUIRED")


def resolve_tools(agent_def: AgentDefinition, parent_tools: list[str]) -> list[str]:
    """
    解析 subagent 實際可用的工具清單。
    
    規則：
    1. agent_def.tools 有值 → 只用白名單（白名單模式）
    2. agent_def.tools 為 None → 繼承 parent 全部工具（繼承模式）
    3. 然後移除 disallowed_tools（黑名單排除）
    4. 永遠移除 "Agent"（subagent 不能生成 sub-subagent）
    """
    if agent_def.tools is not None:
        effective = list(agent_def.tools)
    else:
        effective = list(parent_tools)
    
    # 黑名單排除
    if agent_def.disallowed_tools:
        effective = [t for t in effective if t not in agent_def.disallowed_tools]
    
    # 強制移除 Agent（subagent 不能巢狀）
    effective = [t for t in effective if t != "Agent"]
    
    return effective


# ============================================================
# Part 2: Context 隔離模擬
# ============================================================

@dataclass
class ConversationMessage:
    role: str       # "user" | "assistant" | "tool_result"
    content: str
    tool_name: str | None = None


@dataclass
class AgentContext:
    """每個 agent 的獨立 context。"""
    system_prompt: str
    messages: list[ConversationMessage] = field(default_factory=list)
    available_tools: list[str] = field(default_factory=list)
    agent_name: str = "main"
    
    def add_message(self, role: str, content: str, tool_name: str | None = None):
        self.messages.append(ConversationMessage(role, content, tool_name))
    
    def get_history_summary(self) -> str:
        return f"[{self.agent_name}] {len(self.messages)} messages in history"


def spawn_subagent(
    agent_def: AgentDefinition,
    parent_context: AgentContext,
    task_prompt: str,
    parent_tools: list[str],
) -> AgentContext:
    """
    生成 subagent context。
    
    關鍵：subagent 的 context 是全新的！
    - ✅ 收到：agent_def.prompt（system prompt）+ task_prompt
    - ✅ 收到：resolved tools
    - ❌ 不收到：parent 的 conversation history
    - ❌ 不收到：parent 的 system prompt
    """
    resolved = resolve_tools(agent_def, parent_tools)
    
    sub_context = AgentContext(
        system_prompt=agent_def.prompt,    # subagent 自己的 prompt
        messages=[],                        # 全新！不繼承 parent messages
        available_tools=resolved,
        agent_name=agent_def.description[:30],
    )
    
    # Agent tool 的 prompt 作為第一個 user message
    sub_context.add_message("user", task_prompt)
    
    return sub_context


# ============================================================
# Part 3: Content vs Metadata 結構化資料分離
# ============================================================

@dataclass
class Finding:
    """上游 agent 的結構化輸出：content + metadata 分離。"""
    # Content（業務資料）
    claim: str
    evidence: str
    relevance_score: float
    
    # Metadata（執行 context，用於 attribution）
    source_system: str
    query_timestamp: str
    agent_name: str
    document_name: str | None = None
    data_collection_date: str | None = None

    def to_compact(self) -> dict:
        """Context budget 有限時，只保留必要資訊。"""
        return {
            "claim": self.claim,
            "source": self.source_system,
            "relevance": self.relevance_score,
        }
    
    def to_full(self) -> dict:
        """完整版，含 evidence + metadata。"""
        return {
            "claim": self.claim,
            "evidence": self.evidence,
            "relevance_score": self.relevance_score,
            "source_system": self.source_system,
            "query_timestamp": self.query_timestamp,
            "agent_name": self.agent_name,
            "document_name": self.document_name,
            "data_collection_date": self.data_collection_date,
        }


def trim_context_for_budget(findings: list[Finding], max_items: int = 5) -> list[dict]:
    """
    Context budget 修剪：
    - 按 relevance_score 排序
    - 只取 top N
    - 用 compact 格式（省略 evidence 和詳細 metadata）
    """
    sorted_findings = sorted(findings, key=lambda f: f.relevance_score, reverse=True)
    return [f.to_compact() for f in sorted_findings[:max_items]]


# ============================================================
# Part 4: allowedTools 行為模擬
# ============================================================

@dataclass
class QueryOptions:
    """模擬 SDK query() 的 options。"""
    allowed_tools: list[str] = field(default_factory=list)
    agents: dict[str, AgentDefinition] = field(default_factory=dict)


def can_auto_invoke_subagent(options: QueryOptions) -> bool:
    """
    Coordinator 能否自動 invoke subagent？
    必須在 allowedTools 中包含 "Agent"（或舊版 "Task"）。
    """
    return "Agent" in options.allowed_tools or "Task" in options.allowed_tools


# ============================================================
# Part 5: Dynamic Agent Factory
# ============================================================

def create_procurement_agent(urgency: str) -> AgentDefinition:
    """
    工廠模式：根據 urgency 動態配置 agent。
    - critical → opus 模型，嚴格 prompt，更多工具
    - normal → sonnet 模型，標準 prompt，只讀工具
    """
    is_critical = urgency == "critical"
    
    return AgentDefinition(
        description="Procurement specialist for supplier and AVL queries",
        prompt=(
            f"You are a {'CRITICAL urgency' if is_critical else 'standard'} procurement analyst.\n"
            f"{'PRIORITY: Find alternative suppliers immediately. Check ALL AVL sources.' if is_critical else 'Analyze supplier availability at normal priority.'}\n"
            "Return structured findings with supplier name, lead time, MOQ, and AVL status."
        ),
        tools=(
            ["check_avl", "get_lead_time", "search_alt_suppliers", "send_urgent_rfq"]
            if is_critical else
            ["check_avl", "get_lead_time"]
        ),
        model="opus" if is_critical else "sonnet",
    )


# ============================================================
# Part 6: ODM Scenario Simulation
# ============================================================

def simulate_odm_multi_agent():
    """
    模擬 ODM 供應鏈場景：
    PMC Coordinator 收到客戶急單 → 分解任務 → 委派 3 個 subagent
    """
    print("=" * 70)
    print("🏭 ODM Multi-Agent 供應鏈場景模擬")
    print("=" * 70)
    
    # --- 定義 Agents ---
    inventory_agent = AgentDefinition(
        description="Inventory checker for warehouse stock levels",
        prompt="You are an inventory specialist. Check stock levels across RM/WIP/FG warehouses.",
        tools=["check_stock", "get_location", "check_reservation"],
    )
    
    procurement_agent = create_procurement_agent("critical")
    
    mrp_agent = AgentDefinition(
        description="MRP runner for material requirements calculation",
        prompt="You are an MRP specialist. Calculate net requirements from BOM explosion.",
        tools=["run_mrp", "get_bom", "check_open_po"],
        # 不設 model → 繼承 parent model
    )
    
    # --- Coordinator setup ---
    parent_tools = [
        "Agent", "check_stock", "get_location", "check_reservation",
        "check_avl", "get_lead_time", "search_alt_suppliers", "send_urgent_rfq",
        "run_mrp", "get_bom", "check_open_po", "send_alert"
    ]
    
    options = QueryOptions(
        allowed_tools=["Agent", "send_alert"],  # Auto-approve Agent + alert
        agents={
            "inventory": inventory_agent,
            "procurement": procurement_agent,
            "mrp": mrp_agent,
        }
    )
    
    coordinator_context = AgentContext(
        system_prompt="You are PMC Coordinator. Decompose customer rush orders and delegate to specialists.",
        available_tools=parent_tools,
        agent_name="PMC-Coordinator",
    )
    
    # 模擬 coordinator 收到客戶急單
    rush_order = {
        "customer": "Microsoft Azure",
        "product": "SR670-V3 GPU Server",
        "quantity": 500,
        "request_date": "2026-06-20",
        "priority": "CRITICAL",
        "key_components": ["NVIDIA H200 GPU", "DDR5-5600 256GB", "PCIe Gen5 Riser"]
    }
    
    coordinator_context.add_message("user", f"New rush order: {json.dumps(rush_order, indent=2)}")
    coordinator_context.add_message("assistant", "Analyzing rush order. Decomposing into 3 parallel subagent tasks...")
    
    print(f"\n📋 客戶急單：{rush_order['customer']} - {rush_order['product']} x{rush_order['quantity']}")
    print(f"   交期要求：{rush_order['request_date']}（{(datetime.fromisoformat(rush_order['request_date']) - datetime.now()).days} 天後）")
    print(f"   優先級：{rush_order['priority']}")
    
    # --- Demo 1: Context 隔離 ---
    print(f"\n{'─' * 70}")
    print("📦 Demo 1: Context 隔離驗證")
    print(f"{'─' * 70}")
    
    # 顯式 context 傳遞：把 coordinator 知道的資訊寫在 task prompt 裡
    inventory_prompt = (
        f"Check stock levels for the following components:\n"
        f"Components: {json.dumps(rush_order['key_components'])}\n"
        f"Required quantity: {rush_order['quantity']} sets\n"
        f"Customer: {rush_order['customer']}\n"
        f"Output format: JSON with fields: component, on_hand, reserved, available, warehouse"
    )
    
    inv_context = spawn_subagent(inventory_agent, coordinator_context, inventory_prompt, parent_tools)
    
    print(f"  Coordinator history: {len(coordinator_context.messages)} messages")
    print(f"  Inventory subagent history: {len(inv_context.messages)} messages")
    print(f"  ✅ Subagent 只有 1 條 message（task prompt），不繼承 coordinator 歷史")
    
    # 驗證 subagent 不知道 coordinator 的對話
    coordinator_knows = any("rush order" in m.content.lower() for m in coordinator_context.messages)
    subagent_inherits_history = len(inv_context.messages) > 1
    print(f"  Coordinator 知道 rush order: {coordinator_knows}")
    print(f"  Subagent 繼承了 coordinator 歷史: {subagent_inherits_history}")
    assert not subagent_inherits_history, "Subagent should NOT inherit parent history!"
    
    # 但 subagent 透過 task prompt 知道必要資訊
    subagent_knows_components = "H200" in inv_context.messages[0].content
    print(f"  Subagent 透過 prompt 知道 H200: {subagent_knows_components}")
    assert subagent_knows_components, "Subagent should receive context via prompt!"
    
    # --- Demo 2: Tools 三種模式 ---
    print(f"\n{'─' * 70}")
    print("🔧 Demo 2: Tools 解析三種模式")
    print(f"{'─' * 70}")
    
    # 模式 1: 白名單
    inv_tools = resolve_tools(inventory_agent, parent_tools)
    print(f"  白名單模式（Inventory）: {inv_tools}")
    assert "Agent" not in inv_tools, "Subagent should never have Agent tool!"
    assert inv_tools == ["check_stock", "get_location", "check_reservation"]
    
    # 模式 2: 繼承全部
    inherit_agent = AgentDefinition(
        description="Test agent with no tools specified",
        prompt="Test",
        # tools=None → 繼承全部
    )
    inherited_tools = resolve_tools(inherit_agent, parent_tools)
    print(f"  繼承模式（tools=None）: {len(inherited_tools)} tools, Agent removed: {'Agent' not in inherited_tools}")
    assert "Agent" not in inherited_tools
    assert len(inherited_tools) == len(parent_tools) - 1  # All minus Agent
    
    # 模式 3: 黑名單
    blacklist_agent = AgentDefinition(
        description="Test agent with disallowed tools",
        prompt="Test",
        disallowed_tools=["send_urgent_rfq", "send_alert"],
    )
    blacklist_tools = resolve_tools(blacklist_agent, parent_tools)
    print(f"  黑名單模式: {len(blacklist_tools)} tools, 排除了 send_urgent_rfq 和 send_alert")
    assert "send_urgent_rfq" not in blacklist_tools
    assert "send_alert" not in blacklist_tools
    assert "check_stock" in blacklist_tools  # 其他工具保留
    
    # --- Demo 3: allowedTools 行為 ---
    print(f"\n{'─' * 70}")
    print("🔑 Demo 3: allowedTools auto-approve 行為")
    print(f"{'─' * 70}")
    
    # 有 Agent
    print(f"  Options with 'Agent' in allowedTools: can_auto_invoke = {can_auto_invoke_subagent(options)}")
    assert can_auto_invoke_subagent(options)
    
    # 沒有 Agent
    no_agent_options = QueryOptions(allowed_tools=["send_alert"])
    print(f"  Options WITHOUT 'Agent': can_auto_invoke = {can_auto_invoke_subagent(no_agent_options)}")
    assert not can_auto_invoke_subagent(no_agent_options)
    
    # 舊版 Task
    legacy_options = QueryOptions(allowed_tools=["Task", "send_alert"])
    print(f"  Options with legacy 'Task': can_auto_invoke = {can_auto_invoke_subagent(legacy_options)}")
    assert can_auto_invoke_subagent(legacy_options)
    
    # --- Demo 4: Content vs Metadata 分離 ---
    print(f"\n{'─' * 70}")
    print("📊 Demo 4: Content vs Metadata 結構化分離")
    print(f"{'─' * 70}")
    
    # 模擬 inventory subagent 的查詢結果
    findings = [
        Finding(
            claim="H200 GPU on-hand: 120 units, 80 reserved, 40 available",
            evidence="Warehouse A3 shelf R12-R15, lot #GPU-2026-0601, received 2026-06-01",
            relevance_score=0.98,
            source_system="SAP MM (MMBE)",
            query_timestamp="2026-06-09T07:05:00Z",
            agent_name="inventory-agent",
            document_name="Stock Overview Report",
            data_collection_date="2026-06-09",
        ),
        Finding(
            claim="DDR5-5600 256GB on-hand: 2400 units, 500 reserved, 1900 available",
            evidence="Warehouse B1 bulk area, multiple lots, FIFO compliant",
            relevance_score=0.85,
            source_system="SAP MM (MMBE)",
            query_timestamp="2026-06-09T07:05:01Z",
            agent_name="inventory-agent",
            data_collection_date="2026-06-09",
        ),
        Finding(
            claim="PCIe Gen5 Riser on-hand: 0 units, shortage alert active since 2026-06-05",
            evidence="Zero stock, last receipt 2026-05-20 (50 units), fully consumed by SR670-V2 build",
            relevance_score=0.99,
            source_system="SAP MM (MMBE)",
            query_timestamp="2026-06-09T07:05:02Z",
            agent_name="inventory-agent",
            document_name="Shortage Alert #SA-2026-0605-003",
            data_collection_date="2026-06-09",
        ),
    ]
    
    print("  Full format（完整版，含 evidence + metadata）:")
    full = findings[2].to_full()
    for k, v in full.items():
        print(f"    {k}: {v}")
    
    print(f"\n  Compact format（budget-constrained，只保留 claim + source + relevance）:")
    compact = trim_context_for_budget(findings, max_items=2)
    for item in compact:
        print(f"    {json.dumps(item)}")
    
    print(f"\n  Full: {len(json.dumps([f.to_full() for f in findings]))} chars")
    print(f"  Compact: {len(json.dumps(compact))} chars")
    print(f"  節省: {100 - len(json.dumps(compact)) * 100 // len(json.dumps([f.to_full() for f in findings]))}%")
    
    # --- Demo 5: Dynamic Agent Factory ---
    print(f"\n{'─' * 70}")
    print("🏭 Demo 5: Dynamic Agent Factory（工廠模式）")
    print(f"{'─' * 70}")
    
    normal_agent = create_procurement_agent("normal")
    critical_agent = create_procurement_agent("critical")
    
    print(f"  Normal:   model={normal_agent.model}, tools={len(normal_agent.tools)} {normal_agent.tools}")
    print(f"  Critical: model={critical_agent.model}, tools={len(critical_agent.tools)} {critical_agent.tools}")
    assert normal_agent.model == "sonnet"
    assert critical_agent.model == "opus"
    assert len(critical_agent.tools) > len(normal_agent.tools)
    print(f"  ✅ Critical 用更強模型 + 更多工具（含 send_urgent_rfq）")
    
    # --- Demo 6: Subagent 巢狀限制 ---
    print(f"\n{'─' * 70}")
    print("🚫 Demo 6: Subagent 巢狀限制驗證")
    print(f"{'─' * 70}")
    
    # 即使 parent 有 Agent，subagent resolve 後也不會有
    agent_with_agent = AgentDefinition(
        description="Test",
        prompt="Test",
        tools=["Read", "Agent", "Grep"],  # 故意加 Agent
    )
    resolved = resolve_tools(agent_with_agent, parent_tools)
    print(f"  Input tools: {agent_with_agent.tools}")
    print(f"  Resolved:    {resolved}")
    assert "Agent" not in resolved
    print(f"  ✅ 'Agent' 被強制移除 — subagent 不能生成 sub-subagent")
    
    # --- Demo 7: 平行 vs 串行模擬 ---
    print(f"\n{'─' * 70}")
    print("⚡ Demo 7: 平行 vs 串行 Subagent 執行")
    print(f"{'─' * 70}")
    
    def simulate_subagent_work(name: str, duration_ms: int) -> dict:
        """模擬 subagent 工作。"""
        time.sleep(duration_ms / 1000)
        return {"agent": name, "status": "complete", "duration_ms": duration_ms}
    
    # 串行
    serial_start = time.time()
    r1 = simulate_subagent_work("inventory", 100)
    r2 = simulate_subagent_work("procurement", 150)
    r3 = simulate_subagent_work("mrp", 120)
    serial_time = (time.time() - serial_start) * 1000
    
    print(f"  串行執行: {serial_time:.0f}ms（100+150+120 = 370ms 預期）")
    
    # 平行（模擬：一個 response 多個 tool call）
    # 注意：真正的平行是 SDK 層面，不是 asyncio
    # 這裡用概念演示：最慢的 subagent 決定總時間
    parallel_time = max(100, 150, 120)
    speedup = serial_time / parallel_time
    
    print(f"  平行執行: {parallel_time}ms（取最慢的 = 150ms）")
    print(f"  加速比: {speedup:.1f}x")
    print(f"  ✅ 平行不是 asyncio.gather()，是「一個 coordinator response 發多個 Agent tool call」")
    
    # --- Demo 8: AgentDefinition 驗證 ---
    print(f"\n{'─' * 70}")
    print("⚠️ Demo 8: AgentDefinition 必填欄位驗證")
    print(f"{'─' * 70}")
    
    # 缺 description
    try:
        AgentDefinition(description="", prompt="test")
        print("  ❌ Should have raised error for empty description")
    except ValueError as e:
        print(f"  ✅ 缺 description: {e}")
    
    # 缺 prompt
    try:
        AgentDefinition(description="test", prompt="")
        print("  ❌ Should have raised error for empty prompt")
    except ValueError as e:
        print(f"  ✅ 缺 prompt: {e}")
    
    # 正常
    valid = AgentDefinition(description="Valid agent", prompt="You are a valid agent")
    print(f"  ✅ 正常建立: description='{valid.description}', tools={valid.tools} (None=繼承全部)")


# ============================================================
# Part 7: CCA 模擬考題
# ============================================================

def run_exam_simulation():
    print(f"\n{'=' * 70}")
    print("🎯 CCA 模擬考題 — Subagent Configuration & Context Passing")
    print(f"{'=' * 70}")
    
    questions = [
        {
            "id": 1,
            "scenario": "Multi-Agent Research System",
            "question": (
                "你的 coordinator agent 定義了三個 subagent：web-search、doc-analysis、synthesis。\n"
                "你發現 synthesis subagent 試圖搜索網路來驗證事實，而不是請求 coordinator 協調。\n"
                "哪個方案最佳？"
            ),
            "options": {
                "A": "給 synthesis subagent 一個 scoped verify_fact 工具（用於簡單事實查詢），複雜查詢仍透過 coordinator 委派 web-search",
                "B": "讓 synthesis subagent 批次收集所有需要驗證的事實，一次性傳給 web-search",
                "C": "把 web-search 的所有工具都給 synthesis subagent，讓它自己搜索",
                "D": "用 cache 預先載入可能需要的事實，讓 synthesis 直接查詢 cache",
            },
            "correct": "A",
            "explanation": (
                "A 正確：最小權限原則 — 給 synthesis 只做簡單查詢的 scoped 工具（85% 的 case），\n"
                "複雜查詢仍透過 coordinator（15%）。\n"
                "B 錯：batch 會產生 blocking 依賴（synthesis 步驟可能依賴早期驗證的事實）。\n"
                "C 錯：over-provision，違反 separation of concerns。\n"
                "D 錯：speculative caching 無法可靠預測需要驗證什麼。"
            ),
        },
        {
            "id": 2,
            "scenario": "Customer Support Agent",
            "question": (
                "你的 multi-agent 系統有一個 coordinator 和三個 subagent。\n"
                "Subagent A 完成後，其結果需要傳給 Subagent B 做進一步分析。\n"
                "怎麼傳遞 Subagent A 的結果給 Subagent B？"
            ),
            "options": {
                "A": "Subagent A 直接呼叫 Subagent B，把結果當參數傳入",
                "B": "Coordinator 收到 Subagent A 的結果後，把完整結果寫在 Subagent B 的 task prompt 裡",
                "C": "Subagent B 自動繼承 Subagent A 的 conversation history",
                "D": "用共享記憶體（shared memory）讓兩個 subagent 互相存取",
            },
            "correct": "B",
            "explanation": (
                "B 正確：所有 subagent 間的通信必須經過 coordinator。Coordinator 收到 A 的結果後，\n"
                "把相關 findings 顯式寫在 B 的 Agent tool prompt 裡。\n"
                "A 錯：Subagent 不能生成 subagent（嚴格兩層）。\n"
                "C 錯：Subagent 不繼承任何 parent/sibling 的 history。\n"
                "D 錯：SDK 不支援共享記憶體，subagent 之間完全隔離。"
            ),
        },
        {
            "id": 3,
            "scenario": "Multi-Agent Research System",
            "question": (
                "你的 coordinator 需要 spawn 3 個 subagent 做獨立的研究任務。\n"
                "你在 QueryOptions 中設了 allowedTools=[\"Read\", \"Grep\", \"Glob\"]。\n"
                "執行時 coordinator 沒有生成任何 subagent。為什麼？"
            ),
            "options": {
                "A": "agents 字典沒有定義",
                "B": "allowedTools 沒有包含 'Agent'（或 'Task'），coordinator 無法自動 invoke subagent",
                "C": "subagent 的 tools 欄位為空",
                "D": "coordinator 的 system prompt 沒有提到 subagent",
            },
            "correct": "B",
            "explanation": (
                "B 正確：allowedTools 控制 auto-approve。如果沒有包含 'Agent'，\n"
                "coordinator 嘗試 invoke subagent 時會觸發 permission prompt（非互動模式下直接失敗）。\n"
                "必須在 allowedTools 中加入 'Agent'。\n"
                "A 可能：但題目隱含 agents 已定義。\n"
                "C 不影響：tools 為空不阻止 spawn，只是 subagent 沒工具。\n"
                "D 不需要：coordinator 根據 agent description 自動判斷。"
            ),
        },
        {
            "id": 4,
            "scenario": "Structured Data Extraction",
            "question": (
                "你的 web-search subagent 返回大量 findings 給 synthesis subagent。\n"
                "Synthesis 的 context window 已接近 80% 使用率。\n"
                "最佳的 context 傳遞策略是什麼？"
            ),
            "options": {
                "A": "直接傳完整 findings，讓 synthesis 自己決定哪些重要",
                "B": "修改 web-search subagent 返回結構化摘要（key facts, citations, relevance scores），而非完整推理鏈",
                "C": "增加 synthesis subagent 的 max_tokens 來容納更多 context",
                "D": "用漸進式摘要（progressive summarization）壓縮 coordinator 的 history",
            },
            "correct": "B",
            "explanation": (
                "B 正確：上游 agent 應返回結構化資料（claim + source + relevance），\n"
                "而非冗長的推理過程。Content 和 metadata 分離，讓 coordinator \n"
                "可以按 budget 修剪後再傳給 synthesis。\n"
                "A 錯：context 已 80%，直接傳完整 findings 會超限。\n"
                "C 錯：max_tokens 控制輸出長度，不是 context window。\n"
                "D 有風險：漸進式摘要會丟失數值精度（日期、百分比變成「大約」）。"
            ),
        },
        {
            "id": 5,
            "scenario": "Developer Productivity with Claude",
            "question": (
                "你定義了一個 AgentDefinition 但沒有設 tools 欄位。\n"
                "Parent agent 有 Read, Write, Bash, Grep, Glob, Agent 工具。\n"
                "這個 subagent 實際可用的工具有哪些？"
            ),
            "options": {
                "A": "沒有工具（預設無工具）",
                "B": "Read, Write, Bash, Grep, Glob（繼承 parent 全部，但移除 Agent）",
                "C": "Read, Write, Bash, Grep, Glob, Agent（繼承 parent 全部）",
                "D": "只有 Read 和 Grep（基本唯讀工具）",
            },
            "correct": "B",
            "explanation": (
                "B 正確：tools 省略 = 繼承 parent 所有工具，但 Agent 工具被強制移除\n"
                "（subagent 不能生成 sub-subagent）。\n"
                "A 錯：這是最常見的陷阱！省略 tools ≠ 沒有工具。\n"
                "C 錯：Agent 會被移除。\n"
                "D 錯：沒有「預設唯讀」的行為。"
            ),
        },
    ]
    
    score = 0
    for q in questions:
        print(f"\n{'─' * 50}")
        print(f"Q{q['id']} [{q['scenario']}]")
        print(q["question"])
        for k, v in q["options"].items():
            marker = "→" if k == q["correct"] else " "
            print(f"  {marker} {k}. {v}")
        print(f"\n✅ 正確答案：{q['correct']}")
        print(f"📝 {q['explanation']}")
        score += 1
    
    print(f"\n{'=' * 70}")
    print(f"📊 模擬考結果：{score}/{len(questions)} ({score*100//len(questions)}%)")
    print(f"{'=' * 70}")


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":
    simulate_odm_multi_agent()
    run_exam_simulation()
    
    print(f"\n{'=' * 70}")
    print("📌 Key Takeaways（考試必記）")
    print(f"{'=' * 70}")
    print("""
1. AgentDefinition 必填：description + prompt（僅此兩個）
2. tools 省略 = 繼承 parent 全部工具（不是「沒有工具」！）
3. Agent/Task 工具被強制從 subagent 移除（不能巢狀）
4. Parent → Subagent 唯一管道：Agent tool 的 prompt 字串
5. Context 必須顯式傳遞（subagent 不繼承 parent history）
6. Content vs Metadata 分離（preserv attribution for synthesis）
7. 上游用 structured summary，下游 context budget 有限時可修剪
8. allowedTools 控制 auto-approve，必須含 Agent 才能自動 invoke
9. 平行 = 一個 response 多個 Agent tool call（不是 asyncio）
10. Dynamic Factory Pattern 根據 runtime 調整 model/prompt/tools
""")
