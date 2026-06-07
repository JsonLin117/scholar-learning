"""
Hub-and-Spoke 多 Agent 架構模擬器
==================================
模擬 Claude Agent SDK 的 Hub-and-Spoke 多代理編排：
- AgentDefinition 配置（description, prompt, tools, model）
- Coordinator 任務分解 → 平行/串行 Subagent 呼叫
- Context 隔離（subagent 不繼承 coordinator 歷史）
- 顯式 Context 傳遞（結構化 JSON）
- PreToolUse / PostToolUse Hooks
- Structured Handoff 升級協議

ODM 場景：PO 缺料分析 → 同時查 Inventory + AVL + MRP → 綜合決策 → 升級
"""

import json
import time
import threading
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Optional
from enum import Enum


# ============================================================
# Part 1: AgentDefinition — 模擬 Claude Agent SDK 配置
# ============================================================

@dataclass
class AgentDefinition:
    """模擬 Claude Agent SDK 的 AgentDefinition。
    
    考試重點：
    - description（必填）：Coordinator 據此決定何時 invoke
    - prompt（必填）：Subagent 的 system prompt
    - tools（選填）：限制工具集（最小權限）
    - disallowed_tools（選填）：移除特定工具
    - model（選填）：可用不同模型（簡單任務用便宜模型）
    """
    name: str
    description: str
    prompt: str
    tools: list[str] = field(default_factory=list)
    disallowed_tools: list[str] = field(default_factory=list)
    model: str = "sonnet"

    def __post_init__(self):
        if not self.description:
            raise ValueError("description 是必填的！Coordinator 靠它決定何時 invoke")
        if not self.prompt:
            raise ValueError("prompt 是必填的！定義 subagent 的角色和行為")


# ============================================================
# Part 2: 模擬工具和資料源
# ============================================================

# ODM 模擬資料
INVENTORY_DB = {
    "M-GPU-H100": {"on_hand": 50, "allocated": 45, "safety_stock": 20,
                    "last_updated": "2026-06-08T06:00:00Z"},
    "M-CPU-EPYC": {"on_hand": 200, "allocated": 80, "safety_stock": 50,
                    "last_updated": "1717833600"},  # Unix timestamp (故意不同格式)
    "M-MEM-DDR5": {"on_hand": 1000, "allocated": 950, "safety_stock": 100,
                    "last_updated": "Jun 7, 2026"},  # 又一種格式
    "M-SSD-PM9A3": {"on_hand": 300, "allocated": 100, "safety_stock": 80,
                     "last_updated": "2026-06-08T05:30:00Z"},
}

AVL_DB = {
    "M-GPU-H100": [
        {"vendor": "V-NVIDIA", "avl_status": "Active", "lead_time_days": 45,
         "moq": 100, "price_usd": 25000, "last_delivery_otif": 0.82},
        {"vendor": "V-AMD-MI300", "avl_status": "Qualifying", "lead_time_days": 60,
         "moq": 50, "price_usd": 22000, "last_delivery_otif": None},
    ],
    "M-CPU-EPYC": [
        {"vendor": "V-AMD", "avl_status": "Active", "lead_time_days": 21,
         "moq": 200, "price_usd": 4500, "last_delivery_otif": 0.95},
    ],
    "M-MEM-DDR5": [
        {"vendor": "V-SAMSUNG", "avl_status": "Active", "lead_time_days": 14,
         "moq": 500, "price_usd": 120, "last_delivery_otif": 0.98},
        {"vendor": "V-SK-HYNIX", "avl_status": "Active", "lead_time_days": 18,
         "moq": 500, "price_usd": 115, "last_delivery_otif": 0.96},
    ],
    "M-SSD-PM9A3": [
        {"vendor": "V-SAMSUNG", "avl_status": "Active", "lead_time_days": 10,
         "moq": 100, "price_usd": 280, "last_delivery_otif": 0.97},
    ],
}

MRP_DB = {
    "M-GPU-H100": [
        {"po_number": "PO-2026-1001", "vendor": "V-NVIDIA", "qty": 200,
         "eta": "2026-07-15", "status": "Confirmed"},
    ],
    "M-CPU-EPYC": [
        {"po_number": "PO-2026-1002", "vendor": "V-AMD", "qty": 500,
         "eta": "2026-06-20", "status": "Shipped"},
    ],
    "M-MEM-DDR5": [],  # 沒有在途 PO！
    "M-SSD-PM9A3": [
        {"po_number": "PO-2026-1003", "vendor": "V-SAMSUNG", "qty": 400,
         "eta": "2026-06-12", "status": "Confirmed"},
    ],
}


def tool_query_inventory(material_id: str) -> dict:
    """模擬庫存查詢工具"""
    if material_id not in INVENTORY_DB:
        return {"isError": True, "errorCategory": "not_found",
                "message": f"Material {material_id} not found", "isRetryable": False}
    data = INVENTORY_DB[material_id].copy()
    data["material_id"] = material_id
    data["available"] = data["on_hand"] - data["allocated"]
    data["below_safety_stock"] = data["available"] < data["safety_stock"]
    return data


def tool_query_avl(material_id: str) -> dict:
    """模擬 AVL 查詢工具"""
    if material_id not in AVL_DB:
        return {"isError": True, "errorCategory": "not_found",
                "message": f"No AVL records for {material_id}", "isRetryable": False}
    return {"material_id": material_id, "vendors": AVL_DB[material_id]}


def tool_query_mrp(material_id: str) -> dict:
    """模擬 MRP 查詢工具"""
    if material_id not in MRP_DB:
        return {"isError": True, "errorCategory": "not_found",
                "message": f"No MRP data for {material_id}", "isRetryable": False}
    return {"material_id": material_id, "open_pos": MRP_DB[material_id],
            "total_in_transit": sum(po["qty"] for po in MRP_DB[material_id])}


# ============================================================
# Part 3: Hook 系統 — PreToolUse / PostToolUse
# ============================================================

class HookAction(Enum):
    ALLOW = "allow"
    BLOCK = "block"
    REDIRECT = "redirect"


@dataclass
class HookResult:
    action: HookAction
    reason: str = ""
    redirect_to: str = ""
    modified_result: Optional[dict] = None


def pre_tool_use_hook(tool_name: str, tool_args: dict, context: dict) -> HookResult:
    """PreToolUse Hook：在工具執行前攔截。
    
    考試重點：
    - Hook = 確定性 100%（vs Prompt 指令 = 概率性）
    - 用於「必須保證」的業務規則（財務、安全、合規）
    """
    # 規則 1：擋住超過 $500K 的緊急採購（強制人工審批）
    if tool_name == "create_emergency_po":
        amount = tool_args.get("total_amount_usd", 0)
        if amount > 500_000:
            return HookResult(
                action=HookAction.BLOCK,
                reason=f"Emergency PO ${amount:,.0f} exceeds $500K limit. Requires human approval.",
                redirect_to="escalate_to_procurement_manager"
            )

    # 規則 2：必須先查客戶才能退款（身分驗證前置）
    if tool_name == "process_refund":
        if not context.get("customer_verified"):
            return HookResult(
                action=HookAction.REDIRECT,
                reason="Customer must be verified before processing refund",
                redirect_to="get_customer"
            )

    return HookResult(action=HookAction.ALLOW)


def post_tool_use_hook(tool_name: str, tool_result: dict) -> dict:
    """PostToolUse Hook：在工具結果回傳給模型前正規化。
    
    考試重點：
    - 統一來自不同 MCP 工具的日期格式
    - Unix timestamp → ISO 8601
    - "Jun 7, 2026" → ISO 8601
    - 模型看到一致的格式，推理更準確
    """
    def normalize_date(value):
        if isinstance(value, (int, float)):
            # Unix timestamp → ISO 8601
            return datetime.utcfromtimestamp(value).strftime("%Y-%m-%dT%H:%M:%SZ")
        if isinstance(value, str):
            # Try common formats
            for fmt in ["%b %d, %Y", "%B %d, %Y", "%m/%d/%Y"]:
                try:
                    return datetime.strptime(value, fmt).strftime("%Y-%m-%dT%H:%M:%SZ")
                except ValueError:
                    continue
            # Already ISO? Return as-is
            if "T" in value or len(value) == 10:
                return value
        return value

    # 遞迴正規化所有日期欄位
    normalized = {}
    for key, value in tool_result.items():
        if any(date_hint in key.lower() for date_hint in ["date", "time", "eta", "updated"]):
            normalized[key] = normalize_date(value)
        elif isinstance(value, dict):
            normalized[key] = post_tool_use_hook(tool_name, value)
        elif isinstance(value, list):
            normalized[key] = [
                post_tool_use_hook(tool_name, item) if isinstance(item, dict) else item
                for item in value
            ]
        else:
            normalized[key] = value
    return normalized


# ============================================================
# Part 4: Subagent 執行引擎（模擬 Agent Loop）
# ============================================================

@dataclass
class SubagentResult:
    """Subagent 回傳給 Coordinator 的結果。
    只有最終結果，不包含中間工具呼叫（context 隔離！）
    """
    agent_name: str
    success: bool
    result: dict
    execution_time_ms: int
    model_used: str
    tool_calls_count: int  # 顯示 subagent 內部跑了幾次 agent loop


class SubagentExecutor:
    """模擬 subagent 的 Agent Loop 執行。"""
    
    def __init__(self, definition: AgentDefinition):
        self.definition = definition
        self.conversation_history = []  # ← 每個 subagent 有自己的獨立歷史！
        self.tool_calls = 0
    
    def execute(self, task_prompt: str, tools_registry: dict) -> SubagentResult:
        """
        模擬 subagent 的 agent loop。
        
        關鍵：task_prompt 是唯一的 context 來源。
        Subagent 不知道 coordinator 的對話歷史。
        """
        start = time.time()
        
        # Step 1: 初始化 conversation（只有 system prompt + task prompt）
        self.conversation_history = [
            {"role": "system", "content": self.definition.prompt},
            {"role": "user", "content": task_prompt}
        ]
        
        # Step 2: 從 task_prompt 提取需要查詢的 material_id
        # （在真實 SDK 中，Claude 會從 prompt 推理出要呼叫哪個工具）
        material_id = None
        for word in task_prompt.split():
            if word.startswith("M-"):
                material_id = word.rstrip("。,;")
                break
        
        if not material_id:
            return SubagentResult(
                agent_name=self.definition.name,
                success=False,
                result={"error": "No material_id found in task prompt"},
                execution_time_ms=0, model_used=self.definition.model, tool_calls_count=0
            )
        
        # Step 3: 根據 allowed tools 執行查詢
        result = {}
        for tool_name in self.definition.tools:
            if tool_name in tools_registry:
                # PreToolUse Hook
                hook_result = pre_tool_use_hook(tool_name, {"material_id": material_id}, {})
                if hook_result.action == HookAction.BLOCK:
                    result["blocked"] = hook_result.reason
                    continue
                
                # 執行工具
                raw_result = tools_registry[tool_name](material_id)
                self.tool_calls += 1
                
                # PostToolUse Hook（正規化日期）
                if isinstance(raw_result, dict) and "isError" not in raw_result:
                    raw_result = post_tool_use_hook(tool_name, raw_result)
                
                # 加入 conversation history（subagent 自己的）
                self.conversation_history.append({
                    "role": "tool_result",
                    "tool": tool_name,
                    "content": raw_result
                })
                
                result = raw_result
                break  # 每個 subagent 只用一個主要工具
        
        elapsed = int((time.time() - start) * 1000)
        
        return SubagentResult(
            agent_name=self.definition.name,
            success="isError" not in result,
            result=result,
            execution_time_ms=elapsed,
            model_used=self.definition.model,
            tool_calls_count=self.tool_calls
        )


# ============================================================
# Part 5: Coordinator — Hub-and-Spoke 核心
# ============================================================

class Coordinator:
    """模擬 Hub-and-Spoke 的 Coordinator Agent。"""
    
    def __init__(self):
        self.conversation_history = []
        self.subagent_definitions = {}
        self.tools_registry = {
            "query_inventory": tool_query_inventory,
            "query_avl": tool_query_avl,
            "query_mrp": tool_query_mrp,
        }
        self._define_subagents()
    
    def _define_subagents(self):
        """定義所有 subagent（AgentDefinition 配置）。"""
        
        self.subagent_definitions["inventory-checker"] = AgentDefinition(
            name="inventory-checker",
            description="查詢物料庫存水位。當需要知道 on-hand/allocated/available 時使用。",
            prompt=("你是庫存查詢專家。查詢物料的即時庫存，標記低於安全庫存的項目。"
                    "輸出 JSON：{material_id, on_hand, allocated, available, below_safety_stock}"),
            tools=["query_inventory"],  # 最小權限：只能查庫存
            model="haiku"  # 簡單查詢用便宜模型
        )
        
        self.subagent_definitions["avl-checker"] = AgentDefinition(
            name="avl-checker",
            description="查詢供應商 AVL 狀態和 Lead Time。當需要知道備選供應商時使用。",
            prompt=("你是供應商管理專家。查詢物料的 AVL 記錄，回報 Active 供應商的 Lead Time 和 OTIF。"
                    "輸出 JSON：{material_id, vendors: [{vendor, avl_status, lead_time_days, otif}]}"),
            tools=["query_avl"],
            model="haiku"
        )
        
        self.subagent_definitions["mrp-checker"] = AgentDefinition(
            name="mrp-checker",
            description="查詢 MRP 計劃中的在途 PO。當需要知道預計到貨時使用。",
            prompt=("你是 MRP 計劃分析師。查詢物料的 open PO，計算在途總量和最近 ETA。"
                    "輸出 JSON：{material_id, open_pos: [{po_number, vendor, qty, eta, status}], total_in_transit}"),
            tools=["query_mrp"],
            model="haiku"
        )
    
    def analyze_shortage(self, materials: list[str]) -> dict:
        """
        Coordinator 主流程：
        1. 任務分解
        2. 平行 spawn subagents（每個 material × 3 個 subagent）
        3. 等待結果
        4. 聚合分析
        5. 生成決策和可能的 handoff
        """
        print("=" * 70)
        print("🎯 COORDINATOR: 缺料分析系統啟動")
        print(f"   分析物料：{materials}")
        print("=" * 70)
        
        # --- Step 1: 任務分解 ---
        print("\n📋 Step 1: 任務分解")
        tasks = []
        for mat in materials:
            for agent_name in ["inventory-checker", "avl-checker", "mrp-checker"]:
                tasks.append((agent_name, mat))
        print(f"   拆分為 {len(tasks)} 個子任務（{len(materials)} 物料 × 3 查詢維度）")
        print(f"   執行模式：平行（所有子任務同時執行）")
        
        # --- Step 2: 平行 spawn subagents ---
        print("\n🚀 Step 2: 平行 spawn subagents")
        print("   ⚡ 在一個 turn 中發出多個 Task/Agent tool call → 同時執行")
        
        results: dict[str, dict[str, SubagentResult]] = {mat: {} for mat in materials}
        threads = []
        
        for agent_name, mat in tasks:
            definition = self.subagent_definitions[agent_name]
            executor = SubagentExecutor(definition)
            
            # 顯式 Context 傳遞（不是傳 coordinator 的對話歷史！）
            task_prompt = self._build_task_prompt(agent_name, mat)
            
            def run_subagent(exec=executor, prompt=task_prompt, 
                           name=agent_name, material=mat):
                result = exec.execute(prompt, self.tools_registry)
                results[material][name] = result
            
            t = threading.Thread(target=run_subagent)
            threads.append(t)
            t.start()
        
        # 等待所有 subagent 完成
        for t in threads:
            t.join()
        
        # --- Step 3: 展示 context 隔離 ---
        print("\n🔒 Step 3: Context 隔離驗證")
        print("   Coordinator 對話歷史長度：", len(self.conversation_history))
        print("   Subagent 只回傳最終結果，不暴露中間工具呼叫")
        for mat in materials:
            for agent_name, sr in results[mat].items():
                print(f"   [{agent_name}] {mat}: "
                      f"tool_calls={sr.tool_calls_count}, model={sr.model_used}, "
                      f"time={sr.execution_time_ms}ms")
        
        # --- Step 4: 聚合分析 ---
        print("\n📊 Step 4: 聚合分析（Coordinator 整合所有 subagent 結果）")
        analysis = self._aggregate_results(materials, results)
        
        # --- Step 5: 決策和 Handoff ---
        print("\n🏁 Step 5: 決策和升級判斷")
        handoffs = self._check_escalation(analysis)
        
        return {"analysis": analysis, "handoffs": handoffs}
    
    def _build_task_prompt(self, agent_name: str, material_id: str) -> str:
        """
        建構 subagent 的 task prompt。
        
        考試重點：
        - 不傳 coordinator 的對話歷史
        - 用結構化格式，分離 content 和 metadata
        - 指定目標和品質標準（不是步驟指令）
        """
        base = f"查詢物料 {material_id} 的"
        
        if agent_name == "inventory-checker":
            return (f"{base}即時庫存。"
                    f"目標：確定可用量是否低於安全庫存。"
                    f"品質標準：必須回報 on_hand, allocated, available, below_safety_stock。")
        elif agent_name == "avl-checker":
            return (f"{base}供應商 AVL 記錄。"
                    f"目標：列出所有 Active 供應商及其 Lead Time。"
                    f"品質標準：包含 OTIF 績效數據。")
        elif agent_name == "mrp-checker":
            return (f"{base}MRP 在途訂單。"
                    f"目標：計算在途總量和最近 ETA。"
                    f"品質標準：包含 PO 狀態（Confirmed/Shipped）。")
        return base
    
    def _aggregate_results(self, materials, results) -> list[dict]:
        """Coordinator 聚合所有 subagent 結果，生成綜合分析。"""
        analysis = []
        
        for mat in materials:
            inv = results[mat].get("inventory-checker")
            avl = results[mat].get("avl-checker")
            mrp = results[mat].get("mrp-checker")
            
            inv_data = inv.result if inv and inv.success else {}
            avl_data = avl.result if avl and avl.success else {}
            mrp_data = mrp.result if mrp and mrp.success else {}
            
            available = inv_data.get("available", 0)
            safety = inv_data.get("safety_stock", 0)
            below_safety = inv_data.get("below_safety_stock", False)
            in_transit = mrp_data.get("total_in_transit", 0)
            active_vendors = [v for v in avl_data.get("vendors", []) 
                            if v.get("avl_status") == "Active"]
            
            # 風險評估
            if available <= 0:
                risk = "🔴 CRITICAL"
                action = "IMMEDIATE_EXPEDITE"
            elif below_safety and in_transit == 0:
                risk = "🔴 HIGH"
                action = "EMERGENCY_PO"
            elif below_safety and in_transit > 0:
                risk = "🟡 MEDIUM"
                action = "MONITOR_AND_EXPEDITE"
            else:
                risk = "🟢 LOW"
                action = "NORMAL"
            
            entry = {
                "material_id": mat,
                "risk_level": risk,
                "recommended_action": action,
                "available_qty": available,
                "safety_stock": safety,
                "below_safety_stock": below_safety,
                "in_transit_qty": in_transit,
                "active_vendor_count": len(active_vendors),
                "shortest_lead_time": min((v["lead_time_days"] for v in active_vendors), default=None),
                "single_source": len(active_vendors) <= 1,
            }
            analysis.append(entry)
            
            print(f"\n   {mat}:")
            print(f"     風險等級：{risk}")
            print(f"     可用量：{available}（安全庫存：{safety}）")
            print(f"     在途量：{in_transit}")
            print(f"     Active 供應商：{len(active_vendors)} 家"
                  f"{'（⚠️ 單一來源風險！）' if entry['single_source'] else ''}")
            print(f"     建議行動：{action}")
        
        return analysis
    
    def _check_escalation(self, analysis: list[dict]) -> list[dict]:
        """
        檢查是否需要升級（Structured Handoff）。
        
        考試重點：
        - Handoff 摘要必須結構化（不是對話紀錄）
        - 包含：who, what, why, recommended_action
        - 接手的人類沒有 AI 對話歷史，需要自足的摘要
        """
        handoffs = []
        
        for item in analysis:
            if item["recommended_action"] in ("IMMEDIATE_EXPEDITE", "EMERGENCY_PO"):
                # 計算估計金額（用於 Hook 檢查）
                avl_info = AVL_DB.get(item["material_id"], [])
                active = [v for v in avl_info if v["avl_status"] == "Active"]
                if active:
                    unit_price = active[0]["price_usd"]
                    needed_qty = item["safety_stock"] - item["available_qty"]
                    moq = active[0]["moq"]
                    order_qty = max(needed_qty, moq)
                    total_amount = order_qty * unit_price
                else:
                    total_amount = 0
                    order_qty = 0
                
                # PreToolUse Hook 攔截
                hook_result = pre_tool_use_hook(
                    "create_emergency_po",
                    {"total_amount_usd": total_amount, "material_id": item["material_id"]},
                    {}
                )
                
                if hook_result.action == HookAction.BLOCK:
                    print(f"\n   🚫 HOOK BLOCKED: {hook_result.reason}")
                    print(f"   → 重導向至：{hook_result.redirect_to}")
                
                handoff = {
                    "material_id": item["material_id"],
                    "escalation_level": "PROCUREMENT_MANAGER",
                    "risk_level": item["risk_level"],
                    "root_cause": f"可用量 {item['available_qty']} 低於安全庫存 {item['safety_stock']}，"
                                  f"在途量 {item['in_transit_qty']}",
                    "recommended_action": item["recommended_action"],
                    "estimated_po_amount_usd": total_amount,
                    "estimated_order_qty": order_qty,
                    "hook_blocked": hook_result.action == HookAction.BLOCK,
                    "hook_reason": hook_result.reason if hook_result.action == HookAction.BLOCK else None,
                    "single_source_risk": item["single_source"],
                    "timestamp": datetime.now().isoformat(),
                }
                handoffs.append(handoff)
                
                print(f"\n   📋 STRUCTURED HANDOFF for {item['material_id']}:")
                print(f"     升級至：{handoff['escalation_level']}")
                print(f"     根因：{handoff['root_cause']}")
                print(f"     建議行動：{handoff['recommended_action']}")
                print(f"     預估金額：${total_amount:,.0f}")
                if handoff['hook_blocked']:
                    print(f"     ⚠️ Hook 攔截：需人工審批")
        
        return handoffs


# ============================================================
# Part 6: PostToolUse Hook 示範（日期正規化）
# ============================================================

def demo_post_tool_use_normalization():
    """示範 PostToolUse Hook 如何統一不同系統的日期格式。"""
    print("\n" + "=" * 70)
    print("🔧 DEMO: PostToolUse Hook — 日期格式正規化")
    print("=" * 70)
    
    raw_results = [
        {"material_id": "M-GPU-H100", "last_updated": "2026-06-08T06:00:00Z"},  # ISO
        {"material_id": "M-CPU-EPYC", "last_updated": 1717833600},              # Unix
        {"material_id": "M-MEM-DDR5", "last_updated": "Jun 7, 2026"},           # Human
    ]
    
    print("\n   Before PostToolUse Hook:")
    for r in raw_results:
        print(f"     {r['material_id']}: {r['last_updated']} ({type(r['last_updated']).__name__})")
    
    normalized = [post_tool_use_hook("query_inventory", r) for r in raw_results]
    
    print("\n   After PostToolUse Hook:")
    for r in normalized:
        print(f"     {r['material_id']}: {r['last_updated']} (str, ISO 8601)")
    
    print("\n   ✅ 模型現在看到一致的 ISO 8601 格式，推理更準確")


# ============================================================
# Part 7: Context 隔離驗證
# ============================================================

def demo_context_isolation():
    """驗證 subagent 不繼承 coordinator 的對話歷史。"""
    print("\n" + "=" * 70)
    print("🔒 DEMO: Context 隔離驗證")
    print("=" * 70)
    
    # 模擬 coordinator 有一段對話歷史
    coordinator_history = [
        {"role": "user", "content": "分析 M-GPU-H100 的缺料風險"},
        {"role": "assistant", "content": "我來查詢三個維度..."},
        {"role": "tool_result", "content": "上一輪查詢結果..."},
        {"role": "assistant", "content": "根據結果，GPU 缺料嚴重..."},
        {"role": "user", "content": "那 DDR5 呢？也一起查"},
    ]
    
    # Spawn subagent
    definition = AgentDefinition(
        name="test-subagent",
        description="測試用 subagent",
        prompt="你是測試助手",
        tools=["query_inventory"]
    )
    executor = SubagentExecutor(definition)
    
    # Subagent 的 task prompt 只包含 coordinator 顯式傳遞的內容
    task_prompt = "查詢物料 M-MEM-DDR5 的庫存。目標：確認可用量。"
    
    # 執行前
    print(f"\n   Coordinator 對話歷史：{len(coordinator_history)} 條訊息")
    print(f"   Subagent 對話歷史（執行前）：{len(executor.conversation_history)} 條")
    
    result = executor.execute(task_prompt, {"query_inventory": tool_query_inventory})
    
    # 執行後
    print(f"   Subagent 對話歷史（執行後）：{len(executor.conversation_history)} 條")
    print(f"   Subagent 歷史內容：")
    for msg in executor.conversation_history:
        role = msg["role"]
        content = str(msg["content"])[:60]
        print(f"     [{role}] {content}...")
    
    print(f"\n   ❌ Subagent 不知道 coordinator 討論過 M-GPU-H100")
    print(f"   ✅ Subagent 只知道 task prompt 裡的 M-MEM-DDR5")
    print(f"   ✅ Coordinator 歷史不變（仍然 {len(coordinator_history)} 條）")


# ============================================================
# Part 8: 平行 vs 串行對比
# ============================================================

def demo_parallel_vs_sequential():
    """對比平行和串行 subagent 執行的時間差異。"""
    print("\n" + "=" * 70)
    print("⚡ DEMO: 平行 vs 串行 Subagent 執行")
    print("=" * 70)
    
    materials = ["M-GPU-H100", "M-CPU-EPYC", "M-MEM-DDR5"]
    
    # 串行執行
    start = time.time()
    sequential_results = []
    for mat in materials:
        for tool_fn in [tool_query_inventory, tool_query_avl, tool_query_mrp]:
            time.sleep(0.05)  # 模擬網路延遲
            result = tool_fn(mat)
            sequential_results.append(result)
    sequential_time = time.time() - start
    
    # 平行執行
    start = time.time()
    parallel_results = []
    threads = []
    
    def run_tool(fn, mat):
        time.sleep(0.05)
        parallel_results.append(fn(mat))
    
    for mat in materials:
        for tool_fn in [tool_query_inventory, tool_query_avl, tool_query_mrp]:
            t = threading.Thread(target=run_tool, args=(tool_fn, mat))
            threads.append(t)
            t.start()
    for t in threads:
        t.join()
    parallel_time = time.time() - start
    
    print(f"\n   串行（每個 turn 一個 Task）：{sequential_time:.3f}s")
    print(f"   平行（一個 turn 多個 Task）：{parallel_time:.3f}s")
    print(f"   加速比：{sequential_time/parallel_time:.1f}x")
    print(f"\n   考試重點：平行 = 在一個 coordinator turn 中發多個 Task tool call")
    print(f"   串行 = 每個 Task 結果回來後才決定下一個（有依賴關係時用）")


# ============================================================
# Part 9: 考試模擬題
# ============================================================

def exam_simulation():
    """CCA 考試模擬題。"""
    print("\n" + "=" * 70)
    print("🎯 CCA 考試模擬題 — Hub-and-Spoke Multi-Agent")
    print("=" * 70)
    
    questions = [
        {
            "q": ("Q1: 在 Hub-and-Spoke 架構中，subagent 能否看到 coordinator 的對話歷史？"),
            "options": [
                "A: 可以，subagent 自動繼承 coordinator 的完整對話歷史",
                "B: 可以，但只繼承最近 5 輪",
                "C: 不可以，subagent context 完全隔離，必須在 prompt 中顯式傳遞",
                "D: 取決於 AgentDefinition 的 inherit_context 設定"
            ],
            "answer": "C",
            "explain": "Subagent 不自動繼承 coordinator 的對話歷史。所有需要的 context 必須在 task prompt 中顯式提供。"
        },
        {
            "q": ("Q2: 如何讓 coordinator 同時執行多個 subagent？"),
            "options": [
                "A: 設定 parallel=True 參數",
                "B: 在一個 coordinator response 中發出多個 Task/Agent tool call",
                "C: 用 asyncio.gather() 包裝",
                "D: 在 AgentDefinition 中設定 concurrent=True"
            ],
            "answer": "B",
            "explain": "Coordinator 在單一 turn 中回傳多個 Task tool call，SDK 自動並行執行。不是 across separate turns。"
        },
        {
            "q": ("Q3: 客戶支持場景中，退款前必須先驗證客戶身分。應該用什麼機制保證這個順序？"),
            "options": [
                "A: 在 system prompt 中寫 '必須先驗證客戶'",
                "B: 使用 PreToolUse Hook 在 process_refund 執行前檢查 customer_verified",
                "C: 把 get_customer 和 process_refund 放在同一個 subagent 中",
                "D: 在 allowedTools 中不包含 process_refund"
            ],
            "answer": "B",
            "explain": "需要 deterministic compliance（100% 保證）時用 Hook，不是 prompt 指令（概率性）。"
        },
        {
            "q": ("Q4: Coordinator 的 allowedTools 缺少 'Task'（API）或 'Agent'（SDK），會發生什麼？"),
            "options": [
                "A: Coordinator 會用 Bash 工具來 spawn subagent",
                "B: Coordinator 無法 spawn subagent，只能自己執行所有任務",
                "C: SDK 會自動加入 Task 工具",
                "D: 只影響平行呼叫，串行呼叫不受影響"
            ],
            "answer": "B",
            "explain": "allowedTools 必須包含 Task（API）或 Agent（SDK），否則 coordinator 無法委派子任務。"
        },
        {
            "q": ("Q5: 設計 coordinator prompt 時，以下哪種方式最佳？"),
            "options": [
                "A: 詳細的步驟指令：Step 1 做 X，Step 2 做 Y，Step 3 做 Z",
                "B: 只寫 '分析數據'，讓 subagent 自己決定",
                "C: 指定研究目標和品質標準，不指定具體步驟",
                "D: 用 JSON Schema 嚴格定義每個步驟的輸入輸出"
            ],
            "answer": "C",
            "explain": "指定目標+品質標準，讓 subagent 有適應性（adaptability），比步驟指令更靈活。"
        },
    ]
    
    correct = 0
    for q_data in questions:
        print(f"\n{q_data['q']}")
        for opt in q_data["options"]:
            marker = "  ✅" if opt[0] == q_data["answer"] else "  ❌" 
            print(f"  {opt}{marker if True else ''}")
        print(f"\n  正確答案：{q_data['answer']}")
        print(f"  解釋：{q_data['explain']}")
        correct += 1
    
    print(f"\n  模擬結果：{correct}/{len(questions)} 正確")


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":
    print("=" * 70)
    print("Hub-and-Spoke 多 Agent 架構模擬器")
    print("CCA Domain 1 — Task Statements 1.2, 1.3, 1.4, 1.6")
    print("場景：ODM PO 缺料分析")
    print("=" * 70)
    
    # Demo 1: PostToolUse Hook 日期正規化
    demo_post_tool_use_normalization()
    
    # Demo 2: Context 隔離驗證
    demo_context_isolation()
    
    # Demo 3: 平行 vs 串行
    demo_parallel_vs_sequential()
    
    # Demo 4: 完整的 Hub-and-Spoke 缺料分析
    coordinator = Coordinator()
    result = coordinator.analyze_shortage([
        "M-GPU-H100",    # 缺料嚴重（單一來源 + 低庫存）
        "M-MEM-DDR5",    # 缺料（無在途 PO）
        "M-SSD-PM9A3",   # 正常
    ])
    
    # Demo 5: 考試模擬題
    exam_simulation()
    
    # 最終摘要
    print("\n" + "=" * 70)
    print("📚 學習重點摘要")
    print("=" * 70)
    print("""
    1. Hub-and-Spoke = Coordinator + N Subagents，所有通信經 Coordinator
    2. Context 隔離：Subagent 不繼承 Coordinator 歷史，必須顯式傳遞
    3. 平行呼叫：Coordinator 一個 turn 多個 Task → 同時執行
    4. AgentDefinition：description（必填）、prompt（必填）、tools（限權）、model（可選）
    5. Hook vs Prompt：確定性 100% vs 概率性 >90%
    6. Structured Handoff：結構化摘要，接手人不需要對話歷史
    7. Task Decomposition：目標+品質標準 > 步驟指令
    """)
