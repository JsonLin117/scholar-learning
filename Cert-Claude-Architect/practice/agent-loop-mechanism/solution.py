"""
Agent Loop 機制 — 模擬練習
=========================
模擬 Claude API 的 Agent Loop 協議驅動機制。

場景：ODM 智慧缺料告警 Agent
- 查 SAP 庫存 → 查 AVL 供應商 → 查 Lead Time → 判斷嚴重程度 → 發告警

重點：
1. stop_reason 是唯一的迴圈控制信號
2. 工具結果必須附回完整對話歷史
3. 展示三大反模式 vs 正確實作

日期：2026-06-01
"""

import json
from dataclasses import dataclass, field
from typing import Any
from enum import Enum

# ============================================================
# Part 1: 模擬 Claude API 的核心結構
# ============================================================

class StopReason(Enum):
    TOOL_USE = "tool_use"
    END_TURN = "end_turn"
    MAX_TOKENS = "max_tokens"

@dataclass
class ToolCall:
    id: str
    name: str
    input: dict

@dataclass
class ToolResult:
    tool_use_id: str
    content: str  # JSON string

@dataclass
class ClaudeResponse:
    stop_reason: StopReason
    text: str = ""
    tool_calls: list = field(default_factory=list)

# ============================================================
# Part 2: ODM 供應鏈工具定義
# ============================================================

# 模擬 SAP/MES 系統的資料
SAP_INVENTORY = {
    "GPU-H100": {"on_hand": 50, "open_po": 200, "required": 500, "safety_stock": 100},
    "RAM-DDR5": {"on_hand": 2000, "open_po": 3000, "required": 4000, "safety_stock": 500},
    "PSU-2400W": {"on_hand": 800, "open_po": 400, "required": 600, "safety_stock": 200},
}

AVL_SUPPLIERS = {
    "GPU-H100": [
        {"vendor": "NVIDIA", "avl_status": "Preferred", "lead_time_weeks": 24},
        {"vendor": "AMD-Alt", "avl_status": "Qualified", "lead_time_weeks": 20},
    ],
    "RAM-DDR5": [
        {"vendor": "Samsung", "avl_status": "Preferred", "lead_time_weeks": 8},
        {"vendor": "SK-Hynix", "avl_status": "Approved", "lead_time_weeks": 10},
        {"vendor": "Micron", "avl_status": "Qualified", "lead_time_weeks": 12},
    ],
    "PSU-2400W": [
        {"vendor": "Delta", "avl_status": "Preferred", "lead_time_weeks": 6},
    ],
}

def execute_tool(name: str, args: dict) -> dict:
    """模擬工具執行（SAP 查詢、AVL 查詢等）"""
    if name == "check_inventory":
        part = args.get("part_number")
        inv = SAP_INVENTORY.get(part, {})
        gap = inv.get("required", 0) - inv.get("on_hand", 0) - inv.get("open_po", 0)
        return {
            "part_number": part,
            **inv,
            "shortage_qty": max(0, gap),
            "has_shortage": gap > 0
        }
    
    elif name == "check_avl":
        part = args.get("part_number")
        suppliers = AVL_SUPPLIERS.get(part, [])
        return {
            "part_number": part,
            "supplier_count": len(suppliers),
            "suppliers": suppliers,
            "has_alternative": len(suppliers) > 1
        }
    
    elif name == "assess_severity":
        shortage_qty = args.get("shortage_qty", 0)
        lead_time = args.get("lead_time_weeks", 0)
        has_alt = args.get("has_alternative", False)
        
        if shortage_qty > 200 and lead_time > 16:
            severity = "CRITICAL"
            action = "War Room + Premium Freight NFO"
        elif shortage_qty > 0 and not has_alt:
            severity = "CRITICAL"
            action = "Expedite PO + Escalate to Procurement VP"
        elif shortage_qty > 100:
            severity = "WARNING"
            action = "Expedite PO + Evaluate Alt-Part"
        elif shortage_qty > 0:
            severity = "WATCH"
            action = "Monitor + Confirm open PO ETA"
        else:
            severity = "OK"
            action = "No action needed"
        
        return {
            "severity": severity,
            "recommended_action": action,
            "shortage_qty": shortage_qty
        }
    
    elif name == "send_alert":
        return {
            "status": "sent",
            "channel": args.get("channel", "teams"),
            "message": args.get("message", "")
        }
    
    return {"error": f"Unknown tool: {name}"}


# ============================================================
# Part 3: 模擬 Claude 的決策邏輯（替代真實 API）
# ============================================================

class MockClaudeAgent:
    """
    模擬 Claude 在 Agent Loop 中的行為。
    真實場景中這是 Claude API 的回應，這裡用程式碼模擬。
    """
    def __init__(self):
        self.step = 0
        self.parts_to_check = ["GPU-H100", "RAM-DDR5", "PSU-2400W"]
        self.current_part_idx = 0
        self.current_part = None
        self.shortage_info = {}
        self.avl_info = {}
        self.alerts_sent = []
    
    def respond(self, messages: list) -> ClaudeResponse:
        """根據對話歷史決定下一步（模擬 Claude API 回應）"""
        
        # 分析最新的工具結果
        last_tool_result = None
        for msg in reversed(messages):
            if isinstance(msg.get("content"), list):
                for block in msg["content"]:
                    if isinstance(block, dict) and block.get("type") == "tool_result":
                        last_tool_result = json.loads(block["content"])
                        break
            if last_tool_result:
                break
        
        # 決策邏輯
        if self.current_part_idx >= len(self.parts_to_check):
            # 所有物料檢查完畢
            summary_lines = []
            for part, info in self.shortage_info.items():
                if info.get("has_shortage"):
                    summary_lines.append(
                        f"  🔴 {part}: 缺料 {info['shortage_qty']} 件"
                    )
                else:
                    summary_lines.append(f"  ✅ {part}: 庫存充足")
            
            summary = "缺料檢查完成：\n" + "\n".join(summary_lines)
            if self.alerts_sent:
                summary += f"\n\n已發送 {len(self.alerts_sent)} 個告警。"
            
            return ClaudeResponse(
                stop_reason=StopReason.END_TURN,  # ← 這是結束信號！
                text=summary
            )
        
        self.current_part = self.parts_to_check[self.current_part_idx]
        
        # Step 1: 查庫存
        if self.current_part not in self.shortage_info:
            return ClaudeResponse(
                stop_reason=StopReason.TOOL_USE,
                tool_calls=[ToolCall(
                    id=f"call_{self.step}",
                    name="check_inventory",
                    input={"part_number": self.current_part}
                )]
            )
        
        # Step 2: 如果有缺料，查 AVL
        if (self.shortage_info[self.current_part].get("has_shortage") 
            and self.current_part not in self.avl_info):
            return ClaudeResponse(
                stop_reason=StopReason.TOOL_USE,
                tool_calls=[ToolCall(
                    id=f"call_{self.step}",
                    name="check_avl",
                    input={"part_number": self.current_part}
                )]
            )
        
        # Step 3: 評估嚴重程度
        if self.shortage_info[self.current_part].get("has_shortage"):
            avl = self.avl_info.get(self.current_part, {})
            shortage = self.shortage_info[self.current_part]
            
            if last_tool_result and "severity" in last_tool_result:
                # 已評估完，如果是 CRITICAL 或 WARNING 就發告警
                if last_tool_result["severity"] in ("CRITICAL", "WARNING"):
                    if self.current_part not in [a["part"] for a in self.alerts_sent]:
                        return ClaudeResponse(
                            stop_reason=StopReason.TOOL_USE,
                            tool_calls=[ToolCall(
                                id=f"call_{self.step}",
                                name="send_alert",
                                input={
                                    "channel": "teams",
                                    "message": (
                                        f"⚠️ {last_tool_result['severity']}: "
                                        f"{self.current_part} 缺料 {shortage['shortage_qty']} 件\n"
                                        f"建議：{last_tool_result['recommended_action']}"
                                    )
                                }
                            )]
                        )
                
                # 已處理完這個物料，移到下一個
                self.current_part_idx += 1
                self.step += 1
                return self.respond(messages)  # 遞迴處理下一個
            
            # 還沒評估嚴重程度
            suppliers = avl.get("suppliers", [])
            min_lt = min((s["lead_time_weeks"] for s in suppliers), default=52)
            return ClaudeResponse(
                stop_reason=StopReason.TOOL_USE,
                tool_calls=[ToolCall(
                    id=f"call_{self.step}",
                    name="assess_severity",
                    input={
                        "shortage_qty": shortage["shortage_qty"],
                        "lead_time_weeks": min_lt,
                        "has_alternative": avl.get("has_alternative", False)
                    }
                )]
            )
        else:
            # 沒缺料，下一個物料
            self.current_part_idx += 1
            self.step += 1
            return self.respond(messages)
    
    def process_tool_result(self, tool_name: str, result: dict):
        """處理工具結果（更新內部狀態）"""
        self.step += 1
        if tool_name == "check_inventory":
            self.shortage_info[result["part_number"]] = result
        elif tool_name == "check_avl":
            self.avl_info[result["part_number"]] = result
        elif tool_name == "send_alert":
            self.alerts_sent.append({"part": self.current_part, "status": result["status"]})


# ============================================================
# Part 4: 正確的 Agent Loop 實作
# ============================================================

def correct_agent_loop():
    """
    ✅ 正確實作：用 stop_reason 控制迴圈
    """
    print("=" * 60)
    print("✅ 正確的 Agent Loop 實作")
    print("=" * 60)
    
    agent = MockClaudeAgent()
    messages = [{"role": "user", "content": "檢查所有關鍵物料的缺料狀態，有缺料就發告警"}]
    
    iteration = 0
    max_safety_guard = 20  # 安全護欄（不是主要控制！）
    
    while iteration < max_safety_guard:
        iteration += 1
        response = agent.respond(messages)
        
        print(f"\n--- Iteration {iteration} ---")
        print(f"  stop_reason: {response.stop_reason.value}")
        
        # ✅ 唯一的控制信號：stop_reason
        if response.stop_reason == StopReason.END_TURN:
            print(f"  🏁 Agent 決定結束")
            print(f"  Final: {response.text}")
            break
        
        if response.stop_reason == StopReason.TOOL_USE:
            for tc in response.tool_calls:
                print(f"  🔧 Tool: {tc.name}({tc.input})")
                
                # 執行工具
                result = execute_tool(tc.name, tc.input)
                print(f"  📤 Result: {json.dumps(result, ensure_ascii=False)[:120]}...")
                
                # 附回對話歷史
                messages.append({
                    "role": "assistant",
                    "content": [{"type": "tool_use", "id": tc.id, "name": tc.name, "input": tc.input}]
                })
                messages.append({
                    "role": "user",
                    "content": [{"type": "tool_result", "tool_use_id": tc.id, 
                                "content": json.dumps(result, ensure_ascii=False)}]
                })
                
                # 更新 agent 狀態（模擬 Claude 看到工具結果後的推理）
                agent.process_tool_result(tc.name, result)
    else:
        print(f"\n⚠️ 安全護欄觸發（{max_safety_guard} 次迭代），強制結束")
    
    print(f"\n📊 迴圈統計：")
    print(f"  總迭代次數：{iteration}")
    print(f"  Messages 歷史長度：{len(messages)}")
    print(f"  工具呼叫次數：{sum(1 for m in messages if isinstance(m.get('content'), list) and any(b.get('type') == 'tool_use' for b in m['content'] if isinstance(b, dict)))}")
    
    return messages


# ============================================================
# Part 5: 三大反模式示範
# ============================================================

def anti_pattern_1_text_parsing():
    """
    ❌ 反模式 1：解析自然語言判斷結束
    """
    print("\n" + "=" * 60)
    print("❌ 反模式 1：解析自然語言判斷結束")
    print("=" * 60)
    
    # 模擬：Claude 說了「完成」但其實還需要呼叫工具的情況
    fake_responses = [
        {"text": "我先完成初步分析...", "stop_reason": "tool_use", "needs_tool": True},
        {"text": "分析完成，但讓我再確認一下缺料數量", "stop_reason": "tool_use", "needs_tool": True},
        {"text": "所有檢查已完成。", "stop_reason": "end_turn", "needs_tool": False},
    ]
    
    # ❌ 錯誤寫法：用文本判斷
    print("\n  用文本 '完成' 判斷：")
    for i, resp in enumerate(fake_responses):
        if "完成" in resp["text"]:
            print(f"  Step {i+1}: 偵測到 '完成' → 停止迴圈 ❌")
            print(f"    但 stop_reason 是 '{resp['stop_reason']}'!")
            if resp["needs_tool"]:
                print(f"    ⚠️ 過早停止！Agent 還需要呼叫工具！")
            break
    
    # ✅ 正確寫法：用 stop_reason 判斷
    print("\n  用 stop_reason 判斷：")
    for i, resp in enumerate(fake_responses):
        if resp["stop_reason"] == "end_turn":
            print(f"  Step {i+1}: stop_reason=end_turn → 正確停止 ✅")
            break
        else:
            print(f"  Step {i+1}: stop_reason=tool_use → 繼續執行工具")


def anti_pattern_2_fixed_iterations():
    """
    ❌ 反模式 2：固定迭代次數作為主要控制
    """
    print("\n" + "=" * 60)
    print("❌ 反模式 2：固定迭代次數作為主要控制")
    print("=" * 60)
    
    # 有些任務只需 1 步，有些需要 8 步
    tasks = [
        {"name": "查單一物料庫存", "actual_steps": 1},
        {"name": "查 3 個物料 + AVL + 告警", "actual_steps": 8},
        {"name": "查全廠所有物料 + 供應商", "actual_steps": 15},
    ]
    
    max_iter = 3  # ❌ 固定 3 次
    
    print(f"\n  固定上限 = {max_iter} 次：")
    for task in tasks:
        completed = min(task["actual_steps"], max_iter)
        status = "✅ 完成" if completed >= task["actual_steps"] else "❌ 被截斷"
        print(f"  {task['name']}：需要 {task['actual_steps']} 步，執行了 {completed} 步 → {status}")
    
    print(f"\n  ✅ 正確做法：stop_reason 控制 + 安全護欄（如 max=50）")


def anti_pattern_3_content_check():
    """
    ❌ 反模式 3：檢查文本內容作為完成指標
    """
    print("\n" + "=" * 60)
    print("❌ 反模式 3：檢查文本內容作為完成指標")
    print("=" * 60)
    
    completion_phrases = ["All set", "Here's the result", "I have completed"]
    
    # 模擬：Claude 在產生摘要（含 "Here's the result"）同時還需要呼叫工具
    response_text = "Here's the result of the inventory check. Let me also verify the supplier status."
    stop_reason = "tool_use"  # 注意：stop_reason 說還要繼續！
    
    print(f"\n  Response text: \"{response_text[:60]}...\"")
    print(f"  stop_reason: \"{stop_reason}\"")
    
    # ❌ 文本檢查會被誤導
    for phrase in completion_phrases:
        if phrase in response_text:
            print(f"\n  ❌ 偵測到 '{phrase}' → 錯誤停止！")
            print(f"     但 stop_reason='tool_use'，Agent 還需要查供應商！")
            break
    
    print(f"\n  ✅ 正確做法：只看 stop_reason='{stop_reason}' → 繼續執行工具")


# ============================================================
# Part 6: 主程式
# ============================================================

if __name__ == "__main__":
    print("🤖 Agent Loop 機制 — ODM 缺料告警模擬")
    print("=" * 60)
    
    # 正確實作
    messages = correct_agent_loop()
    
    # 三大反模式示範
    anti_pattern_1_text_parsing()
    anti_pattern_2_fixed_iterations()
    anti_pattern_3_content_check()
    
    # 統計
    print("\n" + "=" * 60)
    print("📊 Agent Loop 關鍵數字")
    print("=" * 60)
    print(f"  對話歷史 messages 數量：{len(messages)}")
    print(f"  API stateless 設計 → 每次呼叫送完整 {len(messages)} 條 messages")
    print(f"  stop_reason 類型使用：")
    print(f"    tool_use（繼續）：{sum(1 for m in messages if isinstance(m.get('content'), list) and any(isinstance(b, dict) and b.get('type') == 'tool_use' for b in m['content']))}")
    print(f"    end_turn（結束）：1")
    print(f"\n✅ 考試重點回顧：")
    print(f"  1. stop_reason 是唯一控制信號 ← 不是文本、不是計數")
    print(f"  2. 工具結果附回完整對話歷史 ← API 無狀態")
    print(f"  3. 三大反模式 ← 文本解析 / 固定迭代 / 內容檢查")
