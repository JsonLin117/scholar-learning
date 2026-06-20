"""
CCA Domain 5: Context Management & Reliability
Practice — Tasks 5.1-5.6 + Mock Exam

ODM 供應鏈場景：採購異常追蹤 Agent
"""

import json
import re
from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime, timedelta
from enum import Enum

# ============================================================
# Module 1: CaseFactsManager — 漸進摘要精度風險 + Case Facts Block
# ============================================================

class CaseFactsManager:
    """
    模擬漸進摘要如何丟失數字精度，
    以及 Case Facts Block 如何解決此問題。
    
    CCA 考點：
    - 漸進摘要將 $127.50 模糊化為「約 100 多美元」
    - Case Facts Block 是獨立於摘要歷史的持久化結構
    - 每次 API call 都必須攜帶 Case Facts
    """
    
    def __init__(self):
        self.case_facts: dict = {}
        self.conversation_history: list = []
        self.summarized_history: str = ""
    
    def extract_facts(self, message: str) -> dict:
        """從對話中提取交易事實（金額、日期、ID）"""
        facts = {}
        
        # 提取金額
        amounts = re.findall(r'\$[\d,]+\.?\d*', message)
        if amounts:
            facts["amounts"] = [float(a.replace('$', '').replace(',', '')) for a in amounts]
        
        # 提取日期
        dates = re.findall(r'\d{4}-\d{2}-\d{2}', message)
        if dates:
            facts["dates"] = dates
        
        # 提取 PO 號
        po_numbers = re.findall(r'PO-\d+', message)
        if po_numbers:
            facts["po_numbers"] = po_numbers
        
        # 提取供應商
        vendor_matches = re.findall(r'vendor[:\s]+(\w+)', message, re.IGNORECASE)
        if vendor_matches:
            facts["vendors"] = vendor_matches
        
        return facts
    
    def update_case_facts(self, new_facts: dict):
        """更新 Case Facts Block（合併不覆蓋）"""
        for key, value in new_facts.items():
            if key in self.case_facts:
                if isinstance(self.case_facts[key], list):
                    existing = set(str(v) for v in self.case_facts[key])
                    for v in value:
                        if str(v) not in existing:
                            self.case_facts[key].append(v)
                else:
                    self.case_facts[key] = value
            else:
                self.case_facts[key] = value
    
    def simulate_progressive_summarization(self, messages: list) -> dict:
        """模擬漸進摘要的精度丟失"""
        # 原始對話
        original_facts = {}
        for msg in messages:
            facts = self.extract_facts(msg)
            for k, v in facts.items():
                original_facts.setdefault(k, []).extend(v)
        
        # 模擬 LLM 摘要（故意模糊化）
        vague_summary = "客戶報告了一筆採購異常，金額大約一百多美元，" \
                       "幾天前下的訂單，涉及某個供應商的零件。"
        
        # 從模糊摘要中提取事實
        summarized_facts = self.extract_facts(vague_summary)
        
        return {
            "original_facts": original_facts,
            "vague_summary": vague_summary,
            "summarized_facts": summarized_facts,
            "precision_lost": {
                "amounts": len(original_facts.get("amounts", [])) - len(summarized_facts.get("amounts", [])),
                "dates": len(original_facts.get("dates", [])) - len(summarized_facts.get("dates", [])),
                "po_numbers": len(original_facts.get("po_numbers", [])) - len(summarized_facts.get("po_numbers", [])),
            }
        }
    
    def build_prompt_with_case_facts(self, current_question: str) -> dict:
        """建構包含 Case Facts Block 的 API 請求"""
        return {
            "system": f"""你是採購異常處理 Agent。

## Case Facts（不可丟失的交易事實）
{json.dumps(self.case_facts, indent=2, ensure_ascii=False)}

## 摘要歷史
{self.summarized_history}
""",
            "messages": [
                {"role": "user", "content": current_question}
            ],
            "note": "Case Facts Block 獨立於 summarized_history，確保精度不丟失"
        }


# ============================================================
# Module 2: LostInMiddleDetector — Lost-in-the-Middle 效應
# ============================================================

class LostInMiddleDetector:
    """
    模擬 lost-in-the-middle 效應和 position-aware ordering。
    
    CCA 考點：
    - 模型可靠處理開頭和結尾，中間容易遺漏
    - 關鍵發現摘要放在開頭
    - 明確的 Section Headers 組織內容
    """
    
    @staticmethod
    def simulate_attention_distribution(sections: list) -> dict:
        """模擬模型對不同位置內容的注意力分布"""
        n = len(sections)
        attention = {}
        
        for i, section in enumerate(sections):
            position = i / (n - 1) if n > 1 else 0.5
            
            # U-shape attention: 高→低→高
            if position < 0.2:
                score = 0.95 - position * 0.5  # 開頭：0.95-0.85
            elif position > 0.8:
                score = 0.75 + (position - 0.8) * 1.0  # 結尾：0.75-0.95
            else:
                score = 0.55 + abs(position - 0.5) * 0.3  # 中間：0.55-0.70
            
            attention[section["title"]] = {
                "position": i,
                "attention_score": round(score, 2),
                "risk": "LOW" if score > 0.8 else "MEDIUM" if score > 0.65 else "HIGH"
            }
        
        return attention
    
    @staticmethod
    def apply_position_aware_ordering(sections: list) -> list:
        """將最重要的 section 放在開頭和結尾"""
        if not sections:
            return sections
        
        sorted_by_importance = sorted(sections, key=lambda s: s["importance"], reverse=True)
        
        result = []
        # Key findings summary at top
        result.append({
            "title": "🔑 Key Findings Summary",
            "content": "\n".join(f"- {s['title']}: {s.get('summary', 'N/A')}" 
                       for s in sorted_by_importance[:3]),
            "importance": 10,
            "is_synthetic": True
        })
        
        # Interleave: high-importance at edges, low-importance in middle
        high = [s for s in sorted_by_importance if s["importance"] >= 7]
        low = [s for s in sorted_by_importance if s["importance"] < 7]
        
        # High importance at beginning
        for s in high[:len(high)//2]:
            result.append(s)
        # Low importance in middle
        result.extend(low)
        # Remaining high importance at end
        for s in high[len(high)//2:]:
            result.append(s)
        
        return result


# ============================================================
# Module 3: ToolOutputTrimmer — Tool Output 修剪
# ============================================================

class ToolOutputTrimmer:
    """
    模擬 PostToolUse Hook 修剪 tool output。
    
    CCA 考點：
    - Order lookup 回傳 40+ 欄位但只需 5 個
    - 修剪在 tool result 進入 context 前完成
    - 節省 token 佔用
    """
    
    # 定義各場景需要保留的欄位
    TRIM_RULES = {
        "lookup_order": {
            "return_case": ["order_id", "status", "total", "items", "return_eligible"],
            "billing_dispute": ["order_id", "total", "payment_method", "billing_address", "charge_date"],
            "delivery_issue": ["order_id", "status", "shipping_carrier", "tracking_number", "estimated_delivery"],
        },
        "get_material_master": {
            "procurement": ["material_number", "description", "mrp_type", "lead_time", "safety_stock", "unit_price"],
            "production": ["material_number", "description", "bom_usage", "routing", "production_version"],
        }
    }
    
    @classmethod
    def simulate_full_order_response(cls) -> dict:
        """模擬 SAP RFC 回傳的完整物料主數據（40+ 欄位）"""
        return {
            "material_number": "MAT-GPU-A100-80G",
            "description": "NVIDIA A100 80GB PCIe",
            "material_type": "ROH",
            "material_group": "GPU",
            "base_unit": "EA",
            "weight_unit": "KG",
            "gross_weight": 1.5,
            "net_weight": 1.2,
            "volume": 0.003,
            "volume_unit": "M3",
            "division": "10",
            "plant": "TW01",
            "storage_location": "RM01",
            "mrp_type": "PD",
            "mrp_controller": "PMC01",
            "lot_size": "EX",
            "minimum_lot_size": 100,
            "maximum_lot_size": 10000,
            "fixed_lot_size": 0,
            "planned_delivery_time": 90,
            "gr_processing_time": 2,
            "safety_stock": 500,
            "reorder_point": 0,
            "procurement_type": "F",
            "special_procurement_type": "",
            "purchasing_group": "GPU",
            "purchasing_organization": "TW10",
            "unit_price": 15000.00,
            "price_unit": 1,
            "currency": "USD",
            "standard_price": 15000.00,
            "moving_average_price": 14850.00,
            "price_control": "V",
            "valuation_class": "3000",
            "profit_center": "PC-SCM",
            "cost_center": "",
            "serial_number_profile": "Z001",
            "batch_management": True,
            "shelf_life": 0,
            "temperature_conditions": "01",
            "hazardous_material": False,
            "created_date": "2024-01-15",
            "created_by": "SAP_ADMIN",
            "last_changed_date": "2026-06-10",
            "last_changed_by": "PMC_WANG",
            "bom_usage": "1",
            "routing": "R-GPU-STD",
            "production_version": "0001",
            "lead_time": 90,
            "avl_status": "active",
            "dual_source": True,
        }
    
    @classmethod
    def trim(cls, tool_name: str, context: str, full_response: dict) -> dict:
        """根據使用場景修剪 tool output"""
        rules = cls.TRIM_RULES.get(tool_name, {})
        fields_to_keep = rules.get(context, list(full_response.keys())[:5])
        
        trimmed = {k: v for k, v in full_response.items() if k in fields_to_keep}
        
        return {
            "trimmed_response": trimmed,
            "original_field_count": len(full_response),
            "trimmed_field_count": len(trimmed),
            "token_savings_estimate": f"~{(len(full_response) - len(trimmed)) * 15} tokens saved",
            "fields_removed": [k for k in full_response if k not in fields_to_keep]
        }


# ============================================================
# Module 4: EscalationEngine — 升級決策引擎
# ============================================================

class EscalationDecision(Enum):
    ESCALATE_IMMEDIATE = "escalate_immediately"
    ATTEMPT_RESOLVE = "attempt_resolution"
    ASK_CLARIFICATION = "ask_for_clarification"
    ESCALATE_POLICY_GAP = "escalate_policy_gap"

class EscalationEngine:
    """
    模擬升級決策邏輯。
    
    CCA 考點：
    - 客戶明確要求人工 → 立即升級
    - Sentiment-based escalation 是反模式 ❌
    - Self-reported confidence 是反模式 ❌
    - 多個匹配 → 要求更多資訊（非 heuristic selection）
    """
    
    @staticmethod
    def evaluate(case: dict) -> dict:
        """評估案件並決定是否升級"""
        
        # Rule 1: 客戶明確要求人工
        if case.get("customer_requests_human", False):
            return {
                "decision": EscalationDecision.ESCALATE_IMMEDIATE.value,
                "reason": "Customer explicitly requested human agent",
                "action": "Transfer immediately, do NOT attempt resolution first"
            }
        
        # Rule 2: 多個客戶匹配
        if case.get("multiple_customer_matches", 0) > 1:
            return {
                "decision": EscalationDecision.ASK_CLARIFICATION.value,
                "reason": f"Found {case['multiple_customer_matches']} customer matches",
                "action": "Ask for additional identifiers (email, phone, order number)",
                "anti_pattern": "DO NOT use heuristic selection (e.g., 'most recent')"
            }
        
        # Rule 3: 政策模糊或沉默
        if case.get("policy_gap", False):
            return {
                "decision": EscalationDecision.ESCALATE_POLICY_GAP.value,
                "reason": "Policy is ambiguous or silent on this request",
                "action": "Escalate with full context, do NOT interpret policy",
                "example": "Customer asks for competitor price matching; policy only covers own-site adjustments"
            }
        
        # Rule 4: 問題在能力範圍內
        if case.get("within_capability", True):
            return {
                "decision": EscalationDecision.ATTEMPT_RESOLVE.value,
                "reason": "Issue is within agent's capability",
                "action": "Acknowledge frustration, attempt resolution. Escalate only if customer reiterates preference for human."
            }
        
        return {
            "decision": EscalationDecision.ESCALATE_IMMEDIATE.value,
            "reason": "Cannot make meaningful progress",
            "action": "Escalate with structured handoff"
        }
    
    @staticmethod
    def demonstrate_anti_patterns() -> list:
        """展示三大反模式"""
        return [
            {
                "anti_pattern": "Sentiment-based escalation",
                "why_wrong": "Customer sentiment does NOT correlate with case complexity. "
                            "High frustration may be a simple issue; low frustration may be a policy exception.",
                "correct_approach": "Use explicit escalation criteria with few-shot examples in system prompt"
            },
            {
                "anti_pattern": "Self-reported confidence scores",
                "why_wrong": "LLM confidence is poorly calibrated. "
                            "The agent may be 'incorrectly confident on hard cases'.",
                "correct_approach": "Use objective criteria (policy coverage, tool results, customer explicit request)"
            },
            {
                "anti_pattern": "Heuristic customer selection",
                "why_wrong": "When multiple customers match, selecting 'most recent' or 'most active' "
                            "may identify the wrong person.",
                "correct_approach": "Ask for additional identifiers to disambiguate"
            }
        ]


# ============================================================
# Module 5: ErrorPropagationSimulator — 多 Agent 錯誤傳播
# ============================================================

class ErrorCategory(Enum):
    TRANSIENT = "transient"      # timeout, rate limit → retryable
    PERMANENT = "permanent"      # auth failure, not found → not retryable
    PARTIAL = "partial"          # some results available
    VALID_EMPTY = "valid_empty"  # successful query, no matches

@dataclass
class AgentError:
    category: ErrorCategory
    is_retryable: bool
    message: str
    attempted_query: str
    partial_results: Optional[dict] = None
    alternative_approaches: list = field(default_factory=list)

class ErrorPropagationSimulator:
    """
    模擬多 Agent 系統的錯誤傳播。
    
    CCA 考點：
    - 結構化錯誤（failure type + attempted query + partial results + alternatives）
    - Access failure vs Valid empty result 的關鍵區分
    - 兩大反模式：silent suppression / entire workflow termination
    - Coverage annotations 標示資料缺口
    """
    
    @staticmethod
    def simulate_multi_agent_query() -> dict:
        """模擬 ODM 採購追蹤的三系統查詢"""
        
        # SAP 查詢成功
        sap_result = {
            "agent": "sap_agent",
            "status": "success",
            "data": {
                "po_number": "PO-2026-45678",
                "vendor": "TSMC",
                "amount": 750000.00,
                "delivery_date": "2026-07-15",
                "status": "partially_delivered"
            }
        }
        
        # WMS 查詢超時（transient error）
        wms_error = AgentError(
            category=ErrorCategory.TRANSIENT,
            is_retryable=True,
            message="WMS database timeout after 30s",
            attempted_query="SELECT * FROM inventory WHERE po_ref = 'PO-2026-45678'",
            partial_results={"warehouse": "TW01", "last_known_qty": 2500},
            alternative_approaches=["Try WMS read replica", "Query by material number instead"]
        )
        
        # MES 查無結果（valid empty — 不是錯誤）
        mes_result = {
            "agent": "mes_agent",
            "status": "success",
            "data": None,  # 合法空結果
            "note": "No production orders linked to this PO yet (materials still in RM warehouse)"
        }
        
        return {
            "sap": sap_result,
            "wms": {
                "agent": "wms_agent",
                "status": "error",
                "error": {
                    "category": wms_error.category.value,
                    "is_retryable": wms_error.is_retryable,
                    "message": wms_error.message,
                    "attempted_query": wms_error.attempted_query,
                    "partial_results": wms_error.partial_results,
                    "alternatives": wms_error.alternative_approaches
                }
            },
            "mes": mes_result
        }
    
    @staticmethod
    def coordinator_recovery(agent_results: dict) -> dict:
        """Coordinator 根據結構化錯誤做恢復決策"""
        
        decisions = {}
        has_complete_data = True
        
        for agent_name, result in agent_results.items():
            if result["status"] == "success":
                decisions[agent_name] = {
                    "action": "use_result",
                    "data_available": result["data"] is not None,
                    "note": "Valid empty" if result["data"] is None else "Full data"
                }
            elif result["status"] == "error":
                error = result["error"]
                has_complete_data = False
                
                if error["is_retryable"]:
                    decisions[agent_name] = {
                        "action": "retry_with_alternative",
                        "retry_strategy": error["alternatives"][0] if error["alternatives"] else "standard_retry",
                        "use_partial": error["partial_results"] is not None,
                        "partial_data": error["partial_results"]
                    }
                else:
                    decisions[agent_name] = {
                        "action": "skip_and_annotate",
                        "coverage_gap": f"Data from {agent_name} unavailable: {error['message']}"
                    }
        
        return {
            "recovery_decisions": decisions,
            "has_complete_data": has_complete_data,
            "coverage_annotation": {
                "well_supported": [k for k, v in decisions.items() if v["action"] == "use_result" and v.get("data_available")],
                "partial": [k for k, v in decisions.items() if v.get("use_partial")],
                "gaps": [k for k, v in decisions.items() if v["action"] == "skip_and_annotate"]
            }
        }
    
    @staticmethod
    def demonstrate_anti_patterns() -> list:
        """展示兩大錯誤傳播反模式"""
        return [
            {
                "anti_pattern": "Silent error suppression",
                "example": {
                    "wms_response": {"status": "success", "data": []},
                    "reality": "WMS was down, no data was retrieved"
                },
                "consequence": "Coordinator thinks 'no inventory data' instead of 'query failed'",
                "correct": "Return isError=True with structured error context"
            },
            {
                "anti_pattern": "Entire workflow termination",
                "example": "WMS timeout → abort entire procurement tracking",
                "consequence": "SAP and MES data (already retrieved) is wasted",
                "correct": "Use partial results, annotate gaps, let coordinator decide"
            }
        ]


# ============================================================
# Module 6: Mock Exam Questions — CCA D5
# ============================================================

MOCK_EXAM = [
    {
        "id": "D5-Q1",
        "scenario": "Customer Support Agent",
        "question": """A customer support agent handles multi-issue sessions where customers report 
multiple problems in a single conversation. After 15+ message exchanges, the agent starts 
confusing order amounts and dates between different issues. What is the MOST effective solution?""",
        "options": {
            "A": "Increase the model's max_tokens to allow longer context",
            "B": "Extract transactional facts into a persistent 'case facts' block included in each prompt, separate from summarized history",
            "C": "Use progressive summarization to compress older messages and free up context",
            "D": "Switch to a model with a larger context window"
        },
        "correct": "B",
        "explanation": "Case Facts Block (B) extracts amounts, dates, order IDs into a persistent structured block "
                      "that is included in every API call, independent of summarized history. "
                      "Progressive summarization (C) is actually the CAUSE of the problem — it loses numerical precision. "
                      "Larger context (A, D) doesn't solve precision loss during summarization."
    },
    {
        "id": "D5-Q2",
        "scenario": "Customer Support Agent",
        "question": """The support agent's escalation rate has increased to 45%, but customer satisfaction 
hasn't improved. Analysis shows the agent escalates whenever sentiment analysis detects 
high frustration (anger score > 0.7). What should be changed?""",
        "options": {
            "A": "Lower the anger threshold from 0.7 to 0.5 to catch more frustrated customers",
            "B": "Add self-reported confidence scores to help the agent decide when to escalate",
            "C": "Replace sentiment-based triggers with explicit escalation criteria and few-shot examples in the system prompt",
            "D": "Implement a two-stage escalation: first to a senior agent, then to human"
        },
        "correct": "C",
        "explanation": "Sentiment-based escalation (A is worse version of current) is an anti-pattern — "
                      "sentiment doesn't correlate with case complexity. A frustrated customer may have a simple issue. "
                      "Self-reported confidence (B) is also unreliable — LLMs are 'incorrectly confident on hard cases'. "
                      "Explicit criteria + few-shot examples (C) gives deterministic, auditable escalation logic. "
                      "Two-stage (D) adds complexity without fixing the root cause."
    },
    {
        "id": "D5-Q3",
        "scenario": "Multi-Agent Research",
        "question": """A multi-agent research system has 3 subagents querying different sources. 
Subagent B times out, and the coordinator receives: {"isError": true, "content": "Search unavailable"}.
The coordinator skips source B and synthesizes from A and C only. What is the PRIMARY improvement needed?""",
        "options": {
            "A": "Add automatic retry logic for all failed subagents",
            "B": "Have subagent B return structured error context including failure type, attempted query, partial results, and alternative approaches",
            "C": "Terminate the entire workflow when any subagent fails to ensure data completeness",
            "D": "Have subagent B silently return empty results so the coordinator can proceed without errors"
        },
        "correct": "B",
        "explanation": "Generic error 'Search unavailable' (current state) hides valuable context from the coordinator. "
                      "Structured error context (B) enables intelligent recovery: Was it a timeout (retry)? Auth failure (skip)? "
                      "Are there partial results to use? What alternatives exist? "
                      "Auto retry (A) doesn't help if the error is permanent. "
                      "Workflow termination (C) wastes results from A and C. "
                      "Silent suppression (D) is the worst anti-pattern — coordinator can't distinguish 'no data' from 'query failed'."
    },
    {
        "id": "D5-Q4",
        "scenario": "Structured Data Extraction",
        "question": """A document extraction system reports 97% overall accuracy across all document types. 
However, users of handwritten form processing complain about frequent errors. 
What is the MOST appropriate next step?""",
        "options": {
            "A": "Increase the training data for the model to improve overall accuracy to 99%",
            "B": "Validate accuracy by document type and field segment before automating high-confidence extractions",
            "C": "Add a confidence threshold of 0.95 and route everything below to human review",
            "D": "Switch to a more powerful model (e.g., Claude Opus) for all document types"
        },
        "correct": "B",
        "explanation": "97% aggregate accuracy masks poor performance on specific segments (handwritten forms). "
                      "Stratified validation (B) reveals the actual per-segment accuracy. "
                      "A single confidence threshold (C) doesn't account for different error patterns by document type. "
                      "More training data (A) or bigger model (D) are brute-force approaches that don't address "
                      "the fundamental issue of measuring accuracy at the right granularity."
    },
    {
        "id": "D5-Q5",
        "scenario": "Multi-Agent Research",
        "question": """A multi-source synthesis agent combines findings from 4 research subagents. 
The final report states 'The market is projected to reach $50B' but doesn't indicate which source 
provided this figure or when the data was collected. A reviewer finds that two sources cite 
different figures ($50B and $67B). What is the BEST approach?""",
        "options": {
            "A": "Use the average of both values ($58.5B) as the reported figure",
            "B": "Select the more recent source's value as it's likely more accurate",
            "C": "Require subagents to output structured claim-source mappings with publication dates, and annotate conflicts with source attribution in the report",
            "D": "Add a verification subagent that fact-checks all numerical claims against a trusted database"
        },
        "correct": "C",
        "explanation": "Claim-source mappings with temporal metadata (C) solve both problems: "
                      "(1) Source attribution is preserved through synthesis, "
                      "(2) Temporal differences ($50B from 2025 Q4, $67B from 2026 Q2) are correctly interpreted "
                      "as time-based evolution, not contradiction. "
                      "Averaging (A) produces a number from no source. "
                      "Selecting 'more recent' (B) arbitrarily discards valid data. "
                      "Verification agent (D) adds complexity without solving the attribution problem."
    }
]


# ============================================================
# Main — Run All Validations
# ============================================================

def run_validations():
    passed = 0
    total = 0
    
    print("=" * 70)
    print("CCA Domain 5: Context Management & Reliability")
    print("=" * 70)
    
    # --- Module 1: CaseFactsManager ---
    print("\n📋 Module 1: CaseFactsManager (Progressive Summarization Risk)")
    print("-" * 50)
    
    cfm = CaseFactsManager()
    messages = [
        "PO-2026-45678 from vendor TSMC, amount $750,000.00, delivery date 2026-07-15",
        "Second issue: PO-2026-45679 from vendor Samsung, amount $320,500.75, delivery 2026-08-01",
    ]
    
    result = cfm.simulate_progressive_summarization(messages)
    
    # V1: 原始事實提取正確
    total += 1
    assert len(result["original_facts"]["amounts"]) == 2, "Should extract 2 amounts"
    assert 750000.0 in result["original_facts"]["amounts"], "Should extract exact $750,000.00"
    print(f"  ✅ V1: Original facts extracted correctly ({len(result['original_facts']['amounts'])} amounts)")
    passed += 1
    
    # V2: 摘要丟失精度
    total += 1
    assert result["precision_lost"]["amounts"] > 0 or result["precision_lost"]["dates"] > 0, \
        "Summarization should lose precision"
    print(f"  ✅ V2: Precision loss detected (amounts lost: {result['precision_lost']['amounts']}, "
          f"dates lost: {result['precision_lost']['dates']})")
    passed += 1
    
    # V3: Case Facts Block 建構
    for msg in messages:
        facts = cfm.extract_facts(msg)
        cfm.update_case_facts(facts)
    
    prompt = cfm.build_prompt_with_case_facts("What's the status of PO-2026-45678?")
    total += 1
    assert "Case Facts" in prompt["system"], "Prompt should contain Case Facts block"
    assert "750000.0" in prompt["system"] or "750,000" in prompt["system"] or "750000" in prompt["system"], \
        "Case Facts should preserve exact amount"
    print(f"  ✅ V3: Case Facts Block preserves exact amounts in prompt")
    passed += 1
    
    # --- Module 2: LostInMiddleDetector ---
    print("\n🔍 Module 2: LostInMiddleDetector (Position-Aware Ordering)")
    print("-" * 50)
    
    sections = [
        {"title": "Factory TW01 OEE Report", "importance": 9, "summary": "OEE dropped to 72%"},
        {"title": "Factory TW02 OEE Report", "importance": 5, "summary": "OEE stable at 88%"},
        {"title": "Factory TW03 OEE Report", "importance": 4, "summary": "OEE at 91%"},
        {"title": "Factory CN01 OEE Report", "importance": 3, "summary": "OEE at 89%"},
        {"title": "Factory CN02 OEE Report", "importance": 6, "summary": "OEE at 85%"},
        {"title": "Factory JP01 OEE Report", "importance": 8, "summary": "New bottleneck found"},
    ]
    
    attention = LostInMiddleDetector.simulate_attention_distribution(sections)
    
    # V4: 中間位置的注意力分數較低
    total += 1
    middle_scores = [v["attention_score"] for k, v in attention.items() 
                     if v["position"] in [2, 3]]
    edge_scores = [v["attention_score"] for k, v in attention.items() 
                   if v["position"] in [0, 5]]
    assert min(edge_scores) > max(middle_scores), \
        "Edge positions should have higher attention than middle"
    print(f"  ✅ V4: Lost-in-middle confirmed (edge avg: {sum(edge_scores)/len(edge_scores):.2f}, "
          f"middle avg: {sum(middle_scores)/len(middle_scores):.2f})")
    passed += 1
    
    # V5: Position-aware ordering 把重要內容移到邊緣
    reordered = LostInMiddleDetector.apply_position_aware_ordering(sections)
    total += 1
    assert reordered[0]["title"] == "🔑 Key Findings Summary", \
        "Key findings summary should be first"
    print(f"  ✅ V5: Key Findings Summary placed at top, {len(reordered)} sections reordered")
    passed += 1
    
    # --- Module 3: ToolOutputTrimmer ---
    print("\n✂️ Module 3: ToolOutputTrimmer (40+ → 5 Fields)")
    print("-" * 50)
    
    full_response = ToolOutputTrimmer.simulate_full_order_response()
    
    # V6: 完整回應有 40+ 欄位
    total += 1
    assert len(full_response) >= 40, f"Full response should have 40+ fields, got {len(full_response)}"
    print(f"  ✅ V6: Full response has {len(full_response)} fields (≥40)")
    passed += 1
    
    # V7: Procurement 場景修剪後只保留 6 欄位
    trimmed = ToolOutputTrimmer.trim("get_material_master", "procurement", full_response)
    total += 1
    assert trimmed["trimmed_field_count"] == 6, \
        f"Procurement trim should keep 6 fields, got {trimmed['trimmed_field_count']}"
    assert "material_number" in trimmed["trimmed_response"]
    assert "lead_time" in trimmed["trimmed_response"]
    print(f"  ✅ V7: Procurement trim: {trimmed['original_field_count']} → {trimmed['trimmed_field_count']} fields "
          f"({trimmed['token_savings_estimate']})")
    passed += 1
    
    # V8: Production 場景保留不同欄位集
    trimmed_prod = ToolOutputTrimmer.trim("get_material_master", "production", full_response)
    total += 1
    assert "bom_usage" in trimmed_prod["trimmed_response"], "Production should include bom_usage"
    assert "lead_time" not in trimmed_prod["trimmed_response"], "Production should not include lead_time"
    print(f"  ✅ V8: Production trim keeps different fields (bom_usage ✓, lead_time ✗)")
    passed += 1
    
    # --- Module 4: EscalationEngine ---
    print("\n🚨 Module 4: EscalationEngine (Escalation Decision)")
    print("-" * 50)
    
    # V9: 客戶明確要求人工 → 立即升級
    total += 1
    result = EscalationEngine.evaluate({"customer_requests_human": True})
    assert result["decision"] == "escalate_immediately"
    assert "do NOT attempt" in result["action"]
    print(f"  ✅ V9: Customer requests human → immediate escalation (no resolution attempt)")
    passed += 1
    
    # V10: 多個客戶匹配 → 要求更多資訊
    total += 1
    result = EscalationEngine.evaluate({"multiple_customer_matches": 3})
    assert result["decision"] == "ask_for_clarification"
    assert "anti_pattern" in result
    print(f"  ✅ V10: Multiple matches → ask clarification (not heuristic selection)")
    passed += 1
    
    # V11: 政策模糊 → 升級
    total += 1
    result = EscalationEngine.evaluate({"policy_gap": True})
    assert result["decision"] == "escalate_policy_gap"
    print(f"  ✅ V11: Policy gap → escalate (don't interpret policy)")
    passed += 1
    
    # V12: 反模式列表完整
    total += 1
    anti_patterns = EscalationEngine.demonstrate_anti_patterns()
    assert len(anti_patterns) == 3
    pattern_names = [ap["anti_pattern"] for ap in anti_patterns]
    assert "Sentiment-based escalation" in pattern_names
    assert "Self-reported confidence scores" in pattern_names
    assert "Heuristic customer selection" in pattern_names
    print(f"  ✅ V12: 3 anti-patterns documented: {', '.join(pattern_names)}")
    passed += 1
    
    # --- Module 5: ErrorPropagationSimulator ---
    print("\n⚡ Module 5: ErrorPropagationSimulator (Multi-Agent Error Handling)")
    print("-" * 50)
    
    agent_results = ErrorPropagationSimulator.simulate_multi_agent_query()
    
    # V13: SAP 成功
    total += 1
    assert agent_results["sap"]["status"] == "success"
    assert agent_results["sap"]["data"]["amount"] == 750000.00
    print(f"  ✅ V13: SAP query success (PO amount: ${agent_results['sap']['data']['amount']:,.2f})")
    passed += 1
    
    # V14: WMS 結構化錯誤（非通用錯誤）
    total += 1
    wms = agent_results["wms"]
    assert wms["status"] == "error"
    assert wms["error"]["category"] == "transient"
    assert wms["error"]["is_retryable"] is True
    assert wms["error"]["partial_results"] is not None
    assert len(wms["error"]["alternatives"]) > 0
    print(f"  ✅ V14: WMS structured error (transient, retryable, has partial results + alternatives)")
    passed += 1
    
    # V15: MES 合法空結果（不是錯誤）
    total += 1
    mes = agent_results["mes"]
    assert mes["status"] == "success"  # 成功，不是錯誤
    assert mes["data"] is None  # 合法空
    print(f"  ✅ V15: MES valid empty result (success with None data, NOT an error)")
    passed += 1
    
    # V16: Coordinator 恢復決策
    total += 1
    recovery = ErrorPropagationSimulator.coordinator_recovery(agent_results)
    assert recovery["recovery_decisions"]["sap"]["action"] == "use_result"
    assert recovery["recovery_decisions"]["wms"]["action"] == "retry_with_alternative"
    assert recovery["recovery_decisions"]["wms"]["use_partial"] is True
    assert recovery["has_complete_data"] is False
    print(f"  ✅ V16: Coordinator recovery: SAP=use, WMS=retry+partial, MES=use(empty)")
    passed += 1
    
    # V17: Coverage annotations
    total += 1
    coverage = recovery["coverage_annotation"]
    assert "sap" in coverage["well_supported"]
    assert "wms" in coverage["partial"]
    print(f"  ✅ V17: Coverage annotations — well-supported: {coverage['well_supported']}, "
          f"partial: {coverage['partial']}, gaps: {coverage['gaps']}")
    passed += 1
    
    # V18: 反模式文檔
    total += 1
    error_anti = ErrorPropagationSimulator.demonstrate_anti_patterns()
    assert len(error_anti) == 2
    assert any("Silent" in ap["anti_pattern"] for ap in error_anti)
    assert any("termination" in ap["anti_pattern"] for ap in error_anti)
    print(f"  ✅ V18: 2 error propagation anti-patterns documented")
    passed += 1
    
    # --- Module 6: Mock Exam ---
    print("\n📝 Module 6: Mock Exam (5 Questions)")
    print("-" * 50)
    
    for q in MOCK_EXAM:
        total += 1
        # 驗證題目結構
        assert q["correct"] in q["options"], f"{q['id']}: correct answer not in options"
        assert len(q["options"]) == 4, f"{q['id']}: should have 4 options"
        assert len(q["explanation"]) > 50, f"{q['id']}: explanation too short"
        print(f"  ✅ {q['id']}: {q['question'][:60]}... → [{q['correct']}]")
        passed += 1
    
    # --- Summary ---
    print("\n" + "=" * 70)
    print(f"📊 Results: {passed}/{total} validations passed")
    print("=" * 70)
    
    if passed == total:
        print("🎉 ALL VALIDATIONS PASSED!")
    else:
        print(f"⚠️ {total - passed} validations failed")
    
    return passed, total


if __name__ == "__main__":
    passed, total = run_validations()
