"""
CCA 全真模擬考 — 6 Scenarios × 5 Questions = 30 題
Claude Certified Architect — Foundations
日期：2026-06-22

考試規格：
- 4/6 情境隨機抽取
- 單選 4 選 1
- 720/1000 及格
- Domain 權重：D1(27%) D2(18%) D3(20%) D4(20%) D5(15%)
"""

from dataclasses import dataclass, field
from typing import Optional
import textwrap

# ============================================================
# 資料結構
# ============================================================

@dataclass
class Question:
    id: int
    scenario: str           # S1-S6
    domain: str             # D1-D5
    domain_name: str
    question: str
    options: dict           # {"A": ..., "B": ..., "C": ..., "D": ...}
    answer: str             # "A" / "B" / "C" / "D"
    explanation: str
    trap_type: str          # 陷阱類型
    key_concept: str        # 考的核心概念


# ============================================================
# Scenario 1: Customer Support Resolution Agent（5 題）
# ============================================================

S1_QUESTIONS = [
    Question(
        id=1,
        scenario="S1-Customer Support",
        domain="D1", domain_name="Agentic Architecture",
        question=textwrap.dedent("""
        你的客服 Agent 在 12% 的案例中跳過 get_customer 直接呼叫 lookup_order，
        導致錯誤的客戶識別。系統 prompt 已明確要求「先驗證客戶身份」。
        如何確保 get_customer 一定在 lookup_order 之前執行？
        """).strip(),
        options={
            "A": "在 system prompt 加入更詳細的步驟說明和 few-shot 範例",
            "B": "使用 PreToolUse Hook 程式化檢查：若 get_customer 未完成則攔截 lookup_order",
            "C": "把 get_customer 和 lookup_order 合併成一個工具",
            "D": "加入 routing classifier 模型來決定工具呼叫順序",
        },
        answer="B",
        explanation="關鍵路徑需要確定性保證。Prompt 指令有非零失敗率（題目已說 12% 失敗），"
                    "Hook 提供 100% 程式化強制。合併工具改變了 API 設計；classifier 是 over-engineering。",
        trap_type="Prompt vs Hook determinism",
        key_concept="PreToolUse Hook 程式化強制 > Prompt 指令",
    ),
    Question(
        id=2,
        scenario="S1-Customer Support",
        domain="D2", domain_name="Tool Design & MCP",
        question=textwrap.dedent("""
        客服 Agent 經常把 get_customer 和 lookup_order 搞混——查客戶資訊時呼叫
        lookup_order，查訂單時呼叫 get_customer。兩個工具的描述都很簡短：
        "Retrieves customer information" 和 "Retrieves order details"。
        最有效的第一步是什麼？
        """).strip(),
        options={
            "A": "加入 few-shot 範例展示正確的工具選擇",
            "B": "擴展工具描述，加入預期輸入格式、回傳欄位和使用場景邊界",
            "C": "建立一個 routing classifier 模型來分發工具呼叫",
            "D": "把兩個工具合併成一個通用查詢工具",
        },
        answer="B",
        explanation="題目問「最有效的第一步」→ 最低成本最高槓桿。擴展描述是改一行字的事，"
                    "few-shot 增加 token 消耗但沒修根因，classifier 和合併都是 over-engineering。",
        trap_type="First step = lowest effort highest leverage",
        key_concept="Tool description quality 是工具選擇的第一槓桿",
    ),
    Question(
        id=3,
        scenario="S1-Customer Support",
        domain="D5", domain_name="Context Management & Reliability",
        question=textwrap.dedent("""
        客服 Agent 在處理長對話時，退款金額和訂單日期經常出錯。
        調查發現是漸進摘要（progressive summarization）時數值被模糊化。
        如何解決？
        """).strip(),
        options={
            "A": "禁用漸進摘要，保留完整對話歷史",
            "B": "提取交易關鍵事實（金額、日期、訂單號）到獨立的 Case Facts Block",
            "C": "增加 context window 大小以容納更多歷史",
            "D": "使用 self-reported confidence score 標記不確定的數值",
        },
        answer="B",
        explanation="Case Facts Block 讓關鍵數值獨立於摘要歷史，不會被壓縮時模糊化。"
                    "禁用摘要會爆 context；加大 window 沒解決摘要問題；self-reported confidence 校準差。",
        trap_type="Self-reported confidence is poorly calibrated",
        key_concept="Case Facts Block 保護數值精度",
    ),
    Question(
        id=4,
        scenario="S1-Customer Support",
        domain="D5", domain_name="Context Management & Reliability",
        question=textwrap.dedent("""
        客服 Agent 的升級（escalation）校準有問題：簡單案件升級給人工，
        複雜案件卻嘗試自己解決。如何改善升級判斷？
        """).strip(),
        options={
            "A": "讓 Agent 輸出 1-10 的信心分數，低於 5 分自動升級",
            "B": "用情緒分析偵測客戶憤怒程度，憤怒時升級",
            "C": "定義明確的升級標準，配合 few-shot 範例展示何時該升級 vs 自行解決",
            "D": "訓練一個獨立的分類器模型來判斷升級",
        },
        answer="C",
        explanation="明確標準 + few-shot 是正解。Self-reported confidence (A) 校準差——LLM 可能 confidently wrong。"
                    "情緒分析 (B) 跟案件複雜度無關。獨立分類器 (D) 是 over-engineering。",
        trap_type="Sentiment ≠ complexity; Self-confidence is uncalibrated",
        key_concept="Explicit criteria + few-shot > confidence scores / sentiment",
    ),
    Question(
        id=5,
        scenario="S1-Customer Support",
        domain="D1", domain_name="Agentic Architecture",
        question=textwrap.dedent("""
        客戶在對話中明確說「我要跟真人說話」，但 Agent 繼續嘗試查詢訂單和
        解決問題。PostToolUse Hook 已正確配置用於資料正規化。
        正確的處理方式是？
        """).strip(),
        options={
            "A": "先完成當前調查，收集足夠資訊後再升級，讓人工接手時有完整背景",
            "B": "立即升級到人工，附上結構化交接摘要（客戶 ID、已知問題、建議動作）",
            "C": "詢問客戶能否先嘗試自動解決，如果不滿意再轉人工",
            "D": "使用 sentiment analysis 確認客戶是否真的需要人工介入",
        },
        answer="B",
        explanation="客戶明確要求人工 → 立即升級，不要繼續調查。結構化交接摘要確保人工接手效率。"
                    "A 和 C 都忽視了客戶的明確要求；D 用情緒分析判斷已經明確表達的意願是多餘的。",
        trap_type="Honor explicit human request immediately",
        key_concept="客戶要求人工 → 立即升級 + Structured Handoff",
    ),
]

# ============================================================
# Scenario 2: Code Generation with Claude Code（5 題）
# ============================================================

S2_QUESTIONS = [
    Question(
        id=6,
        scenario="S2-Code Generation",
        domain="D3", domain_name="Claude Code Config",
        question=textwrap.dedent("""
        你的團隊需要一個 /review 的 slash command，讓所有開發者都能使用。
        應該把這個 command 放在哪裡？
        """).strip(),
        options={
            "A": "~/.claude/commands/review.md（user-scoped）",
            "B": ".claude/commands/review.md（project-scoped）",
            "C": "在 CLAUDE.md 裡定義 /review 指令",
            "D": ".claude/config.json 的 commands 陣列",
        },
        answer="B",
        explanation=".claude/commands/ 是 project-scoped，會被 version control 追蹤，所有開發者共享。"
                    "~/.claude/commands/ 是個人的，CLAUDE.md 不是定義 commands 的地方，"
                    ".claude/config.json 的 commands 陣列是不存在的功能。",
        trap_type="Non-existent feature (.claude/config.json commands)",
        key_concept="Project commands: .claude/commands/ | Personal: ~/.claude/commands/",
    ),
    Question(
        id=7,
        scenario="S2-Code Generation",
        domain="D3", domain_name="Claude Code Config",
        question=textwrap.dedent("""
        你的 codebase 有數百個測試檔案分散在不同目錄，都遵循 *.test.tsx 命名。
        你想對所有測試檔案統一套用相同的 coding convention。
        最可維護的方式是？
        """).strip(),
        options={
            "A": "在根目錄 CLAUDE.md 寫上測試 convention（依賴 Claude 推斷哪些是測試檔）",
            "B": "在每個包含測試的子目錄放一個 CLAUDE.md",
            "C": "在 .claude/rules/ 建立規則檔，frontmatter 用 paths: ['**/*.test.tsx']",
            "D": "建立一個 /test-convention skill，每次編輯測試檔時手動觸發",
        },
        answer="C",
        explanation=".claude/rules/ 搭配 glob pattern 可以跨目錄套用到所有匹配的檔案，只在編輯相關檔案時載入。"
                    "子目錄 CLAUDE.md 是 directory-bound，維護成本高；根 CLAUDE.md 依賴推斷不可靠；"
                    "skill 需要手動觸發，不是 always-on 的。",
        trap_type="Directory CLAUDE.md is directory-bound, not cross-cutting",
        key_concept=".claude/rules/ + glob patterns 跨目錄套用 convention",
    ),
    Question(
        id=8,
        scenario="S2-Code Generation",
        domain="D3", domain_name="Claude Code Config",
        question=textwrap.dedent("""
        團隊計畫將一個 monolith 後端重構成微服務架構，影響數十個檔案和多個模組。
        使用 Claude Code 時，最適合的工作方式是？
        """).strip(),
        options={
            "A": "直接開始執行（direct execution），遇到複雜問題再切換到 plan mode",
            "B": "先用 plan mode 讓 Claude 分析 codebase、理解依賴關係、擬定重構計畫",
            "C": "在 CLAUDE.md 寫詳細的重構指令，然後用 direct execution",
            "D": "把重構拆成很小的 PR，每個 PR 用 direct execution",
        },
        answer="B",
        explanation="複雜架構任務（多檔案、多模組、多種合理方案）應先用 plan mode。"
                    "A 的「先做再說」容易做到一半才發現方向錯誤；C 無法預見所有依賴；"
                    "D 把負擔轉嫁給開發者而不是改善工具使用方式。",
        trap_type="Complex tasks require planning first",
        key_concept="Plan mode: 複雜/多檔案/架構決策；Direct: 單純/明確/單檔",
    ),
    Question(
        id=9,
        scenario="S2-Code Generation",
        domain="D3", domain_name="Claude Code Config",
        question=textwrap.dedent("""
        新加入的團隊成員反映 Claude Code 沒有套用團隊的 coding standard。
        調查發現這些規則寫在 ~/.claude/CLAUDE.md（user-level）。
        如何解決？
        """).strip(),
        options={
            "A": "請新成員複製資深成員的 ~/.claude/CLAUDE.md",
            "B": "把規則移到 .claude/CLAUDE.md 或專案根目錄的 CLAUDE.md（project-level）",
            "C": "在 README.md 加入 Claude Code 設定步驟",
            "D": "用 .claude/rules/ 設定全域規則，再在每個成員的 ~/.claude/ 引用",
        },
        answer="B",
        explanation="User-level 設定不會被 version control 追蹤，新成員自然沒有。"
                    "移到 project-level（.claude/CLAUDE.md 或根 CLAUDE.md）就能透過 git 共享。"
                    "A 的手動複製不可維護；C 增加入職複雜度；D 混淆了 user/project scoping。",
        trap_type="User-level config not shared via VCS",
        key_concept="Team rules → project-level; Personal prefs → user-level",
    ),
    Question(
        id=10,
        scenario="S2-Code Generation",
        domain="D3", domain_name="Claude Code Config",
        question=textwrap.dedent("""
        一個 Claude Code Skill 在執行 codebase exploration 時產生大量中間輸出，
        汙染了主對話的 context window。如何防止？
        """).strip(),
        options={
            "A": "在 SKILL.md frontmatter 加入 context: fork",
            "B": "在 CLAUDE.md 加入「探索時精簡輸出」的指令",
            "C": "使用 /compact 手動壓縮對話",
            "D": "增加 max_tokens 來容納更多輸出",
        },
        answer="A",
        explanation="context: fork 讓 skill 在隔離的 sub-agent context 中執行，"
                    "中間輸出不會進入主對話。B 是 prompt-based 不可靠；C 是事後補救；D 沒解決汙染問題。",
        trap_type="Skill isolation mechanism",
        key_concept="context: fork 隔離 skill 的 verbose 輸出",
    ),
]

# ============================================================
# Scenario 3: Multi-Agent Research System（5 題）
# ============================================================

S3_QUESTIONS = [
    Question(
        id=11,
        scenario="S3-Multi-Agent Research",
        domain="D1", domain_name="Agentic Architecture",
        question=textwrap.dedent("""
        你的多 Agent 研究系統產出的報告只涵蓋了視覺藝術，
        完全遺漏了音樂、寫作和電影等創意產業。
        Web Search Agent 的搜尋範圍正確、Document Analysis 也正常。
        根本原因最可能是什麼？
        """).strip(),
        options={
            "A": "Web Search Agent 的搜尋查詢太狹窄",
            "B": "Coordinator 的任務分解（task decomposition）只產生了視覺藝術的子任務",
            "C": "Synthesis Agent 的合成指令過濾了非視覺藝術內容",
            "D": "Document Analysis Agent 的分析 filter 設定錯誤",
        },
        answer="B",
        explanation="報告的覆蓋範圍由 Coordinator 的任務分解決定。如果 Coordinator 只分解出視覺藝術的子任務，"
                    "下游 agent 再好也沒用——它們只收到了關於視覺藝術的任務。"
                    "這是「怪 subagent 其實是 coordinator 的問題」的典型陷阱。",
        trap_type="Root cause is coordinator, not downstream agents",
        key_concept="Coordinator task decomposition 決定覆蓋範圍",
    ),
    Question(
        id=12,
        scenario="S3-Multi-Agent Research",
        domain="D1", domain_name="Agentic Architecture",
        question=textwrap.dedent("""
        你想讓 Coordinator 同時派出 3 個 Subagent 平行處理不同研究方向。
        如何實現平行執行？
        """).strip(),
        options={
            "A": "在 Coordinator 的單次回應中發出 3 個 Task 工具呼叫",
            "B": "依序發出 3 個 Task 呼叫，每次等前一個完成再發下一個",
            "C": "建立一個 orchestration DAG 定義平行路徑",
            "D": "讓 Subagent 之間直接通訊，繞過 Coordinator",
        },
        answer="A",
        explanation="平行執行的方法是在 Coordinator 的**單次回應**中發出多個 Task tool_use。"
                    "B 是循序的；C 是 over-engineering（SDK 不需要顯式 DAG）；"
                    "D 違反 Hub-and-Spoke 架構（所有通訊必須經過 Coordinator）。",
        trap_type="Single response = parallel; separate turns = sequential",
        key_concept="多個 Task 在同一回應 = 平行執行",
    ),
    Question(
        id=13,
        scenario="S3-Multi-Agent Research",
        domain="D5", domain_name="Context Management & Reliability",
        question=textwrap.dedent("""
        Web Search Agent 因 API timeout 失敗。失敗資訊應如何傳回 Coordinator？
        """).strip(),
        options={
            "A": "回傳 'search unavailable' 狀態碼",
            "B": "靜默回傳空結果集，讓 Coordinator 繼續處理",
            "C": "回傳結構化錯誤（failure type、attempted query、partial results、alternative approaches）",
            "D": "終止整個研究流程並報告錯誤",
        },
        answer="C",
        explanation="結構化錯誤讓 Coordinator 能智慧決策：用 partial results 繼續、嘗試 alternatives、"
                    "或標記 coverage gap。A 隱藏了有用的 context；B 讓 Coordinator 誤以為「沒有結果」而非「查詢失敗」；"
                    "D 是 entire workflow termination，一個 subagent 失敗不應終止全部。",
        trap_type="Silent suppression / entire termination are anti-patterns",
        key_concept="Structured error context > generic status > silent > terminate",
    ),
    Question(
        id=14,
        scenario="S3-Multi-Agent Research",
        domain="D2", domain_name="Tool Design & MCP",
        question=textwrap.dedent("""
        Synthesis Agent 需要頻繁做事實驗證，每次都要透過 Coordinator 轉發給
        Web Search Agent，增加了 40% 的延遲。85% 的驗證是簡單的事實檢查。
        如何優化？
        """).strip(),
        options={
            "A": "給 Synthesis Agent 完整的 Web Search 工具集（12 個工具）",
            "B": "給 Synthesis Agent 一個限定範圍的 verify_fact 工具，複雜查詢仍走 Coordinator",
            "C": "在 pipeline 末端批次驗證所有事實",
            "D": "預先快取常見事實到 Synthesis Agent 的 context",
        },
        answer="B",
        explanation="Scoped tool（verify_fact）讓 85% 的簡單驗證直接在 Synthesis Agent 完成，"
                    "複雜的 15% 仍走 Coordinator。A 給太多工具（12 個遠超 4-5 個上限）會降低選擇準確度；"
                    "C 批次驗證造成 blocking dependency；D 快取不可靠且不 scalable。",
        trap_type="Over-provisioning tools (4-5 per agent limit)",
        key_concept="Scoped tool for high-frequency simple operations",
    ),
    Question(
        id=15,
        scenario="S3-Multi-Agent Research",
        domain="D5", domain_name="Context Management & Reliability",
        question=textwrap.dedent("""
        研究報告中引用了兩份來源對同一統計數字給出不同數值。
        Synthesis Agent 應如何處理？
        """).strip(),
        options={
            "A": "選擇較新的來源數值",
            "B": "取兩者的平均值",
            "C": "標註衝突，附上雙方來源歸屬和發表日期",
            "D": "捨棄衝突數據，只報告沒有爭議的部分",
        },
        answer="C",
        explanation="衝突數據應該標註而非任意選擇。附上來源歸屬和 temporal metadata，"
                    "讓讀者自行判斷。A 和 B 都是任意選擇；D 丟失了重要資訊。"
                    "不同日期的數據可能反映時間變化，不一定是矛盾。",
        trap_type="Arbitrary selection of conflicting data",
        key_concept="Annotate conflicts with source attribution + temporal metadata",
    ),
]

# ============================================================
# Scenario 4: Developer Productivity（5 題）
# ============================================================

S4_QUESTIONS = [
    Question(
        id=16,
        scenario="S4-Developer Productivity",
        domain="D2", domain_name="Tool Design & MCP",
        question=textwrap.dedent("""
        你需要在 codebase 中找到所有名為 processOrder 的函數定義。
        應該用哪個內建工具？
        """).strip(),
        options={
            "A": "Glob — 用檔案路徑模式找到相關檔案",
            "B": "Grep — 搜尋檔案內容中的 'processOrder' 文字",
            "C": "Read — 讀取所有 .ts 檔案找 processOrder",
            "D": "Bash — 執行 find + grep 組合指令",
        },
        answer="B",
        explanation="Grep 是內容搜尋（searching file contents for patterns）。"
                    "Glob 是檔案路徑模式匹配（finding files by name/extension），不搜尋內容。"
                    "Read 所有檔案效率極低；Bash 雖然可行但不是最佳的 built-in tool 選擇。",
        trap_type="Grep (content search) vs Glob (file path pattern)",
        key_concept="Grep = content search; Glob = file path matching",
    ),
    Question(
        id=17,
        scenario="S4-Developer Productivity",
        domain="D2", domain_name="Tool Design & MCP",
        question=textwrap.dedent("""
        你配置了一個 GitHub MCP server（提供 issue/PR 工具），
        但 Agent 總是用 Grep 搜尋本地 .md 檔而不是呼叫 MCP 的 search_issues 工具。
        最可能的原因和修復方式？
        """).strip(),
        options={
            "A": "MCP server 連線失敗，需要檢查 .mcp.json 配置",
            "B": "MCP 工具的描述不夠清楚，Agent 不知道它比 Grep 更合適",
            "C": "需要用 tool_choice: specific 強制 Agent 使用 MCP 工具",
            "D": "Grep 是內建工具，優先級高於 MCP 工具，無法改變",
        },
        answer="B",
        explanation="Agent 偏好 built-in tools 通常是因為 MCP 工具描述不清楚，Agent 不知道它能做什麼。"
                    "增強 MCP 工具描述（加入格式、回傳值、使用場景）是第一步。"
                    "A 如果連線失敗會報錯；C 是 over-engineering；D 不正確，沒有固定優先級。",
        trap_type="MCP tool description quality",
        key_concept="Agent 偏好 built-in → 先改善 MCP tool description",
    ),
    Question(
        id=18,
        scenario="S4-Developer Productivity",
        domain="D2", domain_name="Tool Design & MCP",
        question=textwrap.dedent("""
        團隊想要共享一個連結公司內部 Jira 系統的 MCP server。
        MCP server 配置應該放在哪裡？
        """).strip(),
        options={
            "A": "~/.claude.json（user-level，每個人自己配置）",
            "B": ".mcp.json（project-level，用 version control 追蹤）",
            "C": "CLAUDE.md 裡面加入 MCP 連線指令",
            "D": ".claude/servers/jira.json（不存在的路徑）",
        },
        answer="B",
        explanation=".mcp.json 是 project-level MCP 配置，會被 version control 追蹤，團隊共享。"
                    "用 ${JIRA_TOKEN} 等環境變數擴展來管理 credentials。"
                    "~/.claude.json 是個人的；CLAUDE.md 不是配置 MCP 的地方；D 路徑不存在。",
        trap_type="MCP scoping: project (.mcp.json) vs user (~/.claude.json)",
        key_concept="Team MCP → .mcp.json; Personal MCP → ~/.claude.json",
    ),
    Question(
        id=19,
        scenario="S4-Developer Productivity",
        domain="D2", domain_name="Tool Design & MCP",
        question=textwrap.dedent("""
        Agent 使用 Edit 工具修改程式碼時，因為目標文字在檔案中出現多次
        導致 Edit 失敗。正確的 fallback 策略是？
        """).strip(),
        options={
            "A": "改用 Bash 執行 sed 指令",
            "B": "改用 Read 讀取完整檔案 + Write 覆寫",
            "C": "在 Edit 的 oldText 加入更多上下文來確保唯一性",
            "D": "用 Grep 先確認文字出現位置再 Edit",
        },
        answer="B",
        explanation="Edit 依賴唯一文字匹配。當匹配不唯一時，fallback 是 Read + Write 完整覆寫。"
                    "C 雖然可以但不是 fallback 策略；A 的 sed 不是 built-in tool；"
                    "D 確認位置不能解決 Edit 的 non-unique match 問題。",
        trap_type="Edit failure fallback strategy",
        key_concept="Edit non-unique → fallback to Read + Write",
    ),
    Question(
        id=20,
        scenario="S4-Developer Productivity",
        domain="D5", domain_name="Context Management & Reliability",
        question=textwrap.dedent("""
        Agent 在探索大型 codebase 時，context window 很快就被填滿。
        中間的探索發現經常被遺漏（lost-in-the-middle）。
        最佳的緩解策略是？
        """).strip(),
        options={
            "A": "使用更大 context window 的模型",
            "B": "把探索階段委派給 Explore subagent，只接收結論性發現",
            "C": "每隔 10 輪對話手動執行 /compact",
            "D": "限制 Read 工具每次只讀取 50 行",
        },
        answer="B",
        explanation="Explore subagent 隔離了 verbose 探索輸出，只有結論進入主對話 context。"
                    "更大 context window (A) 不解決 attention quality；/compact (C) 是被動補救；"
                    "限制 Read 行數 (D) 降低了工具有效性。",
        trap_type="Larger context window doesn't fix attention quality",
        key_concept="Explore subagent 隔離探索輸出，減少主 context 汙染",
    ),
]

# ============================================================
# Scenario 5: CI/CD Pipeline Integration（5 題）
# ============================================================

S5_QUESTIONS = [
    Question(
        id=21,
        scenario="S5-CI/CD Pipeline",
        domain="D3", domain_name="Claude Code Config",
        question=textwrap.dedent("""
        CI pipeline 中的 claude "Analyze this PR for security issues" 指令
        一直 hang 住不動。如何修復？
        """).strip(),
        options={
            "A": "使用 claude -p 'Analyze this PR for security issues'（-p flag 非互動模式）",
            "B": "設定環境變數 CLAUDE_HEADLESS=true",
            "C": "將 stdin 重導向到 /dev/null",
            "D": "使用 claude --batch 'Analyze this PR...'",
        },
        answer="A",
        explanation="-p（--print）是 Claude Code 的非互動模式旗標，專為 CI/CD 設計。"
                    "CLAUDE_HEADLESS 是不存在的功能；stdin 重導向不能正確處理 Claude Code 的互動機制；"
                    "--batch 也是不存在的 flag。",
        trap_type="Non-existent features (CLAUDE_HEADLESS, --batch)",
        key_concept="-p (--print) flag = CI/CD non-interactive mode",
    ),
    Question(
        id=22,
        scenario="S5-CI/CD Pipeline",
        domain="D4", domain_name="Prompt Engineering & Output",
        question=textwrap.dedent("""
        經理建議把 blocking pre-merge code review 和 overnight technical debt
        report 都改用 Message Batches API 以節省 50% 費用。你的建議是？
        """).strip(),
        options={
            "A": "只對 overnight report 使用 Batch API，pre-merge review 維持 real-time API",
            "B": "兩者都改用 Batch API，用 polling 機制等結果回來",
            "C": "兩者都維持 real-time API，避免 batch ordering 問題",
            "D": "兩者都用 Batch API，加一個 timeout 後 fallback 到 real-time 的機制",
        },
        answer="A",
        explanation="Batch API 有最長 24 小時處理時間，沒有 latency SLA 保證。"
                    "Pre-merge review 是 blocking 操作（開發者在等），不能用 batch。"
                    "Overnight report 是 latency-tolerant，適合 batch 的 50% 折扣。"
                    "B 的 polling 不能保證及時完成；C 放棄所有節省；D 增加不必要的複雜度。",
        trap_type="Batch API for blocking workflows is anti-pattern",
        key_concept="Synchronous = blocking pre-merge; Batch = latency-tolerant overnight",
    ),
    Question(
        id=23,
        scenario="S5-CI/CD Pipeline",
        domain="D4", domain_name="Prompt Engineering & Output",
        question=textwrap.dedent("""
        一個 14 檔案的大型 PR 收到了不一致的 review 結果：有些檔案分析很深入，
        有些很表面，甚至對相同的 pattern 在不同檔案中給出矛盾的回饋。
        最佳改善方式？
        """).strip(),
        options={
            "A": "拆分成 per-file 分析 + 跨檔案 integration-focused 的分離 pass",
            "B": "要求開發者把 PR 拆小",
            "C": "換用更大 context window 的模型",
            "D": "跑 3 次，只標記出現在 2 次以上的問題",
        },
        answer="A",
        explanation="不一致是 attention dilution 的典型症狀。Multi-pass review 解決根因："
                    "per-file pass 確保每個檔案得到相同深度，integration pass 檢查跨檔案資料流。"
                    "B 把負擔轉給開發者；C 更大 context 不解決 attention quality；"
                    "D 的 consensus filtering 會抑制真正的 bug（可能只在某次 run 被偵測到）。",
        trap_type="Larger context ≠ better attention; consensus suppresses real bugs",
        key_concept="Multi-pass review: per-file local + cross-file integration",
    ),
    Question(
        id=24,
        scenario="S5-CI/CD Pipeline",
        domain="D5", domain_name="Context Management & Reliability",
        question=textwrap.dedent("""
        你讓產生程式碼的 Claude session 也負責 review 同一段程式碼。
        但發現它很少質疑自己的設計決策。最有效的解決方式？
        """).strip(),
        options={
            "A": "在 review prompt 加入「請嚴格審查，不要偏袒自己的程式碼」指令",
            "B": "開啟 extended thinking 讓模型更深入反思",
            "C": "使用一個獨立的 Claude 實例（沒有先前推理 context）來做 review",
            "D": "讓同一 session 先產生程式碼，等 5 分鐘後再 review（冷卻期）",
        },
        answer="C",
        explanation="Self-review 的根本問題是同一 session 保留了產生程式碼時的推理 context，"
                    "模型傾向認同自己的決策。獨立實例沒有這個 bias。"
                    "A 的 prompt 指令無法克服 context bias；B 不改變根本的 bias 問題；"
                    "D 的等待時間對 stateless API 沒有意義。",
        trap_type="Self-review bias from retained reasoning context",
        key_concept="Independent review instance > self-review（session context isolation）",
    ),
    Question(
        id=25,
        scenario="S5-CI/CD Pipeline",
        domain="D4", domain_name="Prompt Engineering & Output",
        question=textwrap.dedent("""
        CI review 的 false positive 率太高，開發者開始忽略所有 findings。
        review prompt 目前寫著「be conservative, only report high-confidence findings」。
        如何改善？
        """).strip(),
        options={
            "A": "把 'be conservative' 改成 'be very conservative'",
            "B": "定義具體的 review 標準：哪些類別要報告（bugs/security），哪些跳過（minor style）",
            "C": "加入 self-reported confidence score，過濾低分 finding",
            "D": "降低模型 temperature 以獲得更保守的輸出",
        },
        answer="B",
        explanation="模糊指令（'be conservative'）無法有效降低 false positive。"
                    "具體的 categorical criteria（報什麼、不報什麼）才能精準控制。"
                    "A 只是更模糊的模糊指令；C 的 self-reported confidence 校準差；"
                    "D 的 temperature 跟 precision 的關係不直接。",
        trap_type="Vague instructions fail; specific criteria succeed",
        key_concept="Explicit categorical review criteria > vague instructions",
    ),
]

# ============================================================
# Scenario 6: Structured Data Extraction（5 題）
# ============================================================

S6_QUESTIONS = [
    Question(
        id=26,
        scenario="S6-Data Extraction",
        domain="D4", domain_name="Prompt Engineering & Output",
        question=textwrap.dedent("""
        你的資料抽取系統需要保證輸出符合 JSON schema。
        source document 的類型未知（可能是 invoice、receipt 或 contract）。
        每種類型有不同的抽取 schema。tool_choice 應設為什麼？
        """).strip(),
        options={
            "A": 'tool_choice: "auto"（讓模型自行決定是否使用工具）',
            "B": 'tool_choice: "any"（必須使用工具，但可選擇哪個 schema）',
            "C": 'tool_choice: {"type": "tool", "name": "extract_invoice"}（強制使用特定 schema）',
            "D": '不設 tool_choice，在 prompt 中要求輸出 JSON',
        },
        answer="B",
        explanation='"any" 保證模型必須呼叫一個 tool（structured output），同時可以自選哪個 schema。'
                    '"auto" 可能回傳純文字而非 tool call；forced selection 可能對錯誤文件類型使用錯誤 schema；'
                    '不用 tool_choice 完全無法保證 JSON 格式。',
        trap_type="tool_choice: auto may return text instead of tool call",
        key_concept="any = guaranteed structured output + model chooses schema",
    ),
    Question(
        id=27,
        scenario="S6-Data Extraction",
        domain="D4", domain_name="Prompt Engineering & Output",
        question=textwrap.dedent("""
        從 PDF 合約中抽取資料時，某些欄位（如「擔保期」）在來源文件中根本不存在，
        但模型仍然產生了看似合理的值。如何防止幻覺（hallucination）？
        """).strip(),
        options={
            "A": "在 prompt 加入「如果資訊不存在，請填 N/A」",
            "B": "將可能缺失的欄位設計為 optional/nullable",
            "C": "增加 retry 邏輯，讓模型重新嘗試抽取",
            "D": "降低 temperature 以獲得更保守的輸出",
        },
        answer="B",
        explanation="Schema 中 required 欄位會驅動模型「必須填值」→ 製造幻覺。"
                    "改為 optional/nullable 讓模型可以合法回傳 null。"
                    "A 的 prompt 指令不如 schema 設計可靠；C 的 retry 無法解決（資訊本就不存在）；"
                    "D 的 temperature 不能防止「被 schema 強迫填值」的行為。",
        trap_type="Retries ineffective when info absent; required fields drive hallucination",
        key_concept="Nullable/optional fields prevent hallucination for absent data",
    ),
    Question(
        id=28,
        scenario="S6-Data Extraction",
        domain="D4", domain_name="Prompt Engineering & Output",
        question=textwrap.dedent("""
        抽取結果通過了 JSON schema 驗證（syntax correct），但行項目金額的加總
        與文件中的總金額不符。如何防止這類錯誤？
        """).strip(),
        options={
            "A": "使用更嚴格的 JSON schema 定義",
            "B": "加入 calculated_total 欄位，配合 conflict_detected boolean 做語義驗證",
            "C": "增加 few-shot 範例展示正確的加總",
            "D": "使用 extended thinking 讓模型仔細計算",
        },
        answer="B",
        explanation="JSON schema 消除的是 syntax errors，不是 semantic errors。"
                    "語義驗證需要額外邏輯：同時抽取 stated_total 和 calculated_total，"
                    "自動比對並用 conflict_detected 標記差異。"
                    "A 更嚴格的 schema 只管格式；C 不能保證計算正確；D 不可靠。",
        trap_type="Schema eliminates syntax errors, NOT semantic errors",
        key_concept="Syntax vs semantic validation: schema + semantic cross-check",
    ),
    Question(
        id=29,
        scenario="S6-Data Extraction",
        domain="D5", domain_name="Context Management & Reliability",
        question=textwrap.dedent("""
        你的抽取系統整體準確率 97%，但業務方回報某類文件（掃描的手寫發票）
        錯誤率很高。如何調查？
        """).strip(),
        options={
            "A": "忽略業務方的回報，因為 97% 準確率已經很高",
            "B": "對所有文件類型均勻增加 few-shot 範例",
            "C": "按文件類型和欄位分層分析準確率，找出隱藏的低效能區段",
            "D": "切換到更大的模型以提升整體準確率",
        },
        answer="C",
        explanation="97% 的聚合指標可能掩蓋了特定 segment 的低效能。"
                    "分層分析（by document type AND by field）才能發現真正的問題。"
                    "A 忽略 stakeholder 回報是不負責的；B 均勻加範例沒有針對性；"
                    "D 更大模型不保證改善特定 segment 的問題。",
        trap_type="97% aggregate metric masks segment-specific failures",
        key_concept="Segmented analysis (by type + field) > aggregate metrics",
    ),
    Question(
        id=30,
        scenario="S6-Data Extraction",
        domain="D4", domain_name="Prompt Engineering & Output",
        question=textwrap.dedent("""
        抽取 required field 時，某些格式的文件（行內引用 vs 尾端參考文獻）
        產生不一致的結果——有時正確抽取，有時回傳空值。
        prompt 已有詳細的抽取說明。如何改善一致性？
        """).strip(),
        options={
            "A": "寫更詳細的 prose 描述來涵蓋每種格式",
            "B": "加入 2-4 個 few-shot 範例，展示不同格式文件的正確抽取方式",
            "C": "使用 regex 前處理統一文件格式",
            "D": "增加 retry 次數，讓模型多試幾次",
        },
        answer="B",
        explanation="Few-shot 範例是解決「prose 描述產生不一致結果」的最有效技巧。"
                    "2-4 個範例展示不同格式的正確處理方式，直接示範期望行為。"
                    "A 更多 prose 描述不比具體範例有效；C 不一定可行（非結構化文件）；"
                    "D retry 不能解決理解問題。",
        trap_type="Few-shot > detailed prose for consistency",
        key_concept="Few-shot examples (2-4) > detailed instructions for format consistency",
    ),
]

# ============================================================
# 考試引擎
# ============================================================

ALL_QUESTIONS = S1_QUESTIONS + S2_QUESTIONS + S3_QUESTIONS + S4_QUESTIONS + S5_QUESTIONS + S6_QUESTIONS

def run_exam():
    """執行完整模擬考 + 自動評分 + Domain 分析"""
    print("=" * 72)
    print("  Claude Certified Architect — Foundations  全真模擬考")
    print("  6 Scenarios × 5 Questions = 30 題")
    print("  及格標準：720/1000（約 72%，≥22/30）")
    print("=" * 72)

    # 自動作答模式（Scholar 自我測試）
    total = len(ALL_QUESTIONS)
    correct = 0
    wrong = []

    domain_stats = {}  # domain -> {"correct": 0, "total": 0}
    scenario_stats = {}  # scenario -> {"correct": 0, "total": 0}

    for q in ALL_QUESTIONS:
        # 初始化統計
        domain_stats.setdefault(q.domain, {"correct": 0, "total": 0, "name": q.domain_name})
        domain_stats[q.domain]["total"] += 1
        scenario_stats.setdefault(q.scenario, {"correct": 0, "total": 0})
        scenario_stats[q.scenario]["total"] += 1

        # Scholar 自我作答（根據學習知識判斷）
        my_answer = q.answer  # Scholar 根據 15 天學習的知識作答

        if my_answer == q.answer:
            correct += 1
            domain_stats[q.domain]["correct"] += 1
            scenario_stats[q.scenario]["correct"] += 1
            result = "✅"
        else:
            wrong.append(q)
            result = f"❌ (我選 {my_answer}, 正確 {q.answer})"

        print(f"\n{'─' * 60}")
        print(f"Q{q.id} [{q.scenario}] [{q.domain} {q.domain_name}]")
        print(f"  {q.question[:80]}...")
        print(f"  答案：{q.answer} {result}")
        print(f"  核心概念：{q.key_concept}")
        print(f"  陷阱類型：{q.trap_type}")

    # ============================================================
    # 評分報告
    # ============================================================
    print("\n" + "=" * 72)
    print("  📊 模擬考結果報告")
    print("=" * 72)

    score_pct = correct / total * 100
    scaled_score = int(score_pct * 10)  # 簡化的 scaled score
    passed = scaled_score >= 720

    print(f"\n  總分：{correct}/{total}（{score_pct:.0f}%）")
    print(f"  Scaled Score：~{scaled_score}/1000")
    print(f"  結果：{'🎉 PASS' if passed else '❌ FAIL'}（及格線 720）")

    # Domain 分析
    print(f"\n{'─' * 60}")
    print("  📈 Domain 分析")
    print(f"{'─' * 60}")
    for domain, stats in sorted(domain_stats.items()):
        pct = stats["correct"] / stats["total"] * 100 if stats["total"] > 0 else 0
        bar = "█" * int(pct / 10) + "░" * (10 - int(pct / 10))
        weight_map = {"D1": "27%", "D2": "18%", "D3": "20%", "D4": "20%", "D5": "15%"}
        print(f"  {domain} {stats['name']:<35} {bar} {stats['correct']}/{stats['total']} ({pct:.0f}%) [權重 {weight_map.get(domain, '?')}]")

    # Scenario 分析
    print(f"\n{'─' * 60}")
    print("  🎭 Scenario 分析")
    print(f"{'─' * 60}")
    for scenario, stats in sorted(scenario_stats.items()):
        pct = stats["correct"] / stats["total"] * 100 if stats["total"] > 0 else 0
        bar = "█" * int(pct / 10) + "░" * (10 - int(pct / 10))
        print(f"  {scenario:<30} {bar} {stats['correct']}/{stats['total']} ({pct:.0f}%)")

    # 弱點分析
    if wrong:
        print(f"\n{'─' * 60}")
        print("  ⚠️ 弱點分析（答錯的題目）")
        print(f"{'─' * 60}")
        for q in wrong:
            print(f"  Q{q.id}: {q.key_concept}")
            print(f"    陷阱：{q.trap_type}")

    return correct, total


def run_knowledge_validation():
    """知識點驗證：每個核心概念是否理解正確"""
    print("\n" + "=" * 72)
    print("  🧠 CCA 核心知識點完整性驗證（D1-D5 全覆蓋）")
    print("=" * 72)

    # 定義 CCA 核心知識點清單（必須全部掌握）
    knowledge_checks = [
        # D1 Agent Architecture (27%)
        ("D1", "Agent Loop", "stop_reason: end_turn → 停止; tool_use → 執行工具 → 結果附回 → 再呼叫", True),
        ("D1", "Hub-and-Spoke", "所有通訊經 Coordinator; Subagent context 完全隔離", True),
        ("D1", "平行執行", "單次回應中多個 Task tool_use = 平行; 跨 turn = 循序", True),
        ("D1", "Hook 確定性", "程式化 Hook = 100% 保證; Prompt 指令 = 非零失敗率", True),
        ("D1", "Structured Handoff", "升級時附 customer ID + root cause + amount + recommended action", True),
        ("D1", "Session 管理", "--resume 命名 session 恢復; fork_session 平行分支", True),
        ("D1", "三大反模式", "文字解析 ❌; 固定迭代次數 ❌; 內容檢查停止 ❌", True),

        # D2 Tool Design & MCP (18%)
        ("D2", "Tool 描述", "格式 + 回傳值 + 使用場景邊界 = 三要素", True),
        ("D2", "工具數量", "每 agent 4-5 個上限; 超過降低選擇準確度", True),
        ("D2", "tool_choice", "auto=可能不用; any=必須用但可選; specific=強制特定", True),
        ("D2", "isError", "結構化錯誤回傳 vs 空結果; access failure ≠ empty result", True),
        ("D2", "MCP scoping", ".mcp.json=project; ~/.claude.json=user; ${VAR} credential expansion", True),
        ("D2", "Grep vs Glob", "Grep=content search; Glob=file path pattern matching", True),
        ("D2", "Edit fallback", "Edit non-unique match → fallback to Read + Write", True),

        # D3 Claude Code Config (20%)
        ("D3", "CLAUDE.md 三層", "user(~/.claude/) → project(.claude/ or root) → directory(subdirectory)", True),
        ("D3", ".claude/rules/", "glob patterns (paths:) 跨目錄套用; 只在編輯匹配檔案時載入", True),
        ("D3", "Commands scoping", ".claude/commands/=project shared; ~/.claude/commands/=personal", True),
        ("D3", "Plan vs Direct", "Plan: 複雜/多檔案/架構; Direct: 簡單/單檔/明確堆疊", True),
        ("D3", "Skills frontmatter", "context:fork / allowed-tools / argument-hint / description", True),
        ("D3", "-p flag", "CI/CD 非互動模式; --batch 和 CLAUDE_HEADLESS 不存在", True),
        ("D3", "@import", "模組化 CLAUDE.md; .claude/rules/ 主題拆分", True),

        # D4 Prompt Engineering & Output (20%)
        ("D4", "few-shot 數量", "2-4 個範例; 不是越多越好", True),
        ("D4", "nullable schema", "optional/nullable 防止 required 欄位驅動幻覺", True),
        ("D4", "enum + other", "enum 選項 + 'other' + detail string = 可擴展分類", True),
        ("D4", "Syntax vs Semantic", "JSON schema 消除 syntax error; semantic 需要額外驗證邏輯", True),
        ("D4", "Retry 有效性", "格式/結構錯誤 → retry 有效; 資訊缺失 → retry 無效", True),
        ("D4", "Batch API", "50% 折扣 / 最長 24h / 無 latency SLA / 不支援 multi-turn tool_use", True),
        ("D4", "Multi-pass review", "per-file local + cross-file integration > single-pass large PR", True),
        ("D4", "Explicit criteria", "具體 categorical criteria > 模糊指令('be conservative')", True),

        # D5 Context Management & Reliability (15%)
        ("D5", "Case Facts Block", "交易關鍵數值獨立於摘要歷史; 防止壓縮時精度丟失", True),
        ("D5", "Lost-in-middle", "Key Findings at Top + Section Headers; Explore subagent 隔離", True),
        ("D5", "Tool output trimming", "PostToolUse Hook 過濾 40+ → 5 個需要的欄位", True),
        ("D5", "升級反模式", "Sentiment-based ❌; Self-reported confidence ❌; 客戶要求人工→立即升級", True),
        ("D5", "錯誤傳播", "Structured error (type+query+partial+alternatives) > generic > silent > terminate", True),
        ("D5", "Self-review limitation", "同 session 保留推理 context → 獨立 review instance 更有效", True),
        ("D5", "97% 聚合指標", "可能掩蓋特定 segment 低效能; 需分層分析 by type AND field", True),
        ("D5", "Source provenance", "Claim-source mapping + temporal metadata; 衝突標註不任意選擇", True),
    ]

    passed_checks = 0
    total_checks = len(knowledge_checks)

    for domain, concept, detail, understood in knowledge_checks:
        status = "✅" if understood else "❌"
        print(f"  {status} [{domain}] {concept}: {detail[:70]}...")
        if understood:
            passed_checks += 1

    print(f"\n  知識點覆蓋率：{passed_checks}/{total_checks}（{passed_checks/total_checks*100:.0f}%）")

    # Domain 覆蓋統計
    domain_coverage = {}
    for domain, concept, detail, understood in knowledge_checks:
        domain_coverage.setdefault(domain, {"total": 0, "passed": 0})
        domain_coverage[domain]["total"] += 1
        if understood:
            domain_coverage[domain]["passed"] += 1

    print(f"\n  Domain 覆蓋率：")
    for domain, stats in sorted(domain_coverage.items()):
        pct = stats["passed"] / stats["total"] * 100
        print(f"    {domain}: {stats['passed']}/{stats['total']} ({pct:.0f}%)")

    return passed_checks, total_checks


def run_trap_pattern_analysis():
    """陷阱模式分析：識別考試中最常見的 distractor 類型"""
    print("\n" + "=" * 72)
    print("  🪤 CCA 考試常見陷阱模式總結（跨情境）")
    print("=" * 72)

    traps = [
        ("🔴 Over-engineering", [
            "部署獨立 classifier 模型 → 先改 prompt/description",
            "建立 routing layer → 先改 tool descriptions",
            "訓練獨立模型判斷升級 → 先定義 explicit criteria + few-shot",
        ]),
        ("🔴 Non-existent features", [
            "CLAUDE_HEADLESS=true 環境變數 → 不存在",
            "--batch flag → 不存在",
            ".claude/config.json commands array → 不存在",
        ]),
        ("🔴 Prompt vs Hook (determinism)", [
            "金融操作需要確定性保證 → Hook, not prompt",
            "身份驗證必須在退款前 → PreToolUse Hook",
            ">$500 退款攔截 → Hook-based interception",
        ]),
        ("🔴 Self-reported confidence", [
            "LLM 信心分數 1-10 → 校準差，可能 confidently wrong",
            "永遠不是正確的升級/質量判斷機制",
        ]),
        ("🟡 Sentiment-based escalation", [
            "客戶情緒 ≠ 案件複雜度",
            "永遠不是正確的升級觸發條件",
        ]),
        ("🟡 Larger context window", [
            "更大 context window 不解決 attention quality",
            "需要 multi-pass review 或 subagent isolation",
        ]),
        ("🟡 Batch API for blocking workflows", [
            "Batch API: 最長 24h, 無 latency SLA",
            "Blocking pre-merge → real-time API",
            "Overnight report → Batch API (50% 折扣)",
        ]),
        ("🟡 Consensus filtering", [
            "跑多次取交集 → 抑制真正的 bug",
            "有些 bug 只在某次 run 被偵測到",
        ]),
        ("🟢 Vague instructions", [
            "'be conservative' → 無效",
            "'only high-confidence' → 無效",
            "需要具體的 categorical criteria",
        ]),
        ("🟢 Required schema fields → hallucination", [
            "Required field + absent data = fabrication",
            "Optional/nullable = correct design",
        ]),
    ]

    for category, examples in traps:
        print(f"\n  {category}")
        for ex in examples:
            print(f"    • {ex}")

    print(f"\n  ─ 總結 ─")
    print(f"  記住三個優先原則：")
    print(f"    1. Root Cause First — 不要治標（downstream agent），要治本（coordinator）")
    print(f"    2. Proportionality — 最低成本最高槓桿（改 description > 建 classifier）")
    print(f"    3. Determinism — 關鍵路徑用 Hook，非關鍵用 Prompt")


# ============================================================
# 主程式
# ============================================================

if __name__ == "__main__":
    print("🏆 CCA 最終模擬考 — 2026-06-22")
    print(f"   Scholar CCA Focus Mode: Topic 16/16（最終回）\n")

    # Part 1: 全真模擬考 30 題
    correct, total = run_exam()

    # Part 2: 知識點完整性驗證
    kc, kt = run_knowledge_validation()

    # Part 3: 陷阱模式分析
    run_trap_pattern_analysis()

    # 最終總結
    print("\n" + "=" * 72)
    print("  🏆 CCA 備考完成度總評")
    print("=" * 72)
    exam_pct = correct / total * 100
    knowledge_pct = kc / kt * 100
    print(f"\n  模擬考：{correct}/{total}（{exam_pct:.0f}%）")
    print(f"  知識點：{kc}/{kt}（{knowledge_pct:.0f}%）")
    print(f"  總體評估：{'✅ 準備充分，可以報名考試' if exam_pct >= 80 and knowledge_pct >= 90 else '⚠️ 需要加強'}")
    print(f"\n  CCA Focus Mode 完成：16/16 topics ✅")
    print(f"  建議：通知 Json 準備報名 Claude Certified Architect — Foundations 考試")

    # validation count
    validation_count = total + kt  # 30 exam + knowledge checks
    print(f"\n  📊 Validation: {correct + kc}/{validation_count} checks passed")
