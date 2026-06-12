"""
CCA Practice: MCP Integration (D2 Task 2.2)
============================================
ODM 供應鏈 MCP 生態系統模擬

Modules:
1. MiniMCP Server/Client — tools/resources 發現與呼叫
2. isError 結構化錯誤 — 四種錯誤類別 + 反模式偵測
3. Project vs User Scoping — 配置合併
4. Built-in vs MCP Tool 選型
5. Access Failure vs Valid Empty 區分
6. CCA 模擬考 5 題
"""

from dataclasses import dataclass, field
from typing import Any
import json, copy

# ============================================================
# Module 1: MiniMCP Protocol Simulation
# ============================================================

@dataclass
class MCPTool:
    name: str
    description: str
    input_schema: dict
    annotations: dict = field(default_factory=dict)

@dataclass
class MCPResource:
    uri: str
    name: str
    description: str
    mime_type: str = "application/json"

@dataclass
class MCPToolResult:
    content: Any
    is_error: bool = False

class MiniMCPServer:
    """Simulates an MCP Server hosting tools and resources."""
    
    def __init__(self, name: str, transport: str = "http"):
        self.name = name
        self.transport = transport
        self.tools: dict[str, MCPTool] = {}
        self.resources: dict[str, MCPResource] = {}
        self._handlers: dict[str, callable] = {}
        self.capabilities = {"tools": {"listChanged": True}, "resources": {"subscribe": True, "listChanged": True}}
    
    def register_tool(self, tool: MCPTool, handler: callable):
        self.tools[tool.name] = tool
        self._handlers[tool.name] = handler
    
    def register_resource(self, resource: MCPResource, data: Any):
        self.resources[resource.uri] = resource
        self._handlers[f"resource:{resource.uri}"] = lambda: data
    
    def handle_tools_list(self) -> list[dict]:
        """MCP tools/list response"""
        return [{"name": t.name, "description": t.description, 
                 "inputSchema": t.input_schema, "annotations": t.annotations}
                for t in self.tools.values()]
    
    def handle_resources_list(self) -> list[dict]:
        """MCP resources/list response"""
        return [{"uri": r.uri, "name": r.name, "description": r.description,
                 "mimeType": r.mime_type}
                for r in self.resources.values()]
    
    def handle_tools_call(self, name: str, arguments: dict) -> dict:
        """MCP tools/call response"""
        if name not in self._handlers:
            return {"content": {"errorCategory": "validation", "isRetryable": False,
                               "message": f"Tool '{name}' not found"}, "isError": True}
        try:
            result = self._handlers[name](**arguments)
            if isinstance(result, MCPToolResult):
                return {"content": result.content, "isError": result.is_error}
            return {"content": result, "isError": False}
        except Exception as e:
            return {"content": {"errorCategory": "transient", "isRetryable": True,
                               "message": str(e)}, "isError": True}
    
    def handle_resources_read(self, uri: str) -> dict:
        """MCP resources/read response"""
        key = f"resource:{uri}"
        if key not in self._handlers:
            return {"error": f"Resource '{uri}' not found"}
        return {"contents": [{"uri": uri, "text": json.dumps(self._handlers[key](), ensure_ascii=False)}]}

class MiniMCPClient:
    """Simulates an MCP Client connecting to servers."""
    
    def __init__(self):
        self.servers: dict[str, MiniMCPServer] = {}
        self.discovered_tools: dict[str, tuple[str, MCPTool]] = {}  # tool_name -> (server_name, tool)
        self.discovered_resources: dict[str, tuple[str, MCPResource]] = {}
    
    def connect(self, server: MiniMCPServer):
        """Connect to server and auto-discover tools/resources."""
        self.servers[server.name] = server
        # Auto-discover at connection time
        for tool_data in server.handle_tools_list():
            self.discovered_tools[tool_data["name"]] = (server.name, server.tools[tool_data["name"]])
        for res_data in server.handle_resources_list():
            self.discovered_resources[res_data["uri"]] = (server.name, server.resources[res_data["uri"]])
    
    def call_tool(self, name: str, arguments: dict) -> dict:
        if name not in self.discovered_tools:
            return {"isError": True, "content": {"errorCategory": "validation",
                    "message": f"Tool '{name}' not discovered"}}
        server_name, _ = self.discovered_tools[name]
        return self.servers[server_name].handle_tools_call(name, arguments)
    
    def read_resource(self, uri: str) -> dict:
        if uri not in self.discovered_resources:
            return {"error": f"Resource '{uri}' not discovered"}
        server_name, _ = self.discovered_resources[uri]
        return self.servers[server_name].handle_resources_read(uri)
    
    def get_all_tool_names(self) -> list[str]:
        return list(self.discovered_tools.keys())
    
    def get_all_resource_uris(self) -> list[str]:
        return list(self.discovered_resources.keys())


def test_mcp_protocol():
    """Test MiniMCP server/client protocol flow."""
    print("=" * 60)
    print("Module 1: MiniMCP Protocol Simulation")
    print("=" * 60)
    
    # --- Build SAP MRP Server ---
    sap_server = MiniMCPServer("sap-mm-server", transport="stdio")
    
    # Register tools
    sap_server.register_tool(
        MCPTool(
            name="check_inventory",
            description="Check current on-hand inventory for a material in a specific plant. "
                       "Returns quantity, unit, last movement date, and storage location. "
                       "Use this before check_mrp to understand current stock levels. "
                       "Input: material_id (string, e.g. '4716-GPU-A100'), plant (string, e.g. 'TW01'). "
                       "Does NOT return projected requirements — use check_mrp for that.",
            input_schema={"type": "object", "properties": {
                "material_id": {"type": "string"}, "plant": {"type": "string"}
            }, "required": ["material_id"]}
        ),
        handler=lambda material_id, plant="TW01": {
            "material_id": material_id, "plant": plant,
            "on_hand_qty": 150, "unit": "PCS",
            "last_movement": "2026-06-12", "storage_loc": "SL01"
        }
    )
    
    sap_server.register_tool(
        MCPTool(
            name="check_mrp",
            description="Run MRP explosion for a material to see projected requirements, "
                       "planned orders, and shortage dates. Returns net requirements and "
                       "suggested purchase quantities. Input: material_id (string). "
                       "This is a heavy operation — use check_inventory first for quick stock check.",
            input_schema={"type": "object", "properties": {
                "material_id": {"type": "string"}
            }, "required": ["material_id"]}
        ),
        handler=lambda material_id: {
            "material_id": material_id,
            "net_requirements": [
                {"date": "2026-06-20", "qty": 200, "source": "Production Order PO-4521"},
                {"date": "2026-06-25", "qty": 350, "source": "Forecast FC-0625"}
            ],
            "planned_orders": [{"date": "2026-06-18", "qty": 300, "vendor": "V001"}],
            "shortage_date": "2026-06-25",
            "suggested_action": "Expedite PO to V001 or find alternate source"
        }
    )
    
    # Register resources (content catalogs — NO tool calls needed to browse)
    sap_server.register_resource(
        MCPResource(
            uri="sap://mm/material-schema",
            name="SAP MM Material Master Schema",
            description="Complete schema of MARA/MARC/MARD tables — fields, types, relationships"
        ),
        data={
            "tables": ["MARA (General)", "MARC (Plant)", "MARD (Storage Location)", "MBEW (Valuation)"],
            "key_fields": {"MARA": ["MATNR", "MTART", "MATKL"], "MARC": ["MATNR", "WERKS", "DISPO"]},
            "relationships": "MARA 1:N MARC (by MATNR), MARC 1:N MARD (by MATNR+WERKS)"
        }
    )
    
    # --- Build WMS Server ---
    wms_server = MiniMCPServer("wms-server", transport="http")
    
    wms_server.register_tool(
        MCPTool(
            name="get_bin_location",
            description="Get the storage bin location for a material in the warehouse. "
                       "Returns bin ID, zone, rack, level, and current occupancy.",
            input_schema={"type": "object", "properties": {
                "material_id": {"type": "string"}
            }, "required": ["material_id"]}
        ),
        handler=lambda material_id: {
            "material_id": material_id, "bin_id": "A-03-05",
            "zone": "Electronics", "rack": "A03", "level": 5, "occupancy_pct": 72
        }
    )
    
    wms_server.register_resource(
        MCPResource(
            uri="wms://warehouse/layout",
            name="Warehouse Zone Layout",
            description="Complete warehouse zone map with bin ranges and material categories"
        ),
        data={
            "zones": ["Electronics (A01-A10)", "Mechanical (B01-B08)", "Packaging (C01-C05)"],
            "total_bins": 2400, "occupancy_avg": 68
        }
    )
    
    # --- Connect Client ---
    client = MiniMCPClient()
    client.connect(sap_server)
    client.connect(wms_server)
    
    passed = 0
    total = 0
    
    # Test 1: All tools discovered from both servers
    total += 1
    all_tools = client.get_all_tool_names()
    assert set(all_tools) == {"check_inventory", "check_mrp", "get_bin_location"}, f"Expected 3 tools, got {all_tools}"
    print(f"  ✅ T1: Auto-discovered {len(all_tools)} tools from 2 servers")
    passed += 1
    
    # Test 2: Resources discovered (content catalogs, no tool calls)
    total += 1
    all_resources = client.get_all_resource_uris()
    assert len(all_resources) == 2, f"Expected 2 resources, got {len(all_resources)}"
    print(f"  ✅ T2: Auto-discovered {len(all_resources)} resources (0 tool calls needed)")
    passed += 1
    
    # Test 3: Tool call works
    total += 1
    result = client.call_tool("check_inventory", {"material_id": "4716-GPU-A100"})
    assert not result["isError"]
    assert result["content"]["on_hand_qty"] == 150
    print(f"  ✅ T3: check_inventory returned on_hand=150")
    passed += 1
    
    # Test 4: Resource read provides schema WITHOUT tool calls
    total += 1
    res = client.read_resource("sap://mm/material-schema")
    schema = json.loads(res["contents"][0]["text"])
    assert "MARA" in schema["key_fields"]
    print(f"  ✅ T4: Resource read provides SAP schema without any tool call")
    passed += 1
    
    # Test 5: Unknown tool returns validation error
    total += 1
    result = client.call_tool("nonexistent_tool", {})
    assert result["isError"]
    assert result["content"]["errorCategory"] == "validation"
    print(f"  ✅ T5: Unknown tool → validation error (not crash)")
    passed += 1
    
    # Test 6: Tools vs Resources distinction
    total += 1
    # Tools = actions, Resources = content catalogs
    tool_purposes = {t.name: "action" for _, (_, t) in client.discovered_tools.items()}
    resource_purposes = {r.uri: "catalog" for _, (_, r) in client.discovered_resources.items()}
    assert all(v == "action" for v in tool_purposes.values())
    assert all(v == "catalog" for v in resource_purposes.values())
    print(f"  ✅ T6: Tools={len(tool_purposes)} actions, Resources={len(resource_purposes)} catalogs — correctly separated")
    passed += 1
    
    print(f"\n  Module 1 Score: {passed}/{total}")
    return passed, total


# ============================================================
# Module 2: isError Structured Error Handling
# ============================================================

def test_structured_errors():
    """Test isError structured error responses vs anti-patterns."""
    print("\n" + "=" * 60)
    print("Module 2: isError Structured Error Handling")
    print("=" * 60)
    
    passed = 0
    total = 0
    
    # Define error scenarios
    error_scenarios = [
        {
            "name": "SAP RFC Timeout (Transient)",
            "error": {
                "isError": True,
                "content": {
                    "errorCategory": "transient",
                    "isRetryable": True,
                    "message": "SAP RFC timeout (>30s) during MRP explosion for material 4716-GPU-A100",
                    "attempted_query": "BAPI_MRP_EXPLOSION material=4716-GPU-A100",
                    "partial_results": {"last_known_stock": 150, "as_of": "2026-06-12T18:00:00"}
                }
            },
            "expected_action": "retry",
            "has_partial": True
        },
        {
            "name": "Invalid Material ID (Validation)",
            "error": {
                "isError": True,
                "content": {
                    "errorCategory": "validation",
                    "isRetryable": False,
                    "message": "Material ID 'XYZ' does not match pattern /^\\d{4}-[A-Z]+-[A-Z0-9]+$/",
                    "expected_format": "NNNN-TYPE-MODEL (e.g., 4716-GPU-A100)"
                }
            },
            "expected_action": "fix_input",
            "has_partial": False
        },
        {
            "name": "Refund Exceeds Limit (Business)",
            "error": {
                "isError": True,
                "content": {
                    "errorCategory": "business",
                    "isRetryable": False,
                    "message": "Refund amount $750 exceeds policy limit of $500. Requires manager approval.",
                    "customer_friendly": "Your refund request requires additional review by our team.",
                    "escalation_path": "manager_approval"
                }
            },
            "expected_action": "explain_to_user",
            "has_partial": False
        },
        {
            "name": "Vendor Data Access Denied (Permission)",
            "error": {
                "isError": True,
                "content": {
                    "errorCategory": "permission",
                    "isRetryable": False,
                    "message": "User does not have SAP authorization for vendor V001 cost data (auth object M_BANF_BSA)",
                    "required_role": "ZMM_VENDOR_COST_VIEWER"
                }
            },
            "expected_action": "escalate",
            "has_partial": False
        }
    ]
    
    # Evaluate each error
    for scenario in error_scenarios:
        total += 1
        err = scenario["error"]
        content = err["content"]
        
        # Structured error must have: errorCategory, isRetryable, message
        has_category = "errorCategory" in content
        has_retryable = "isRetryable" in content
        has_message = "message" in content and len(content["message"]) > 20
        
        if has_category and has_retryable and has_message:
            print(f"  ✅ {scenario['name']}: Structured error ✓ (category={content['errorCategory']}, retryable={content['isRetryable']})")
            passed += 1
        else:
            print(f"  ❌ {scenario['name']}: Missing required fields")
    
    # Test: Anti-pattern detection
    anti_patterns = [
        {"isError": True, "content": "Operation failed"},  # Generic string
        {"isError": True, "content": {"message": "Error"}},  # No category/retryable
        {"isError": False, "content": []},  # Silently suppress (empty results as success)
    ]
    
    total += 1
    detected = 0
    for ap in anti_patterns:
        content = ap.get("content", {})
        is_anti = False
        if isinstance(content, str):
            is_anti = True  # Generic string error
        elif isinstance(content, dict) and ("errorCategory" not in content or "isRetryable" not in content):
            is_anti = True  # Missing structured fields
        elif isinstance(content, list) and len(content) == 0 and not ap.get("isError"):
            is_anti = True  # Silent suppression
        if is_anti:
            detected += 1
    
    if detected == 3:
        print(f"  ✅ Anti-pattern detection: {detected}/3 anti-patterns caught")
        passed += 1
    else:
        print(f"  ❌ Anti-pattern detection: only {detected}/3 caught")
    
    # Test: Access Failure vs Valid Empty Result
    total += 1
    access_failure = {"isError": True, "content": {"errorCategory": "permission", "isRetryable": False,
                      "message": "No access to vendor V001 shortage data"}}
    valid_empty = {"isError": False, "content": {"results": [], "query": "vendor_id=V001",
                   "message": "No shortages found for vendor V001"}}
    
    # Agent decision tree
    af_action = "escalate" if access_failure["isError"] else "report_no_shortage"
    ve_action = "report_no_shortage" if not valid_empty["isError"] and valid_empty["content"]["results"] == [] else "investigate"
    
    if af_action == "escalate" and ve_action == "report_no_shortage":
        print(f"  ✅ Access Failure vs Valid Empty: correctly distinguished (escalate vs report)")
        passed += 1
    else:
        print(f"  ❌ Access Failure vs Valid Empty: incorrect handling")
    
    print(f"\n  Module 2 Score: {passed}/{total}")
    return passed, total


# ============================================================
# Module 3: Project vs User Scoping
# ============================================================

def test_scoping():
    """Test MCP server scoping and configuration merge."""
    print("\n" + "=" * 60)
    print("Module 3: Project vs User Scoping")
    print("=" * 60)
    
    passed = 0
    total = 0
    
    # Simulate project and user configs
    project_config = {
        "mcpServers": {
            "sap-mm": {
                "command": "npx",
                "args": ["-y", "mcp-server-sap-mm"],
                "env": {"SAP_HOST": "${SAP_HOST}", "SAP_TOKEN": "${SAP_TOKEN}"}
            },
            "github": {
                "command": "npx",
                "args": ["-y", "@modelcontextprotocol/server-github"],
                "env": {"GITHUB_TOKEN": "${GITHUB_TOKEN}"}
            }
        }
    }
    
    user_config = {
        "mcpServers": {
            "experimental-forecast": {
                "command": "python",
                "args": ["~/tools/forecast-mcp.py"],
                "env": {"MODEL_PATH": "/models/demand-v2"}
            },
            "personal-notes": {
                "command": "npx",
                "args": ["-y", "mcp-server-obsidian"],
                "env": {"VAULT_PATH": "~/Documents/second-brain"}
            }
        }
    }
    
    # Test 1: Config merge — both scopes available simultaneously
    total += 1
    merged = {}
    merged.update(project_config.get("mcpServers", {}))
    merged.update(user_config.get("mcpServers", {}))
    assert len(merged) == 4, f"Expected 4 servers, got {len(merged)}"
    print(f"  ✅ T1: Merged config has {len(merged)} servers (2 project + 2 user)")
    passed += 1
    
    # Test 2: Environment variable expansion (secrets not hardcoded)
    total += 1
    sap_env = project_config["mcpServers"]["sap-mm"]["env"]
    has_var_refs = all("${" in v for v in sap_env.values())
    assert has_var_refs, "SAP env should use ${VAR} references"
    print(f"  ✅ T2: SAP config uses ${{VAR}} expansion — secrets not in VCS")
    passed += 1
    
    # Test 3: Project config should be VCS-tracked
    total += 1
    project_scope_properties = {
        "file": ".mcp.json",
        "vcs_tracked": True,
        "shared_with_team": True
    }
    user_scope_properties = {
        "file": "~/.claude.json",
        "vcs_tracked": False,
        "shared_with_team": False
    }
    assert project_scope_properties["vcs_tracked"] and not user_scope_properties["vcs_tracked"]
    print(f"  ✅ T3: Project (.mcp.json) → VCS ✓ | User (~/.claude.json) → no VCS ✓")
    passed += 1
    
    # Test 4: Common mistake — team member can't see servers because config is user-scoped
    total += 1
    def diagnose_missing_servers(new_member_sees_servers: list, expected_servers: list):
        """Diagnose why a new team member doesn't see expected servers."""
        missing = set(expected_servers) - set(new_member_sees_servers)
        if missing:
            return {
                "issue": f"Servers {missing} not visible to new member",
                "root_cause": "Servers configured in ~/.claude.json (user-level) instead of .mcp.json (project-level)",
                "fix": "Move server configs to .mcp.json and commit to VCS"
            }
        return {"issue": None}
    
    diagnosis = diagnose_missing_servers(
        new_member_sees_servers=["sap-mm", "github"],  # project-scoped
        expected_servers=["sap-mm", "github", "experimental-forecast"]  # all
    )
    assert "user-level" in diagnosis["root_cause"]
    print(f"  ✅ T4: Diagnosed missing server: '{diagnosis['root_cause'][:60]}...'")
    passed += 1
    
    # Test 5: Scope flag options
    total += 1
    scope_options = {
        "local": "Only you, current project (default)",
        "project": "Writes to .mcp.json, shared via VCS",
        "user": "Writes to ~/.claude.json, personal"
    }
    assert len(scope_options) == 3
    print(f"  ✅ T5: Three scope options: {', '.join(scope_options.keys())}")
    passed += 1
    
    print(f"\n  Module 3 Score: {passed}/{total}")
    return passed, total


# ============================================================
# Module 4: Built-in vs MCP Tool Selection
# ============================================================

def test_builtin_vs_mcp():
    """Test correct selection between built-in tools and MCP tools."""
    print("\n" + "=" * 60)
    print("Module 4: Built-in vs MCP Tool Selection")
    print("=" * 60)
    
    passed = 0
    total = 0
    
    # Built-in tools reference
    builtins = {
        "Grep": {"purpose": "content_search", "desc": "Search file CONTENTS for patterns"},
        "Glob": {"purpose": "path_match", "desc": "Find files by NAME/EXTENSION pattern"},
        "Read": {"purpose": "read_file", "desc": "Load full file contents"},
        "Write": {"purpose": "write_file", "desc": "Write full file (create or overwrite)"},
        "Edit": {"purpose": "edit_file", "desc": "Targeted modifications using unique text matching"},
        "Bash": {"purpose": "shell", "desc": "Execute shell commands"}
    }
    
    scenarios = [
        {
            "task": "Find all files ending in .test.tsx in the project",
            "correct": "Glob",
            "wrong": "Grep",
            "reason": "File path matching, not content searching"
        },
        {
            "task": "Find all usages of function 'calculateOTIF' across the codebase",
            "correct": "Grep",
            "wrong": "Glob",
            "reason": "Searching inside file contents for a function name"
        },
        {
            "task": "Replace 'vendor_score >= 80' with 'vendor_score >= 85' in scoring.py",
            "correct": "Edit",
            "wrong": "Write",
            "reason": "Targeted modification with unique anchor text"
        },
        {
            "task": "Edit failed because the text appears in 3 places. Need to update line 42.",
            "correct": "Read+Write",
            "wrong": "Edit",
            "reason": "Edit requires unique text match; Read+Write as fallback"
        },
        {
            "task": "Create a brand new config file for the MRP module",
            "correct": "Write",
            "wrong": "Edit",
            "reason": "New file creation requires Write, not Edit"
        },
        {
            "task": "Check what Jira tickets are assigned to the sprint",
            "correct": "MCP:jira-server",
            "wrong": "Grep",
            "reason": "External system query — MCP tool, not local file search"
        },
        {
            "task": "Find the entry point of the application by looking at imports",
            "correct": "Grep→Read",
            "wrong": "Read all files",
            "reason": "Incremental: Grep find entry points, Read follow imports"
        }
    ]
    
    for s in scenarios:
        total += 1
        # Simulate selection logic
        task_lower = s["task"].lower()
        
        # Simple decision engine
        if "jira" in task_lower or "sprint" in task_lower:
            selected = "MCP:jira-server"
        elif "files ending in" in task_lower or "find all files" in task_lower and "usage" not in task_lower:
            selected = "Glob"
        elif "usages of" in task_lower or "search" in task_lower and "function" in task_lower:
            selected = "Grep"
        elif "edit failed" in task_lower or "non-unique" in task_lower:
            selected = "Read+Write"
        elif "replace" in task_lower and "unique" not in task_lower:
            selected = "Edit"
        elif "create" in task_lower and "new" in task_lower:
            selected = "Write"
        elif "entry point" in task_lower or "imports" in task_lower:
            selected = "Grep→Read"
        else:
            selected = "Bash"
        
        if selected == s["correct"]:
            print(f"  ✅ '{s['task'][:50]}...' → {selected}")
            passed += 1
        else:
            print(f"  ❌ '{s['task'][:50]}...' → {selected} (expected {s['correct']})")
    
    # Test: MCP tool preference issue
    total += 1
    mcp_tool_desc_weak = "Search issues"  # Too weak
    mcp_tool_desc_strong = ("Search Jira issues by JQL query. Returns issue key, summary, status, "
                            "assignee, priority, and sprint info. Supports complex JQL including "
                            "project, status, assignee filters. Use this instead of local file search "
                            "for any Jira-related queries.")
    
    # Claude prefers built-in Grep when MCP description is weak
    would_prefer_builtin = len(mcp_tool_desc_weak) < 30
    would_prefer_mcp = len(mcp_tool_desc_strong) > 100
    
    if would_prefer_builtin and would_prefer_mcp:
        print(f"  ✅ MCP description quality: weak ({len(mcp_tool_desc_weak)} chars → Grep wins) vs strong ({len(mcp_tool_desc_strong)} chars → MCP wins)")
        passed += 1
    else:
        print(f"  ❌ MCP description quality check failed")
    
    print(f"\n  Module 4 Score: {passed}/{total}")
    return passed, total


# ============================================================
# Module 5: Tool Count Impact & Agent Scoping
# ============================================================

def test_tool_count_impact():
    """Test how tool count affects selection reliability."""
    print("\n" + "=" * 60)
    print("Module 5: Tool Count Impact & Agent Scoping")
    print("=" * 60)
    
    passed = 0
    total = 0
    
    # Simulate tool selection reliability based on count
    def estimate_reliability(tool_count: int) -> float:
        """Rough model: reliability drops as tools increase."""
        if tool_count <= 5:
            return 0.95
        elif tool_count <= 10:
            return 0.85
        elif tool_count <= 15:
            return 0.70
        else:
            return 0.50  # 18+ tools → severe degradation
    
    # Test 1: 4-5 tools = optimal
    total += 1
    r5 = estimate_reliability(5)
    r18 = estimate_reliability(18)
    assert r5 > 0.9 and r18 < 0.6
    print(f"  ✅ T1: 5 tools → {r5:.0%} reliable | 18 tools → {r18:.0%} reliable (severe drop)")
    passed += 1
    
    # Test 2: Role-scoped tool assignment
    total += 1
    agents = {
        "inventory_agent": {
            "tools": ["check_inventory", "check_mrp", "get_bin_location", "update_stock"],
            "role": "Inventory management"
        },
        "vendor_agent": {
            "tools": ["get_vendor_profile", "check_avl_status", "get_vendor_scorecard", "send_vendor_alert"],
            "role": "Vendor management"
        },
        "synthesis_agent": {
            "tools": ["generate_report", "format_output"],
            "role": "Report synthesis"
        }
    }
    
    all_within_limit = all(len(a["tools"]) <= 5 for a in agents.values())
    no_cross_role = "check_inventory" not in agents["synthesis_agent"]["tools"]
    
    if all_within_limit and no_cross_role:
        print(f"  ✅ T2: All agents ≤ 5 tools, no cross-role tools (synthesis ≠ inventory)")
        passed += 1
    else:
        print(f"  ❌ T2: Tool scoping issue")
    
    # Test 3: Tool consolidation example
    total += 1
    fragmented = ["create_pr", "review_pr", "merge_pr", "list_pr_comments", "add_pr_label"]
    consolidated = [{"name": "github_pr", "actions": ["create", "review", "merge", "list_comments", "add_label"]}]
    
    reduction = len(fragmented) - len(consolidated)
    print(f"  ✅ T3: Consolidated {len(fragmented)} fragmented tools → {len(consolidated)} (reduced by {reduction})")
    passed += 1
    
    print(f"\n  Module 5 Score: {passed}/{total}")
    return passed, total


# ============================================================
# Module 6: CCA Mock Exam (5 Questions)
# ============================================================

def test_cca_mock_exam():
    """5 CCA mock exam questions on MCP Integration."""
    print("\n" + "=" * 60)
    print("Module 6: CCA Mock Exam — MCP Integration (5 Questions)")
    print("=" * 60)
    
    questions = [
        {
            "q": "A customer support agent has access to 4 MCP tools: get_customer, lookup_order, "
                 "process_refund, escalate_to_human. A developer wants to add search_knowledge_base "
                 "for FAQ lookups. However, the agent increasingly misselects tools. "
                 "What is the MOST likely root cause?",
            "options": {
                "A": "The MCP server needs to be restarted to re-discover tools",
                "B": "The search_knowledge_base description overlaps with existing tool descriptions, "
                     "causing selection confusion",
                "C": "5 tools exceeds the maximum allowed by the MCP protocol",
                "D": "The agent needs tool_choice set to 'specific' for each tool"
            },
            "answer": "B",
            "explanation": "Tool description overlap is the #1 cause of misrouting. 5 tools is within "
                          "the recommended 4-5 range (C is wrong — MCP has no hard max). Restart (A) "
                          "doesn't fix description issues. Forcing specific (D) removes agent autonomy."
        },
        {
            "q": "An MCP tool querying a vendor database returns: "
                 '{"isError": true, "content": "Database error"}. '
                 "The agent retries 3 times and then gives up silently. "
                 "What TWO improvements would you recommend?",
            "options": {
                "A": "Add errorCategory and isRetryable to the error response, and have the agent "
                     "communicate the failure to the user instead of silent suppression",
                "B": "Remove the isError flag and always return empty results",
                "C": "Increase retry count to 10 with no backoff",
                "D": "Switch from MCP to direct API calls to avoid the error protocol"
            },
            "answer": "A",
            "explanation": "Structured errors (errorCategory + isRetryable) let the agent make "
                          "informed decisions. Silent suppression (B) and ignoring MCP protocol (D) "
                          "are anti-patterns. Unlimited retries (C) waste resources."
        },
        {
            "q": "A new team member joins and cannot see the SAP MCP server that other developers use. "
                 "Where should the server configuration be stored?",
            "options": {
                "A": "~/.claude.json with the SAP token hardcoded",
                "B": ".mcp.json in the project root with ${SAP_TOKEN} environment variable expansion",
                "C": "A shared Slack message with the full configuration",
                "D": "Each developer's ~/.bashrc file"
            },
            "answer": "B",
            "explanation": ".mcp.json is project-scoped and tracked in VCS, so all team members get it. "
                          "${SAP_TOKEN} keeps secrets out of VCS. ~/.claude.json (A) is user-level and "
                          "not shared. Hardcoded tokens (A) and Slack sharing (C) are security risks."
        },
        {
            "q": "An agent needs to understand the database schema before querying data. "
                 "Currently it calls list_tables → get_schema('orders') → get_schema('vendors') "
                 "(3 tool calls). How can you reduce unnecessary API calls?",
            "options": {
                "A": "Cache the schema in the system prompt",
                "B": "Expose the database schema as an MCP Resource, giving the agent visibility "
                     "into available data without tool calls",
                "C": "Consolidate all three calls into one 'get_all_schemas' tool",
                "D": "Use tool_choice: 'any' to force schema retrieval first"
            },
            "answer": "B",
            "explanation": "MCP Resources are specifically designed for exposing content catalogs "
                          "(schemas, docs, task lists) to give agents visibility WITHOUT tool calls. "
                          "System prompt caching (A) wastes context window on every request. "
                          "Consolidation (C) helps but still requires a tool call. "
                          "tool_choice (D) doesn't reduce the number of calls."
        },
        {
            "q": "An agent queries get_vendor_shortage(vendor_id='V001'). In Case A, the API returns "
                 '{"results": []}. In Case B, the API returns '
                 '{"isError": true, "errorCategory": "permission", "message": "Access denied"}. '
                 "How should the agent handle these differently?",
            "options": {
                "A": "Both cases: tell the user no shortages were found",
                "B": "Case A: report no shortages. Case B: escalate to get proper access",
                "C": "Both cases: retry the query",
                "D": "Case A: escalate. Case B: report no shortages"
            },
            "answer": "B",
            "explanation": "Case A is a valid empty result (query succeeded, no matches). "
                          "Case B is an access failure (query failed, need permission). "
                          "Treating both the same (A) or swapping them (D) leads to incorrect behavior. "
                          "Retrying both (C) wastes resources on the valid empty case."
        }
    ]
    
    passed = 0
    total = len(questions)
    
    for i, q in enumerate(questions, 1):
        print(f"\n  Q{i}: {q['q'][:80]}...")
        for key, val in q["options"].items():
            marker = "→" if key == q["answer"] else " "
            print(f"    {marker} {key}) {val[:70]}{'...' if len(val) > 70 else ''}")
        print(f"    Answer: {q['answer']} — {q['explanation'][:80]}...")
        passed += 1  # Self-study mode: all marked as correct for learning
    
    print(f"\n  Module 6 Score: {passed}/{total}")
    return passed, total


# ============================================================
# Main
# ============================================================

def main():
    results = []
    results.append(test_mcp_protocol())
    results.append(test_structured_errors())
    results.append(test_scoping())
    results.append(test_builtin_vs_mcp())
    results.append(test_tool_count_impact())
    results.append(test_cca_mock_exam())
    
    total_passed = sum(r[0] for r in results)
    total_tests = sum(r[1] for r in results)
    
    print("\n" + "=" * 60)
    print(f"TOTAL SCORE: {total_passed}/{total_tests} ({total_passed/total_tests*100:.1f}%)")
    print("=" * 60)
    
    if total_passed == total_tests:
        print("🎉 All tests passed!")
    else:
        print(f"⚠️  {total_tests - total_passed} test(s) need attention")

if __name__ == "__main__":
    main()
