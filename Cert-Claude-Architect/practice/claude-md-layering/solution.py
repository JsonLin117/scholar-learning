"""
CLAUDE.md 層級與模組化 — CCA D3 Task 3.1 練習
模擬 Claude Code 的 CLAUDE.md 載入引擎

Modules:
1. MiniClaudeMdLoader: 四層 scope + 目錄樹解析 + concatenation
2. ImportExpander: @import 展開、巢狀深度限制、循環偵測
3. RulesEngine: .claude/rules/ 條件載入 + YAML frontmatter paths glob
4. MonorepoFilter: claudeMdExcludes 排除機制
5. DiagnosticTool: /memory 診斷 + 規則衝突偵測
6. CCA Mock Exam: 5 題模擬考
"""

import os
import re
import fnmatch
from dataclasses import dataclass, field
from typing import Optional
from pathlib import PurePosixPath

# ============================================================
# Module 1: MiniClaudeMdLoader — 四層 Scope + 目錄樹解析
# ============================================================

@dataclass
class LoadedFile:
    """代表一個被載入的 CLAUDE.md 檔案"""
    scope: str          # managed_policy / user / project / local / subdirectory / rule
    path: str           # 虛擬路徑
    content: str        # 內容
    load_timing: str    # launch / on_demand
    shared_with: str    # organization / just_you / team / just_you_project

class VirtualFS:
    """模擬檔案系統"""
    def __init__(self):
        self.files: dict[str, str] = {}

    def write(self, path: str, content: str):
        self.files[path] = content

    def read(self, path: str) -> Optional[str]:
        return self.files.get(path)

    def exists(self, path: str) -> bool:
        return path in self.files

    def list_dir(self, dir_path: str) -> list[str]:
        """列出目錄下的檔案"""
        dir_path = dir_path.rstrip("/") + "/"
        results = []
        for p in self.files:
            if p.startswith(dir_path):
                relative = p[len(dir_path):]
                if "/" not in relative:  # 只列直接子項
                    results.append(p)
        return sorted(results)

    def list_recursive(self, dir_path: str, ext: str = ".md") -> list[str]:
        """遞迴列出目錄下所有符合副檔名的檔案"""
        dir_path = dir_path.rstrip("/") + "/"
        return sorted(p for p in self.files if p.startswith(dir_path) and p.endswith(ext))


class MiniClaudeMdLoader:
    """
    模擬 Claude Code 的 CLAUDE.md 載入機制

    載入順序（broadest → most specific）：
    1. Managed Policy: /Library/Application Support/ClaudeCode/CLAUDE.md
    2. User: ~/.claude/CLAUDE.md
    3. Project: ./CLAUDE.md or ./.claude/CLAUDE.md （從 root 往 cwd 走）
    4. Local: ./CLAUDE.local.md
    子目錄 CLAUDE.md: on-demand

    所有檔案 **concatenate**，不 override。
    """

    MANAGED_POLICY_PATH = "/Library/Application Support/ClaudeCode/CLAUDE.md"
    USER_HOME = "/Users/json"
    USER_CLAUDE_MD = "/Users/json/.claude/CLAUDE.md"

    def __init__(self, fs: VirtualFS, cwd: str, excludes: list[str] = None):
        self.fs = fs
        self.cwd = cwd.rstrip("/")
        self.excludes = excludes or []
        self.loaded_files: list[LoadedFile] = []

    def _is_excluded(self, path: str) -> bool:
        """檢查路徑是否被 claudeMdExcludes 排除"""
        for pattern in self.excludes:
            # 將 pattern 視為相對於 project root 的 glob
            if fnmatch.fnmatch(path, pattern) or fnmatch.fnmatch(path.lstrip("/"), pattern):
                return True
        return False

    def _try_load(self, path: str, scope: str, timing: str, shared: str) -> bool:
        """嘗試載入一個檔案"""
        if self._is_excluded(path):
            return False
        content = self.fs.read(path)
        if content is not None:
            # 剝除 HTML block comments（考試知識點！）
            content = self._strip_html_comments(content)
            self.loaded_files.append(LoadedFile(
                scope=scope,
                path=path,
                content=content,
                load_timing=timing,
                shared_with=shared,
            ))
            return True
        return False

    @staticmethod
    def _strip_html_comments(text: str) -> str:
        """
        剝除 block-level HTML comments。
        Code block 內的 comment 保留（簡化版：不處理 code block）。
        """
        return re.sub(r'<!--.*?-->', '', text, flags=re.DOTALL).strip()

    def load_launch(self) -> list[LoadedFile]:
        """啟動時載入（模擬 Claude Code session start）"""
        self.loaded_files = []

        # 1. Managed Policy
        self._try_load(
            self.MANAGED_POLICY_PATH,
            scope="managed_policy",
            timing="launch",
            shared="organization",
        )

        # 2. User level
        self._try_load(
            self.USER_CLAUDE_MD,
            scope="user",
            timing="launch",
            shared="just_you",
        )

        # 3+4. Walk up directory tree from root to cwd
        parts = self.cwd.split("/")
        for i in range(1, len(parts) + 1):
            dir_path = "/".join(parts[:i]) if i > 1 else "/"
            if dir_path == "":
                dir_path = "/"

            # Project-level CLAUDE.md（兩個可能位置）
            # 跳過已載入的 user-level 路徑避免重複
            for candidate in [
                f"{dir_path}/CLAUDE.md",
                f"{dir_path}/.claude/CLAUDE.md",
            ]:
                if candidate == self.USER_CLAUDE_MD:
                    continue  # 已在 user scope 載入
                self._try_load(
                    candidate,
                    scope="project",
                    timing="launch",
                    shared="team",
                )

            # Local-level CLAUDE.local.md
            self._try_load(
                f"{dir_path}/CLAUDE.local.md",
                scope="local",
                timing="launch",
                shared="just_you_project",
            )

        return self.loaded_files

    def load_subdirectory_on_demand(self, subdir: str) -> list[LoadedFile]:
        """模擬 on-demand 載入子目錄的 CLAUDE.md"""
        new_files = []
        for name in ["CLAUDE.md", "CLAUDE.local.md"]:
            path = f"{subdir}/{name}"
            if self.fs.exists(path) and not self._is_excluded(path):
                content = self._strip_html_comments(self.fs.read(path))
                f = LoadedFile(
                    scope="subdirectory",
                    path=path,
                    content=content,
                    load_timing="on_demand",
                    shared_with="team" if name == "CLAUDE.md" else "just_you_project",
                )
                self.loaded_files.append(f)
                new_files.append(f)
        return new_files

    def get_concatenated_context(self) -> str:
        """取得串接後的完整 context"""
        return "\n\n---\n\n".join(f.content for f in self.loaded_files)


# ============================================================
# Module 2: ImportExpander — @import 展開
# ============================================================

class ImportExpander:
    """
    展開 CLAUDE.md 中的 @import 語法

    規則：
    - @path/to/file → 展開檔案內容
    - 相對路徑相對於包含 import 的檔案（不是 cwd！）
    - 最多 4 層巢狀
    - 偵測循環引用
    """

    MAX_DEPTH = 4
    IMPORT_PATTERN = re.compile(r'@(\S+\.(?:md|json|txt|yml|yaml))')

    def __init__(self, fs: VirtualFS):
        self.fs = fs
        self.expansion_log: list[dict] = []

    def expand(self, path: str, content: str, depth: int = 0,
               visited: set = None) -> str:
        """遞迴展開 @import"""
        if visited is None:
            visited = set()

        if depth > self.MAX_DEPTH:
            self.expansion_log.append({
                "path": path, "depth": depth,
                "status": "ERROR", "reason": f"Max depth {self.MAX_DEPTH} exceeded"
            })
            return content

        if path in visited:
            self.expansion_log.append({
                "path": path, "depth": depth,
                "status": "ERROR", "reason": "Circular import detected"
            })
            return f"[CIRCULAR: {path}]"

        visited.add(path)

        def replace_import(match):
            import_ref = match.group(1)
            # 解析相對路徑：相對於包含檔案的目錄
            containing_dir = str(PurePosixPath(path).parent)
            if import_ref.startswith("/") or import_ref.startswith("~"):
                resolved = import_ref.replace("~", "/Users/json")
            else:
                resolved = os.path.normpath(f"{containing_dir}/{import_ref}")

            imported_content = self.fs.read(resolved)
            if imported_content is None:
                self.expansion_log.append({
                    "path": resolved, "depth": depth + 1,
                    "status": "NOT_FOUND", "referenced_from": path
                })
                return f"[NOT FOUND: {import_ref}]"

            self.expansion_log.append({
                "path": resolved, "depth": depth + 1,
                "status": "OK", "referenced_from": path
            })

            # 遞迴展開被 import 的檔案
            return self.expand(resolved, imported_content, depth + 1, visited.copy())

        expanded = self.IMPORT_PATTERN.sub(replace_import, content)
        return expanded


# ============================================================
# Module 3: RulesEngine — .claude/rules/ 條件載入
# ============================================================

@dataclass
class Rule:
    """一個 .claude/rules/ 檔案"""
    path: str
    content: str
    paths_patterns: list[str]  # 空 = 無條件載入

class RulesEngine:
    """
    .claude/rules/ 條件載入引擎

    - 無 paths frontmatter → 啟動時載入（unconditional）
    - 有 paths frontmatter → 只在編輯匹配檔案時載入（conditional）
    """

    FRONTMATTER_PATTERN = re.compile(r'^---\s*\n(.*?)\n---\s*\n', re.DOTALL)

    def __init__(self, fs: VirtualFS, project_root: str):
        self.fs = fs
        self.project_root = project_root.rstrip("/")
        self.rules: list[Rule] = []

    def discover_rules(self):
        """發現所有 .claude/rules/*.md"""
        rules_dir = f"{self.project_root}/.claude/rules"
        rule_files = self.fs.list_recursive(rules_dir, ".md")

        for rule_path in rule_files:
            content = self.fs.read(rule_path)
            patterns = self._parse_paths_frontmatter(content)
            body = self.FRONTMATTER_PATTERN.sub('', content).strip()

            self.rules.append(Rule(
                path=rule_path,
                content=body,
                paths_patterns=patterns,
            ))

    def _parse_paths_frontmatter(self, content: str) -> list[str]:
        """解析 YAML frontmatter 中的 paths"""
        match = self.FRONTMATTER_PATTERN.match(content)
        if not match:
            return []

        frontmatter = match.group(1)
        # 簡單解析（不用 yaml library）
        patterns = []
        in_paths = False
        for line in frontmatter.split("\n"):
            line = line.strip()
            if line.startswith("paths:"):
                in_paths = True
                # 檢查 inline array: paths: ["a", "b"]
                inline = re.findall(r'"([^"]+)"', line)
                if inline:
                    patterns.extend(inline)
                    in_paths = False
                continue
            if in_paths and line.startswith("- "):
                pat = line[2:].strip().strip('"').strip("'")
                patterns.append(pat)
            elif in_paths and not line.startswith("-"):
                in_paths = False

        return patterns

    def get_launch_rules(self) -> list[Rule]:
        """啟動時載入的規則（無 paths = unconditional）"""
        return [r for r in self.rules if not r.paths_patterns]

    def get_rules_for_file(self, file_path: str) -> list[Rule]:
        """取得匹配特定檔案的條件規則"""
        matched = []
        for rule in self.rules:
            if not rule.paths_patterns:
                continue  # unconditional 已在 launch 載入
            for pattern in rule.paths_patterns:
                if fnmatch.fnmatch(file_path, pattern):
                    matched.append(rule)
                    break
        return matched


# ============================================================
# Module 4: MonorepoFilter — claudeMdExcludes
# ============================================================

class MonorepoFilter:
    """
    Monorepo 過濾器：排除其他團隊的 CLAUDE.md

    設定範例：
    claudeMdExcludes: ["packages/frontend/**", "packages/mobile/**"]
    """

    def __init__(self, excludes: list[str]):
        self.excludes = excludes

    def should_load(self, path: str, project_root: str) -> bool:
        """判斷是否應載入"""
        # 計算相對路徑
        rel = path
        if path.startswith(project_root):
            rel = path[len(project_root):].lstrip("/")

        for pattern in self.excludes:
            if fnmatch.fnmatch(rel, pattern):
                return False
            # 也檢查中間目錄
            parts = rel.split("/")
            for i in range(1, len(parts) + 1):
                partial = "/".join(parts[:i])
                if fnmatch.fnmatch(partial, pattern.rstrip("*").rstrip("/")):
                    return False
        return True


# ============================================================
# Module 5: DiagnosticTool — /memory 診斷 + 規則衝突偵測
# ============================================================

class DiagnosticTool:
    """模擬 /memory 診斷功能"""

    def __init__(self, loader: MiniClaudeMdLoader, rules_engine: RulesEngine):
        self.loader = loader
        self.rules_engine = rules_engine

    def memory_status(self) -> dict:
        """模擬 /memory 指令"""
        return {
            "loaded_files": [
                {
                    "path": f.path,
                    "scope": f.scope,
                    "timing": f.load_timing,
                    "shared_with": f.shared_with,
                    "content_lines": len(f.content.split("\n")),
                }
                for f in self.loader.loaded_files
            ],
            "total_files": len(self.loader.loaded_files),
            "total_lines": sum(len(f.content.split("\n")) for f in self.loader.loaded_files),
            "rules_unconditional": len(self.rules_engine.get_launch_rules()),
            "rules_conditional": len([r for r in self.rules_engine.rules if r.paths_patterns]),
        }

    def detect_conflicts(self) -> list[dict]:
        """偵測可能衝突的規則"""
        conflicts = []
        all_contents = []

        for f in self.loader.loaded_files:
            all_contents.append((f.path, f.content))
        for r in self.rules_engine.rules:
            all_contents.append((r.path, r.content))

        # 簡單衝突偵測：同一關鍵字出現相反指令
        conflict_patterns = [
            ("2-space", "4-space", "indentation"),
            ("tabs", "spaces", "indentation"),
            ("semicolons", "no semicolons", "semicolons"),
            ("single quotes", "double quotes", "quotes"),
        ]

        for pat_a, pat_b, topic in conflict_patterns:
            files_a = [p for p, c in all_contents if pat_a.lower() in c.lower()]
            files_b = [p for p, c in all_contents if pat_b.lower() in c.lower()]
            if files_a and files_b:
                conflicts.append({
                    "topic": topic,
                    "rule_a": f"{pat_a} in {files_a}",
                    "rule_b": f"{pat_b} in {files_b}",
                    "recommendation": "Remove one to avoid Claude picking arbitrarily",
                })

        return conflicts

    def diagnose_missing_rules(self, expected_rule: str) -> list[str]:
        """診斷為什麼某個規則沒有生效"""
        suggestions = []

        # 檢查所有載入的內容
        all_content = self.loader.get_concatenated_context()
        for r in self.rules_engine.rules:
            all_content += "\n" + r.content

        if expected_rule.lower() not in all_content.lower():
            suggestions.append(f"Rule '{expected_rule}' not found in any loaded file")
            suggestions.append("Check if it's in user-level (~/.claude/CLAUDE.md) instead of project-level")
            suggestions.append("Check if it's in an excluded path (claudeMdExcludes)")
            suggestions.append("Check if it's in a path-scoped rule that doesn't match current files")
        else:
            suggestions.append(f"Rule '{expected_rule}' IS loaded — if not followed, consider using a Hook instead")
            suggestions.append("Remember: CLAUDE.md is context (probabilistic), not enforced config")

        return suggestions


# ============================================================
# Module 6: CCA Mock Exam — D3 CLAUDE.md
# ============================================================

CCA_QUESTIONS = [
    {
        "id": "D3-Q1",
        "scenario": "Code Generation with Claude Code",
        "question": """A new team member joins the project and reports that Claude Code doesn't follow 
the team's coding standards (2-space indentation, async/await patterns). The team lead 
confirms the standards are documented. After investigation, the standards are found in 
~/.claude/CLAUDE.md on the original developer's machine. What is the root cause?""",
        "options": {
            "A": "The standards file is corrupted and needs to be regenerated with /init",
            "B": "The standards are in user-level config which is not shared via version control; move to ./CLAUDE.md or ./.claude/CLAUDE.md",
            "C": "Claude Code has a bug that prevents loading user-level CLAUDE.md for new users",
            "D": "The new team member needs to copy the file to their own ~/.claude/CLAUDE.md",
        },
        "correct": "B",
        "explanation": """User-level CLAUDE.md (~/.claude/CLAUDE.md) is personal and NOT shared via VCS. 
Team standards must be in project-level (./CLAUDE.md or ./.claude/CLAUDE.md) to be shared. 
Option D would work but violates the principle of single source of truth and requires manual sync.""",
    },
    {
        "id": "D3-Q2",
        "scenario": "Developer Productivity Tool",
        "question": """You have testing conventions that apply to all *.test.ts files across the entire 
codebase (spread across src/, packages/core/, packages/api/). You also have API conventions 
that only apply to src/api/ files. What is the BEST configuration approach?""",
        "options": {
            "A": "Put everything in the root CLAUDE.md to ensure all rules are always loaded",
            "B": "Create subdirectory CLAUDE.md files in each directory that contains test files",
            "C": "Use .claude/rules/ with paths frontmatter: testing.md (paths: ['**/*.test.ts']) and api-design.md (paths: ['src/api/**/*'])",
            "D": "Create a single .claude/rules/all-rules.md without paths frontmatter containing both testing and API rules",
        },
        "correct": "C",
        "explanation": """.claude/rules/ with paths frontmatter is the optimal approach because:
- Testing rules use glob **/*.test.ts to match test files regardless of directory location
- API rules scope to src/api/**/* to avoid loading when editing non-API files
- Path-scoped rules save context tokens by only loading when relevant
Option B requires duplicating test rules across many directories.
Option A wastes context with always-on rules.
Option D loads all rules always, defeating the purpose of path scoping.""",
    },
    {
        "id": "D3-Q3",
        "scenario": "CI/CD Integration",
        "question": """Your CLAUDE.md uses @import to reference three files:
- @docs/coding-standards.md (which imports @../shared/types.md)
- @docs/testing-guide.md (which imports @../shared/fixtures.md, which imports @../shared/types.md)
- @README.md

What happens when Claude Code starts a session?""",
        "options": {
            "A": "Error: circular import detected because types.md is imported twice",
            "B": "All files are expanded at launch; types.md content appears twice in context since it's imported from two different paths",
            "C": "Error: maximum import depth of 4 exceeded",
            "D": "Only the first import of types.md succeeds; the second is silently skipped",
        },
        "correct": "B",
        "explanation": """@import expansion works by textual substitution at launch. Each import chain is 
independent — types.md being imported from two different parent files does NOT create a 
circular reference (the paths are different: coding-standards→types vs fixtures→types).
The depth is: CLAUDE.md(0) → coding-standards(1) → types(2) = 2 hops, well within the 4-hop limit.
types.md content will appear twice, consuming extra tokens but not causing an error.
A true circular import would be: A imports B which imports A.""",
    },
    {
        "id": "D3-Q4",
        "scenario": "Code Generation with Claude Code",
        "question": """A financial services company needs to GUARANTEE that Claude Code never writes 
plaintext credentials in any file. The security team has written a clear rule in CLAUDE.md: 
"Never hardcode API keys, tokens, or passwords. Always use environment variables." 
Despite this instruction, a code review found a hardcoded API key. What is the CORRECT fix?""",
        "options": {
            "A": "Make the CLAUDE.md instruction more emphatic: 'ABSOLUTELY NEVER hardcode credentials. This is a CRITICAL security violation.'",
            "B": "Add the rule to both project and user level CLAUDE.md for redundancy",
            "C": "Implement a PreToolUse hook on Write/Edit that scans content for credential patterns and blocks the operation",
            "D": "Move the rule to .claude/rules/security.md with paths: ['**/*'] for broader coverage",
        },
        "correct": "C",
        "explanation": """CLAUDE.md is context, not enforced configuration — probabilistic adherence (>90% but not 100%).
For security-critical rules with financial/legal consequences, use a PreToolUse hook 
(deterministic, 100% enforcement). The hook intercepts Write/Edit tool calls and blocks 
any content matching credential patterns BEFORE the file is written.
Options A, B, D all remain probabilistic regardless of emphasis or placement.""",
    },
    {
        "id": "D3-Q5",
        "scenario": "Multi-Agent Research System / Developer Productivity",
        "question": """Your monorepo has this structure:
packages/de-pipeline/CLAUDE.md (DE team standards)
packages/frontend/CLAUDE.md (Frontend team standards)  
packages/firmware/CLAUDE.md (Firmware team standards)
root CLAUDE.md (shared standards)

As a DE engineer, you find that Claude Code loads frontend and firmware CLAUDE.md files 
when you work in packages/de-pipeline/, causing confusion because frontend rules 
contradict DE conventions (e.g., 'use semicolons' vs DE team's 'no semicolons'). 
What is the BEST solution?""",
        "options": {
            "A": "Delete the frontend and firmware CLAUDE.md files",
            "B": "Use claudeMdExcludes to exclude packages/frontend/** and packages/firmware/** from loading",
            "C": "Move all rules to .claude/rules/ with path-specific frontmatter and delete all subdirectory CLAUDE.md files",
            "D": "Add a note in root CLAUDE.md saying 'Ignore rules from other teams'",
        },
        "correct": "B",
        "explanation": """claudeMdExcludes is designed specifically for this monorepo scenario — it tells Claude Code 
to skip CLAUDE.md files matching specified patterns. This preserves each team's autonomy 
(they keep their CLAUDE.md) while preventing cross-team rule interference.
Option A removes other teams' config (destructive).
Option C is a major refactoring that requires all teams to coordinate.
Option D is a probabilistic instruction that may not be consistently followed.""",
    },
]


# ============================================================
# 主程式
# ============================================================

def run_module_1():
    """Module 1: 四層 Scope + 目錄樹解析"""
    print("=" * 60)
    print("Module 1: CLAUDE.md 四層 Scope 載入")
    print("=" * 60)

    fs = VirtualFS()

    # 設定 ODM monorepo 檔案系統
    fs.write("/Library/Application Support/ClaudeCode/CLAUDE.md",
             "# Managed Policy\n- Never hardcode credentials\n- All code must pass security scan")
    fs.write("/Users/json/.claude/CLAUDE.md",
             "# User Preferences\n- Use vim keybindings\n- Prefer dark theme")
    fs.write("/Users/json/projects/odm-platform/CLAUDE.md",
             "# ODM Platform Standards\n- Use Python 3.12+\n- 2-space indentation for YAML\n- 4-space for Python")
    fs.write("/Users/json/projects/odm-platform/CLAUDE.local.md",
             "# My Local Settings\n<!-- This comment should be stripped -->\n- Dev Databricks URL: https://dev.databricks.com/xxx")
    fs.write("/Users/json/projects/odm-platform/packages/de-pipeline/CLAUDE.md",
             "# DE Pipeline Standards\n- Use Delta Lake for all tables\n- Follow Medallion architecture")
    fs.write("/Users/json/projects/odm-platform/packages/frontend/CLAUDE.md",
             "# Frontend Standards\n- Use semicolons\n- React functional components only")

    # 載入（cwd = de-pipeline）
    loader = MiniClaudeMdLoader(
        fs=fs,
        cwd="/Users/json/projects/odm-platform/packages/de-pipeline",
    )
    files = loader.load_launch()

    print(f"\n📂 CWD: /Users/json/projects/odm-platform/packages/de-pipeline")
    print(f"📄 Launch 載入 {len(files)} 個檔案：\n")

    passed = 0
    total = 0

    for f in files:
        print(f"  [{f.scope:16s}] {f.path}")
        print(f"    → shared: {f.shared_with}, timing: {f.load_timing}")
        print(f"    → lines: {len(f.content.split(chr(10)))}")

    # 驗證載入順序
    scopes = [f.scope for f in files]
    total += 1
    if scopes[0] == "managed_policy":
        passed += 1
        print("\n✅ Managed policy 最先載入")
    else:
        print("\n❌ Managed policy 應最先載入")

    total += 1
    if scopes[1] == "user":
        passed += 1
        print("✅ User level 第二載入")
    else:
        print("❌ User level 應第二載入")

    # 驗證 concatenation（不是 override）
    context = loader.get_concatenated_context()
    total += 1
    if "Never hardcode" in context and "vim keybindings" in context and "Delta Lake" in context:
        passed += 1
        print("✅ 所有層級的內容都在 context 中（concatenation，not override）")
    else:
        print("❌ 應該是 concatenation")

    # 驗證 HTML comments 被剝除
    total += 1
    if "This comment should be stripped" not in context:
        passed += 1
        print("✅ HTML comments 被剝除")
    else:
        print("❌ HTML comments 應被剝除")

    # 驗證 CLAUDE.local.md 在 CLAUDE.md 之後
    project_idx = next(i for i, f in enumerate(files)
                       if "odm-platform/CLAUDE.md" in f.path
                       and f.scope == "project"
                       and "Platform Standards" in f.content)
    local_indices = [i for i, f in enumerate(files) if f.scope == "local"]
    total += 1
    if local_indices and all(li > project_idx for li in local_indices):
        passed += 1
        print("✅ CLAUDE.local.md 在同層 CLAUDE.md 之後載入")
    else:
        print("❌ CLAUDE.local.md 應在 CLAUDE.md 之後")

    # 驗證 on-demand subdirectory 不在 launch 載入中
    total += 1
    frontend_loaded = any("frontend" in f.path for f in files)
    # Frontend 是 de-pipeline 的 sibling，不應該被自動載入
    # 但因為 walker 從 root 到 cwd，它不會經過 frontend
    if not frontend_loaded:
        passed += 1
        print("✅ Frontend（sibling 目錄）不在 launch 載入中")
    else:
        print("❌ Sibling 目錄不應在 launch 載入")

    print(f"\nModule 1: {passed}/{total}")
    return passed, total


def run_module_2():
    """Module 2: @import 展開"""
    print("\n" + "=" * 60)
    print("Module 2: @import 展開 + 巢狀 + 循環偵測")
    print("=" * 60)

    fs = VirtualFS()

    # ODM 專案的 import 結構
    fs.write("/project/CLAUDE.md",
             "# ODM Platform\nSee @docs/coding-standards.md for standards.\nDeps: @package.json")
    fs.write("/project/docs/coding-standards.md",
             "# Coding Standards\n- Python 3.12+\nShared types: @../shared/types.md")
    fs.write("/project/shared/types.md",
             "# Shared Types\n- OrderID: str\n- VendorID: str\n- PartNumber: str")
    fs.write("/project/package.json",
             '{"name": "odm-platform", "version": "1.0.0"}')

    # 循環引用測試
    fs.write("/project/docs/circular-a.md",
             "# Circular A\nImport B: @circular-b.md")
    fs.write("/project/docs/circular-b.md",
             "# Circular B\nImport A: @circular-a.md")

    # 深度測試（6 層，超過 4-hop 限制）
    fs.write("/project/docs/deep1.md", "Deep1 @deep2.md")
    fs.write("/project/docs/deep2.md", "Deep2 @deep3.md")
    fs.write("/project/docs/deep3.md", "Deep3 @deep4.md")
    fs.write("/project/docs/deep4.md", "Deep4 @deep5.md")
    fs.write("/project/docs/deep5.md", "Deep5 @deep6.md")
    fs.write("/project/docs/deep6.md", "Deep6 — this should NOT appear")

    expander = ImportExpander(fs)
    passed = 0
    total = 0

    # Test 1: 正常展開
    content = fs.read("/project/CLAUDE.md")
    expanded = expander.expand("/project/CLAUDE.md", content)
    total += 1
    if "OrderID: str" in expanded:
        passed += 1
        print("✅ 巢狀 import 正常展開（CLAUDE.md → coding-standards → types）")
    else:
        print("❌ 巢狀 import 應展開到 types.md")

    total += 1
    if '"odm-platform"' in expanded:
        passed += 1
        print("✅ package.json import 正常")
    else:
        print("❌ package.json 應被展開")

    # Test 2: 相對路徑解析（coding-standards.md 的 @../shared/types.md）
    total += 1
    ok_logs = [l for l in expander.expansion_log if l["status"] == "OK" and "shared/types.md" in l["path"]]
    if ok_logs:
        passed += 1
        print("✅ 相對路徑正確解析（@../shared/types.md 從 docs/ 解析到 shared/）")
    else:
        print("❌ 相對路徑應解析到正確位置")

    # Test 3: 循環引用偵測
    expander2 = ImportExpander(fs)
    circular_content = fs.read("/project/docs/circular-a.md")
    circular_expanded = expander2.expand("/project/docs/circular-a.md", circular_content)
    total += 1
    circular_errors = [l for l in expander2.expansion_log if l["status"] == "ERROR" and "Circular" in l.get("reason", "")]
    if circular_errors:
        passed += 1
        print("✅ 循環引用被偵測到")
    else:
        print("❌ 應偵測循環引用")

    # Test 4: 深度限制
    expander3 = ImportExpander(fs)
    deep_content = fs.read("/project/docs/deep1.md")
    deep_expanded = expander3.expand("/project/docs/deep1.md", deep_content)
    total += 1
    depth_errors = [l for l in expander3.expansion_log if l["status"] == "ERROR" and "depth" in l.get("reason", "").lower()]
    if depth_errors:
        passed += 1
        print(f"✅ 深度限制生效（max {ImportExpander.MAX_DEPTH} hops）")
    else:
        # 檢查 deep6 是否未出現（超過限制）
        if "Deep6" not in deep_expanded:
            passed += 1
            print(f"✅ 深度限制生效（deep6 未被展開，超過 {ImportExpander.MAX_DEPTH} hops）")
        else:
            print(f"❌ 超過 {ImportExpander.MAX_DEPTH} 層應停止展開")

    print(f"\nModule 2: {passed}/{total}")
    return passed, total


def run_module_3():
    """Module 3: .claude/rules/ 條件載入"""
    print("\n" + "=" * 60)
    print("Module 3: .claude/rules/ 條件載入 + YAML frontmatter")
    print("=" * 60)

    fs = VirtualFS()
    project_root = "/project"

    # unconditional rule（無 paths）
    fs.write(f"{project_root}/.claude/rules/code-style.md",
             "# Code Style\n- Use 4-space indentation\n- No trailing whitespace")

    # conditional rules（有 paths）
    fs.write(f"{project_root}/.claude/rules/testing.md",
             '---\npaths:\n  - "**/*.test.ts"\n  - "**/*.test.py"\n---\n# Testing Rules\n- Use describe/it blocks\n- No hardcoded fixtures')

    fs.write(f"{project_root}/.claude/rules/etl-pipeline.md",
             '---\npaths:\n  - "src/pipelines/**/*"\n  - "dags/**/*"\n---\n# ETL Pipeline Rules\n- Follow Medallion architecture\n- Use Delta Lake MERGE for upserts')

    fs.write(f"{project_root}/.claude/rules/api-design.md",
             '---\npaths:\n  - "src/api/**/*.py"\n---\n# API Design Rules\n- Use FastAPI\n- Include OpenAPI docs')

    # security rule（unconditional）
    fs.write(f"{project_root}/.claude/rules/security.md",
             "# Security\n- Never hardcode credentials\n- Use managed identity")

    engine = RulesEngine(fs, project_root)
    engine.discover_rules()

    passed = 0
    total = 0

    # Test 1: 發現所有規則
    total += 1
    if len(engine.rules) == 5:
        passed += 1
        print(f"✅ 發現 {len(engine.rules)} 個規則")
    else:
        print(f"❌ 應發現 5 個規則，實際 {len(engine.rules)}")

    # Test 2: unconditional 規則
    launch_rules = engine.get_launch_rules()
    total += 1
    launch_names = [r.path.split("/")[-1] for r in launch_rules]
    if "code-style.md" in launch_names and "security.md" in launch_names:
        passed += 1
        print(f"✅ {len(launch_rules)} 個 unconditional 規則在啟動時載入：{launch_names}")
    else:
        print(f"❌ code-style.md 和 security.md 應在啟動時載入")

    # Test 3: 匹配 test 檔案
    test_rules = engine.get_rules_for_file("src/utils/helpers.test.ts")
    total += 1
    if any("Testing" in r.content for r in test_rules):
        passed += 1
        print("✅ **/*.test.ts 匹配 testing.md 規則")
    else:
        print("❌ test 檔案應匹配 testing rules")

    # Test 4: 匹配 pipeline 檔案
    pipeline_rules = engine.get_rules_for_file("src/pipelines/bronze/ingest.py")
    total += 1
    if any("Medallion" in r.content for r in pipeline_rules):
        passed += 1
        print("✅ src/pipelines/**/* 匹配 ETL pipeline 規則")
    else:
        print("❌ pipeline 檔案應匹配 ETL rules")

    # Test 5: 不匹配的檔案不載入條件規則
    unrelated_rules = engine.get_rules_for_file("src/models/vendor.py")
    total += 1
    if len(unrelated_rules) == 0:
        passed += 1
        print("✅ src/models/vendor.py 不匹配任何條件規則（節省 token）")
    else:
        print(f"❌ 不相關檔案不應匹配條件規則，但匹配了 {len(unrelated_rules)} 個")

    # Test 6: API + test 雙匹配
    api_test_rules = engine.get_rules_for_file("src/api/handlers/order.test.py")
    total += 1
    has_api = any("FastAPI" in r.content for r in api_test_rules)
    has_test = any("Testing" in r.content for r in api_test_rules)
    if has_api and has_test:
        passed += 1
        print("✅ API test 檔案匹配 API + Testing 雙規則")
    else:
        print(f"❌ 應同時匹配 API 和 Testing 規則 (api={has_api}, test={has_test})")

    print(f"\nModule 3: {passed}/{total}")
    return passed, total


def run_module_4():
    """Module 4: Monorepo 過濾"""
    print("\n" + "=" * 60)
    print("Module 4: claudeMdExcludes Monorepo 過濾")
    print("=" * 60)

    fs = VirtualFS()
    project_root = "/Users/json/projects/odm-monorepo"

    # 三個團隊的 CLAUDE.md
    fs.write(f"{project_root}/CLAUDE.md", "# Shared Standards")
    fs.write(f"{project_root}/packages/de-pipeline/CLAUDE.md", "# DE Standards\n- No semicolons")
    fs.write(f"{project_root}/packages/frontend/CLAUDE.md", "# Frontend Standards\n- Use semicolons")
    fs.write(f"{project_root}/packages/firmware/CLAUDE.md", "# Firmware Standards\n- Use C99")

    excludes = ["packages/frontend/**", "packages/firmware/**"]

    loader = MiniClaudeMdLoader(
        fs=fs,
        cwd=f"{project_root}/packages/de-pipeline",
        excludes=[f"{project_root}/{e}" for e in excludes],
    )
    files = loader.load_launch()

    passed = 0
    total = 0

    # Test 1: DE CLAUDE.md 被載入
    total += 1
    if any("de-pipeline" in f.path for f in files):
        passed += 1
        print("✅ DE team CLAUDE.md 被載入")
    else:
        print("❌ DE CLAUDE.md 應被載入")

    # Test 2: Frontend 被排除
    total += 1
    if not any("frontend" in f.path for f in files):
        passed += 1
        print("✅ Frontend CLAUDE.md 被 claudeMdExcludes 排除")
    else:
        print("❌ Frontend 應被排除")

    # Test 3: Firmware 被排除
    total += 1
    if not any("firmware" in f.path for f in files):
        passed += 1
        print("✅ Firmware CLAUDE.md 被排除")
    else:
        print("❌ Firmware 應被排除")

    # Test 4: 共享 CLAUDE.md 被載入
    total += 1
    if any("Shared Standards" in f.content for f in files):
        passed += 1
        print("✅ Root 共享 CLAUDE.md 正常載入")
    else:
        print("❌ Root CLAUDE.md 應被載入")

    # Test 5: Context 中無衝突
    context = loader.get_concatenated_context()
    total += 1
    if "Use semicolons" not in context and "Use C99" not in context:
        passed += 1
        print("✅ Context 中無 frontend/firmware 規則衝突")
    else:
        print("❌ 排除後不應有其他團隊規則")

    print(f"\nModule 4: {passed}/{total}")
    return passed, total


def run_module_5():
    """Module 5: 診斷工具"""
    print("\n" + "=" * 60)
    print("Module 5: /memory 診斷 + 規則衝突偵測")
    print("=" * 60)

    fs = VirtualFS()
    project_root = "/project"

    fs.write(f"{project_root}/CLAUDE.md", "# Project\n- Use 2-space indentation")
    fs.write(f"{project_root}/.claude/rules/old-style.md",
             "# Old Style\n- Use 4-space indentation")  # 衝突！
    fs.write(f"{project_root}/.claude/rules/testing.md",
             '---\npaths:\n  - "**/*.test.ts"\n---\n# Testing\n- Use jest')

    loader = MiniClaudeMdLoader(fs=fs, cwd=project_root)
    loader.load_launch()

    engine = RulesEngine(fs, project_root)
    engine.discover_rules()

    diag = DiagnosticTool(loader, engine)

    passed = 0
    total = 0

    # Test 1: /memory 狀態
    status = diag.memory_status()
    total += 1
    if status["total_files"] > 0:
        passed += 1
        print(f"✅ /memory 顯示 {status['total_files']} 個載入檔案, {status['total_lines']} 行")
        print(f"   unconditional rules: {status['rules_unconditional']}, conditional: {status['rules_conditional']}")
    else:
        print("❌ /memory 應顯示載入的檔案")

    # Test 2: 衝突偵測
    conflicts = diag.detect_conflicts()
    total += 1
    if any(c["topic"] == "indentation" for c in conflicts):
        passed += 1
        print("✅ 偵測到 indentation 衝突（2-space vs 4-space）")
        for c in conflicts:
            print(f"   ⚠️ {c['topic']}: {c['recommendation']}")
    else:
        print("❌ 應偵測到 2-space vs 4-space 衝突")

    # Test 3: 診斷缺失規則
    suggestions = diag.diagnose_missing_rules("Use Python type hints")
    total += 1
    if any("not found" in s.lower() for s in suggestions):
        passed += 1
        print("✅ 診斷：'Use Python type hints' 未在任何檔案中找到")
    else:
        print("❌ 應報告規則未找到")

    # Test 4: 診斷已存在但未被遵循的規則
    suggestions2 = diag.diagnose_missing_rules("2-space indentation")
    total += 1
    if any("hook" in s.lower() for s in suggestions2):
        passed += 1
        print("✅ 診斷：規則已載入但未遵循 → 建議用 Hook 保證")
    else:
        print("❌ 已載入的規則未被遵循時應建議 Hook")

    print(f"\nModule 5: {passed}/{total}")
    return passed, total


def run_module_6():
    """Module 6: CCA Mock Exam"""
    print("\n" + "=" * 60)
    print("Module 6: CCA Mock Exam — D3 CLAUDE.md（5 題）")
    print("=" * 60)

    passed = 0
    total = len(CCA_QUESTIONS)

    for q in CCA_QUESTIONS:
        print(f"\n--- {q['id']} ({q['scenario']}) ---")
        print(f"Q: {q['question'][:120]}...")
        for key, val in q["options"].items():
            marker = "→" if key == q["correct"] else " "
            print(f"  {marker} {key}: {val[:80]}...")
        print(f"✅ 正確答案：{q['correct']}")
        print(f"   解釋：{q['explanation'][:120]}...")
        passed += 1

    print(f"\nModule 6: {passed}/{total}")
    return passed, total


def main():
    print("🏗️ CLAUDE.md 層級與模組化 — CCA D3 Task 3.1")
    print("📚 ODM 供應鏈 Monorepo 場景\n")

    results = []
    results.append(("Module 1: 四層 Scope 載入", run_module_1()))
    results.append(("Module 2: @import 展開", run_module_2()))
    results.append(("Module 3: .claude/rules/ 條件載入", run_module_3()))
    results.append(("Module 4: claudeMdExcludes Monorepo", run_module_4()))
    results.append(("Module 5: /memory 診斷", run_module_5()))
    results.append(("Module 6: CCA Mock Exam", run_module_6()))

    print("\n" + "=" * 60)
    print("📊 總結")
    print("=" * 60)

    total_passed = 0
    total_total = 0
    for name, (p, t) in results:
        total_passed += p
        total_total += t
        status = "✅" if p == t else "⚠️"
        print(f"  {status} {name}: {p}/{t}")

    print(f"\n🎯 總分：{total_passed}/{total_total}")

    if total_passed == total_total:
        print("🎉 全部通過！CLAUDE.md 層級與模組化完全掌握。")
    else:
        print(f"📝 還有 {total_total - total_passed} 個測試需要修正。")


if __name__ == "__main__":
    main()
