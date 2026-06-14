"""
Skills & Slash Commands Practice — ODM 供應鏈 Skill 設計與驗證
CCA D3 Task 3.2 練習

重點：
1. 產生三個 SKILL.md 範例（不同觸發模式）
2. 解析 frontmatter 並驗證設計規則
3. 模擬 Skill 發現機制（目錄掃描）
"""

import yaml
import os
import re
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional


# ─── Part 1: Skill 資料模型 ───

@dataclass
class SkillFrontmatter:
    """Claude Code SKILL.md frontmatter 的 Python 表示"""
    name: str = ""
    description: str = ""
    when_to_use: str = ""
    argument_hint: str = ""
    arguments: list = field(default_factory=list)
    disable_model_invocation: bool = False
    user_invocable: bool = True
    allowed_tools: list = field(default_factory=list)
    disallowed_tools: list = field(default_factory=list)
    model: str = ""
    effort: str = ""
    context: str = ""  # "" or "fork"
    agent: str = ""
    paths: list = field(default_factory=list)
    shell: str = "bash"

    @classmethod
    def from_yaml(cls, yaml_dict: dict) -> "SkillFrontmatter":
        """從 YAML dict 建立 frontmatter"""
        fm = cls()
        fm.name = yaml_dict.get("name", "")
        fm.description = yaml_dict.get("description", "")
        fm.when_to_use = yaml_dict.get("when_to_use", "")
        fm.argument_hint = yaml_dict.get("argument-hint", "")

        # arguments 可以是 string 或 list
        args = yaml_dict.get("arguments", [])
        fm.arguments = args.split() if isinstance(args, str) else args

        fm.disable_model_invocation = yaml_dict.get("disable-model-invocation", False)
        fm.user_invocable = yaml_dict.get("user-invocable", True)

        # allowed-tools 可以是 string 或 list
        tools = yaml_dict.get("allowed-tools", [])
        fm.allowed_tools = [t.strip() for t in tools.split(",")] if isinstance(tools, str) else tools

        dtools = yaml_dict.get("disallowed-tools", [])
        fm.disallowed_tools = [t.strip() for t in dtools.split(",")] if isinstance(dtools, str) else dtools

        fm.model = yaml_dict.get("model", "")
        fm.effort = yaml_dict.get("effort", "")
        fm.context = yaml_dict.get("context", "")
        fm.agent = yaml_dict.get("agent", "")

        paths = yaml_dict.get("paths", [])
        fm.paths = [p.strip() for p in paths.split(",")] if isinstance(paths, str) else paths

        fm.shell = yaml_dict.get("shell", "bash")
        return fm

    @property
    def effective_description_length(self) -> int:
        """description + when_to_use 合計長度（上限 1536 字元）"""
        combined = self.description
        if self.when_to_use:
            combined += " " + self.when_to_use
        return len(combined)


# ─── Part 2: SKILL.md 解析器 ───

def parse_skill_md(content: str) -> tuple[SkillFrontmatter, str]:
    """解析 SKILL.md，回傳 (frontmatter, body)"""
    # 找 YAML frontmatter（--- 之間）
    match = re.match(r'^---\s*\n(.*?)\n---\s*\n(.*)', content, re.DOTALL)
    if not match:
        return SkillFrontmatter(), content

    yaml_str, body = match.group(1), match.group(2)
    yaml_dict = yaml.safe_load(yaml_str) or {}
    return SkillFrontmatter.from_yaml(yaml_dict), body


def find_dynamic_injections(body: str) -> list[str]:
    """找出所有 !`command` 動態注入"""
    return re.findall(r'!`([^`]+)`', body)


def find_substitutions(body: str) -> list[str]:
    """找出所有 $VARIABLE 替換"""
    # 排除被 \ 跳脫的
    return re.findall(r'(?<!\\)\$(?:ARGUMENTS(?:\[\d+\])?|\d+|\{[A-Z_]+\}|\w+)', body)


# ─── Part 3: 產生三個 ODM Skill 範例 ───

SKILLS = {
    "pipeline-check": {
        "frontmatter": {
            "name": "pipeline-check",
            "description": (
                "Check Airflow DAG status and Delta Lake table health. "
                "Use when the user asks about pipeline status, data freshness, "
                "or table health metrics."
            ),
            "allowed-tools": "Read, Bash(airflow dags list-runs*), Bash(delta-rs*)",
            "effort": "low",
        },
        "body": """## Current Pipeline Status

!`airflow dags list-runs --dag-id odm_daily_etl --limit 5 --output json 2>/dev/null || echo "Airflow CLI not available"`

## Delta Lake Table Health

!`python3 -c "
from delta.tables import DeltaTable
tables = ['raw.purchase_orders', 'curated.inventory_snapshot', 'mart.supplier_scorecard']
for t in tables:
    try:
        dt = DeltaTable.forName(spark, t)
        h = dt.history(1).collect()[0]
        print(f'{t}: v{h.version}, {h.timestamp}, {h.operation}')
    except Exception as e:
        print(f'{t}: ERROR - {e}')
" 2>/dev/null || echo "Delta Lake check requires PySpark session"`

## Instructions

Summarize the pipeline status:
1. List any failed or running DAGs
2. Report table freshness (time since last write)
3. Flag any tables with version gaps > 10 (potential compaction needed)
4. If everything is healthy, say so concisely
""",
    },

    "deploy-pipeline": {
        "frontmatter": {
            "name": "deploy-pipeline",
            "description": "Deploy a data pipeline to production environment",
            "argument-hint": "[dag-name] [environment]",
            "arguments": ["dag", "env"],
            "disable-model-invocation": True,  # ⚠️ 有副作用，必須手動觸發
            "context": "fork",  # 隔離執行
            "allowed-tools": "Read, Bash, Write",
            "disallowed-tools": "AskUserQuestion",  # 部署中不該問問題
            "effort": "high",
        },
        "body": """## Pre-deployment Checklist

Target DAG: $dag
Environment: $env

### Step 1: Validate DAG syntax
```!
cd ${CLAUDE_SKILL_DIR}/scripts
./validate-dag.sh $dag
```

### Step 2: Run integration tests
```!
cd /opt/airflow
python -m pytest tests/dags/test_$dag.py -v --tb=short 2>&1 | tail -30
```

### Step 3: Deploy
Execute the deployment:
1. Back up current DAG version
2. Copy validated DAG to $env environment
3. Trigger a test run with `--conf '{"dry_run": true}'`
4. Verify test run succeeded
5. Report deployment status

### Step 4: Post-deploy verification
- Check DAG appears in Airflow UI
- Verify connections and variables are set
- Confirm first scheduled run time
""",
    },

    "schema-review": {
        "frontmatter": {
            "name": "schema-review",
            "description": (
                "Review SQL schema changes for data warehouse best practices. "
                "Checks naming conventions, index strategy, partition design, "
                "and backward compatibility."
            ),
            "when_to_use": (
                "Trigger when the user modifies .sql files under migrations/ or schemas/, "
                "or asks to review a schema change."
            ),
            "context": "fork",  # 獨立 context，不污染主對話
            "paths": "migrations/**/*.sql, schemas/**/*.sql",
            "allowed-tools": "Read, Grep, Bash(git diff*)",
        },
        "body": """## Schema Changes Under Review

!`git diff HEAD -- 'migrations/*.sql' 'schemas/*.sql'`

## Current Schema Baseline

!`cat schemas/current_schema.sql 2>/dev/null || echo "No baseline schema found"`

## Review Criteria

Analyze the schema changes above against these ODM data warehouse standards:

### Naming
- Tables: `<layer>_<domain>_<entity>` (e.g., `raw_procurement_purchase_orders`)
- Columns: snake_case, no abbreviations except standard ones (qty, amt, dt)
- Indexes: `idx_<table>_<columns>`

### Data Types
- Timestamps: always `TIMESTAMP WITH TIME ZONE`
- IDs: `BIGINT` for surrogate keys, `VARCHAR(50)` for natural keys
- Money: `DECIMAL(18,4)` not FLOAT

### Partition Strategy
- Fact tables: partition by date column
- Tables > 10M rows: must have partition

### Backward Compatibility
- ❌ Dropping columns without deprecation period
- ❌ Changing column types without migration
- ✅ Adding nullable columns is safe
- ✅ Adding new tables is safe

### Output
List findings as: `[PASS|WARN|FAIL] <check> — <detail>`
""",
    },
}


# ─── Part 4: Skill 設計規則驗證器 ───

class SkillLintResult:
    def __init__(self, skill_name: str):
        self.skill_name = skill_name
        self.errors: list[str] = []
        self.warnings: list[str] = []
        self.info: list[str] = []

    @property
    def passed(self) -> bool:
        return len(self.errors) == 0


def lint_skill(name: str, fm: SkillFrontmatter, body: str) -> SkillLintResult:
    """驗證 Skill 設計是否符合最佳實踐"""
    result = SkillLintResult(name)

    # Rule 1: description 不能為空
    if not fm.description:
        result.errors.append("Missing description — Claude won't know when to use this skill")

    # Rule 2: description + when_to_use 不超過 1536 字元
    desc_len = fm.effective_description_length
    if desc_len > 1536:
        result.errors.append(
            f"Description too long ({desc_len} chars, max 1536) — will be truncated"
        )
    elif desc_len > 1200:
        result.warnings.append(
            f"Description approaching limit ({desc_len}/1536 chars)"
        )

    # Rule 3: 有副作用的 skill 必須 disable-model-invocation
    side_effect_keywords = ["deploy", "delete", "drop", "send", "publish", "push"]
    has_side_effects = any(kw in name.lower() or kw in body.lower()[:500]
                         for kw in side_effect_keywords)
    if has_side_effects and not fm.disable_model_invocation:
        result.errors.append(
            "Skill has side effects but disable-model-invocation is not set — "
            "Claude might trigger deployment automatically!"
        )

    # Rule 4: 背景/自動 skill 不應有 AskUserQuestion
    if fm.context == "fork" and "AskUserQuestion" not in fm.disallowed_tools:
        result.warnings.append(
            "context:fork skill without disallowed-tools: AskUserQuestion — "
            "subagent might hang waiting for user input"
        )

    # Rule 5: disable-model-invocation + user-invocable:false = 永遠不會被觸發
    if fm.disable_model_invocation and not fm.user_invocable:
        result.errors.append(
            "Both disable-model-invocation and user-invocable:false — "
            "this skill can never be invoked by anyone!"
        )

    # Rule 6: 有動態注入的 skill 應該考慮 context:fork（避免大輸出污染 context）
    injections = find_dynamic_injections(body)
    if len(injections) > 3 and fm.context != "fork":
        result.warnings.append(
            f"Skill has {len(injections)} dynamic injections — "
            "consider context:fork to avoid context bloat"
        )

    # Rule 7: body 行數建議 < 500
    body_lines = body.strip().split('\n')
    if len(body_lines) > 500:
        result.warnings.append(
            f"Skill body is {len(body_lines)} lines — keep under 500, "
            "move reference material to supporting files"
        )

    # Info: 報告找到的特性
    if injections:
        result.info.append(f"Dynamic injections: {', '.join(injections[:3])}...")
    subs = find_substitutions(body)
    if subs:
        result.info.append(f"Substitutions: {', '.join(set(subs))}")
    if fm.context == "fork":
        result.info.append("Runs in forked subagent context")
    if fm.paths:
        result.info.append(f"Path-scoped: {', '.join(fm.paths)}")

    return result


# ─── Part 5: Skill 發現模擬 ───

def simulate_skill_discovery(project_root: str) -> dict[str, dict]:
    """模擬 Claude Code 的 Skill 發現機制"""
    discovered = {}

    # 1. Personal skills
    personal_dir = Path.home() / ".claude" / "skills"
    if personal_dir.exists():
        for skill_dir in personal_dir.iterdir():
            skill_md = skill_dir / "SKILL.md"
            if skill_md.exists():
                discovered[skill_dir.name] = {
                    "scope": "personal",
                    "path": str(skill_md),
                    "priority": 2,  # personal overrides project
                }

    # 2. Project skills (walk up to repo root)
    current = Path(project_root).resolve()
    while current != current.parent:
        skill_dir = current / ".claude" / "skills"
        if skill_dir.exists():
            for sd in skill_dir.iterdir():
                skill_md = sd / "SKILL.md"
                if skill_md.exists() and sd.name not in discovered:
                    discovered[sd.name] = {
                        "scope": "project",
                        "path": str(skill_md),
                        "priority": 1,
                    }
        # Stop at git root
        if (current / ".git").exists():
            break
        current = current.parent

    return discovered


# ─── Part 6: 觸發矩陣模擬 ───

def invocation_matrix(fm: SkillFrontmatter) -> dict:
    """回傳 Skill 的觸發矩陣"""
    return {
        "user_can_invoke": fm.user_invocable,
        "claude_can_invoke": not fm.disable_model_invocation,
        "description_in_context": not fm.disable_model_invocation,
        "when_loaded": (
            "Never (unreachable)" if fm.disable_model_invocation and not fm.user_invocable
            else "On /name invocation only" if fm.disable_model_invocation
            else "When Claude deems relevant" if not fm.user_invocable
            else "Description always; full content on invocation"
        ),
    }


# ─── Main ───

def main():
    print("=" * 70)
    print("🔧 CCA D3 Practice: Skills & Slash Commands — ODM 供應鏈 Skill 設計")
    print("=" * 70)

    all_passed = True

    for skill_name, skill_data in SKILLS.items():
        print(f"\n{'─' * 60}")
        print(f"📦 Skill: /{skill_name}")
        print(f"{'─' * 60}")

        # 產生 SKILL.md 內容
        fm_dict = skill_data["frontmatter"]
        body = skill_data["body"]

        # 解析 frontmatter
        fm = SkillFrontmatter.from_yaml(fm_dict)

        # 顯示觸發矩陣
        matrix = invocation_matrix(fm)
        print(f"\n  👤 User can invoke:      {matrix['user_can_invoke']}")
        print(f"  🤖 Claude can invoke:    {matrix['claude_can_invoke']}")
        print(f"  📋 Desc in context:      {matrix['description_in_context']}")
        print(f"  ⏰ When loaded:          {matrix['when_loaded']}")

        # 顯示動態注入
        injections = find_dynamic_injections(body)
        if injections:
            print(f"\n  💉 Dynamic injections ({len(injections)}):")
            for inj in injections:
                preview = inj[:60] + "..." if len(inj) > 60 else inj
                print(f"     !`{preview}`")

        # 顯示替換變數
        subs = find_substitutions(body)
        if subs:
            print(f"\n  🔤 Substitutions: {', '.join(sorted(set(subs)))}")

        # Lint 驗證
        lint = lint_skill(skill_name, fm, body)
        print(f"\n  🔍 Lint Results:")
        if lint.errors:
            for e in lint.errors:
                print(f"     ❌ ERROR: {e}")
            all_passed = False
        if lint.warnings:
            for w in lint.warnings:
                print(f"     ⚠️  WARN: {w}")
        if lint.info:
            for i in lint.info:
                print(f"     ℹ️  INFO: {i}")
        if lint.passed:
            print(f"     ✅ PASSED")

    # 模擬 Skill 發現
    print(f"\n{'─' * 60}")
    print(f"🔍 Skill Discovery Simulation (current directory)")
    print(f"{'─' * 60}")
    discovered = simulate_skill_discovery(os.getcwd())
    if discovered:
        for name, info in sorted(discovered.items()):
            print(f"  /{name} [{info['scope']}] → {info['path']}")
    else:
        print("  No skills discovered (no .claude/skills/ found)")

    # 總結
    print(f"\n{'=' * 70}")
    if all_passed:
        print("✅ All skill designs passed lint validation!")
    else:
        print("❌ Some skills have design issues — fix before using in production")

    # Key takeaways
    print(f"\n📝 Key Takeaways for CCA Exam:")
    print("  1. disable-model-invocation=true → 副作用操作必設")
    print("  2. context:fork → 長任務/重操作用 subagent 隔離")
    print("  3. !`cmd` → 動態注入即時資料，Claude 看到的是輸出不是命令")
    print("  4. description 上限 1536 字元（含 when_to_use）")
    print("  5. frontmatter name 欄位只在 plugin root 才決定命令名")
    print("  6. Skills 覆蓋順序：Enterprise > Personal > Project")
    print("  7. allowed-tools → 最小權限原則")

    print(f"\n{'=' * 70}")
    return 0 if all_passed else 1


if __name__ == "__main__":
    exit(main())
