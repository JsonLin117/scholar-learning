"""
dbt Cloud Architecture — 多環境設計 + Branch 策略 + CI/CD + RBAC
Cert-dbt-Architect Topic #1 Practice
Date: 2026-07-03

Covers:
1. Environment Manager (Dev/Staging/Prod configuration)
2. Branch Strategy Simulator (upstream flow git model)
3. CI Job Engine (state:modified+ selector, deferral, temp schema)
4. RBAC Designer (groups, permission sets, environment-level permissions)
5. Environment Variable Resolver (env_var() cross-environment)
6. Mock Exam Questions (5 questions)
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
from datetime import datetime
import hashlib

# ============================================================
# SECTION 1: Environment Manager
# ============================================================

class EnvType(Enum):
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"

class CredentialType(Enum):
    OAUTH = "oauth"
    SERVICE_ACCOUNT = "service_account"
    KEY_PAIR = "key_pair"

@dataclass
class WarehouseConnection:
    """Warehouse connection config for a dbt Cloud environment."""
    host: str
    database: str
    schema: str
    credential_type: CredentialType
    service_account: Optional[str] = None
    oauth_client_id: Optional[str] = None

@dataclass
class Environment:
    """dbt Cloud environment configuration."""
    name: str
    env_type: EnvType
    git_branch: str
    connection: WarehouseConnection
    is_production: bool = False
    env_vars: dict = field(default_factory=dict)
    dbt_version: str = "1.12.0"

    def validate(self):
        """Validate environment configuration."""
        errors = []

        # Production must be flagged
        if self.env_type == EnvType.PRODUCTION and not self.is_production:
            errors.append("Production environment must have 'Set as Production' enabled")

        # Deploy envs should use service account
        if self.env_type in (EnvType.PRODUCTION, EnvType.STAGING):
            if self.connection.credential_type == CredentialType.OAUTH:
                errors.append(f"{self.env_type.value} should use service account, not personal OAuth")

        # Dev env should use OAuth
        if self.env_type == EnvType.DEVELOPMENT:
            if self.connection.credential_type == CredentialType.SERVICE_ACCOUNT:
                errors.append("Development environment should use OAuth for individual credentials")

        return errors

class EnvironmentManager:
    """Manages dbt Cloud project environments (max 1 Production)."""

    def __init__(self, project_name: str):
        self.project_name = project_name
        self.environments: dict[str, Environment] = {}

    def add_environment(self, env: Environment) -> list[str]:
        errors = env.validate()

        # Check production uniqueness
        if env.is_production:
            existing_prod = [e for e in self.environments.values() if e.is_production]
            if existing_prod:
                errors.append(
                    f"Only 1 Production environment allowed. "
                    f"Existing: '{existing_prod[0].name}'"
                )

        if not errors:
            self.environments[env.name] = env

        return errors

    def get_env(self, name: str) -> Optional[Environment]:
        return self.environments.get(name)

    def get_production(self) -> Optional[Environment]:
        for env in self.environments.values():
            if env.is_production:
                return env
        return None


# ============================================================
# SECTION 2: Branch Strategy Simulator
# ============================================================

@dataclass
class GitBranch:
    name: str
    protected: bool = False
    require_pr: bool = False
    require_ci: bool = False
    require_review: bool = False

@dataclass
class PullRequest:
    id: int
    source_branch: str
    target_branch: str
    title: str
    status: str = "open"  # open, merged, closed
    ci_status: str = "pending"  # pending, running, passed, failed
    commits: list = field(default_factory=list)

class BranchStrategy:
    """Simulates upstream git flow: feature → staging → main."""

    def __init__(self):
        self.branches: dict[str, GitBranch] = {}
        self.pull_requests: list[PullRequest] = []
        self._pr_counter = 0

        # Setup default branches
        self._setup_upstream_flow()

    def _setup_upstream_flow(self):
        self.branches["main"] = GitBranch(
            "main", protected=True, require_pr=True,
            require_ci=True, require_review=True
        )
        self.branches["staging"] = GitBranch(
            "staging", protected=True, require_pr=True,
            require_ci=True, require_review=False
        )

    def create_feature_branch(self, name: str) -> GitBranch:
        branch = GitBranch(name)
        self.branches[name] = branch
        return branch

    def open_pr(self, source: str, target: str, title: str) -> PullRequest:
        self._pr_counter += 1
        target_branch = self.branches.get(target)

        pr = PullRequest(
            id=self._pr_counter,
            source_branch=source,
            target_branch=target,
            title=title,
        )
        self.pull_requests.append(pr)
        return pr

    def can_merge(self, pr: PullRequest) -> tuple[bool, list[str]]:
        """Check if PR meets merge requirements."""
        blockers = []
        target = self.branches.get(pr.target_branch)
        if not target:
            return False, ["Target branch does not exist"]

        if target.require_ci and pr.ci_status != "passed":
            blockers.append(f"CI check required (current: {pr.ci_status})")
        if target.require_review:
            blockers.append("Code review required")
        if pr.status != "open":
            blockers.append(f"PR is {pr.status}")

        return len(blockers) == 0, blockers

    def merge_pr(self, pr: PullRequest) -> bool:
        can, blockers = self.can_merge(pr)
        if can or not self.branches[pr.target_branch].require_ci:
            pr.status = "merged"
            return True
        return False


# ============================================================
# SECTION 3: CI Job Engine
# ============================================================

@dataclass
class DbtModel:
    """Represents a dbt model node."""
    name: str
    sql: str
    refs: list[str] = field(default_factory=list)  # upstream dependencies
    modified: bool = False

@dataclass
class CIRun:
    run_id: str
    job_id: int
    pr_id: int
    commit_sha: str
    status: str = "queued"  # queued, running, passed, failed, cancelled
    temp_schema: str = ""
    selected_models: list = field(default_factory=list)
    deferred_to: str = "production"

class CIJobEngine:
    """Simulates dbt Cloud CI job with state:modified+ and deferral."""

    def __init__(self, job_id: int = 1862):
        self.job_id = job_id
        self.runs: list[CIRun] = []
        self.production_models: dict[str, DbtModel] = {}  # "production state"
        self.pr_models: dict[str, DbtModel] = {}           # "PR state"

    def set_production_state(self, models: dict[str, DbtModel]):
        self.production_models = models

    def set_pr_state(self, models: dict[str, DbtModel]):
        self.pr_models = models

    def _generate_temp_schema(self, pr_id: int) -> str:
        return f"dbt_cloud_pr_{self.job_id}_{pr_id}"

    def _find_modified_plus(self) -> list[str]:
        """state:modified+ — find modified nodes and all their downstream."""
        modified = set()

        # Find directly modified models
        for name, model in self.pr_models.items():
            prod_model = self.production_models.get(name)
            if not prod_model:
                modified.add(name)  # new model
            elif model.sql != prod_model.sql:
                modified.add(name)  # changed SQL

        # Find downstream (+ selector)
        all_selected = set(modified)
        changed = True
        while changed:
            changed = False
            for name, model in self.pr_models.items():
                if name not in all_selected:
                    # Check if any ref is in selected set
                    if any(ref in all_selected for ref in model.refs):
                        all_selected.add(name)
                        changed = True

        return sorted(all_selected)

    def trigger_ci(self, pr_id: int, commit_sha: str) -> CIRun:
        """Trigger CI run — with smart cancellation."""
        # Smart cancellation: cancel stale runs for same PR
        for run in self.runs:
            if run.pr_id == pr_id and run.status in ("queued", "running"):
                run.status = "cancelled"

        selected = self._find_modified_plus()
        run = CIRun(
            run_id=f"run_{len(self.runs) + 1}",
            job_id=self.job_id,
            pr_id=pr_id,
            commit_sha=commit_sha,
            temp_schema=self._generate_temp_schema(pr_id),
            selected_models=selected,
            deferred_to="production",
        )
        self.runs.append(run)
        return run

    def execute_run(self, run: CIRun) -> bool:
        """Simulate executing the CI run."""
        if run.status == "cancelled":
            return False
        run.status = "running"
        # Simulate: all models build successfully
        run.status = "passed"
        return True


# ============================================================
# SECTION 4: RBAC Designer
# ============================================================

class PermissionSet(Enum):
    ACCOUNT_ADMIN = "Account Admin"
    ADMIN = "Admin"
    DEVELOPER = "Developer"
    ANALYST = "Analyst"
    ANALYST_READ = "Analyst Read"
    DATABASE_ADMIN = "Database Admin"
    ACCOUNT_VIEWER = "Account Viewer"
    BILLING_ADMIN = "Billing Admin"

# Permission capabilities matrix
PERMISSION_CAPABILITIES = {
    PermissionSet.ACCOUNT_ADMIN: {
        "scope": "account",
        "ide_access": True,
        "create_jobs": True,
        "edit_env": True,
        "manage_users": True,
        "create_groups": True,
        "view_catalog": True,
        "create_projects": True,
    },
    PermissionSet.ADMIN: {
        "scope": "project",
        "ide_access": True,
        "create_jobs": True,
        "edit_env": True,
        "manage_users": True,
        "create_groups": False,
        "view_catalog": True,
        "create_projects": False,
    },
    PermissionSet.DEVELOPER: {
        "scope": "project",
        "ide_access": True,
        "create_jobs": False,
        "edit_env": False,
        "manage_users": False,
        "create_groups": False,
        "view_catalog": True,
        "create_projects": False,
    },
    PermissionSet.ANALYST: {
        "scope": "project",
        "ide_access": True,
        "create_jobs": False,
        "edit_env": False,
        "manage_users": False,
        "create_groups": False,
        "view_catalog": True,
        "create_projects": False,
    },
    PermissionSet.ANALYST_READ: {
        "scope": "project",
        "ide_access": False,
        "create_jobs": False,
        "edit_env": False,
        "manage_users": False,
        "create_groups": False,
        "view_catalog": True,
        "create_projects": False,
    },
    PermissionSet.DATABASE_ADMIN: {
        "scope": "project",
        "ide_access": False,
        "create_jobs": False,
        "edit_env": True,
        "manage_users": False,
        "create_groups": False,
        "view_catalog": True,
        "create_projects": False,
    },
}

@dataclass
class DbtGroup:
    name: str
    permission_sets: list[PermissionSet]
    projects: list[str]  # project names this group has access to
    sso_mapping: Optional[str] = None  # SSO group name

@dataclass
class DbtUser:
    name: str
    email: str
    groups: list[str]
    license_type: str = "developer"  # developer, read-only, IT

class RBACDesigner:
    """Design and validate dbt Cloud RBAC for an ODM organization."""

    def __init__(self):
        self.groups: dict[str, DbtGroup] = {}
        self.users: dict[str, DbtUser] = {}

    def create_group(self, group: DbtGroup):
        self.groups[group.name] = group

    def add_user(self, user: DbtUser):
        self.users[user.email] = user

    def get_effective_permissions(self, user_email: str) -> dict:
        """Resolve effective permissions (highest wins)."""
        user = self.users.get(user_email)
        if not user:
            return {}

        effective = {
            "ide_access": False,
            "create_jobs": False,
            "edit_env": False,
            "manage_users": False,
            "create_groups": False,
            "view_catalog": False,
            "create_projects": False,
        }

        for group_name in user.groups:
            group = self.groups.get(group_name)
            if not group:
                continue
            for perm_set in group.permission_sets:
                caps = PERMISSION_CAPABILITIES.get(perm_set, {})
                for key in effective:
                    if caps.get(key, False):
                        effective[key] = True  # highest wins

        return effective

    def validate_design(self) -> list[str]:
        """Check for common RBAC design issues."""
        issues = []

        # Check: too many account admins
        admin_count = sum(
            1 for u in self.users.values()
            if any(
                PermissionSet.ACCOUNT_ADMIN in self.groups[g].permission_sets
                for g in u.groups if g in self.groups
            )
        )
        if admin_count > 3:
            issues.append(f"⚠️ {admin_count} Account Admins — recommend ≤ 3")

        # Check: users with no groups
        for email, user in self.users.items():
            if not user.groups:
                issues.append(f"⚠️ User {email} has no group assignment")

        # Check: read-only license with IDE access
        for email, user in self.users.items():
            if user.license_type == "read-only":
                perms = self.get_effective_permissions(email)
                if perms.get("ide_access"):
                    issues.append(
                        f"⚠️ {email} has read-only license but IDE access via permissions"
                    )

        return issues


# ============================================================
# SECTION 5: Environment Variable Resolver
# ============================================================

class EnvVarResolver:
    """Simulates dbt env_var() function across environments."""

    def __init__(self):
        self.env_configs: dict[str, dict[str, str]] = {
            "development": {},
            "staging": {},
            "production": {},
        }

    def set_var(self, env: str, key: str, value: str):
        self.env_configs[env][key] = value

    def resolve(self, env: str, expression: str) -> str:
        """
        Resolve {{ env_var('KEY') }} or {{ env_var('KEY', 'default') }}.
        """
        if "env_var" not in expression:
            return expression

        # Parse env_var call
        import re
        match = re.search(r"env_var\(['\"](\w+)['\"](?:,\s*['\"]([^'\"]*)['\"])?\)", expression)
        if not match:
            return expression

        key = match.group(1)
        default = match.group(2)

        value = self.env_configs.get(env, {}).get(key)
        if value is None:
            if default is not None:
                return default
            raise ValueError(f"env_var('{key}') not set in {env} and no default provided")
        return value

    def resolve_source_yml(self, env: str, source_config: dict) -> dict:
        """Resolve all env_var references in a source YAML config."""
        resolved = {}
        for key, value in source_config.items():
            if isinstance(value, str) and "env_var" in value:
                resolved[key] = self.resolve(env, value)
            else:
                resolved[key] = value
        return resolved


# ============================================================
# SECTION 6: Mock Exam Questions
# ============================================================

MOCK_EXAM = [
    {
        "id": 1,
        "question": (
            "A company has a dbt Cloud Enterprise account with one project. "
            "They need a staging environment where developers can trigger ad-hoc jobs "
            "but cannot modify production data. What is the correct setup?"
        ),
        "options": {
            "A": "Create a General deployment environment and give developers Admin permissions",
            "B": "Create a Staging deployment environment with its own credentials pointing to a staging schema, and assign Developer permissions for the staging environment",
            "C": "Create a second Production environment for staging",
            "D": "Use the Development environment for staging by changing its credentials",
        },
        "answer": "B",
        "explanation": (
            "Staging is a specific deployment environment type that isolates dev from prod. "
            "Developer permissions allow IDE access and limited job control. "
            "C is wrong because only 1 Production environment is allowed. "
            "D conflates dev and staging purposes."
        ),
    },
    {
        "id": 2,
        "question": (
            "A CI job is configured with deferral to the Production environment. "
            "When a developer pushes 2 commits to the same PR within 5 minutes, "
            "what happens to the CI runs?"
        ),
        "options": {
            "A": "Both CI runs execute in parallel since they're from the same PR",
            "B": "The first CI run completes, then the second one starts",
            "C": "The first CI run is cancelled (smart cancellation), and only the latest commit's CI run executes",
            "D": "Both CI runs are cancelled and must be manually re-triggered",
        },
        "answer": "C",
        "explanation": (
            "Smart cancellation: same PR + different commit SHA → dbt cancels the stale in-flight run "
            "and enqueues the new one. Different PRs would run concurrently."
        ),
    },
    {
        "id": 3,
        "question": (
            "In a multi-project dbt Mesh setup, Project B has a cross-project ref to a model in Project A. "
            "When a developer opens the IDE for Project B, where does the cross-project ref resolve to?"
        ),
        "options": {
            "A": "Project A's Production environment always",
            "B": "Project A's Development environment",
            "C": "Project A's Staging environment (if configured), otherwise Production",
            "D": "The ref fails because cross-project refs only work in deployment",
        },
        "answer": "C",
        "explanation": (
            "Cross-project refs in IDE/Staging resolve to the Staging environment of the upstream project, "
            "keeping production data isolated. Only the Production environment resolves to Production."
        ),
    },
    {
        "id": 4,
        "question": (
            "An ODM company needs to rotate warehouse credentials for their dbt Cloud production "
            "environment without downtime. What is the recommended approach?"
        ),
        "options": {
            "A": "Update credentials directly in the dbt Cloud UI during off-hours",
            "B": "Use key pair authentication and rotate via the dbt Cloud Administrative API",
            "C": "Create a new Production environment with new credentials and delete the old one",
            "D": "Use OAuth for the Production environment so credentials auto-refresh",
        },
        "answer": "B",
        "explanation": (
            "Key pair auth allows API-driven rotation without manual UI changes. "
            "A requires manual work and has a change window. "
            "C would delete all associated jobs. "
            "D is incorrect — OAuth is for individual dev credentials, not service accounts."
        ),
    },
    {
        "id": 5,
        "question": (
            "A dbt Cloud project uses environment variables to control test severity. "
            "The team wants tests to warn in Development, fail in CI, and warn in Production. "
            "What is the correct configuration?"
        ),
        "options": {
            "A": "Set DBT_TEST_SEVERITY=warn as project default, override to error in the CI environment",
            "B": "Set DBT_TEST_SEVERITY=error as project default, override to warn in Development and Production",
            "C": "Use dbt_project.yml to hardcode severity per environment",
            "D": "Create separate YAML files for each environment with different severity settings",
        },
        "answer": "A",
        "explanation": (
            "Environment variables support project-wide defaults with per-environment overrides. "
            "Setting warn as default (covers Dev and Prod) and error only in CI is the simplest "
            "and most maintainable approach. B works but requires more overrides. "
            "C and D don't use dbt Cloud's native env var mechanism."
        ),
    },
]


# ============================================================
# MAIN: Run all demos
# ============================================================

def demo_environment_manager():
    print("=" * 70)
    print("DEMO 1: Environment Manager — Three-Tier Setup")
    print("=" * 70)

    mgr = EnvironmentManager("odm-analytics")

    # Dev environment
    dev = Environment(
        name="Development",
        env_type=EnvType.DEVELOPMENT,
        git_branch="*",  # any feature branch
        connection=WarehouseConnection(
            host="odm-databricks.azuredatabricks.net",
            database="dev_catalog",
            schema="dbt_${user}",
            credential_type=CredentialType.OAUTH,
            oauth_client_id="oauth-app-id-123",
        ),
        env_vars={"SAP_SOURCE_DATABASE": "dev_sap_raw", "DBT_TEST_SEVERITY": "warn"},
    )
    errors = mgr.add_environment(dev)
    print(f"✅ Dev environment: {len(errors)} errors")

    # Staging environment
    staging = Environment(
        name="Staging",
        env_type=EnvType.STAGING,
        git_branch="staging",
        connection=WarehouseConnection(
            host="odm-databricks.azuredatabricks.net",
            database="staging_catalog",
            schema="staging",
            credential_type=CredentialType.SERVICE_ACCOUNT,
            service_account="svc-dbt-staging@odm.com",
        ),
        env_vars={"SAP_SOURCE_DATABASE": "staging_sap_raw", "DBT_TEST_SEVERITY": "error"},
    )
    errors = mgr.add_environment(staging)
    print(f"✅ Staging environment: {len(errors)} errors")

    # Production environment
    prod = Environment(
        name="Production",
        env_type=EnvType.PRODUCTION,
        git_branch="main",
        is_production=True,
        connection=WarehouseConnection(
            host="odm-databricks.azuredatabricks.net",
            database="prod_catalog",
            schema="analytics",
            credential_type=CredentialType.SERVICE_ACCOUNT,
            service_account="svc-dbt-prod@odm.com",
        ),
        env_vars={"SAP_SOURCE_DATABASE": "prod_sap_raw", "DBT_TEST_SEVERITY": "warn"},
    )
    errors = mgr.add_environment(prod)
    print(f"✅ Production environment: {len(errors)} errors")
    print(f"   Production env: {mgr.get_production().name}")

    # Try adding second production → should fail
    bad_prod = Environment(
        name="Prod-2",
        env_type=EnvType.PRODUCTION,
        git_branch="main",
        is_production=True,
        connection=WarehouseConnection(
            host="x", database="x", schema="x",
            credential_type=CredentialType.SERVICE_ACCOUNT,
        ),
    )
    errors = mgr.add_environment(bad_prod)
    print(f"❌ Second Production: {errors}")

    # Try dev with service account → should warn
    bad_dev = Environment(
        name="Dev-Bad",
        env_type=EnvType.DEVELOPMENT,
        git_branch="*",
        connection=WarehouseConnection(
            host="x", database="x", schema="x",
            credential_type=CredentialType.SERVICE_ACCOUNT,
        ),
    )
    errors = bad_dev.validate()
    print(f"⚠️  Dev with service account: {errors}")
    print()


def demo_branch_strategy():
    print("=" * 70)
    print("DEMO 2: Branch Strategy — Upstream Flow (feature→staging→main)")
    print("=" * 70)

    git = BranchStrategy()

    # Developer creates feature branch
    git.create_feature_branch("feature/add-vendor-scorecard")
    print("📌 Created branch: feature/add-vendor-scorecard")

    # Open PR to staging
    pr1 = git.open_pr("feature/add-vendor-scorecard", "staging",
                       "Add vendor scorecard mart model")
    print(f"📋 PR #{pr1.id}: {pr1.title} → staging")

    # Check merge requirements
    can, blockers = git.can_merge(pr1)
    print(f"   Can merge? {can} | Blockers: {blockers}")

    # CI passes
    pr1.ci_status = "passed"
    can, blockers = git.can_merge(pr1)
    print(f"   After CI pass → Can merge? {can} | Blockers: {blockers}")

    # Merge to staging (no reviewer required for staging)
    pr1.ci_status = "passed"
    result = git.merge_pr(pr1)
    print(f"   Merged to staging: {result}")

    # Open PR from staging to main
    pr2 = git.open_pr("staging", "main", "Release: vendor scorecard to production")
    pr2.ci_status = "passed"
    can, blockers = git.can_merge(pr2)
    print(f"\n📋 PR #{pr2.id}: staging → main")
    print(f"   Can merge? {can} | Blockers: {blockers}")
    # main requires review → still blocked
    print()


def demo_ci_engine():
    print("=" * 70)
    print("DEMO 3: CI Job Engine — state:modified+ & Smart Cancellation")
    print("=" * 70)

    ci = CIJobEngine(job_id=1862)

    # Production state (existing models)
    ci.set_production_state({
        "stg_sap_matdoc": DbtModel("stg_sap_matdoc", "SELECT * FROM {{ source('sap', 'matdoc') }}"),
        "stg_sap_ekpo": DbtModel("stg_sap_ekpo", "SELECT * FROM {{ source('sap', 'ekpo') }}"),
        "int_inventory": DbtModel("int_inventory", "SELECT ... JOIN ...",
                                   refs=["stg_sap_matdoc"]),
        "int_po_detail": DbtModel("int_po_detail", "SELECT ... JOIN ...",
                                   refs=["stg_sap_ekpo"]),
        "mart_vendor_score": DbtModel("mart_vendor_score", "SELECT ... GROUP BY ...",
                                       refs=["int_po_detail"]),
        "mart_inventory_kpi": DbtModel("mart_inventory_kpi", "SELECT ... GROUP BY ...",
                                        refs=["int_inventory"]),
    })

    # PR state: modified stg_sap_ekpo SQL
    ci.set_pr_state({
        "stg_sap_matdoc": DbtModel("stg_sap_matdoc", "SELECT * FROM {{ source('sap', 'matdoc') }}"),
        "stg_sap_ekpo": DbtModel("stg_sap_ekpo", "SELECT *, UPPER(vendor) as vendor_clean FROM {{ source('sap', 'ekpo') }}"),
        "int_inventory": DbtModel("int_inventory", "SELECT ... JOIN ...",
                                   refs=["stg_sap_matdoc"]),
        "int_po_detail": DbtModel("int_po_detail", "SELECT ... JOIN ...",
                                   refs=["stg_sap_ekpo"]),
        "mart_vendor_score": DbtModel("mart_vendor_score", "SELECT ... GROUP BY ...",
                                       refs=["int_po_detail"]),
        "mart_inventory_kpi": DbtModel("mart_inventory_kpi", "SELECT ... GROUP BY ...",
                                        refs=["int_inventory"]),
        "mart_vendor_risk": DbtModel("mart_vendor_risk", "SELECT ... WHERE risk > 0.8",
                                      refs=["mart_vendor_score"]),  # NEW model
    })

    # Trigger CI for PR #1704, commit 1
    run1 = ci.trigger_ci(pr_id=1704, commit_sha="abc123")
    print(f"🚀 CI Run 1: {run1.run_id}")
    print(f"   Temp schema: {run1.temp_schema}")
    print(f"   Modified models (state:modified+): {run1.selected_models}")
    print(f"   Deferred to: {run1.deferred_to}")

    # Push new commit → smart cancellation
    run1.status = "running"
    run2 = ci.trigger_ci(pr_id=1704, commit_sha="def456")
    print(f"\n🔄 New commit pushed → Smart cancellation")
    print(f"   Run 1 status: {run1.status} (cancelled)")
    print(f"   Run 2 status: {run2.status}")
    print(f"   Run 2 selected: {run2.selected_models}")

    # Different PR → concurrent
    run3 = ci.trigger_ci(pr_id=1705, commit_sha="ghi789")
    print(f"\n⚡ Different PR #{run3.pr_id} → Concurrent")
    print(f"   Run 2 (PR 1704): {run2.status}")
    print(f"   Run 3 (PR 1705): {run3.status}")
    print(f"   Temp schemas: {run2.temp_schema} | {run3.temp_schema}")

    # Execute
    ci.execute_run(run2)
    ci.execute_run(run3)
    print(f"\n✅ Results: Run 2={run2.status}, Run 3={run3.status}")
    print()


def demo_rbac():
    print("=" * 70)
    print("DEMO 4: RBAC Designer — ODM Team Permission Design")
    print("=" * 70)

    rbac = RBACDesigner()

    # Create groups
    rbac.create_group(DbtGroup(
        name="DE-Leads",
        permission_sets=[PermissionSet.ADMIN],
        projects=["odm-analytics"],
        sso_mapping="AzureAD-DE-Leads",
    ))
    rbac.create_group(DbtGroup(
        name="DE-Developers",
        permission_sets=[PermissionSet.DEVELOPER],
        projects=["odm-analytics"],
        sso_mapping="AzureAD-DE-Team",
    ))
    rbac.create_group(DbtGroup(
        name="BI-Analysts",
        permission_sets=[PermissionSet.ANALYST_READ],
        projects=["odm-analytics"],
        sso_mapping="AzureAD-BI-Team",
    ))
    rbac.create_group(DbtGroup(
        name="DBA-Team",
        permission_sets=[PermissionSet.DATABASE_ADMIN],
        projects=["odm-analytics"],
        sso_mapping="AzureAD-DBA",
    ))
    rbac.create_group(DbtGroup(
        name="Platform-Admin",
        permission_sets=[PermissionSet.ACCOUNT_ADMIN],
        projects=["odm-analytics"],
    ))

    # Add users
    rbac.add_user(DbtUser("Json (Lead)", "json@odm.com", ["DE-Leads"]))
    rbac.add_user(DbtUser("Alice (DE)", "alice@odm.com", ["DE-Developers"]))
    rbac.add_user(DbtUser("Bob (BI)", "bob@odm.com", ["BI-Analysts"], license_type="read-only"))
    rbac.add_user(DbtUser("Carol (DBA)", "carol@odm.com", ["DBA-Team"]))
    rbac.add_user(DbtUser("Dave (Admin)", "dave@odm.com", ["Platform-Admin"]))

    # Show effective permissions
    print("\n📋 Effective Permissions Matrix:")
    print(f"{'User':<20} {'IDE':>5} {'Jobs':>5} {'Env':>5} {'Users':>6} {'Catalog':>8}")
    print("-" * 55)
    for email, user in rbac.users.items():
        perms = rbac.get_effective_permissions(email)
        print(
            f"{user.name:<20} "
            f"{'✅' if perms['ide_access'] else '❌':>5} "
            f"{'✅' if perms['create_jobs'] else '❌':>5} "
            f"{'✅' if perms['edit_env'] else '❌':>5} "
            f"{'✅' if perms['manage_users'] else '❌':>6} "
            f"{'✅' if perms['view_catalog'] else '❌':>8}"
        )

    # Validate
    issues = rbac.validate_design()
    print(f"\n🔍 Design validation: {len(issues)} issues")
    for issue in issues:
        print(f"   {issue}")
    print()


def demo_env_var_resolver():
    print("=" * 70)
    print("DEMO 5: Environment Variable Resolver — Cross-Environment")
    print("=" * 70)

    resolver = EnvVarResolver()

    # Configure env vars
    resolver.set_var("development", "SAP_SOURCE_DATABASE", "dev_sap_raw")
    resolver.set_var("staging", "SAP_SOURCE_DATABASE", "staging_sap_raw")
    resolver.set_var("production", "SAP_SOURCE_DATABASE", "prod_sap_raw")

    resolver.set_var("development", "DBT_TEST_SEVERITY", "warn")
    resolver.set_var("staging", "DBT_TEST_SEVERITY", "error")
    resolver.set_var("production", "DBT_TEST_SEVERITY", "warn")

    # Source YAML with env_var
    source_config = {
        "name": "sap_mm",
        "database": "{{ env_var('SAP_SOURCE_DATABASE') }}",
        "schema": "mm",
    }

    print("\n📋 Source YAML resolution per environment:")
    for env in ["development", "staging", "production"]:
        resolved = resolver.resolve_source_yml(env, source_config)
        severity = resolver.resolve(env, "{{ env_var('DBT_TEST_SEVERITY') }}")
        print(f"  {env:>12}: database={resolved['database']:<20} severity={severity}")

    # Test default value
    default_val = resolver.resolve("development", "{{ env_var('NONEXISTENT_KEY', 'fallback_value') }}")
    print(f"\n  Default value test: {default_val}")

    # Test missing var (no default)
    try:
        resolver.resolve("development", "{{ env_var('NONEXISTENT_KEY') }}")
    except ValueError as e:
        print(f"  Missing var error: {e}")
    print()


def demo_mock_exam():
    print("=" * 70)
    print("DEMO 6: Mock Exam — 5 dbt Architect Questions")
    print("=" * 70)

    correct = 0
    for q in MOCK_EXAM:
        print(f"\n📝 Q{q['id']}: {q['question'][:100]}...")
        for opt, text in q["options"].items():
            marker = "✅" if opt == q["answer"] else "  "
            print(f"   {marker} {opt}. {text[:80]}")
        print(f"   → Answer: {q['answer']} — {q['explanation'][:100]}...")
        correct += 1

    print(f"\n🎯 Score: {correct}/{len(MOCK_EXAM)} ({correct/len(MOCK_EXAM)*100:.0f}%)")
    print()


if __name__ == "__main__":
    print("🏗️  dbt Cloud Architecture Practice — Cert-dbt-Architect #1")
    print(f"📅 Date: 2026-07-03")
    print(f"🎯 Topics: Environments, Branch Strategy, CI/CD, RBAC, Env Vars")
    print()

    demo_environment_manager()
    demo_branch_strategy()
    demo_ci_engine()
    demo_rbac()
    demo_env_var_resolver()
    demo_mock_exam()

    print("=" * 70)
    print("✅ All 6 demos completed successfully!")
    print("=" * 70)
