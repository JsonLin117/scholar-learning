"""
CI/CD 基礎實作：Airflow DAG Validation Test Suite
===================================================
場景：ODM 供應鏈公司的 Airflow CI pipeline
日期：2026-05-08

這個腳本模擬一套 DAG 驗證測試，展示 CI 階段最基本的品質閘門。
因為不依賴真實 Airflow 環境，用 Python AST 和 regex 做靜態分析。

使用方式：
  cd /Users/json/Projects/scholar-learning
  .venv/bin/python3 DevOps-CICD/practice/cicd-what-is-it/solution.py
"""

import ast
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import List


# ============================================================
# 1. 模擬的 DAG 檔案（真實場景這些是 dags/ 資料夾裡的 .py）
# ============================================================

SAMPLE_DAG_GOOD = '''
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# 符合規範的 DAG
with DAG(
    dag_id="odm_bom_etl",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["etl", "bom", "production"],
    default_args={"owner": "data-team"},
) as dag:
    def extract_bom():
        """從 ERP 抽取 BOM 資料"""
        pass

    t1 = PythonOperator(task_id="extract_bom", python_callable=extract_bom)
'''

SAMPLE_DAG_BAD_CATCHUP = '''
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# 反模式：catchup=True 會觸發大量歷史任務
with DAG(
    dag_id="odm_inventory_sync",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@hourly",
    catchup=True,  # ❌ 危險！
    tags=["inventory"],
) as dag:
    pass
'''

SAMPLE_DAG_BAD_SECRET = '''
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests

DB_PASSWORD = "SuperSecret123!"  # ❌ 密碼寫死在程式碼
API_KEY = "sk-abc123def456"      # ❌ API Key 寫死

with DAG(
    dag_id="odm_erp_extract",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["erp"],
) as dag:
    def call_api():
        # 反模式：硬編碼 credentials
        resp = requests.get(
            "https://erp.company.com/api",
            headers={"Authorization": f"Bearer {API_KEY}"}
        )
    t1 = PythonOperator(task_id="call_api", python_callable=call_api)
'''

SAMPLE_DAG_IMPORT_ERROR = '''
from airflow import DAG
from nonexistent_module import FakeOperator  # ❌ 這個 module 不存在
'''


# ============================================================
# 2. 驗證邏輯（模擬 CI pipeline 中的測試）
# ============================================================

@dataclass
class ValidationResult:
    """單一驗證結果"""
    dag_file: str
    check_name: str
    passed: bool
    message: str = ""


@dataclass
class CIReport:
    """CI 測試報告"""
    results: List[ValidationResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(r.passed for r in self.results)

    @property
    def total(self) -> int:
        return len(self.results)

    @property
    def failures(self) -> int:
        return sum(1 for r in self.results if not r.passed)

    def add(self, result: ValidationResult):
        self.results.append(result)

    def print_report(self):
        """打印 CI 報告（模擬 pytest 輸出風格）"""
        print("\n" + "=" * 60)
        print("🔧 DAG Validation CI Report")
        print("=" * 60)

        for r in self.results:
            status = "✅ PASS" if r.passed else "❌ FAIL"
            print(f"  {status} | {r.dag_file} | {r.check_name}")
            if not r.passed and r.message:
                print(f"         → {r.message}")

        print("-" * 60)
        color = "\033[92m" if self.passed else "\033[91m"
        reset = "\033[0m"
        print(f"  {color}Total: {self.total} | Passed: {self.total - self.failures} | Failed: {self.failures}{reset}")
        print("=" * 60)

        if self.passed:
            print("\n🎉 All checks passed! Safe to deploy.")
        else:
            print("\n🚫 CI FAILED — Fix issues before merging.")


def check_syntax(filename: str, source: str, report: CIReport):
    """
    Check 1：語法/Import 檢查
    模擬 DagBag 載入 — 用 AST parse 檢查語法，用 compile 檢查基本 import
    """
    try:
        ast.parse(source)
        report.add(ValidationResult(filename, "syntax_check", True))
    except SyntaxError as e:
        report.add(ValidationResult(
            filename, "syntax_check", False,
            f"SyntaxError at line {e.lineno}: {e.msg}"
        ))


def check_catchup(filename: str, source: str, report: CIReport):
    """
    Check 2：catchup=False 合規
    ODM 廠資料量大，意外 backfill 6 個月的資料會癱瘓系統
    """
    # 簡易 regex：找 catchup=True
    if re.search(r'catchup\s*=\s*True', source):
        report.add(ValidationResult(
            filename, "catchup_check", False,
            "catchup=True detected! 在 ODM 環境可能觸發大量歷史任務"
        ))
    elif re.search(r'catchup\s*=\s*False', source):
        report.add(ValidationResult(filename, "catchup_check", True))
    else:
        report.add(ValidationResult(
            filename, "catchup_check", False,
            "未明確設定 catchup=False（Airflow 預設 True）"
        ))


def check_tags(filename: str, source: str, report: CIReport):
    """
    Check 3：DAG 必須有 tags
    方便 Airflow UI 分類管理（尤其多 team 共用時）
    """
    if re.search(r'tags\s*=\s*\[', source):
        report.add(ValidationResult(filename, "tags_check", True))
    else:
        report.add(ValidationResult(
            filename, "tags_check", False,
            "DAG 缺少 tags — 在多團隊環境中無法分類"
        ))


def check_secrets(filename: str, source: str, report: CIReport):
    """
    Check 4：敏感資訊掃描
    掃描常見的密碼/API Key pattern，防止 credentials 推到 Git
    """
    # 常見 secret patterns（簡化版，真實場景用 detect-secrets 或 gitleaks）
    secret_patterns = [
        (r'(?i)(password|passwd|pwd)\s*=\s*["\'][^"\']+["\']', "硬編碼密碼"),
        (r'(?i)(api[_-]?key|apikey)\s*=\s*["\'][^"\']+["\']', "硬編碼 API Key"),
        (r'(?i)(secret[_-]?key|secretkey)\s*=\s*["\'][^"\']+["\']', "硬編碼 Secret Key"),
        (r'sk-[a-zA-Z0-9]{20,}', "疑似 OpenAI API Key"),
        (r'(?i)bearer\s+[a-zA-Z0-9\-._~+/]+=*', "硬編碼 Bearer Token"),
    ]

    found_secrets = []
    for pattern, description in secret_patterns:
        matches = re.findall(pattern, source)
        if matches:
            found_secrets.append(description)

    if found_secrets:
        report.add(ValidationResult(
            filename, "secret_scan", False,
            f"發現敏感資訊: {', '.join(found_secrets)}。請使用 Airflow Connection/Variable 或 Vault"
        ))
    else:
        report.add(ValidationResult(filename, "secret_scan", True))


# ============================================================
# 3. 主程式：模擬 CI Pipeline 執行
# ============================================================

def main():
    print("🚀 Starting DAG Validation CI Pipeline...")
    print("   (模擬 GitHub Actions / Jenkins 的 CI 階段)\n")

    # 模擬 dags/ 資料夾裡的 DAG 檔案
    dag_files = {
        "dags/odm_bom_etl.py": SAMPLE_DAG_GOOD,
        "dags/odm_inventory_sync.py": SAMPLE_DAG_BAD_CATCHUP,
        "dags/odm_erp_extract.py": SAMPLE_DAG_BAD_SECRET,
        "dags/odm_broken.py": SAMPLE_DAG_IMPORT_ERROR,
    }

    report = CIReport()

    # 對每個 DAG 執行所有驗證
    checks = [check_syntax, check_catchup, check_tags, check_secrets]

    for filename, source in dag_files.items():
        print(f"📄 Checking {filename}...")
        for check_fn in checks:
            check_fn(filename, source, report)

    # 產生報告
    report.print_report()

    # ============================================================
    # 4. 展示 GitHub Actions workflow 結構
    # ============================================================
    print("\n\n📋 對應的 GitHub Actions Workflow：")
    print("-" * 60)
    github_actions_yaml = """
# .github/workflows/dag-ci.yml
name: Airflow DAG CI
on:
  push:
    branches: [main, develop]
    paths: ['dags/**', 'plugins/**']
  pull_request:
    branches: [main]

jobs:
  validate-dags:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          pip install apache-airflow pytest ruff

      - name: Lint (ruff)
        run: ruff check dags/

      - name: DAG validation tests
        run: pytest tests/test_dag_validation.py -v

      - name: Secret scan (gitleaks)
        uses: gitleaks/gitleaks-action@v2

  unit-test:
    runs-on: ubuntu-latest
    needs: validate-dags  # 驗證通過才跑 unit test
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: pip install -r requirements-test.txt

      - name: Run unit tests
        run: pytest tests/unit/ -v --tb=short
"""
    print(github_actions_yaml)

    # ============================================================
    # 5. 印出學習重點
    # ============================================================
    print("\n📝 Today's Key Takeaways：")
    print("  1. CI 最基本的三個檢查：syntax → compliance → secrets")
    print("  2. Data pipeline CI 要額外驗證 catchup、tags、data contract")
    print("  3. Secret detection 是 CI 的安全底線（用 gitleaks/detect-secrets）")
    print("  4. 先 validate → 再 unit test → 再 integration test（金字塔）")
    print("  5. ODM 廠建議 Continuous Delivery（有人工 gate），不是全自動")

    # 回傳 exit code（模擬 CI：失敗 = exit 1）
    sys.exit(0 if report.passed else 1)


if __name__ == "__main__":
    main()
