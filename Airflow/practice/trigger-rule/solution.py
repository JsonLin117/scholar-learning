"""
Airflow Trigger Rule 模擬器
=========================
不需要安裝 Airflow，模擬 11 種 trigger rule 的判斷邏輯。
用 ODM 供應鏈 ETL DAG 展示各種 trigger rule 的實際效果。

重點模擬：
1. 11 種 trigger rule 的 evaluation 邏輯
2. Skip cascade（branching 後的 skip 傳播）
3. Eager evaluation（one_success/one_failed 不等待所有上游）
4. Branching 的 skip 注入行為
"""
from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Optional
import time


# ============================================================
# 1. 定義 Task 狀態和 Trigger Rule（對齊 Airflow 3.x）
# ============================================================

class TaskState(Enum):
    NONE = "none"               # 尚未排程
    SCHEDULED = "scheduled"     # 可以跑了
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    UPSTREAM_FAILED = "upstream_failed"


class TriggerRule(Enum):
    ALL_SUCCESS = "all_success"           # 預設：所有上游成功
    ALL_FAILED = "all_failed"             # 所有上游失敗/upstream_failed
    ALL_DONE = "all_done"                 # 所有上游完成（任何狀態）
    ALL_SKIPPED = "all_skipped"           # 所有上游被跳過
    ONE_SUCCESS = "one_success"           # 至少一個成功（急性子）
    ONE_FAILED = "one_failed"             # 至少一個失敗（急性子）
    ONE_DONE = "one_done"                 # 至少一個完成
    NONE_FAILED = "none_failed"           # 沒有失敗（成功或跳過都 OK）
    NONE_FAILED_MIN_ONE_SUCCESS = "none_failed_min_one_success"
    NONE_SKIPPED = "none_skipped"         # 沒有被跳過
    ALWAYS = "always"                     # 永遠執行

    def __repr__(self):
        return self.value


# ============================================================
# 2. Mini Task 和 DAG 結構
# ============================================================

@dataclass
class MiniTask:
    """模擬 Airflow Task Instance"""
    task_id: str
    trigger_rule: TriggerRule = TriggerRule.ALL_SUCCESS
    upstream_ids: list[str] = field(default_factory=list)
    downstream_ids: list[str] = field(default_factory=list)
    state: TaskState = TaskState.NONE
    callable_fn: Optional[Callable] = None
    is_branch: bool = False
    branch_result: Optional[list[str]] = None  # branching 選中的 task_ids

    def __repr__(self):
        return f"Task({self.task_id}, state={self.state.value}, rule={self.trigger_rule.value})"


class MiniDAGScheduler:
    """模擬 Airflow Scheduler 的 trigger rule 評估邏輯"""

    def __init__(self):
        self.tasks: dict[str, MiniTask] = {}
        self.execution_order: list[str] = []

    def add_task(self, task: MiniTask) -> MiniTask:
        self.tasks[task.task_id] = task
        return task

    def set_dependencies(self, upstream_id: str, downstream_id: str):
        self.tasks[upstream_id].downstream_ids.append(downstream_id)
        self.tasks[downstream_id].upstream_ids.append(upstream_id)

    def get_upstream_states(self, task_id: str) -> dict[str, TaskState]:
        """取得上游 tasks 的狀態"""
        return {
            uid: self.tasks[uid].state
            for uid in self.tasks[task_id].upstream_ids
        }

    def evaluate_trigger_rule(self, task_id: str) -> bool:
        """
        核心邏輯：評估 trigger rule 是否滿足。
        回傳 True = 可以執行，False = 不能執行。
        """
        task = self.tasks[task_id]
        upstream_states = self.get_upstream_states(task_id)

        # 沒有上游 → 可以執行（根 task）
        if not upstream_states:
            return True

        states = list(upstream_states.values())
        total = len(states)
        
        # 計算各狀態數量
        success = sum(1 for s in states if s == TaskState.SUCCESS)
        failed = sum(1 for s in states if s == TaskState.FAILED)
        upstream_failed = sum(1 for s in states if s == TaskState.UPSTREAM_FAILED)
        skipped = sum(1 for s in states if s == TaskState.SKIPPED)
        done = sum(1 for s in states if s in (
            TaskState.SUCCESS, TaskState.FAILED,
            TaskState.SKIPPED, TaskState.UPSTREAM_FAILED
        ))
        pending = total - done

        rule = task.trigger_rule

        if rule == TriggerRule.ALL_SUCCESS:
            return success == total

        elif rule == TriggerRule.ALL_FAILED:
            return (failed + upstream_failed) == total

        elif rule == TriggerRule.ALL_DONE:
            return done == total  # 不管成功/失敗/跳過

        elif rule == TriggerRule.ALL_SKIPPED:
            return skipped == total

        elif rule == TriggerRule.ONE_SUCCESS:
            # ⚡ 急性子：一有成功就觸發，不等其他完成
            return success >= 1

        elif rule == TriggerRule.ONE_FAILED:
            # ⚡ 急性子：一有失敗就觸發
            return failed >= 1

        elif rule == TriggerRule.ONE_DONE:
            return (success + failed) >= 1

        elif rule == TriggerRule.NONE_FAILED:
            # 所有都完成，且沒有 failed/upstream_failed
            return done == total and (failed + upstream_failed) == 0

        elif rule == TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS:
            return done == total and (failed + upstream_failed) == 0 and success >= 1

        elif rule == TriggerRule.NONE_SKIPPED:
            return done == total and skipped == 0

        elif rule == TriggerRule.ALWAYS:
            return True

        return False

    def determine_task_state(self, task_id: str) -> TaskState:
        """決定 task 應該是什麼狀態"""
        task = self.tasks[task_id]
        upstream_states = self.get_upstream_states(task_id)

        if not upstream_states:
            return TaskState.SCHEDULED

        # 檢查是否所有上游都完成了（對急性子規則不需要全完成）
        all_done = all(
            s in (TaskState.SUCCESS, TaskState.FAILED, TaskState.SKIPPED, TaskState.UPSTREAM_FAILED)
            for s in upstream_states.values()
        )

        eager_rules = {TriggerRule.ONE_SUCCESS, TriggerRule.ONE_FAILED, TriggerRule.ONE_DONE, TriggerRule.ALWAYS}
        
        if not all_done and task.trigger_rule not in eager_rules:
            return TaskState.NONE  # 還不到評估時機

        if self.evaluate_trigger_rule(task_id):
            return TaskState.SCHEDULED
        elif all_done:
            # 上游都完成了但 trigger rule 不滿足 → 根據規則決定跳過還是 upstream_failed
            states = list(upstream_states.values())
            has_failure = any(s in (TaskState.FAILED, TaskState.UPSTREAM_FAILED) for s in states)
            has_skip = any(s == TaskState.SKIPPED for s in states)

            if task.trigger_rule in (TriggerRule.ALL_SUCCESS,) and has_skip:
                return TaskState.SKIPPED  # Skip cascade!
            elif has_failure:
                return TaskState.UPSTREAM_FAILED
            else:
                return TaskState.SKIPPED
        return TaskState.NONE

    def run(self, simulate_failures: dict[str, bool] = None,
            branch_decisions: dict[str, list[str]] = None):
        """執行 DAG 模擬"""
        simulate_failures = simulate_failures or {}
        branch_decisions = branch_decisions or {}

        max_iterations = 100
        iteration = 0

        while iteration < max_iterations:
            iteration += 1
            progress = False

            for task_id, task in self.tasks.items():
                if task.state != TaskState.NONE:
                    continue

                new_state = self.determine_task_state(task_id)

                if new_state == TaskState.SCHEDULED:
                    # 模擬執行
                    if simulate_failures.get(task_id, False):
                        task.state = TaskState.FAILED
                    elif task_id in branch_decisions:
                        task.state = TaskState.SUCCESS
                        task.is_branch = True
                        task.branch_result = branch_decisions[task_id]
                        # 注入 skip 到非選中的下游
                        for ds_id in task.downstream_ids:
                            if ds_id not in task.branch_result:
                                self.tasks[ds_id].state = TaskState.SKIPPED
                    else:
                        task.state = TaskState.SUCCESS

                    self.execution_order.append(task_id)
                    progress = True

                elif new_state in (TaskState.SKIPPED, TaskState.UPSTREAM_FAILED):
                    task.state = new_state
                    self.execution_order.append(task_id)
                    progress = True

            if not progress:
                break

    def print_results(self, title: str = ""):
        """印出執行結果"""
        if title:
            print(f"\n{'='*60}")
            print(f"  {title}")
            print(f"{'='*60}")

        # 按執行順序印出
        for task_id in self.execution_order:
            task = self.tasks[task_id]
            state_icon = {
                TaskState.SUCCESS: "✅",
                TaskState.FAILED: "❌",
                TaskState.SKIPPED: "⏭️",
                TaskState.UPSTREAM_FAILED: "🔴",
            }.get(task.state, "⬜")
            
            rule_str = f" [{task.trigger_rule.value}]" if task.trigger_rule != TriggerRule.ALL_SUCCESS else ""
            print(f"  {state_icon} {task.task_id}{rule_str} → {task.state.value}")

        # 未執行的 tasks
        for task_id, task in self.tasks.items():
            if task_id not in self.execution_order:
                print(f"  ⬜ {task.task_id} → {task.state.value} (未排程)")


# ============================================================
# 3. 場景模擬
# ============================================================

def scenario_1_basic_error_handling():
    """
    場景 1：ODM ERP ETL + 錯誤處理
    
    extract → transform → load → notify (all_done)
                                ↘ alert (one_failed)
                                → cleanup (all_done)
    """
    dag = MiniDAGScheduler()
    
    dag.add_task(MiniTask("extract_erp"))
    dag.add_task(MiniTask("transform"))
    dag.add_task(MiniTask("load_delta"))
    dag.add_task(MiniTask("notify", trigger_rule=TriggerRule.ALL_DONE))
    dag.add_task(MiniTask("alert", trigger_rule=TriggerRule.ONE_FAILED))
    dag.add_task(MiniTask("cleanup", trigger_rule=TriggerRule.ALL_DONE))

    dag.set_dependencies("extract_erp", "transform")
    dag.set_dependencies("transform", "load_delta")
    dag.set_dependencies("load_delta", "notify")
    dag.set_dependencies("load_delta", "alert")
    dag.set_dependencies("load_delta", "cleanup")

    # 場景 A：全部成功
    print("\n" + "="*60)
    print("  場景 1A：全部成功（alert 不會觸發）")
    print("="*60)
    dag.run()
    dag.print_results()

    # 場景 B：load 失敗
    print("\n" + "="*60)
    print("  場景 1B：load_delta 失敗")
    print("="*60)
    dag2 = MiniDAGScheduler()
    dag2.add_task(MiniTask("extract_erp"))
    dag2.add_task(MiniTask("transform"))
    dag2.add_task(MiniTask("load_delta"))
    dag2.add_task(MiniTask("notify", trigger_rule=TriggerRule.ALL_DONE))
    dag2.add_task(MiniTask("alert", trigger_rule=TriggerRule.ONE_FAILED))
    dag2.add_task(MiniTask("cleanup", trigger_rule=TriggerRule.ALL_DONE))
    dag2.set_dependencies("extract_erp", "transform")
    dag2.set_dependencies("transform", "load_delta")
    dag2.set_dependencies("load_delta", "notify")
    dag2.set_dependencies("load_delta", "alert")
    dag2.set_dependencies("load_delta", "cleanup")
    dag2.run(simulate_failures={"load_delta": True})
    dag2.print_results()


def scenario_2_branch_skip_cascade():
    """
    場景 2：Branching + Skip Cascade 比較
    
    fetch → route_branch ──→ process_electronic ──→ join_WRONG (all_success)
                          └→ process_mechanical ──→ join_RIGHT (none_failed_min_one_success)
    
    展示 all_success 在 branching 後會被 skip cascade 影響
    """
    # 2A：錯誤寫法 - join 用 all_success
    dag_wrong = MiniDAGScheduler()
    dag_wrong.add_task(MiniTask("fetch"))
    dag_wrong.add_task(MiniTask("route_branch"))
    dag_wrong.add_task(MiniTask("process_electronic"))
    dag_wrong.add_task(MiniTask("process_mechanical"))
    dag_wrong.add_task(MiniTask("join_report", trigger_rule=TriggerRule.ALL_SUCCESS))

    dag_wrong.set_dependencies("fetch", "route_branch")
    dag_wrong.set_dependencies("route_branch", "process_electronic")
    dag_wrong.set_dependencies("route_branch", "process_mechanical")
    dag_wrong.set_dependencies("process_electronic", "join_report")
    dag_wrong.set_dependencies("process_mechanical", "join_report")

    # branching 選擇 electronic，skip mechanical
    dag_wrong.run(branch_decisions={"route_branch": ["process_electronic"]})
    dag_wrong.print_results("場景 2A：❌ join 用 all_success → 被 skip cascade")

    # 2B：正確寫法 - join 用 none_failed_min_one_success
    dag_right = MiniDAGScheduler()
    dag_right.add_task(MiniTask("fetch"))
    dag_right.add_task(MiniTask("route_branch"))
    dag_right.add_task(MiniTask("process_electronic"))
    dag_right.add_task(MiniTask("process_mechanical"))
    dag_right.add_task(MiniTask("join_report",
                                trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS))

    dag_right.set_dependencies("fetch", "route_branch")
    dag_right.set_dependencies("route_branch", "process_electronic")
    dag_right.set_dependencies("route_branch", "process_mechanical")
    dag_right.set_dependencies("process_electronic", "join_report")
    dag_right.set_dependencies("process_mechanical", "join_report")

    dag_right.run(branch_decisions={"route_branch": ["process_electronic"]})
    dag_right.print_results("場景 2B：✅ join 用 none_failed_min_one_success → 正確合併")


def scenario_3_multi_source_one_success():
    """
    場景 3：多數據源 — 任一成功就生成報告

    extract_sap ──┐
    extract_mes ──┼→ partial_report (one_success)
    extract_wms ──┘   → full_report (all_done)
    
    SAP 成功就先出報告，不等 MES 和 WMS
    """
    dag = MiniDAGScheduler()
    dag.add_task(MiniTask("extract_sap"))
    dag.add_task(MiniTask("extract_mes"))
    dag.add_task(MiniTask("extract_wms"))
    dag.add_task(MiniTask("partial_report", trigger_rule=TriggerRule.ONE_SUCCESS))
    dag.add_task(MiniTask("full_report", trigger_rule=TriggerRule.ALL_DONE))

    dag.set_dependencies("extract_sap", "partial_report")
    dag.set_dependencies("extract_mes", "partial_report")
    dag.set_dependencies("extract_wms", "partial_report")
    dag.set_dependencies("extract_sap", "full_report")
    dag.set_dependencies("extract_mes", "full_report")
    dag.set_dependencies("extract_wms", "full_report")

    # MES 和 WMS 失敗，但 SAP 成功
    dag.run(simulate_failures={"extract_mes": True, "extract_wms": True})
    dag.print_results("場景 3：多源拉取 — SAP 成功 → partial_report 先出")


def scenario_4_trigger_rule_matrix():
    """
    場景 4：觸發規則矩陣測試
    
    對每種 trigger rule，模擬不同的上游狀態組合，
    驗證 trigger rule 的判斷邏輯是否正確。
    """
    print("\n" + "="*60)
    print("  場景 4：Trigger Rule 判斷矩陣")
    print("="*60)

    # 定義測試案例：(上游狀態列表, 預期結果 per rule)
    test_cases = [
        ("全成功", [TaskState.SUCCESS, TaskState.SUCCESS]),
        ("全失敗", [TaskState.FAILED, TaskState.FAILED]),
        ("一成一敗", [TaskState.SUCCESS, TaskState.FAILED]),
        ("一成一跳", [TaskState.SUCCESS, TaskState.SKIPPED]),
        ("全跳過", [TaskState.SKIPPED, TaskState.SKIPPED]),
        ("成+upstream_failed", [TaskState.SUCCESS, TaskState.UPSTREAM_FAILED]),
    ]

    rules_to_test = [
        TriggerRule.ALL_SUCCESS,
        TriggerRule.ALL_FAILED,
        TriggerRule.ALL_DONE,
        TriggerRule.ONE_SUCCESS,
        TriggerRule.ONE_FAILED,
        TriggerRule.NONE_FAILED,
        TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        TriggerRule.ALL_SKIPPED,
        TriggerRule.NONE_SKIPPED,
        TriggerRule.ALWAYS,
    ]

    # 印表頭
    header = f"  {'場景':<16}" + "".join(f"{'':>5}" for _ in rules_to_test)
    print(f"\n  {'場景':<16}", end="")
    for r in rules_to_test:
        short = r.value.replace("none_failed_min_one_success", "NF+1S") \
                       .replace("none_failed", "noneFail") \
                       .replace("all_success", "allSuc") \
                       .replace("all_failed", "allFail") \
                       .replace("all_done", "allDone") \
                       .replace("one_success", "oneSuc") \
                       .replace("one_failed", "oneFail") \
                       .replace("all_skipped", "allSkip") \
                       .replace("none_skipped", "noneSkip") \
                       .replace("always", "always")
        print(f" {short:>9}", end="")
    print()
    print("  " + "-" * (16 + 10 * len(rules_to_test)))

    for name, upstream_states in test_cases:
        print(f"  {name:<16}", end="")
        for rule in rules_to_test:
            # 建立臨時 DAG 來測試
            dag = MiniDAGScheduler()
            for i, state in enumerate(upstream_states):
                t = dag.add_task(MiniTask(f"up_{i}"))
                t.state = state
            target = dag.add_task(MiniTask("target", trigger_rule=rule))
            for i in range(len(upstream_states)):
                dag.set_dependencies(f"up_{i}", "target")

            result = dag.evaluate_trigger_rule("target")
            icon = "✅" if result else "  "
            print(f" {icon:>9}", end="")
        print()


def scenario_5_odm_full_pipeline():
    """
    場景 5：ODM 完整供應鏈 ETL DAG
    
    一個真實的 ODM daily pipeline 設計：
    
    extract_sap_po ────┐
    extract_sap_gr ────┤
    extract_mes_wip ──→┼→ validate (all_success)
                       │     │
                       │     ├→ branch_quality_check
                       │     │     ├→ normal_load (all_success)
                       │     │     └→ mrb_alert (all_success)
                       │     │
                       │     └→ join_load (none_failed_min_one_success)
                       │           │
                       │           └→ build_dashboard (all_success)
                       │
                       └→ notify_pmc (all_done)
                       └→ cleanup (all_done)
    """
    dag = MiniDAGScheduler()

    # 數據源層
    dag.add_task(MiniTask("extract_sap_po"))
    dag.add_task(MiniTask("extract_sap_gr"))
    dag.add_task(MiniTask("extract_mes_wip"))

    # 驗證層
    dag.add_task(MiniTask("validate"))

    # 品質分支
    dag.add_task(MiniTask("branch_quality"))
    dag.add_task(MiniTask("normal_load"))
    dag.add_task(MiniTask("mrb_alert"))

    # 合併層
    dag.add_task(MiniTask("join_load",
                          trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS))
    dag.add_task(MiniTask("build_dashboard"))

    # 通知與清理
    dag.add_task(MiniTask("notify_pmc", trigger_rule=TriggerRule.ALL_DONE))
    dag.add_task(MiniTask("cleanup", trigger_rule=TriggerRule.ALL_DONE))

    # 依賴關係
    for src in ["extract_sap_po", "extract_sap_gr", "extract_mes_wip"]:
        dag.set_dependencies(src, "validate")
        dag.set_dependencies(src, "notify_pmc")
        dag.set_dependencies(src, "cleanup")

    dag.set_dependencies("validate", "branch_quality")
    dag.set_dependencies("branch_quality", "normal_load")
    dag.set_dependencies("branch_quality", "mrb_alert")
    dag.set_dependencies("normal_load", "join_load")
    dag.set_dependencies("mrb_alert", "join_load")
    dag.set_dependencies("join_load", "build_dashboard")

    # 品質正常 → 走 normal_load，skip mrb_alert
    dag.run(branch_decisions={"branch_quality": ["normal_load"]})
    dag.print_results("場景 5：ODM 完整 Pipeline（品質正常 → normal_load）")


# ============================================================
# 4. 統計摘要
# ============================================================

def print_summary():
    print("\n" + "="*60)
    print("  📊 Trigger Rule 使用指南摘要")
    print("="*60)
    
    guide = [
        ("清理工作", "all_done", "不管成功失敗都要清理 temp 資源"),
        ("錯誤告警", "one_failed", "第一時間通知，不等所有上游完成"),
        ("分支合併", "none_failed_min_one_success", "跳過不算失敗，至少一條路成功"),
        ("多源任一", "one_success", "任一數據源成功就能出部分報告"),
        ("完成通知", "all_done", "成功/失敗都通知 PMC"),
        ("正常流程", "all_success", "預設就好，最安全"),
        ("監控審計", "always", "與上游完全無關"),
    ]
    
    for scenario, rule, reason in guide:
        print(f"  {scenario:<12} → {rule:<35} | {reason}")

    print("\n  ⚠️  最常見的坑：")
    print("  1. Branching 後用 all_success → skip cascade 導致 join 被跳過")
    print("  2. one_success/one_failed 是急性子，其他上游可能還在跑")
    print("  3. upstream_failed ≠ failed，all_failed 不匹配 upstream_failed")
    print("  4. Airflow 3.x 推薦 Setup/Teardown 取代部分 all_done 清理")


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":
    print("🔧 Airflow Trigger Rule 模擬器")
    print("=" * 60)
    print("模擬 Airflow 11 種 trigger rule 的行為")
    print("場景：ODM 供應鏈 ERP ETL Pipeline")
    print()

    scenario_1_basic_error_handling()
    scenario_2_branch_skip_cascade()
    scenario_3_multi_source_one_success()
    scenario_4_trigger_rule_matrix()
    scenario_5_odm_full_pipeline()
    print_summary()

    print("\n✅ 所有場景模擬完成！")
