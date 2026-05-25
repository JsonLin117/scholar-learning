"""
Flink Lesson 1 Practice: Stream Processing vs Batch
===================================================
Dependency-free Python simulation of event-time stream processing.

Scenario: ODM SMT machines emit temperature events. We compare a daily batch
report with a MiniFlink-style streaming job that uses keyed state, watermarks,
checkpoints, replay, and late-event side outputs.
"""

from __future__ import annotations

import copy
import random
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from math import inf
from statistics import mean
from typing import Iterable


# ──────────────────────────────────────────────
# Part 1: Domain events
# ──────────────────────────────────────────────


@dataclass(frozen=True)
class SensorEvent:
    """One SMT sensor event.

    event_time  = when the temperature was measured at the machine.
    arrival_time = when the event arrives at the stream processor.
    """

    event_id: str
    machine_id: str
    event_time: datetime
    arrival_time: datetime
    temperature_c: float


@dataclass
class WindowState:
    total_temp: float = 0.0
    count: int = 0
    max_temp: float = float("-inf")
    event_ids: list[str] = field(default_factory=list)

    def add(self, event: SensorEvent) -> None:
        self.total_temp += event.temperature_c
        self.count += 1
        self.max_temp = max(self.max_temp, event.temperature_c)
        self.event_ids.append(event.event_id)

    @property
    def avg_temp(self) -> float:
        return self.total_temp / self.count if self.count else 0.0


@dataclass(frozen=True)
class WindowResult:
    machine_id: str
    window_start: datetime
    window_end: datetime
    avg_temp: float
    max_temp: float
    event_count: int
    alert: bool
    finalized_at_watermark: datetime | str

    @property
    def key(self) -> tuple[str, datetime]:
        return self.machine_id, self.window_start


# ──────────────────────────────────────────────
# Part 2: Test data — out-of-order ODM factory stream
# ──────────────────────────────────────────────


def generate_smt_events() -> list[SensorEvent]:
    """Create deterministic events with normal disorder and a few very-late events."""
    rng = random.Random(20260526)
    base = datetime(2026, 5, 26, 8, 0, 0)
    machines = ["SMT-01", "SMT-02", "SMT-03"]
    events: list[SensorEvent] = []

    for tick in range(40):  # 40 * 10 seconds = 6m40s per machine
        event_time = base + timedelta(seconds=tick * 10)
        for machine in machines:
            normal_temp = {
                "SMT-01": 62.0,
                "SMT-02": 64.0,
                "SMT-03": 59.0,
            }[machine]
            jitter = rng.gauss(0, 1.4)

            # SMT-02 overheats for roughly one minute: stream should alert fast.
            if machine == "SMT-02" and base + timedelta(minutes=3) <= event_time < base + timedelta(minutes=4):
                temperature = 76.5 + rng.gauss(0, 1.2)
            else:
                temperature = normal_temp + jitter

            # Most events arrive quickly; a few arrive very late to demonstrate side output.
            network_delay_seconds = rng.randint(0, 18)
            if tick in {8, 17, 26} and machine in {"SMT-01", "SMT-03"}:
                network_delay_seconds += 120  # too late for a 30s watermark strategy
            elif tick % 11 == 0 and machine == "SMT-02":
                network_delay_seconds += 38  # borderline disorder

            event_id = f"{machine}-{tick:03d}"
            events.append(
                SensorEvent(
                    event_id=event_id,
                    machine_id=machine,
                    event_time=event_time,
                    arrival_time=event_time + timedelta(seconds=network_delay_seconds),
                    temperature_c=round(temperature, 2),
                )
            )

    return sorted(events, key=lambda e: (e.arrival_time, e.event_id))


# ──────────────────────────────────────────────
# Part 3: Batch final report — bounded dataset
# ──────────────────────────────────────────────


def floor_to_minute(ts: datetime) -> datetime:
    return ts.replace(second=0, microsecond=0)


def batch_report(events: Iterable[SensorEvent], alert_threshold_c: float) -> dict[tuple[str, datetime], WindowResult]:
    """Batch waits for all data and computes the final answer over a bounded dataset."""
    groups: dict[tuple[str, datetime], list[SensorEvent]] = defaultdict(list)
    for event in events:
        groups[(event.machine_id, floor_to_minute(event.event_time))].append(event)

    results: dict[tuple[str, datetime], WindowResult] = {}
    for (machine_id, window_start), rows in sorted(groups.items()):
        temps = [r.temperature_c for r in rows]
        window_end = window_start + timedelta(minutes=1)
        results[(machine_id, window_start)] = WindowResult(
            machine_id=machine_id,
            window_start=window_start,
            window_end=window_end,
            avg_temp=round(mean(temps), 2),
            max_temp=round(max(temps), 2),
            event_count=len(rows),
            alert=mean(temps) >= alert_threshold_c,
            finalized_at_watermark="batch-end",
        )
    return results


# ──────────────────────────────────────────────
# Part 4: MiniFlink — keyed state + watermark + checkpoint
# ──────────────────────────────────────────────


@dataclass
class Checkpoint:
    source_index: int
    state: dict[tuple[str, datetime], WindowState]
    emitted_keys: set[tuple[str, datetime]]
    outputs: list[WindowResult]
    late_events: list[SensorEvent]
    max_event_time_seen: datetime | None
    watermark: datetime | None


class MiniFlinkJob:
    """Small stream processor that models the first Flink concepts.

    It is intentionally not a full Flink clone. The goal is to make the core
    mental model visible in 200 lines of Python.
    """

    def __init__(
        self,
        allowed_lateness_seconds: int = 30,
        checkpoint_interval_records: int = 25,
        alert_threshold_c: float = 70.0,
    ):
        self.allowed_lateness = timedelta(seconds=allowed_lateness_seconds)
        self.checkpoint_interval_records = checkpoint_interval_records
        self.alert_threshold_c = alert_threshold_c
        self.state: dict[tuple[str, datetime], WindowState] = defaultdict(WindowState)
        self.outputs: list[WindowResult] = []
        self.late_events: list[SensorEvent] = []
        self.emitted_keys: set[tuple[str, datetime]] = set()
        self.max_event_time_seen: datetime | None = None
        self.watermark: datetime | None = None
        self.latest_checkpoint: Checkpoint | None = None
        self.checkpoint_history: list[tuple[int, datetime | None, int]] = []
        self.replayed_records = 0
        self.failure_injected = False
        self.process_attempts = 0

    def _update_watermark(self, event_time: datetime) -> None:
        if self.max_event_time_seen is None or event_time > self.max_event_time_seen:
            self.max_event_time_seen = event_time
        self.watermark = self.max_event_time_seen - self.allowed_lateness

    def _is_too_late(self, event: SensorEvent) -> bool:
        return self.watermark is not None and event.event_time <= self.watermark

    def _finalize_ready_windows(self) -> None:
        if self.watermark is None:
            return

        ready_keys = []
        for key in self.state:
            _machine_id, window_start = key
            window_end = window_start + timedelta(minutes=1)
            if window_end <= self.watermark and key not in self.emitted_keys:
                ready_keys.append(key)

        for key in sorted(ready_keys, key=lambda item: (item[1], item[0])):
            machine_id, window_start = key
            window_end = window_start + timedelta(minutes=1)
            window_state = self.state[key]
            result = WindowResult(
                machine_id=machine_id,
                window_start=window_start,
                window_end=window_end,
                avg_temp=round(window_state.avg_temp, 2),
                max_temp=round(window_state.max_temp, 2),
                event_count=window_state.count,
                alert=window_state.avg_temp >= self.alert_threshold_c,
                finalized_at_watermark=self.watermark,
            )
            self.outputs.append(result)
            self.emitted_keys.add(key)

    def process_event(self, event: SensorEvent) -> None:
        self.process_attempts += 1
        self._update_watermark(event.event_time)

        if self._is_too_late(event):
            self.late_events.append(event)
            return

        window_start = floor_to_minute(event.event_time)
        self.state[(event.machine_id, window_start)].add(event)
        self._finalize_ready_windows()

    def checkpoint(self, source_index: int) -> None:
        self.latest_checkpoint = Checkpoint(
            source_index=source_index,
            state=copy.deepcopy(dict(self.state)),
            emitted_keys=set(self.emitted_keys),
            outputs=copy.deepcopy(self.outputs),
            late_events=copy.deepcopy(self.late_events),
            max_event_time_seen=self.max_event_time_seen,
            watermark=self.watermark,
        )
        self.checkpoint_history.append((source_index, self.watermark, len(self.outputs)))

    def restore_latest_checkpoint(self, failed_at_index: int) -> int:
        if self.latest_checkpoint is None:
            raise RuntimeError("Cannot restore: no checkpoint exists")

        cp = self.latest_checkpoint
        self.state = defaultdict(WindowState, copy.deepcopy(cp.state))
        self.emitted_keys = set(cp.emitted_keys)
        self.outputs = copy.deepcopy(cp.outputs)
        self.late_events = copy.deepcopy(cp.late_events)
        self.max_event_time_seen = cp.max_event_time_seen
        self.watermark = cp.watermark
        self.replayed_records += failed_at_index - cp.source_index
        return cp.source_index

    def close(self) -> None:
        """End of bounded test stream: advance watermark to infinity and flush."""
        self.watermark = datetime.max
        self._finalize_ready_windows()

    def run(self, events_by_arrival: list[SensorEvent], fail_at_source_index: int = 72) -> None:
        source_index = 0
        while source_index < len(events_by_arrival):
            if (not self.failure_injected) and source_index == fail_at_source_index:
                print(f"\n💥 模擬 TaskManager 失敗：source offset={source_index}")
                restored_index = self.restore_latest_checkpoint(failed_at_index=source_index)
                print(f"♻️  從 checkpoint offset={restored_index} 還原，replay {source_index - restored_index} 筆事件")
                source_index = restored_index
                self.failure_injected = True
                continue

            event = events_by_arrival[source_index]
            self.process_event(event)
            source_index += 1

            if source_index % self.checkpoint_interval_records == 0:
                self.checkpoint(source_index)

        self.close()


# ──────────────────────────────────────────────
# Part 5: Reporting helpers
# ──────────────────────────────────────────────


def fmt_time(value: datetime | str | None) -> str:
    if value is None:
        return "None"
    if isinstance(value, str):
        return value
    if value == datetime.max:
        return "end-of-stream"
    return value.strftime("%H:%M:%S")


def print_event_sample(events: list[SensorEvent]) -> None:
    print("\n📥 Arrival-order sample（可看到 event_time 與 arrival_time 不同）")
    print("event_id     machine  event_time  arrival_time  temp")
    for event in events[:10]:
        print(
            f"{event.event_id:<11} {event.machine_id:<7} "
            f"{event.event_time.strftime('%H:%M:%S')}    "
            f"{event.arrival_time.strftime('%H:%M:%S')}      "
            f"{event.temperature_c:>5.1f}°C"
        )


def print_alerts(title: str, results: Iterable[WindowResult]) -> None:
    alerts = [r for r in results if r.alert]
    print(f"\n🚨 {title}")
    if not alerts:
        print("No alerts")
        return
    print("machine  window_start  avg   max   count  finalized")
    for r in sorted(alerts, key=lambda x: (x.window_start, x.machine_id)):
        print(
            f"{r.machine_id:<7} {r.window_start.strftime('%H:%M')}         "
            f"{r.avg_temp:>5.1f} {r.max_temp:>5.1f} {r.event_count:>5}  "
            f"{fmt_time(r.finalized_at_watermark)}"
        )


def compare_batch_and_stream(
    batch: dict[tuple[str, datetime], WindowResult],
    stream_outputs: list[WindowResult],
    late_events: list[SensorEvent],
) -> None:
    stream = {r.key: r for r in stream_outputs}
    late_by_window: dict[tuple[str, datetime], int] = defaultdict(int)
    for event in late_events:
        late_by_window[(event.machine_id, floor_to_minute(event.event_time))] += 1

    changed = []
    for key, batch_result in batch.items():
        stream_result = stream.get(key)
        if stream_result is None:
            continue
        if late_by_window[key] or batch_result.event_count != stream_result.event_count:
            changed.append((batch_result, stream_result, late_by_window[key]))

    print("\n🔍 Batch final answer vs Stream finalized result（late events 會造成補償需求）")
    print("machine window  batch_count stream_count late_side_output  batch_avg stream_avg")
    for batch_result, stream_result, late_count in changed[:8]:
        print(
            f"{batch_result.machine_id:<7} {batch_result.window_start.strftime('%H:%M')}  "
            f"{batch_result.event_count:>11} {stream_result.event_count:>12} "
            f"{late_count:>16}  {batch_result.avg_temp:>8.2f} {stream_result.avg_temp:>9.2f}"
        )


# ──────────────────────────────────────────────
# Main validation
# ──────────────────────────────────────────────


def main() -> None:
    alert_threshold = 70.0
    events = generate_smt_events()
    batch = batch_report(events, alert_threshold_c=alert_threshold)

    print("🏭 ODM SMT Temperature Stream — MiniFlink Practice")
    print(f"Total source events: {len(events)}")
    print_event_sample(events)

    job = MiniFlinkJob(
        allowed_lateness_seconds=30,
        checkpoint_interval_records=25,
        alert_threshold_c=alert_threshold,
    )
    job.run(events, fail_at_source_index=72)

    # Exactly-once-ish validation for this toy model: no duplicated finalized windows.
    output_keys = [r.key for r in job.outputs]
    assert len(output_keys) == len(set(output_keys)), "duplicate finalized window outputs detected"
    assert job.failure_injected, "failure simulation did not run"
    assert job.replayed_records > 0, "source replay did not happen"
    assert job.outputs, "stream produced no finalized windows"
    assert job.late_events, "test data should contain late events"

    print("\n✅ Checkpoint history")
    print("offset  watermark  emitted_windows")
    for offset, watermark, output_count in job.checkpoint_history:
        print(f"{offset:>6}  {fmt_time(watermark):<9}  {output_count:>15}")

    print("\n📊 Stream job metrics")
    print(f"process attempts（含 replay）: {job.process_attempts}")
    print(f"replayed records: {job.replayed_records}")
    print(f"finalized windows: {len(job.outputs)}")
    print(f"late side-output events: {len(job.late_events)}")
    print(f"current keyed state entries: {len(job.state)}")

    print_alerts("Batch T+1 report alerts", batch.values())
    print_alerts("Streaming finalized alerts", job.outputs)
    compare_batch_and_stream(batch, job.outputs, job.late_events)

    print("\n🧠 Takeaway")
    print("- Batch 對 bounded dataset 算出最終答案，但只能等資料完整後輸出。")
    print("- Stream 對 unbounded events 持續更新，用 event time + watermark 在亂序下關窗。")
    print("- Checkpoint 把 source offset + operator state 綁在一起，失敗後 replay 不會重複輸出。")
    print("- Late events 不是小問題：SA 必須設計 side output / correction / allowed lateness 策略。")


if __name__ == "__main__":
    main()
