"""
Kafka Lesson 1 Practice: 同步 vs 非同步 ODM 事件流模擬
=======================================================
用 Python 原生 queue 模擬 Kafka 核心行為，對比同步 API 呼叫 vs 非同步事件驅動。

場景：ODM 工廠 MES 產生工單事件，通知 4 個下游系統。
"""

import time
import random
import hashlib
import threading
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any


# ──────────────────────────────────────────────
# Part 1: MiniKafka — 模擬 Kafka 核心機制
# ──────────────────────────────────────────────

@dataclass
class KafkaRecord:
    """模擬一個 Kafka Record"""
    key: str
    value: dict
    timestamp: float
    offset: int = 0
    partition: int = 0


class MiniTopic:
    """
    模擬 Kafka Topic：
    - 多個 Partition（按 key hash 分配）
    - Append-only log（消費後不刪除）
    - Consumer Group offset 追蹤
    """
    def __init__(self, name: str, num_partitions: int = 3):
        self.name = name
        self.num_partitions = num_partitions
        # 每個 partition 是一個 list（append-only log）
        self.partitions: list[list[KafkaRecord]] = [[] for _ in range(num_partitions)]
        # Consumer Group → {partition_id: committed_offset}
        self.consumer_offsets: dict[str, dict[int, int]] = {}
        self._lock = threading.Lock()

    def _get_partition(self, key: str) -> int:
        """按 key hash 決定 partition（模擬 Kafka 的 partitioner）"""
        return int(hashlib.md5(key.encode()).hexdigest(), 16) % self.num_partitions

    def produce(self, key: str, value: dict) -> KafkaRecord:
        """Producer 發送 event（thread-safe）"""
        with self._lock:
            partition_id = self._get_partition(key)
            offset = len(self.partitions[partition_id])
            record = KafkaRecord(
                key=key,
                value=value,
                timestamp=time.time(),
                offset=offset,
                partition=partition_id
            )
            self.partitions[partition_id].append(record)
            return record

    def consume(self, group_id: str, partition_id: int) -> list[KafkaRecord]:
        """Consumer 從指定 partition 讀取未消費的 records"""
        with self._lock:
            if group_id not in self.consumer_offsets:
                self.consumer_offsets[group_id] = {p: 0 for p in range(self.num_partitions)}

            current_offset = self.consumer_offsets[group_id].get(partition_id, 0)
            records = self.partitions[partition_id][current_offset:]
            return records

    def commit_offset(self, group_id: str, partition_id: int, offset: int):
        """Commit consumer offset（模擬 Kafka 的 offset commit）"""
        with self._lock:
            if group_id not in self.consumer_offsets:
                self.consumer_offsets[group_id] = {}
            self.consumer_offsets[group_id][partition_id] = offset

    def get_total_records(self) -> int:
        return sum(len(p) for p in self.partitions)


class MiniKafka:
    """模擬 Kafka Cluster"""
    def __init__(self):
        self.topics: dict[str, MiniTopic] = {}

    def create_topic(self, name: str, num_partitions: int = 3) -> MiniTopic:
        self.topics[name] = MiniTopic(name, num_partitions)
        return self.topics[name]

    def get_topic(self, name: str) -> MiniTopic:
        return self.topics[name]


# ──────────────────────────────────────────────
# Part 2: ODM 下游系統模擬
# ──────────────────────────────────────────────

class DownstreamSystem:
    """模擬一個下游系統（倉庫/產線/品質/ERP）"""
    def __init__(self, name: str, avg_latency_ms: float = 50, failure_rate: float = 0.05):
        self.name = name
        self.avg_latency_ms = avg_latency_ms
        self.failure_rate = failure_rate
        self.processed = 0
        self.failed = 0

    def process(self, event: dict) -> bool:
        """處理事件（模擬延遲和隨機失敗）"""
        # 模擬處理延遲
        latency = random.gauss(self.avg_latency_ms, self.avg_latency_ms * 0.3)
        time.sleep(max(0.0001, latency / 10000))  # 加速 10x（模擬用）

        # 模擬隨機失敗
        if random.random() < self.failure_rate:
            self.failed += 1
            raise ConnectionError(f"{self.name} 系統暫時不可用")

        self.processed += 1
        return True


# ──────────────────────────────────────────────
# Part 3: 同步模式 — 直接 API 呼叫
# ──────────────────────────────────────────────

def sync_process_workorder(event: dict, systems: list[DownstreamSystem]) -> dict:
    """
    同步模式：MES 依序呼叫 4 個下游系統。
    任一失敗 → 整個工單處理失敗。
    """
    start = time.time()
    results = {"success": True, "latency_ms": 0, "failed_system": None}

    for system in systems:
        try:
            system.process(event)
        except ConnectionError as e:
            results["success"] = False
            results["failed_system"] = system.name
            break  # 一個失敗就停止（模擬同步 cascading failure）

    results["latency_ms"] = (time.time() - start) * 1000
    return results


# ──────────────────────────────────────────────
# Part 4: 非同步模式 — 經過 MiniKafka
# ──────────────────────────────────────────────

def async_produce_workorder(topic: MiniTopic, event: dict) -> float:
    """
    非同步模式的 Producer 端：
    只需要把事件發到 Kafka，不需要等下游處理。
    """
    start = time.time()
    record = topic.produce(
        key=event["wo_id"],
        value=event
    )
    latency = (time.time() - start) * 1000
    return latency


def async_consumer_worker(
    topic: MiniTopic,
    group_id: str,
    partition_id: int,
    system: DownstreamSystem,
    results: dict
):
    """
    非同步模式的 Consumer 端：
    各自消費、各自處理、互不影響。
    失敗了可以重試（不影響其他 Consumer）。
    """
    records = topic.consume(group_id, partition_id)
    for record in records:
        max_retries = 3
        for attempt in range(max_retries):
            try:
                system.process(record.value)
                topic.commit_offset(group_id, partition_id, record.offset + 1)
                results["processed"] += 1
                break
            except ConnectionError:
                if attempt < max_retries - 1:
                    time.sleep(0.01)  # 短暫等待後重試
                    results["retries"] += 1
                else:
                    results["failed"] += 1


# ──────────────────────────────────────────────
# Part 5: 測試與對比
# ──────────────────────────────────────────────

def generate_workorder_events(count: int) -> list[dict]:
    """產生模擬的工單事件"""
    products = ["GB300-4U", "GB200-8U", "OCP-2U", "Storage-4U"]
    customers = ["Microsoft", "Google", "Meta", "Amazon"]
    events = []
    for i in range(count):
        events.append({
            "type": "work_order_created",
            "wo_id": f"WO-2026-0520-{i:04d}",
            "plant": f"WKS-F{random.randint(1, 5)}",
            "product": random.choice(products),
            "quantity": random.randint(10, 500),
            "customer": random.choice(customers),
        })
    return events


def run_sync_test(events: list[dict], failure_rate: float = 0.05) -> dict:
    """跑同步模式測試"""
    systems = [
        DownstreamSystem("倉庫扣料", avg_latency_ms=30, failure_rate=failure_rate),
        DownstreamSystem("產線排程", avg_latency_ms=50, failure_rate=failure_rate),
        DownstreamSystem("品質系統", avg_latency_ms=20, failure_rate=failure_rate),
        DownstreamSystem("ERP回報", avg_latency_ms=80, failure_rate=failure_rate),
    ]

    total_start = time.time()
    success_count = 0
    fail_count = 0
    latencies = []

    for event in events:
        result = sync_process_workorder(event, systems)
        latencies.append(result["latency_ms"])
        if result["success"]:
            success_count += 1
        else:
            fail_count += 1

    total_time = (time.time() - total_start) * 1000

    return {
        "mode": "同步（Sequential API Calls）",
        "total_events": len(events),
        "success": success_count,
        "failed": fail_count,
        "success_rate": f"{success_count / len(events) * 100:.1f}%",
        "avg_latency_ms": f"{sum(latencies) / len(latencies):.1f}",
        "p99_latency_ms": f"{sorted(latencies)[int(len(latencies) * 0.99)]:.1f}",
        "total_time_ms": f"{total_time:.1f}",
        "throughput_events_per_sec": f"{len(events) / total_time * 1000:.1f}",
    }


def run_async_test(events: list[dict], failure_rate: float = 0.05) -> dict:
    """跑非同步模式測試"""
    kafka = MiniKafka()
    topic = kafka.create_topic("odm.workorders", num_partitions=4)

    # --- Producer Phase ---
    produce_start = time.time()
    produce_latencies = []
    for event in events:
        lat = async_produce_workorder(topic, event)
        produce_latencies.append(lat)
    produce_time = (time.time() - produce_start) * 1000

    # --- Consumer Phase ---
    # 4 個 Consumer Group，每個代表一個下游系統
    consumer_groups = [
        ("warehouse-group", DownstreamSystem("倉庫扣料", avg_latency_ms=30, failure_rate=failure_rate)),
        ("production-group", DownstreamSystem("產線排程", avg_latency_ms=50, failure_rate=failure_rate)),
        ("quality-group", DownstreamSystem("品質系統", avg_latency_ms=20, failure_rate=failure_rate)),
        ("erp-group", DownstreamSystem("ERP回報", avg_latency_ms=80, failure_rate=failure_rate)),
    ]

    consumer_start = time.time()
    all_results = []
    threads = []

    for group_id, system in consumer_groups:
        group_result = {"processed": 0, "failed": 0, "retries": 0}
        all_results.append((group_id, group_result))
        for partition_id in range(topic.num_partitions):
            t = threading.Thread(
                target=async_consumer_worker,
                args=(topic, group_id, partition_id, system, group_result)
            )
            threads.append(t)
            t.start()

    for t in threads:
        t.join()

    consumer_time = (time.time() - consumer_start) * 1000

    # 計算每個 Consumer Group 的處理結果
    total_processed = sum(r["processed"] for _, r in all_results)
    total_failed = sum(r["failed"] for _, r in all_results)
    total_retries = sum(r["retries"] for _, r in all_results)

    return {
        "mode": "非同步（Event-Driven via MiniKafka）",
        "total_events": len(events),
        "kafka_records": topic.get_total_records(),
        "producer_total_ms": f"{produce_time:.1f}",
        "producer_avg_latency_ms": f"{sum(produce_latencies) / len(produce_latencies):.3f}",
        "consumer_total_ms": f"{consumer_time:.1f}",
        "consumer_groups": len(consumer_groups),
        "total_processed": total_processed,
        "total_failed": total_failed,
        "total_retries": total_retries,
        "per_group_detail": {
            gid: {
                "processed": r["processed"],
                "failed": r["failed"],
                "retries": r["retries"]
            }
            for gid, r in all_results
        },
        "producer_throughput_eps": f"{len(events) / produce_time * 1000:.1f}",
    }


# ──────────────────────────────────────────────
# Part 6: 執行與報告
# ──────────────────────────────────────────────

def main():
    print("=" * 70)
    print("  Kafka Lesson 1: 同步 vs 非同步 ODM 事件流模擬")
    print("  場景：ODM 工廠 MES 工單事件 → 4 個下游系統")
    print("=" * 70)

    NUM_EVENTS = 100
    FAILURE_RATE = 0.08  # 8% 失敗率（模擬真實系統的不穩定）

    events = generate_workorder_events(NUM_EVENTS)
    print(f"\n📋 產生 {NUM_EVENTS} 個工單事件，下游失敗率 {FAILURE_RATE*100}%\n")

    # ── Test 1: 同步模式 ──
    print("─" * 50)
    print("🔄 Test 1: 同步模式（Sequential API Calls）")
    print("─" * 50)
    sync_result = run_sync_test(events, FAILURE_RATE)
    for k, v in sync_result.items():
        print(f"  {k}: {v}")

    # ── Test 2: 非同步模式 ──
    print()
    print("─" * 50)
    print("⚡ Test 2: 非同步模式（Event-Driven via MiniKafka）")
    print("─" * 50)
    async_result = run_async_test(events, FAILURE_RATE)
    for k, v in async_result.items():
        if isinstance(v, dict):
            print(f"  {k}:")
            for gid, detail in v.items():
                print(f"    {gid}: {detail}")
        else:
            print(f"  {k}: {v}")

    # ── 對比分析 ──
    print()
    print("=" * 70)
    print("📊 對比分析")
    print("=" * 70)

    sync_success = float(sync_result["success_rate"].rstrip("%"))
    # 非同步模式的 Producer 成功率是 100%（只要 Kafka 可用）
    async_producer_success = 100.0

    print(f"""
  ┌─────────────────────┬──────────────────┬──────────────────────┐
  │ 維度                │ 同步模式         │ 非同步模式           │
  ├─────────────────────┼──────────────────┼──────────────────────┤
  │ Producer 成功率     │ {sync_success:>14.1f}% │ {async_producer_success:>18.1f}% │
  │ Producer 延遲       │ {sync_result['avg_latency_ms']:>13s}ms │ {async_result['producer_avg_latency_ms']:>17s}ms │
  │ Producer 吞吐量     │ {sync_result['throughput_events_per_sec']:>12s}/s │ {async_result['producer_throughput_eps']:>16s}/s │
  │ 故障隔離            │         ❌ 無    │           ✅ 有      │
  │ Consumer 可重試     │         ❌ 否    │           ✅ 是      │
  │ 事件可回放          │         ❌ 否    │           ✅ 是      │
  └─────────────────────┴──────────────────┴──────────────────────┘
    """)

    # ── 核心洞察 ──
    print("💡 核心洞察：")
    print("  1. 同步模式的成功率 ≈ (1 - failure_rate)^4，4 個系統任一失敗就全失敗")
    print(f"     理論成功率: {(1-FAILURE_RATE)**4*100:.1f}%，實測: {sync_success:.1f}%")
    print("  2. 非同步模式 Producer 永遠成功（只要 Kafka 可用），Consumer 失敗可獨立重試")
    print("  3. 非同步模式的 Producer 延遲極低（只是 append to log），不受下游延遲影響")
    print("  4. 但非同步帶來 trade-off：最終一致性、需要冪等 Consumer、debug 更複雜")

    # ── Partition 分佈展示 ──
    print()
    print("─" * 50)
    print("📦 MiniKafka Partition 分佈（模擬 key-based partitioning）")
    print("─" * 50)
    kafka = MiniKafka()
    topic = kafka.create_topic("demo.partition.distribution", num_partitions=4)
    for event in events[:20]:
        topic.produce(event["wo_id"], event)

    for pid in range(topic.num_partitions):
        records = topic.partitions[pid]
        keys = [r.key for r in records]
        print(f"  Partition {pid}: {len(records)} records")
        if keys:
            print(f"    keys sample: {keys[:5]}")

    print()
    print("  → 相同 key 的事件一定在同一 Partition（保證局部順序性）")
    print("  → Partition 數量 = 最大平行 Consumer 數量")

    print()
    print("✅ 練習完成！")
    print("   核心收穫：消息佇列的價值不在「速度」，在「解耦 + 故障隔離 + 可擴展性」")


if __name__ == "__main__":
    main()
