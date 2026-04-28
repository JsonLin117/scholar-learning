"""
Airflow Sensor 概念驗證（不需要 Airflow 安裝）
展示 poke vs reschedule mode 的資源效率差異
以及 Sensor 的核心等待邏輯
"""

import time
import random
from datetime import datetime, timedelta


# ──────────────────────────────────────────────────────────────────────────────
# 模擬 Sensor 的等待邏輯
# ──────────────────────────────────────────────────────────────────────────────

class MockFileSensor:
    """模擬 FileSensor 的等待行為（不需要 Airflow）"""
    
    def __init__(self, filepath, poke_interval=5, timeout=30, mode="reschedule"):
        self.filepath = filepath
        self.poke_interval = poke_interval
        self.timeout = timeout
        self.mode = mode
        self._available_slots = 10
        self._used_slots = 0
    
    def _check_file_exists(self, filepath) -> bool:
        """模擬檔案存在性檢查（假設 5 次後檔案出現）"""
        # 為了 demo，隨機決定檔案是否出現
        return random.random() > 0.6  # 40% 機率找到
    
    def poke(self, attempt: int) -> bool:
        """執行一次 poke 檢查"""
        exists = self._check_file_exists(self.filepath)
        status = "✅ 找到!" if exists else "❌ 不存在，繼續等..."
        print(f"  [Poke #{attempt}] {self.filepath} → {status}")
        return exists
    
    def run_demo(self):
        """模擬整個 Sensor 運行過程"""
        print(f"\n{'='*60}")
        print(f"FileSensor: {self.filepath}")
        print(f"Mode: {self.mode} | Poke Interval: {self.poke_interval}s | Timeout: {self.timeout}s")
        print(f"{'='*60}")
        
        start_time = datetime.now()
        attempt = 1
        
        while True:
            elapsed = (datetime.now() - start_time).seconds
            
            if elapsed > self.timeout:
                print(f"\n⏰ TIMEOUT 後 {elapsed}s！DAG FAIL")
                return False
            
            # 模式差異說明
            if self.mode == "poke":
                print(f"\n[{elapsed}s] Mode=poke: 持續佔住 1 個 worker slot...")
            else:
                print(f"\n[{elapsed}s] Mode=reschedule: 短暫使用 slot 做 poke，然後釋放...")
            
            if self.poke(attempt):
                print(f"\n🎉 檔案找到！總等待 {elapsed}s，共 {attempt} 次 poke")
                return True
            
            if self.mode == "reschedule":
                print(f"  → 釋放 slot，{self.poke_interval}s 後再回來")
            
            time.sleep(0.5)  # Demo 加速（實際是 poke_interval 秒）
            attempt += 1


def demo_resource_comparison():
    """對比 poke vs reschedule 的資源使用"""
    print("\n" + "="*70)
    print("📊 Sensor Mode 資源比較（等待 2 小時，10 個 worker slots 池）")
    print("="*70)
    
    wait_hours = 2
    total_slots = 10
    concurrent_sensors = 10  # 假設 10 個 Sensor 同時在等
    poke_interval_sec = 300   # 5 分鐘
    poke_duration_sec = 5     # poke 本身耗時 5 秒
    
    # poke 模式
    poke_slots_occupied = concurrent_sensors  # 整個等待期間佔住
    poke_available = max(0, total_slots - poke_slots_occupied)
    
    print(f"\n❌ poke mode（{concurrent_sensors} 個 Sensor 各等 {wait_hours}h）:")
    print(f"   Worker slots 佔用: {poke_slots_occupied}/{total_slots}")
    print(f"   剩餘給正常任務: {poke_available}/{total_slots}")
    print(f"   → 正常任務幾乎無法執行！")
    
    # reschedule 模式
    # 只在 poke 時佔 slot，平均佔用 = poke_duration / interval
    occupancy_ratio = poke_duration_sec / poke_interval_sec
    avg_slots_used = concurrent_sensors * occupancy_ratio
    avg_available = total_slots - avg_slots_used
    
    print(f"\n✅ reschedule mode（{concurrent_sensors} 個 Sensor，每 {poke_interval_sec}s 查 {poke_duration_sec}s）:")
    print(f"   平均 slot 佔用: {avg_slots_used:.2f}/{total_slots} (每次只佔 {poke_duration_sec}s/{poke_interval_sec}s)")
    print(f"   剩餘給正常任務: {avg_available:.1f}/{total_slots}")
    print(f"   → 正常任務可以正常執行！")
    
    savings = (1 - occupancy_ratio) * 100
    print(f"\n💰 節省 slot 資源: {savings:.0f}%")
    
    print(f"\n{'='*70}")
    print("📋 選型口訣：")
    print(f"  等待 < 5 分鐘   → poke（簡單，slot 影響小）")
    print(f"  等待 > 5 分鐘   → reschedule（⭐ 推薦，節省資源）")
    print(f"  資源極度緊張   → defer（需要 Triggerer 組件，完全不佔 slot）")


def demo_external_task_sensor_logic():
    """展示 ExternalTaskSensor 的 execution_date 對齊問題"""
    print("\n" + "="*70)
    print("🔗 ExternalTaskSensor execution_date 對齊邏輯")
    print("="*70)
    
    # 場景：DAG 1 每天跑，DAG 2 每週一跑，需要等 DAG 1 本週所有執行
    dag1_runs = [
        datetime(2026, 4, 20) + timedelta(days=i) for i in range(7)
    ]  # 週一到週日
    
    dag2_execution_date = datetime(2026, 4, 27)  # 週日週總結 DAG
    
    print(f"\nDAG 2 execution_date: {dag2_execution_date.strftime('%Y-%m-%d (%A)')}")
    print(f"需要等待 DAG 1 的哪些執行？")
    print()
    
    for run in dag1_runs:
        print(f"  DAG 1 execution_date: {run.strftime('%Y-%m-%d (%A)')}")
    
    print(f"\n如果用 execution_delta=timedelta(days=6):")
    print(f"  只能等到 {(dag2_execution_date - timedelta(days=6)).strftime('%Y-%m-%d')} 那天的 DAG 1")
    print(f"  → 只等到週一那天，其餘 6 天不管！❌")
    
    print(f"\n正確做法：用 execution_date_fn 動態計算，或改用 Assets（Airflow 3.2+）")


if __name__ == "__main__":
    # 1. 展示資源比較
    demo_resource_comparison()
    
    # 2. 展示 ExternalTaskSensor 邏輯
    demo_external_task_sensor_logic()
    
    # 3. 模擬 FileSensor 運行
    print(f"\n{'='*70}")
    print("🔄 FileSensor 運行模擬（reschedule mode）")
    print("="*70)
    sensor = MockFileSensor(
        filepath="/mnt/azure_blob/erp/purchase_orders/2026-04-29/po_data.parquet",
        poke_interval=5,
        timeout=60,
        mode="reschedule"
    )
    sensor.run_demo()
