"""
XCom 限制與最佳實踐 - 實作練習
場景：緯穎每日採購資料 Pipeline（從 SAP MM 到 Delta Lake）
日期：2026-04-28

學習重點：
1. ❌ 反模式：傳 DataFrame 給 XCom（metadata DB 炸掉）
2. ✅ 最佳實踐：傳路徑 + metadata（< 1 KB）
3. 驗證 XCom 大小
"""

import json
import sys
from datetime import datetime

# ============================================================
# Part 1: 識別反模式（模擬版，不需要真正跑 Airflow）
# ============================================================

print("=" * 60)
print("Part 1: 反模式 vs 最佳實踐的 XCom 大小比較")
print("=" * 60)

# ❌ 反模式：假設有 50 萬筆採購訂單資料
# 實際上這樣的 XCom 會讓 metadata DB 膨脹爆炸
anti_pattern_xcom = {
    "po_data": [
        {
            "po_number": f"PO-{i:08d}",
            "vendor_code": f"V{i % 1000:04d}",
            "material": f"MAT{i % 5000:05d}",
            "quantity": i * 10,
            "unit_price": round(i * 0.15, 2),
            "delivery_date": "2026-04-30",
            "status": "OPEN"
        }
        for i in range(10000)  # 只用 1 萬筆模擬（真實是 50 萬）
    ]
}

anti_pattern_size = sys.getsizeof(json.dumps(anti_pattern_xcom, default=str))
print(f"\n❌ 反模式 XCom 大小（1 萬筆模擬）：{anti_pattern_size:,} bytes = {anti_pattern_size / 1024:.1f} KB")
print(f"   推算 50 萬筆：{anti_pattern_size / 1024 * 50:.0f} KB = {anti_pattern_size / 1024 / 1024 * 50:.1f} MB")
print(f"   PostgreSQL 理論上限 1 GB，但這會嚴重拖慢 scheduler！")
print(f"   MySQL 上限 64 KB，直接 ERROR！")

# ✅ 最佳實踐：只傳 metadata
best_practice_xcom = {
    "bronze_path": "abfs://datalake/bronze/sap-po/2026-04-28/",
    "record_count": 500000,
    "extract_date": "2026-04-28",
    "source_system": "SAP_MM",
    "extract_duration_sec": 45.2,
    "checksum": "sha256:abc123..."
}

best_practice_size = sys.getsizeof(json.dumps(best_practice_xcom))
print(f"\n✅ 最佳實踐 XCom 大小（路徑 + metadata）：{best_practice_size} bytes")
print(f"   無論 50 萬筆還是 5000 萬筆，XCom 永遠是這個大小！")
print(f"   節省了 {anti_pattern_size / best_practice_size:.0f}x 的 metadata DB 空間")


# ============================================================
# Part 2: 模擬 P2P Pipeline 的 XCom 資料流
# ============================================================

print("\n" + "=" * 60)
print("Part 2: P2P Pipeline 的 XCom 流向模擬")
print("=" * 60)

def simulate_extract_from_sap(execution_date: str) -> dict:
    """模擬：從 SAP 抽取採購訂單資料"""
    # 實際工作：用 JDBC 連 SAP，寫到 Delta Lake bronze layer
    # 這裡只模擬最後 return 的 XCom 值

    bronze_path = f"abfs://datalake/bronze/sap-po/{execution_date}/"
    record_count = 500_213  # 50 萬筆

    xcom_value = {
        "bronze_path": bronze_path,
        "record_count": record_count,
        "extract_date": execution_date,
        "source_system": "SAP_MM",
    }
    return xcom_value

def simulate_validate_and_clean(extract_meta: dict) -> dict:
    """模擬：資料品質驗證 + 轉換到 silver layer"""
    # 實際工作：Great Expectations 驗證，PySpark 清洗轉換

    silver_path = extract_meta["bronze_path"].replace("bronze", "silver")

    # 假設發現 213 筆有空值的 row（正常情況）
    null_rows = 213
    valid_count = extract_meta["record_count"] - null_rows

    xcom_value = {
        "silver_path": silver_path,
        "valid_count": valid_count,
        "null_count": null_rows,
        "null_rate": round(null_rows / extract_meta["record_count"] * 100, 3),
        "validation_passed": null_rows < extract_meta["record_count"] * 0.01,  # 允許 1% 空值
        "extract_date": extract_meta["extract_date"]
    }
    return xcom_value

def simulate_check_shortage(transform_meta: dict) -> dict:
    """模擬：計算採購缺口"""
    # 實際工作：讀 silver，JOIN MRP 需求表，計算 PO 量 vs 需求量

    if not transform_meta["validation_passed"]:
        return {
            "shortage_count": 0,
            "critical_parts": [],
            "skip_alert": True,
            "reason": "Validation failed upstream"
        }

    # 模擬缺料結果
    critical_parts = ["P123-MEM-32G", "P456-CPU-AMD", "P789-NIC-100G"]

    xcom_value = {
        "shortage_count": len(critical_parts),
        "critical_parts": critical_parts,  # 只傳 part number 清單，不傳整個缺料明細
        "shortage_date": transform_meta["extract_date"],
        "skip_alert": False
    }
    return xcom_value

# 執行模擬 pipeline
today = "2026-04-28"

print(f"\n🚀 執行日期：{today}")
print("\n── Step 1: extract_from_sap ──")
extract_meta = simulate_extract_from_sap(today)
size_1 = sys.getsizeof(json.dumps(extract_meta))
print(f"   XCom 值：{json.dumps(extract_meta, indent=2, ensure_ascii=False)}")
print(f"   XCom 大小：{size_1} bytes ✅")

print("\n── Step 2: validate_and_clean ──")
transform_meta = simulate_validate_and_clean(extract_meta)
size_2 = sys.getsizeof(json.dumps(transform_meta))
print(f"   XCom 值：{json.dumps(transform_meta, indent=2, ensure_ascii=False)}")
print(f"   XCom 大小：{size_2} bytes ✅")

print("\n── Step 3: check_shortage ──")
shortage_meta = simulate_check_shortage(transform_meta)
size_3 = sys.getsizeof(json.dumps(shortage_meta))
print(f"   XCom 值：{json.dumps(shortage_meta, indent=2, ensure_ascii=False)}")
print(f"   XCom 大小：{size_3} bytes ✅")

print("\n── Step 4: alert_scm ──")
if shortage_meta["skip_alert"]:
    print("   ⏭️ 跳過通知（上游驗證失敗）")
elif shortage_meta["shortage_count"] > 0:
    parts = ", ".join(shortage_meta["critical_parts"])
    print(f"   ⚠️ 缺料通知：{shortage_meta['shortage_count']} 個缺料")
    print(f"   Parts: {parts}")
    print(f"   實際：發送 Teams 訊息給 SCM 團隊")

# ============================================================
# Part 3: 驗證總結
# ============================================================

print("\n" + "=" * 60)
print("Part 3: XCom 大小驗證總結")
print("=" * 60)

total_xcom_bytes = size_1 + size_2 + size_3
print(f"\n{'Task':<30} {'XCom 大小':>15}")
print("-" * 47)
print(f"{'extract_from_sap':<30} {size_1:>12} bytes")
print(f"{'validate_and_clean':<30} {size_2:>12} bytes")
print(f"{'check_shortage':<30} {size_3:>12} bytes")
print("-" * 47)
print(f"{'總計（整個 DAG run）':<30} {total_xcom_bytes:>12} bytes")
print(f"\n✅ 所有 XCom 加總 = {total_xcom_bytes} bytes = {total_xcom_bytes / 1024:.2f} KB")
print(f"   MySQL 上限 64 KB：{'✅ 安全' if total_xcom_bytes < 65536 else '❌ 超限！'}")
print(f"   對比反模式（1萬筆模擬）：{anti_pattern_size / 1024:.1f} KB")
print(f"   節省比例：{(anti_pattern_size - total_xcom_bytes) / anti_pattern_size * 100:.1f}%")

print("\n📌 核心結論：")
print("   XCom 傳路徑 + metadata，無論資料量多大，都是固定小尺寸")
print("   50 萬筆 vs 5000 萬筆，XCom 大小完全一樣")
print("   這才是 production-grade Airflow 的設計方式")
