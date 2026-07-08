"""
ODM 供應鏈平台選型模擬器：AKS vs ACA vs Azure Functions

場景：緯穎在 Azure 上建置「供應商 Portal + 即時缺料預警」平台。
- 供應商 Portal API：同步、中等流量
- MRP 缺料通知：事件驅動、Service Bus 訊息（含 DG 貨物需標記 dg_classification）
- ML 需求預測：GPU 推論

用今天學到的 Decision Tree 自動判斷選型，並模擬三種服務的月成本。
"""

from dataclasses import dataclass, field
from enum import Enum


class ComputeService(Enum):
    AKS = "Azure Kubernetes Service"
    ACA = "Azure Container Apps"
    FUNCTIONS = "Azure Functions"


class DGClassification(Enum):
    """IATA DGR Packing Instructions for lithium batteries in servers."""
    NONE = "no_lithium_battery"
    PI965 = "standalone_battery"          # 獨立包裝電池
    PI966 = "packed_with_equipment"       # 電池同箱但未安裝
    PI967 = "installed_in_equipment"      # 電池已安裝在設備內（如 RAID BBU）


@dataclass
class Workload:
    name: str
    needs_k8s_api: bool = False       # 需要 kubectl/Helm/Operator/CRD
    needs_gpu: bool = False           # ML inference / CUDA
    is_event_driven: bool = False     # 事件驅動、短生命週期、無狀態
    monthly_requests: int = 0         # 每月請求/事件數
    avg_duration_sec: float = 0.5     # 平均執行時間（秒）
    idle_hours_per_day: float = 0.0   # 每天閒置時數（用於 scale-to-zero 效益估算）


@dataclass
class DecisionResult:
    workload_name: str
    service: ComputeService
    reason: str


def decide_compute_service(w: Workload) -> DecisionResult:
    """
    Decision Tree（依序判斷，第一個 yes 就是起點）：
    1. 需要 Kubernetes API/CRD/Operator？ -> AKS
    2. 需要 GPU？ -> AKS
    3. 事件驅動、短生命週期、無狀態？ -> Azure Functions
    4. 一般容器化服務，需要 scale-to-zero + managed ingress，但不需要 K8s API -> ACA
    """
    if w.needs_k8s_api:
        return DecisionResult(w.name, ComputeService.AKS, "需要 Kubernetes API/CRD/Operator，唯一支援選項")
    if w.needs_gpu:
        return DecisionResult(w.name, ComputeService.AKS, "GPU workload，ACA/Functions 皆不支援 GPU")
    if w.is_event_driven and w.avg_duration_sec < 600:
        return DecisionResult(w.name, ComputeService.FUNCTIONS, "事件驅動、短生命週期（<10分鐘），Functions 最適合")
    return DecisionResult(w.name, ComputeService.ACA, "一般容器化服務，不需要 K8s API，ACA 是 2026 年新預設值")


# ---------------------------------------------------------------------------
# 成本模擬（依筆記中的比較矩陣簡化估算，單位：EUR/月）
# ---------------------------------------------------------------------------

AKS_BASE_MONTHLY_COST = 150.0       # 最小節點池成本下限（筆記：€150+/月）
AKS_MGMT_FEE_MONTHLY = 73.0         # cluster management fee（€0.10/hour * 730h）
ACA_COST_PER_MILLION_REQ = 20.0     # 簡化估算：每百萬請求 vCPU-sec 費用
FUNCTIONS_COST_PER_MILLION_REQ = 15.0  # Consumption plan，比 ACA 略便宜（更細粒度 scale-to-zero）
ACA_IDLE_COST = 0.0                 # scale-to-zero
FUNCTIONS_IDLE_COST = 0.0           # scale-to-zero


def estimate_monthly_cost(w: Workload, service: ComputeService) -> float:
    if service == ComputeService.AKS:
        return AKS_BASE_MONTHLY_COST + AKS_MGMT_FEE_MONTHLY
    if service == ComputeService.ACA:
        req_millions = w.monthly_requests / 1_000_000
        return req_millions * ACA_COST_PER_MILLION_REQ + ACA_IDLE_COST
    if service == ComputeService.FUNCTIONS:
        req_millions = w.monthly_requests / 1_000_000
        return req_millions * FUNCTIONS_COST_PER_MILLION_REQ + FUNCTIONS_IDLE_COST
    raise ValueError(f"Unknown service: {service}")


# ---------------------------------------------------------------------------
# DG 缺料通知事件：dg_classification 標記如何影響 Buffer Lead Time
# ---------------------------------------------------------------------------

# 呼應 SC Daily #51：含 DG 貨物的訂單需要更長的緩衝，因為可能被迫從空運轉海運或
# 從 PAX 客機轉 CAO 純貨機（班次少、前置期更長）
DG_BUFFER_LEAD_TIME_DAYS = {
    DGClassification.NONE: 0,
    DGClassification.PI967: 1,   # 已安裝電池，分類相對單純，緩衝最少
    DGClassification.PI966: 2,   # 同箱未安裝，文件與標籤要求更多
    DGClassification.PI965: 3,   # 獨立包裝大量電池，最可能被迫轉 CAO，緩衝最長
}


@dataclass
class ShortageEvent:
    order_id: str
    material: str
    dg_classification: DGClassification
    base_lead_time_days: int


def compute_adjusted_lead_time(event: ShortageEvent) -> int:
    """把 DG buffer 加進基礎 Lead Time，模擬交期預警系統的 fact_order_fulfillment 欄位邏輯。"""
    buffer_days = DG_BUFFER_LEAD_TIME_DAYS[event.dg_classification]
    return event.base_lead_time_days + buffer_days


def main() -> None:
    print("=" * 70)
    print("ODM 供應鏈平台選型模擬：AKS vs ACA vs Azure Functions")
    print("=" * 70)

    workloads = [
        Workload(
            name="供應商 Portal API",
            needs_k8s_api=False,
            needs_gpu=False,
            is_event_driven=False,
            monthly_requests=3_000_000,
            idle_hours_per_day=10.0,
        ),
        Workload(
            name="MRP 缺料通知（含 DG 標記）",
            needs_k8s_api=False,
            needs_gpu=False,
            is_event_driven=True,
            monthly_requests=50_000,
            avg_duration_sec=2.0,
        ),
        Workload(
            name="ML 需求預測模型",
            needs_k8s_api=False,
            needs_gpu=True,
            is_event_driven=False,
            monthly_requests=100_000,
        ),
    ]

    print("\n--- 1. Decision Tree 選型結果 ---")
    results = []
    for w in workloads:
        r = decide_compute_service(w)
        results.append(r)
        cost = estimate_monthly_cost(w, r.service)
        print(f"[{w.name}] -> {r.service.value}")
        print(f"  理由：{r.reason}")
        print(f"  預估月成本：€{cost:.2f}")

    # ---- 斷言驗證 Decision Tree 邏輯 ----
    assert results[0].service == ComputeService.ACA, "供應商 Portal API 應選 ACA（無 K8s API/GPU 需求，非事件驅動）"
    assert results[1].service == ComputeService.FUNCTIONS, "MRP 缺料通知應選 Functions（事件驅動、短生命週期）"
    assert results[2].service == ComputeService.AKS, "ML 需求預測應選 AKS（GPU workload，唯一支援選項）"
    print("\n✅ Decision Tree 邏輯驗證通過：3 個工作負載分別對應 ACA / Functions / AKS")

    print("\n--- 2. 成本對比驗證：低流量 API 若誤用 AKS 會貴多少 ---")
    portal_workload = workloads[0]
    aca_cost = estimate_monthly_cost(portal_workload, ComputeService.ACA)
    aks_cost = estimate_monthly_cost(portal_workload, ComputeService.AKS)
    print(f"供應商 Portal API 若用 ACA：€{aca_cost:.2f}/月")
    print(f"供應商 Portal API 若誤用 AKS：€{aks_cost:.2f}/月（含 management fee，尚未計入隱藏維運成本）")
    assert aca_cost < aks_cost, "驗證筆記結論：低流量、無 K8s API 需求的工作負載，ACA 應比 AKS 便宜"
    savings_pct = (1 - aca_cost / aks_cost) * 100
    print(f"✅ ACA 比 AKS 省 {savings_pct:.1f}%，驗證「ACA 是 2026 年新預設值」的成本假設成立")

    print("\n--- 3. DG 缺料通知事件：dg_classification 影響 Buffer Lead Time ---")
    shortage_events = [
        ShortageEvent("PO-2026-0701", "RAID_Card_BBU", DGClassification.PI967, base_lead_time_days=5),
        ShortageEvent("PO-2026-0702", "UPS_Backup_Battery", DGClassification.PI966, base_lead_time_days=5),
        ShortageEvent("PO-2026-0703", "Spare_Li-ion_Cells_Bulk", DGClassification.PI965, base_lead_time_days=5),
        ShortageEvent("PO-2026-0704", "Server_Chassis", DGClassification.NONE, base_lead_time_days=5),
    ]

    adjusted = []
    for e in shortage_events:
        lt = compute_adjusted_lead_time(e)
        adjusted.append((e, lt))
        print(f"[{e.order_id}] {e.material} | DG={e.dg_classification.value} | "
              f"基礎LT={e.base_lead_time_days}天 -> 調整後LT={lt}天")

    # ---- 斷言驗證：DG 分類越複雜（PI965 > PI966 > PI967 > NONE），緩衝天數越長 ----
    lt_map = {e.dg_classification: lt for e, lt in adjusted}
    assert lt_map[DGClassification.NONE] < lt_map[DGClassification.PI967], "無 DG 應比 PI967 緩衝短"
    assert lt_map[DGClassification.PI967] < lt_map[DGClassification.PI966], "PI967（已安裝）應比 PI966（同箱未安裝）緩衝短"
    assert lt_map[DGClassification.PI966] < lt_map[DGClassification.PI965], "PI966 應比 PI965（大量獨立包裝）緩衝短"
    print("\n✅ DG Buffer Lead Time 遞增關係驗證通過：NONE < PI967 < PI966 < PI965")

    print("\n" + "=" * 70)
    print("全部驗證通過：Decision Tree 選型邏輯 + ACA 成本優勢 + DG Buffer 遞增關係")
    print("=" * 70)


if __name__ == "__main__":
    main()
