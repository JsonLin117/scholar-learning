"""
dbt Sources 實作練習 — ODM 供應鏈多系統 Source 管理
2026-07-02 | Scholar Practice

Demo 1: sources.yml 解析器（YAML → Source Registry）
Demo 2: source() 編譯器（source('sap', 'matdoc') → full table path）
Demo 3: Freshness Checker（loaded_at_field → warn/error SLA 判斷）
Demo 4: DAG Lineage Tracker（source → staging → mart 血緣）
Demo 5: ECO 延遲場景（SAP 資料延遲 18h → freshness error）
Demo 6: Multi-system Source Architecture（SAP/MES/WMS 統一管理）
"""

from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Optional
import json


# ===== Data Models =====

@dataclass
class FreshnessConfig:
    """Freshness SLA 配置"""
    warn_after_hours: Optional[float] = None
    error_after_hours: Optional[float] = None
    loaded_at_field: Optional[str] = None
    freshness_filter: Optional[str] = None

    def is_enabled(self) -> bool:
        return self.warn_after_hours is not None or self.error_after_hours is not None


@dataclass
class ColumnDef:
    """Column 定義"""
    name: str
    description: str = ""
    data_tests: list = field(default_factory=list)


@dataclass
class SourceTable:
    """Source table 定義"""
    name: str
    identifier: Optional[str] = None  # 實際表名（可不同於 name）
    description: str = ""
    freshness: Optional[FreshnessConfig] = None
    columns: list = field(default_factory=list)

    def get_identifier(self) -> str:
        return self.identifier or self.name


@dataclass
class Source:
    """Source 定義（一組 tables）"""
    name: str
    database: Optional[str] = None
    schema_name: Optional[str] = None  # 預設 = name
    description: str = ""
    freshness: Optional[FreshnessConfig] = None  # source-level 預設
    meta: dict = field(default_factory=dict)
    tags: list = field(default_factory=list)
    tables: list = field(default_factory=list)

    def get_schema(self) -> str:
        return self.schema_name or self.name


# ===== Source Registry =====

class SourceRegistry:
    """dbt sources.yml 解析與管理"""

    def __init__(self, target_database: str = "analytics"):
        self.sources: dict[str, Source] = {}
        self.target_database = target_database

    def register(self, source: Source):
        self.sources[source.name] = source

    def resolve_table(self, source_name: str, table_name: str) -> str:
        """
        source('sap', 'matdoc') → 'raw.sap_mm.MATDOC_CDC'
        核心邏輯：database.schema.identifier
        """
        if source_name not in self.sources:
            raise ValueError(f"Source '{source_name}' not declared. "
                           f"Available: {list(self.sources.keys())}")

        source = self.sources[source_name]
        table = next((t for t in source.tables if t.name == table_name), None)
        if table is None:
            table_names = [t.name for t in source.tables]
            raise ValueError(f"Table '{table_name}' not in source '{source_name}'. "
                           f"Available: {table_names}")

        db = source.database or self.target_database
        schema = source.get_schema()
        identifier = table.get_identifier()

        return f"{db}.{schema}.{identifier}"

    def get_effective_freshness(self, source_name: str, table_name: str) -> Optional[FreshnessConfig]:
        """
        取得 table 的有效 freshness 配置
        層級：table-level > source-level（繼承覆寫）
        """
        source = self.sources[source_name]
        table = next((t for t in source.tables if t.name == table_name), None)
        if table is None:
            return None

        # table-level 明確設為 None → 不檢查
        if table.freshness is not None and not table.freshness.is_enabled():
            return None

        # table-level 有設定 → 用 table 的（可繼承 source 的 loaded_at_field）
        if table.freshness is not None and table.freshness.is_enabled():
            result = FreshnessConfig(
                warn_after_hours=table.freshness.warn_after_hours,
                error_after_hours=table.freshness.error_after_hours,
                loaded_at_field=table.freshness.loaded_at_field or
                               (source.freshness.loaded_at_field if source.freshness else None),
                freshness_filter=table.freshness.freshness_filter
            )
            return result

        # 繼承 source-level
        return source.freshness

    def generate_freshness_query(self, source_name: str, table_name: str) -> Optional[str]:
        """生成 freshness check SQL"""
        freshness = self.get_effective_freshness(source_name, table_name)
        if freshness is None or not freshness.is_enabled():
            return None

        full_path = self.resolve_table(source_name, table_name)
        loaded_field = freshness.loaded_at_field or "_etl_loaded_at"

        query = f"""SELECT
    MAX({loaded_field}) AS max_loaded_at,
    CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()) AS calculated_at
FROM {full_path}"""

        if freshness.freshness_filter:
            query += f"\nWHERE {freshness.freshness_filter}"

        return query


# ===== Freshness Checker =====

class FreshnessChecker:
    """Source Freshness 檢查器"""

    def __init__(self, registry: SourceRegistry):
        self.registry = registry

    def check(self, source_name: str, table_name: str,
              max_loaded_at: datetime, now: Optional[datetime] = None) -> dict:
        """
        檢查 freshness 狀態
        Returns: {status: pass/warn/error, age_hours, thresholds, query}
        """
        now = now or datetime.utcnow()
        freshness = self.registry.get_effective_freshness(source_name, table_name)

        if freshness is None or not freshness.is_enabled():
            return {"status": "skip", "reason": "freshness not configured"}

        age = now - max_loaded_at
        age_hours = age.total_seconds() / 3600

        status = "pass"
        if freshness.error_after_hours and age_hours > freshness.error_after_hours:
            status = "error"
        elif freshness.warn_after_hours and age_hours > freshness.warn_after_hours:
            status = "warn"

        return {
            "source": source_name,
            "table": table_name,
            "status": status,
            "age_hours": round(age_hours, 1),
            "max_loaded_at": max_loaded_at.isoformat(),
            "warn_threshold_hours": freshness.warn_after_hours,
            "error_threshold_hours": freshness.error_after_hours,
            "query": self.registry.generate_freshness_query(source_name, table_name)
        }


# ===== DAG Lineage Tracker =====

class DAGLineageTracker:
    """source → staging → mart 血緣追蹤"""

    def __init__(self, registry: SourceRegistry):
        self.registry = registry
        self.models: dict[str, dict] = {}  # model_name → {refs, sources}

    def register_model(self, name: str, sources: list = None, refs: list = None):
        """註冊一個 model 及其依賴"""
        self.models[name] = {
            "sources": sources or [],  # [(source_name, table_name), ...]
            "refs": refs or [],        # [model_name, ...]
        }

    def get_upstream(self, model_name: str) -> dict:
        """取得 model 的完整上游血緣"""
        if model_name not in self.models:
            return {"error": f"Model '{model_name}' not registered"}

        model = self.models[model_name]
        result = {
            "model": model_name,
            "direct_sources": [],
            "direct_refs": model["refs"],
            "full_lineage": []
        }

        # 直接 source 依賴
        for src_name, tbl_name in model["sources"]:
            full_path = self.registry.resolve_table(src_name, tbl_name)
            result["direct_sources"].append({
                "source": src_name,
                "table": tbl_name,
                "full_path": full_path
            })

        # 遞迴追蹤 ref 上游
        visited = set()

        def trace(name, depth=0):
            if name in visited:
                return
            visited.add(name)
            if name in self.models:
                m = self.models[name]
                for src_name, tbl_name in m["sources"]:
                    result["full_lineage"].append({
                        "depth": depth,
                        "type": "source",
                        "path": f"source('{src_name}', '{tbl_name}')"
                    })
                for ref_name in m["refs"]:
                    result["full_lineage"].append({
                        "depth": depth,
                        "type": "ref",
                        "path": f"ref('{ref_name}')"
                    })
                    trace(ref_name, depth + 1)

        trace(model_name)
        return result


# ===== Build ODM Source Architecture =====

def build_odm_sources() -> SourceRegistry:
    """建立 ODM 三系統 source 架構"""
    registry = SourceRegistry(target_database="analytics")

    # === SAP MM/PP ===
    sap = Source(
        name="sap",
        database="raw",
        schema_name="sap_mm",
        description="SAP MM/PP 原始資料，由 ADF CDC pipeline 每小時載入",
        freshness=FreshnessConfig(
            warn_after_hours=12,
            error_after_hours=24,
            loaded_at_field="_etl_loaded_at"
        ),
        meta={"owner": "@data-platform-team", "contains_pii": False},
        tags=["erp", "sap"],
        tables=[
            SourceTable(
                name="matdoc",
                identifier="MATDOC_CDC",
                description="物料文件行項目（庫存異動），每筆 GR/GI 一條",
                freshness=FreshnessConfig(
                    warn_after_hours=6,
                    error_after_hours=12,
                    freshness_filter="budat >= DATEADD('day', -3, CURRENT_DATE)"
                ),
                columns=[
                    ColumnDef("mblnr", "物料文件編號", ["not_null"]),
                    ColumnDef("mjahr", "物料文件年份", ["not_null"]),
                    ColumnDef("zeile", "行項目編號", ["not_null"]),
                    ColumnDef("matnr", "物料號碼"),
                    ColumnDef("bwart", "移動類型", ["accepted_values:101,102,261,262,601,602"]),
                ]
            ),
            SourceTable(
                name="ekpo",
                description="採購訂單行項目",
            ),
            SourceTable(
                name="ekko",
                description="採購訂單抬頭",
            ),
            SourceTable(
                name="mara",
                description="物料主檔（日批次更新）",
                freshness=FreshnessConfig()  # 明確停用 freshness
            ),
            SourceTable(
                name="aenr",
                identifier="AENR_CDC",
                description="ECO 變更主記錄",
                freshness=FreshnessConfig(
                    warn_after_hours=12,
                    error_after_hours=24,
                )
            ),
        ]
    )

    # === MES/SFCS ===
    mes = Source(
        name="mes",
        database="raw",
        schema_name="sfcs",
        description="MES Shop Floor Control System，近即時資料（5 min CDC）",
        freshness=FreshnessConfig(
            warn_after_hours=1,
            error_after_hours=3,
            loaded_at_field="event_timestamp"
        ),
        meta={"owner": "@manufacturing-it"},
        tags=["mes", "production"],
        tables=[
            SourceTable(
                name="station_log",
                description="過站記錄（每個工站每個序號一條）",
                columns=[
                    ColumnDef("serial_number", "產品序號", ["not_null"]),
                    ColumnDef("station_id", "工站代碼", ["not_null"]),
                    ColumnDef("result", "過站結果", ["accepted_values:PASS,FAIL,SKIP"]),
                ]
            ),
            SourceTable(
                name="defect_log",
                description="不良記錄",
            ),
        ]
    )

    # === WMS ===
    wms = Source(
        name="wms",
        database="raw",
        schema_name="warehouse",
        description="WMS 倉庫管理系統，批次更新每 30 分鐘",
        freshness=FreshnessConfig(
            warn_after_hours=2,
            error_after_hours=6,
            loaded_at_field="_wms_sync_at"
        ),
        tags=["wms", "inventory"],
        tables=[
            SourceTable(
                name="inventory_snapshot",
                description="庫存快照（每半小時全量）",
            ),
            SourceTable(
                name="shipment_records",
                description="出貨記錄",
            ),
        ]
    )

    registry.register(sap)
    registry.register(mes)
    registry.register(wms)

    return registry


# ===== DEMO EXECUTION =====

def demo_1_source_registry():
    """Demo 1: sources.yml 解析與 Source Registry"""
    print("=" * 70)
    print("Demo 1: Source Registry — ODM 三系統 Source 架構")
    print("=" * 70)

    registry = build_odm_sources()

    for src_name, src in registry.sources.items():
        print(f"\n📦 Source: {src_name}")
        print(f"   Database: {src.database or '(target)'}")
        print(f"   Schema: {src.get_schema()}")
        print(f"   Description: {src.description[:60]}...")
        print(f"   Tags: {src.tags}")
        freshness_status = "✅ enabled" if src.freshness and src.freshness.is_enabled() else "❌ disabled"
        if src.freshness and src.freshness.is_enabled():
            freshness_status += f" (warn:{src.freshness.warn_after_hours}h, error:{src.freshness.error_after_hours}h)"
        print(f"   Default Freshness: {freshness_status}")
        print(f"   Tables:")
        for t in src.tables:
            eff = registry.get_effective_freshness(src_name, t.name)
            if eff and eff.is_enabled():
                f_str = f"warn:{eff.warn_after_hours}h, error:{eff.error_after_hours}h"
            else:
                f_str = "null (skipped)"
            full_path = registry.resolve_table(src_name, t.name)
            print(f"     - {t.name} → {full_path}  [freshness: {f_str}]")

    print(f"\n✅ 3 systems registered, {sum(len(s.tables) for s in registry.sources.values())} tables total")


def demo_2_source_compiler():
    """Demo 2: source() 編譯器"""
    print("\n" + "=" * 70)
    print("Demo 2: source() 編譯器 — Jinja → SQL 全路徑")
    print("=" * 70)

    registry = build_odm_sources()

    test_cases = [
        ("sap", "matdoc",    "raw.sap_mm.MATDOC_CDC"),
        ("sap", "ekpo",      "raw.sap_mm.ekpo"),
        ("sap", "mara",      "raw.sap_mm.mara"),
        ("sap", "aenr",      "raw.sap_mm.AENR_CDC"),
        ("mes", "station_log", "raw.sfcs.station_log"),
        ("wms", "inventory_snapshot", "raw.warehouse.inventory_snapshot"),
    ]

    all_pass = True
    for src, tbl, expected in test_cases:
        result = registry.resolve_table(src, tbl)
        status = "✅" if result == expected else "❌"
        if result != expected:
            all_pass = False
        print(f"  {status} source('{src}', '{tbl}') → {result}")

    # 錯誤處理
    print(f"\n  Error handling:")
    try:
        registry.resolve_table("crm", "leads")
    except ValueError as e:
        print(f"  ✅ source('crm', 'leads') → {e}")

    try:
        registry.resolve_table("sap", "nonexistent")
    except ValueError as e:
        print(f"  ✅ source('sap', 'nonexistent') → {e}")

    print(f"\n✅ source() compiler: {'all pass' if all_pass else 'FAILURES'}")


def demo_3_freshness_checker():
    """Demo 3: Freshness Checker — SLA 監控"""
    print("\n" + "=" * 70)
    print("Demo 3: Freshness Checker — Source Freshness SLA")
    print("=" * 70)

    registry = build_odm_sources()
    checker = FreshnessChecker(registry)
    now = datetime(2026, 7, 2, 7, 0, 0)  # 今天 07:00 UTC

    scenarios = [
        # (source, table, last_loaded, expected_status, description)
        ("sap", "matdoc", now - timedelta(hours=3), "pass", "SAP MATDOC 3小時前更新 → OK"),
        ("sap", "matdoc", now - timedelta(hours=8), "warn", "SAP MATDOC 8小時前 → ⚠️ warn (>6h)"),
        ("sap", "matdoc", now - timedelta(hours=18), "error", "SAP MATDOC 18小時前 → ❌ error (>12h, ECO 導致延遲)"),
        ("sap", "ekko", now - timedelta(hours=15), "warn", "SAP EKKO 15小時前 → ⚠️ warn (繼承 source 12h)"),
        ("sap", "mara", None, "skip", "SAP MARA → freshness: null (主檔不檢查)"),
        ("mes", "station_log", now - timedelta(minutes=30), "pass", "MES 30分鐘前 → OK (near-realtime)"),
        ("mes", "station_log", now - timedelta(hours=2), "warn", "MES 2小時前 → ⚠️ warn (>1h)"),
        ("wms", "inventory_snapshot", now - timedelta(hours=1), "pass", "WMS 1小時前 → OK"),
    ]

    results_summary = {"pass": 0, "warn": 0, "error": 0, "skip": 0}

    for src, tbl, loaded_at, expected, desc in scenarios:
        if loaded_at is None:
            result = checker.check(src, tbl, now, now)
        else:
            result = checker.check(src, tbl, loaded_at, now)

        actual = result["status"]
        match = "✅" if actual == expected else "❌"
        results_summary[actual] += 1

        icon = {"pass": "🟢", "warn": "🟡", "error": "🔴", "skip": "⏭️"}[actual]
        age_str = f"({result.get('age_hours', '—')}h)" if 'age_hours' in result else ""
        print(f"  {match} {icon} {desc} {age_str}")

    print(f"\n📊 Summary: {results_summary}")

    # 顯示一條 freshness query
    print(f"\n📝 Sample freshness query for sap.matdoc:")
    query = registry.generate_freshness_query("sap", "matdoc")
    print(f"  {query}")


def demo_4_dag_lineage():
    """Demo 4: DAG Lineage — source → staging → mart 血緣追蹤"""
    print("\n" + "=" * 70)
    print("Demo 4: DAG Lineage Tracker — 完整血緣追蹤")
    print("=" * 70)

    registry = build_odm_sources()
    tracker = DAGLineageTracker(registry)

    # 註冊 ODM dbt models（staging → intermediate → mart）
    # Staging: 1:1 source mapping
    tracker.register_model("stg_sap__matdoc",
                          sources=[("sap", "matdoc")])
    tracker.register_model("stg_sap__ekpo",
                          sources=[("sap", "ekpo")])
    tracker.register_model("stg_sap__ekko",
                          sources=[("sap", "ekko")])
    tracker.register_model("stg_sap__materials",
                          sources=[("sap", "mara")])
    tracker.register_model("stg_sap__eco",
                          sources=[("sap", "aenr")])
    tracker.register_model("stg_mes__station_logs",
                          sources=[("mes", "station_log")])
    tracker.register_model("stg_wms__inventory",
                          sources=[("wms", "inventory_snapshot")])

    # Intermediate: joins across staging
    tracker.register_model("int_purchase_orders",
                          refs=["stg_sap__ekpo", "stg_sap__ekko", "stg_sap__materials"])
    tracker.register_model("int_inventory_movements",
                          refs=["stg_sap__matdoc", "stg_sap__materials"])
    tracker.register_model("int_production_quality",
                          refs=["stg_mes__station_logs", "stg_sap__materials"])

    # Mart: business-ready
    tracker.register_model("mart_vendor_scorecard",
                          refs=["int_purchase_orders", "int_inventory_movements"])
    tracker.register_model("mart_oee_dashboard",
                          refs=["int_production_quality", "stg_wms__inventory"])

    # 追蹤 mart_vendor_scorecard 的完整血緣
    print("\n📊 Lineage for mart_vendor_scorecard:")
    lineage = tracker.get_upstream("mart_vendor_scorecard")

    print(f"  Direct refs: {lineage['direct_refs']}")
    print(f"  Full lineage:")
    for item in lineage["full_lineage"]:
        indent = "  " * (item["depth"] + 2)
        icon = "📦" if item["type"] == "source" else "🔗"
        print(f"{indent}{icon} {item['path']}")

    # 追蹤 mart_oee_dashboard
    print(f"\n📊 Lineage for mart_oee_dashboard:")
    lineage2 = tracker.get_upstream("mart_oee_dashboard")
    print(f"  Direct refs: {lineage2['direct_refs']}")
    print(f"  Full lineage:")
    for item in lineage2["full_lineage"]:
        indent = "  " * (item["depth"] + 2)
        icon = "📦" if item["type"] == "source" else "🔗"
        print(f"{indent}{icon} {item['path']}")

    print(f"\n✅ {len(tracker.models)} models tracked, full lineage from mart → source")


def demo_5_eco_delay_scenario():
    """Demo 5: ECO 延遲場景 — SAP 資料因 ECO 延遲 18 小時"""
    print("\n" + "=" * 70)
    print("Demo 5: ECO 延遲場景 — Freshness Error Alert")
    print("=" * 70)

    registry = build_odm_sources()
    checker = FreshnessChecker(registry)
    now = datetime(2026, 7, 2, 7, 0, 0)

    print("\n📋 場景：客戶發出 ECO，SAP BOM 更新中，CDC pipeline 被鎖定")
    print("   ECO 開始時間：2026-07-01 13:00")
    print("   SAP 恢復時間：2026-07-02 07:00（預計）")
    print("   影響：MATDOC、EKPO、AENR 全部延遲")

    eco_affected_tables = [
        ("sap", "matdoc", now - timedelta(hours=18)),
        ("sap", "ekpo", now - timedelta(hours=18)),
        ("sap", "aenr", now - timedelta(hours=18)),
        ("sap", "ekko", now - timedelta(hours=18)),
        ("sap", "mara", None),  # 主檔不受影響
        ("mes", "station_log", now - timedelta(minutes=10)),  # MES 正常
        ("wms", "inventory_snapshot", now - timedelta(hours=1)),  # WMS 正常
    ]

    print(f"\n🔍 dbt source freshness 結果：\n")
    errors = []
    warns = []
    for src, tbl, loaded_at in eco_affected_tables:
        if loaded_at is None:
            result = checker.check(src, tbl, now, now)
        else:
            result = checker.check(src, tbl, loaded_at, now)

        status = result["status"]
        icon = {"pass": "🟢", "warn": "🟡", "error": "🔴", "skip": "⏭️"}[status]
        age = result.get("age_hours", "—")
        print(f"  {icon} {src}.{tbl}: {status} (age: {age}h)")

        if status == "error":
            errors.append(f"{src}.{tbl}")
        elif status == "warn":
            warns.append(f"{src}.{tbl}")

    print(f"\n📊 Alert Summary:")
    print(f"  🔴 Error ({len(errors)}): {', '.join(errors)}")
    print(f"  🟡 Warn ({len(warns)}): {', '.join(warns)}")
    print(f"  Action: 通知 Data Platform Team + PMC 確認 ECO 處理進度")


def demo_6_source_vs_ref_comparison():
    """Demo 6: source() vs ref() 對比 + 最佳實踐總結"""
    print("\n" + "=" * 70)
    print("Demo 6: source() vs ref() 對比 + Multi-System Architecture")
    print("=" * 70)

    comparison = {
        "面向": ["指向目標", "語法", "DAG 邊", "Freshness", "所有權", "測試"],
        "source()": [
            "raw table (EL 載入)",
            "source('sap', 'matdoc')",
            "raw → staging model",
            "✅ warn/error SLA",
            "EL pipeline team",
            "可加 data_tests"
        ],
        "ref()": [
            "dbt model (transformed)",
            "ref('stg_sap__matdoc')",
            "model → model",
            "❌ 不適用",
            "dbt analytics team",
            "可加 data_tests"
        ]
    }

    print(f"\n{'面向':<15} {'source()':<30} {'ref()':<30}")
    print("-" * 75)
    for i, aspect in enumerate(comparison["面向"]):
        s = comparison["source()"][i]
        r = comparison["ref()"][i]
        print(f"{aspect:<15} {s:<30} {r:<30}")

    print(f"\n📐 ODM Source 架構最佳實踐：")
    best_practices = [
        "1. 按系統分 source YAML：_sap__sources.yml、_mes__sources.yml、_wms__sources.yml",
        "2. 每個 source table 對應一個 staging model（1:1 mapping）",
        "3. staging model 只做 rename + cast + filter，不做 business logic",
        "4. freshness SLA 按資料時效性分級：",
        "   - MES 過站記錄：warn 1h / error 3h（near-realtime）",
        "   - SAP 交易資料：warn 6h / error 12h（CDC 每小時）",
        "   - SAP 主檔：freshness: null（日批次更新）",
        "5. loaded_at_field 用 ETL 載入時間（_etl_loaded_at），不用業務時間",
        "6. 大型表加 freshness filter（只掃最近 3 天）減少 freshness query cost",
        "7. identifier 處理表名差異（SAP 原始表名 vs CDC 表名）",
        "8. meta.owner 標注維護團隊，description 說明資料更新頻率",
    ]
    for bp in best_practices:
        print(f"  {bp}")


def main():
    print("🔧 dbt Sources 實作練習 — ODM 供應鏈多系統 Source 管理")
    print("=" * 70)

    demo_1_source_registry()
    demo_2_source_compiler()
    demo_3_freshness_checker()
    demo_4_dag_lineage()
    demo_5_eco_delay_scenario()
    demo_6_source_vs_ref_comparison()

    print("\n" + "=" * 70)
    print("✅ All 6 demos completed!")
    print("=" * 70)


if __name__ == "__main__":
    main()
