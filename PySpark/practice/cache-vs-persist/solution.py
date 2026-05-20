"""
PySpark cache() vs persist() 實作練習 — ODM Shop Floor OEE Pipeline
2026-05-21 | PySpark Lesson 8

核心示範：
1. cache()/persist() 是 lazy，必須 action 才真正 materialize
2. Branching DAG 下，不 cache 會讓昂貴 lineage 被每個下游 action 重跑
3. unpersist() 是 production pipeline 必備清理動作
"""

from __future__ import annotations

import time
from dataclasses import dataclass

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    broadcast,
    col,
    concat,
    count,
    floor,
    lit,
    max as spark_max,
    rand,
    round as spark_round,
    sum as spark_sum,
    udf,
    when,
)
from pyspark.sql.types import DoubleType


NUM_EVENTS = 80_000


@dataclass
class ExperimentResult:
    name: str
    elapsed: float
    expensive_calls: int
    output_rows: int


def create_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("CacheVsPersistPractice")
        .master("local[4]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def build_source_data(spark: SparkSession) -> tuple[DataFrame, DataFrame, DataFrame]:
    """建立 ODM Shop Floor 事件、BOM 維度、供應商維度。"""
    events = (
        spark.range(NUM_EVENTS)
        .withColumn("event_id", concat(lit("EVT-"), col("id")))
        .withColumn("plant", concat(lit("P"), (floor(rand(seed=11) * 3) + 1).cast("int")))
        .withColumn("line", concat(lit("L"), (floor(rand(seed=12) * 8) + 1).cast("int")))
        .withColumn("material_id", concat(lit("MAT-"), (floor(rand(seed=13) * 120) + 1).cast("int")))
        .withColumn("supplier_id", concat(lit("SUP-"), (floor(rand(seed=14) * 30) + 1).cast("int")))
        .withColumn("planned_minutes", (rand(seed=15) * 50 + 10).cast("double"))
        .withColumn("actual_minutes", (col("planned_minutes") * (rand(seed=16) * 0.8 + 0.8)).cast("double"))
        .withColumn("good_units", (floor(rand(seed=17) * 80) + 20).cast("int"))
        .withColumn("scrap_units", floor(rand(seed=18) * 8).cast("int"))
        .drop("id")
    )

    bom = (
        spark.range(1, 121)
        .withColumn("material_id", concat(lit("MAT-"), col("id")))
        .withColumn(
            "material_group",
            when(col("id") % 4 == 0, "GPU")
            .when(col("id") % 4 == 1, "PCB")
            .when(col("id") % 4 == 2, "Memory")
            .otherwise("Thermal"),
        )
        .withColumn("standard_cycle_min", (rand(seed=21) * 20 + 5).cast("double"))
        .drop("id")
    )

    suppliers = (
        spark.range(1, 31)
        .withColumn("supplier_id", concat(lit("SUP-"), col("id")))
        .withColumn(
            "supplier_tier",
            when(col("id") <= 5, "Strategic").when(col("id") <= 18, "Approved").otherwise("Backup"),
        )
        .drop("id")
    )
    return events, bom, suppliers


def enrich_with_expensive_score(
    spark: SparkSession, events: DataFrame, bom: DataFrame, suppliers: DataFrame, label: str
) -> tuple[DataFrame, object]:
    """建立昂貴 enrichment DAG，並用 accumulator 計算被執行幾次。"""
    calls = spark.sparkContext.accumulator(0)

    def expensive_quality_score(good_units: int, scrap_units: int, actual_minutes: float) -> float:
        # 在真實世界這可能是複雜 UDF、正規化、或外部規則；這裡用 accumulator 觀察重算次數。
        calls.add(1)
        total = max(good_units + scrap_units, 1)
        yield_rate = good_units / total
        speed_penalty = min(actual_minutes / 60.0, 2.0) * 0.03
        return round(max(yield_rate - speed_penalty, 0.0), 4)

    score_udf = udf(expensive_quality_score, DoubleType())

    enriched = (
        events.join(broadcast(bom), "material_id", "left")
        .join(broadcast(suppliers), "supplier_id", "left")
        .withColumn("quality_score", score_udf("good_units", "scrap_units", "actual_minutes"))
        .withColumn("total_units", col("good_units") + col("scrap_units"))
        .withColumn("availability", col("planned_minutes") / col("actual_minutes"))
        .withColumn("performance", col("standard_cycle_min") / col("actual_minutes"))
        .withColumn("quality", col("good_units") / col("total_units"))
        .withColumn("oee", col("availability") * col("performance") * col("quality"))
    )
    print(f"\n🧱 Built lineage for {label}: no Spark job yet (lazy transformation).")
    return enriched, calls


def run_three_downstream_reports(df: DataFrame) -> int:
    """同一個 enriched DF 餵給三個下游報表，每個 collect/count 都是 action。"""
    oee_rows = (
        df.groupBy("plant", "line")
        .agg(
            spark_round(avg("oee"), 4).alias("avg_oee"),
            spark_round(avg("quality_score"), 4).alias("avg_quality_score"),
            spark_sum("good_units").alias("good_units"),
            count("*").alias("events"),
        )
        .collect()
    )

    risk_rows = (
        df.filter((col("quality_score") < 0.9) | (col("scrap_units") >= 5))
        .groupBy("material_group")
        .agg(count("*").alias("risk_events"), spark_max("scrap_units").alias("max_scrap"))
        .collect()
    )

    supplier_rows = (
        df.groupBy("supplier_tier")
        .agg(
            spark_round(avg("quality_score"), 4).alias("avg_quality_score"),
            spark_sum("scrap_units").alias("scrap_units"),
        )
        .collect()
    )
    return len(oee_rows) + len(risk_rows) + len(supplier_rows)


def experiment_no_cache(spark: SparkSession, events: DataFrame, bom: DataFrame, suppliers: DataFrame) -> ExperimentResult:
    enriched, calls = enrich_with_expensive_score(spark, events, bom, suppliers, "NO CACHE")
    start = time.time()
    output_rows = run_three_downstream_reports(enriched)
    elapsed = time.time() - start
    return ExperimentResult("No cache: lineage replayed by each downstream action", elapsed, calls.value, output_rows)


def experiment_cache(spark: SparkSession, events: DataFrame, bom: DataFrame, suppliers: DataFrame) -> ExperimentResult:
    enriched, calls = enrich_with_expensive_score(spark, events, bom, suppliers, "CACHE")

    print("📌 Calling enriched.cache() only marks the plan. Accumulator is still:", calls.value)
    cached = enriched.cache()
    print("   Storage level before action:", cached.storageLevel)

    start = time.time()
    cached.count()  # materialize cache; this is the first real Spark job for cached data
    after_materialize = calls.value
    print(f"✅ First action materialized cache. Expensive UDF calls: {after_materialize:,}")

    output_rows = run_three_downstream_reports(cached)
    elapsed = time.time() - start
    cached.unpersist()
    print("🧹 cache().unpersist() done.")
    return ExperimentResult("cache(): one materialization reused by three downstream reports", elapsed, calls.value, output_rows)


def experiment_persist_disk_only(
    spark: SparkSession, events: DataFrame, bom: DataFrame, suppliers: DataFrame
) -> ExperimentResult:
    enriched, calls = enrich_with_expensive_score(spark, events, bom, suppliers, "PERSIST DISK_ONLY")
    persisted = enriched.persist(StorageLevel.DISK_ONLY)
    print("💾 persist(StorageLevel.DISK_ONLY) storage level:", persisted.storageLevel)

    start = time.time()
    persisted.count()  # materialize to disk cache
    output_rows = run_three_downstream_reports(persisted)
    elapsed = time.time() - start
    persisted.unpersist()
    print("🧹 persist(DISK_ONLY).unpersist() done.")
    return ExperimentResult("persist(DISK_ONLY): memory-safe cache alternative", elapsed, calls.value, output_rows)


def print_result_table(results: list[ExperimentResult]) -> None:
    print("\n" + "=" * 72)
    print("📊 實驗結果：cache / persist 對 branching DAG 的影響")
    print("=" * 72)
    print(f"{'策略':<56} {'秒數':>8} {'昂貴計算次數':>14}")
    print("-" * 72)
    for result in results:
        print(f"{result.name:<56} {result.elapsed:>8.2f} {result.expensive_calls:>14,}")

    no_cache, cached, disk = results
    expected_no_cache = NUM_EVENTS * 3
    expected_cached = NUM_EVENTS
    print("\n🎯 核心觀察")
    print(f"- No cache 約等於 {NUM_EVENTS:,} rows × 3 downstream actions = {expected_no_cache:,} 次昂貴計算")
    print(f"- cache()/persist() 先 materialize，後續分支重用 cached result，約 {expected_cached:,} 次昂貴計算")
    print("- cache() 預設是 MEMORY_AND_DISK_DESER；persist() 讓你依記憶體壓力選 DISK_ONLY / SER / OFF_HEAP 等級")
    print("- 真實 production 要在 Spark UI > Storage Tab 確認 cached partitions，並在 finally block unpersist()")

    if cached.expensive_calls < no_cache.expensive_calls and disk.expensive_calls < no_cache.expensive_calls:
        print("\n✅ 驗證通過：cache()/persist() 確實避免 branching DAG 重複計算。")
    else:
        raise AssertionError("cache/persist did not reduce expensive recomputation as expected")


def main() -> None:
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    print("=" * 72)
    print("🏭 PySpark cache() vs persist() — ODM Shop Floor OEE Pipeline")
    print("=" * 72)
    print(f"Generating {NUM_EVENTS:,} shop-floor events...")

    events, bom, suppliers = build_source_data(spark)
    print(f"Source events: {events.count():,}; BOM rows: {bom.count():,}; supplier rows: {suppliers.count():,}")

    results = [
        experiment_no_cache(spark, events, bom, suppliers),
        experiment_cache(spark, events, bom, suppliers),
        experiment_persist_disk_only(spark, events, bom, suppliers),
    ]
    print_result_table(results)

    spark.stop()


if __name__ == "__main__":
    main()
