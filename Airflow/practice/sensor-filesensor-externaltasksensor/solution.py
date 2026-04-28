"""
Airflow Sensor 實作練習
場景：ODM 供應鏈 — ERP 落檔等待 + 跨 DAG 依賴
日期：2026-04-29

重點：
  - FileSensor 等外部 ERP parquet 出現
  - ExternalTaskSensor 等上游 DAG task 完成
  - 都用 reschedule mode，避免佔住 worker slot
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable

# ──────────────────────────────────────────────────────────────────────────────
# DAG 1：erp_po_ingestion
# 職責：等待 ERP 的 PO parquet 出現 → 讀進 Bronze → 驗證
# ──────────────────────────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["data-team@wistron.com"],
}

with DAG(
    dag_id="erp_po_ingestion",
    start_date=datetime(2026, 1, 1),
    schedule="0 1 * * *",          # 凌晨 1:00 開始等，給 ERP 最多 3.5 小時
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["supply-chain", "erp", "bronze"],
) as dag_ingestion:

    # FileSensor：等待 Azure Blob 上的 ERP PO 檔案
    # - mode="reschedule": 不佔 worker slot，每 5 分鐘醒來查一次
    # - timeout=10800: 超過 3 小時就 FAIL（到早上 4:00 都沒出檔 = 異常）
    # - poke_interval=300: 每 5 分鐘 poke 一次
    wait_for_erp_file = FileSensor(
        task_id="wait_for_erp_po_parquet",
        filepath="/mnt/azure_blob/erp/purchase_orders/{{ ds }}/po_data.parquet",
        # 實際環境用 fs_conn_id 指向 Azure Blob Connection
        # fs_conn_id="azure_blob_erp_conn",
        poke_interval=300,                  # 每 5 分鐘
        timeout=10800,                      # 3 小時超時
        mode="reschedule",                  # ⭐ 關鍵：長等待用 reschedule
        soft_fail=False,                    # timeout 就 FAIL（不 SKIP）
    )

    def ingest_to_bronze(**context):
        """把 ERP PO parquet 讀進 Delta Lake Bronze 層"""
        ds = context["ds"]
        file_path = f"/mnt/azure_blob/erp/purchase_orders/{ds}/po_data.parquet"
        
        print(f"[Bronze Ingestion] Processing: {file_path}")
        # 實際：用 PySpark 讀 parquet 寫 Delta Lake
        # spark.read.parquet(file_path) \
        #     .write.format("delta") \
        #     .mode("overwrite") \
        #     .partitionBy("order_date") \
        #     .save(f"abfs://datalake/bronze/purchase_orders/{ds}")
        
        # 把寫入的 row count 透過 XCom 傳給下游
        return {"rows_ingested": 50000, "partition": ds}

    ingest_bronze = PythonOperator(
        task_id="ingest_to_bronze",
        python_callable=ingest_to_bronze,
        provide_context=True,
    )

    def validate_data(**context):
        """基本資料品質驗證"""
        ds = context["ds"]
        ti = context["ti"]
        stats = ti.xcom_pull(task_ids="ingest_to_bronze")  # 從 XCom 拉
        
        rows = stats.get("rows_ingested", 0)
        print(f"[Validation] Date: {ds}, Rows: {rows}")
        
        # 簡單驗證：行數不能是 0
        if rows == 0:
            raise ValueError(f"Bronze ingestion got 0 rows for {ds}！")
        
        print(f"[Validation] ✅ Passed: {rows} rows")

    validate_data_task = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
        provide_context=True,
    )

    # 依賴鏈：等檔案 → 讀 Bronze → 驗證
    wait_for_erp_file >> ingest_bronze >> validate_data_task


# ──────────────────────────────────────────────────────────────────────────────
# DAG 2：supply_chain_kpi
# 職責：等 DAG 1 的 validate_data 完成 → 計算供應商 OTIF 指標
# ──────────────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="supply_chain_kpi",
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",          # 凌晨 2:00 開始等（有可能需要等 DAG 1）
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["supply-chain", "kpi", "gold"],
) as dag_kpi:

    # ExternalTaskSensor：等 erp_po_ingestion 的 validate_data 完成
    # - 不用等整個 DAG，只等關鍵 task 完成
    # - execution_delta=timedelta(hours=1): 等同一天 DAG 1 的執行（兩個 DAG schedule 差 1h）
    # ⚠️ 注意：兩個 DAG 的 schedule_interval 不同時，execution_date 要對準！
    wait_for_ingestion = ExternalTaskSensor(
        task_id="wait_for_erp_ingestion_complete",
        external_dag_id="erp_po_ingestion",
        external_task_id="validate_data",      # 等這個 task 完成就好，不用等整個 DAG
        # execution_delta=timedelta(hours=1),   # DAG 1 在 01:00 跑，DAG 2 在 02:00 跑
        # 或用 execution_date_fn 處理複雜的時間對應（推薦）
        poke_interval=60,                      # 每分鐘查一次（DAG 內等待時間短）
        timeout=7200,                          # 2 小時超時
        mode="reschedule",                     # ⭐ reschedule mode
        allowed_states=["success"],            # 只有 success 才繼續，skipped 不算
    )

    def calculate_supplier_otif(**context):
        """計算供應商 On-Time In-Full (OTIF) 指標"""
        ds = context["ds"]
        print(f"[OTIF] Calculating supplier OTIF for {ds}")
        
        # 實際：從 Silver layer 讀採購訂單和入庫記錄，計算 OTIF
        # df = spark.read.format("delta").load("abfs://datalake/silver/purchase_orders")
        # df_otif = df.filter(F.col("order_date") == ds) \
        #             .withColumn("is_on_time", F.col("actual_delivery") <= F.col("promised_delivery")) \
        #             .groupBy("supplier_id") \
        #             .agg(F.avg("is_on_time").alias("otif_rate"))
        
        print(f"[OTIF] ✅ Supplier OTIF calculated and written to Gold layer")

    calculate_otif = PythonOperator(
        task_id="calculate_supplier_otif",
        python_callable=calculate_supplier_otif,
        provide_context=True,
    )

    end = EmptyOperator(task_id="done")

    wait_for_ingestion >> calculate_otif >> end


# ──────────────────────────────────────────────────────────────────────────────
# 驗證：不用 Spark 的簡單邏輯測試
# ──────────────────────────────────────────────────────────────────────────────

def demo_sensor_comparison():
    """
    對比三種等待模式的資源使用（模擬計算）
    實際執行不需要 Airflow，只是說明邏輯
    """
    print("=" * 60)
    print("Sensor Mode 資源比較（等待 2 小時，10 個 worker slots）")
    print("=" * 60)
    
    # poke 模式：整個等待期間佔住 slot
    poke_slots_used = 10  # 假設 10 個 Sensor 同時在等
    poke_available = 10 - poke_slots_used
    print(f"\n❌ poke mode（2小時等待）:")
    print(f"   佔用 slots: {poke_slots_used}/10 → 剩 {poke_available} slots 給正常任務")
    
    # reschedule 模式：每 5 分鐘只佔幾秒
    check_time_ratio = 5 / (5 * 60)  # 5秒 check / 300秒 interval
    reschedule_avg_slots = round(10 * check_time_ratio, 2)
    print(f"\n✅ reschedule mode（每5分鐘查5秒）:")
    print(f"   平均佔用 slots: ~{reschedule_avg_slots}/10 → 幾乎不影響正常任務")
    
    print(f"\n結論：長等待必用 reschedule mode，節省 {round((1-check_time_ratio)*100)}% slot 資源")

if __name__ == "__main__":
    demo_sensor_comparison()
