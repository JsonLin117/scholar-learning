# Scholar 練習環境 - Docker 服務

## 快速啟動

### Kafka（含 UI）
```bash
cd ~/Projects/scholar-learning/docker
docker compose -f kafka-compose.yml up -d

# 停止
docker compose -f kafka-compose.yml down
```
- Kafka Broker: `localhost:9092`
- Kafka UI: http://localhost:8080

### Flink
```bash
cd ~/Projects/scholar-learning/docker
docker compose -f flink-compose.yml up -d

# 停止
docker compose -f flink-compose.yml down
```
- Flink Dashboard: http://localhost:8081

### Airflow
```bash
cd ~/Projects/scholar-learning/docker/airflow

# 第一次初始化（只需執行一次）
docker compose up airflow-init

# 啟動
docker compose up -d

# 停止
docker compose down
```
- Airflow UI: http://localhost:8080
- 帳號：airflow / 密碼：airflow
- DAG 放在 `./dags/` 資料夾

## Python 練習環境（PySpark / dbt / Iceberg）
```bash
cd ~/Projects/scholar-learning
source .venv/bin/activate
JAVA_HOME=/opt/homebrew/opt/openjdk@17 python3 <練習檔案>
```

## 注意事項
- Airflow 和 Kafka UI 都用 8080，不要同時啟動
- 各服務用完記得 `docker compose down` 釋放資源
