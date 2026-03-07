# 🛠️ Технологический стек и обоснование выбора

## Философия выбора технологий

При выборе технологий для платформы мы руководствовались следующими критериями:
- **Production-ready**: Проверенные в production решения
- **Community support**: Активное сообщество и документация
- **Integration**: Хорошая интеграция с другими компонентами
- **Performance**: Высокая производительность для больших объемов данных
- **Cost-effective**: Оптимальное соотношение цена/качество
- **Future-proof**: Активное развитие и поддержка

## 📊 Сравнительный анализ технологий

### Streaming Platforms

| Критерий | Apache Kafka | RabbitMQ | AWS Kinesis | Apache Pulsar |
|----------|-------------|----------|-------------|---------------|
| **Throughput** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| **Latency** | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐ |
| **Scalability** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| **Ecosystem** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐ |
| **Learning Curve** | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐ |
| **Cost** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐ | ⭐⭐⭐⭐ |

**Выбор: Apache Kafka** ✅
- Де-факто стандарт для event streaming
- Отличная экосистема (Kafka Connect, Kafka Streams)
- Proven scalability (LinkedIn обрабатывает 7 триллионов сообщений в день)

### Stream Processing

| Критерий | Apache Spark | Apache Flink | Apache Storm | Kafka Streams |
|----------|-------------|--------------|--------------|---------------|
| **Batch + Stream** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐ | ⭐⭐⭐ |
| **Low Latency** | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ |
| **SQL Support** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐ | ⭐⭐⭐ |
| **ML Libraries** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐ | ⭐⭐ |
| **Community** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐ |
| **Complexity** | ⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐ | ⭐⭐⭐⭐⭐ |

**Выбор: Apache Spark + Apache Flink** ✅
- Spark для batch и micro-batch processing
- Flink для ultra-low latency use cases
- Комплементарные технологии для разных сценариев

## 🗄️ Детальный анализ каждой технологии

### Apache Kafka 3.6

**Что это?**
Распределенная платформа потоковой передачи событий, способная обрабатывать триллионы событий в день.

**Почему Kafka?**
```yaml
Преимущества:
  - Высокая пропускная способность: До 2М сообщений/сек на broker
  - Надежность: Репликация, гарантии доставки (at-least-once, exactly-once)
  - Масштабируемость: Линейное масштабирование через партиции
  - Durability: Персистентное хранение с настраиваемым retention
  - Экосистема: Kafka Connect, Kafka Streams, KSQL

Недостатки:
  - Сложность операций: Требует опыта для тюнинга
  - Zookeeper dependency: До версии 2.8 (KRaft в 3.x решает это)
  - Ordering гарантии: Только в рамках партиции

Альтернативы рассмотрены:
  - RabbitMQ: Ниже throughput, лучше для low-volume
  - Apache Pulsar: Новее, меньше adoption
  - AWS Kinesis: Vendor lock-in, дороже
```

**Конфигурация для fraud detection:**
```properties
# server.properties оптимизации
num.network.threads=8
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600

# Оптимизации для low latency
replica.lag.time.max.ms=10000
min.insync.replicas=2

# Compression для экономии bandwidth
compression.type=lz4
```

### Apache Spark 3.5

**Что это?**
Унифицированный движок для крупномасштабной обработки данных с поддержкой batch и stream processing.

**Почему Spark?**
```python
# Преимущества в коде
# 1. Unified API для batch и streaming
df_batch = spark.read.parquet("s3://data/historical/")
df_stream = spark.readStream.format("kafka").load()

# 2. Богатые SQL возможности
df.createOrReplaceTempView("transactions")
spark.sql("""
    SELECT user_id, COUNT(*) as fraud_count
    FROM transactions
    WHERE is_fraud = true
    GROUP BY user_id
""")

# 3. Встроенная поддержка ML
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier

rf = RandomForestClassifier(featuresCol="features", labelCol="is_fraud")
pipeline = Pipeline(stages=[featureAssembler, rf])
```

**Оптимизации для production:**
```scala
// Adaptive Query Execution (AQE)
spark.sql.adaptive.enabled = true
spark.sql.adaptive.coalescePartitions.enabled = true

// Dynamic Resource Allocation
spark.dynamicAllocation.enabled = true
spark.dynamicAllocation.minExecutors = 2
spark.dynamicAllocation.maxExecutors = 20

// Tungsten optimizations
spark.sql.tungsten.enabled = true
```

### Apache Flink 1.18

**Что это?**
Фреймворк и движок распределенной обработки для stateful вычислений над неограниченными и ограниченными потоками данных.

**Почему Flink?**
```java
// True event-time processing
DataStream<Transaction> transactions = env
    .addSource(kafkaSource)
    .assignTimestampsAndWatermarks(
        WatermarkStrategy.<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(20))
            .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
    );

// Complex Event Processing (CEP)
Pattern<Transaction, ?> fraudPattern = Pattern.<Transaction>begin("first")
    .where(new SimpleCondition<Transaction>() {
        @Override
        public boolean filter(Transaction value) {
            return value.getAmount() > 5000;
        }
    })
    .followedBy("second")
    .where(new SimpleCondition<Transaction>() {
        @Override
        public boolean filter(Transaction value) {
            return value.getMerchantCategory().equals("jewelry");
        }
    })
    .within(Time.minutes(10));
```

**Сравнение Spark vs Flink для нашего use case:**
| Аспект | Spark Streaming | Flink |
|--------|----------------|-------|
| **Модель обработки** | Micro-batch | True streaming |
| **Latency** | Секунды | Миллисекунды |
| **State management** | updateStateByKey | Robust state backend |
| **Event time** | Structured Streaming | Native support |
| **Use case** | Aggregations, ML | Complex event patterns |

### PostgreSQL 15

**Что это?**
Мощная объектно-реляционная система управления базами данных с акцентом на расширяемость и соответствие стандартам.

**Почему PostgreSQL?**
```sql
-- 1. JSON support для semi-structured data
CREATE TABLE transactions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    data JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_user_id ON transactions((data->>'user_id'));
CREATE INDEX idx_amount ON transactions((data->>'amount')::numeric);

-- 2. Advanced indexing
CREATE INDEX idx_gin_tags ON transactions USING GIN (data);
CREATE INDEX idx_brin_timestamp ON transactions USING BRIN (created_at);

-- 3. Partitioning для масштабирования
CREATE TABLE transactions_2024_01 PARTITION OF transactions
FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

-- 4. Расширения
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";      -- UUID generation
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements"; -- Query analysis
CREATE EXTENSION IF NOT EXISTS "postgres_fdw";    -- Foreign data
```

**Performance tuning:**
```ini
# postgresql.conf оптимизации
shared_buffers = 8GB
effective_cache_size = 24GB
maintenance_work_mem = 2GB
checkpoint_completion_target = 0.9
wal_buffers = 16MB
default_statistics_target = 100
random_page_cost = 1.1
effective_io_concurrency = 200
work_mem = 20MB
min_wal_size = 1GB
max_wal_size = 4GB
```

### MinIO

**Что это?**
Высокопроизводительное S3-совместимое объектное хранилище, оптимизированное для больших объектов.

**Почему MinIO?**
- **S3 API совместимость**: Работает с любым S3 SDK
- **Performance**: 183 GB/s read, 171 GB/s write
- **Kubernetes native**: Operator для легкого deployment
- **Erasure coding**: Защита от потери данных
- **Multi-tenancy**: Изоляция данных

**Использование в проекте:**
```python
# Интеграция с Spark
spark.conf.set("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
spark.conf.set("spark.hadoop.fs.s3a.access.key", "your_access_key")
spark.conf.set("spark.hadoop.fs.s3a.secret.key", "your_secret_key")
spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")

df.write.mode("overwrite").parquet("s3a://fraud-detection/processed/")
```

### Redis 7

**Что это?**
In-memory структура данных, используемая как база данных, кеш и message broker.

**Почему Redis?**
```python
# 1. Feature caching для ML
redis_client.hset(
    f"features:user:{user_id}",
    mapping={
        "transaction_count_1h": 5,
        "avg_amount": 125.50,
        "risk_score": 0.23
    }
)
redis_client.expire(f"features:user:{user_id}", 3600)  # TTL 1 час

# 2. Rate limiting
def is_rate_limited(client_id: str, limit: int = 100) -> bool:
    key = f"rate_limit:{client_id}:{datetime.now().minute}"
    
    pipe = redis_client.pipeline()
    pipe.incr(key)
    pipe.expire(key, 60)
    result = pipe.execute()
    
    return result[0] > limit

# 3. Real-time leaderboards
redis_client.zadd("fraud_scores:today", {
    "USER_123": 0.95,
    "USER_456": 0.87
})
top_risks = redis_client.zrevrange("fraud_scores:today", 0, 9, withscores=True)
```

### Apache Airflow 3.1.6

**Что это?**
Платформа для программного создания, планирования и мониторинга workflows.

**Почему Airflow?**
```python
# Декларативный подход к pipeline
from airflow.decorators import dag, task
from datetime import datetime, timedelta

@dag(
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
    }
)
def fraud_detection_pipeline():
    @task
    def extract_data():
        # Извлечение данных
        return {"records": 1000000}
    
    @task
    def validate_data(data):
        # Great Expectations валидация
        return data
    
    @task
    def feature_engineering(data):
        # Feature generation
        return {"features": data["records"] * 50}
    
    @task
    def train_model(features):
        # Model training
        return {"model_version": "v2.3.1"}
    
    # DAG definition
    data = extract_data()
    validated = validate_data(data)
    features = feature_engineering(validated)
    train_model(features)

dag = fraud_detection_pipeline()
```

### dbt (Data Build Tool)

**Что это?**
Инструмент трансформации данных, который позволяет аналитикам и инженерам трансформировать данные в хранилище используя SQL.

**Почему dbt?**
```sql
-- models/staging/stg_transactions.sql
{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    on_schema_change='fail',
    partition_by={
        "field": "created_at",
        "data_type": "timestamp",
        "granularity": "day"
    }
) }}

WITH source_data AS (
    SELECT * FROM {{ source('raw', 'transactions') }}
    {% if is_incremental() %}
        WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
    {% endif %}
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['transaction_id']) }} as sk_transaction,
    *,
    {{ var('fraud_threshold', 5000) }} as high_risk_threshold,
    CURRENT_TIMESTAMP as dbt_updated_at
FROM source_data

-- Встроенное тестирование
-- tests/assert_positive_amounts.sql
SELECT *
FROM {{ ref('stg_transactions') }}
WHERE amount < 0
```

### MLflow

**Что это?**
Open source платформа для управления end-to-end машинного обучения lifecycle.

**Почему MLflow?**
```python
import mlflow
import mlflow.sklearn
from sklearn.ensemble import RandomForestClassifier

# Experiment tracking
with mlflow.start_run(run_name="fraud_detection_rf"):
    # Log parameters
    mlflow.log_param("n_estimators", 100)
    mlflow.log_param("max_depth", 10)
    
    # Train model
    model = RandomForestClassifier(n_estimators=100, max_depth=10)
    model.fit(X_train, y_train)
    
    # Log metrics
    accuracy = model.score(X_test, y_test)
    mlflow.log_metric("accuracy", accuracy)
    mlflow.log_metric("f1_score", f1_score(y_test, model.predict(X_test)))
    
    # Log model
    mlflow.sklearn.log_model(
        model, 
        "model",
        registered_model_name="fraud_detection_rf",
        signature=mlflow.models.infer_signature(X_test, y_test)
    )
    
    # Log artifacts
    mlflow.log_artifact("feature_importance.png")
```

### Feast (Feature Store)

**Что это?**
Open source feature store для машинного обучения.

**Почему Feast?**
```python
# feature_definitions.py
from feast import Entity, FeatureView, Field
from feast.types import Float32, Int64, String

# Define entities
user = Entity(
    name="user",
    join_keys=["user_id"],
)

# Define feature views
user_transaction_stats = FeatureView(
    name="user_transaction_stats",
    entities=[user],
    schema=[
        Field(name="transaction_count_1h", dtype=Int64),
        Field(name="transaction_count_24h", dtype=Int64),
        Field(name="avg_transaction_amount", dtype=Float32),
        Field(name="max_transaction_amount", dtype=Float32),
    ],
    online=True,
    source=kafka_source,
    ttl=timedelta(days=1),
)

# Training
training_df = store.get_historical_features(
    entity_df=entity_df,
    features=[
        "user_transaction_stats:transaction_count_1h",
        "user_transaction_stats:avg_transaction_amount"
    ],
).to_df()

# Serving
online_features = store.get_online_features(
    features=[
        "user_transaction_stats:transaction_count_1h",
        "user_transaction_stats:avg_transaction_amount"
    ],
    entity_rows=[{"user_id": "USER_123"}],
).to_dict()
```

### FastAPI

**Что это?**
Современный, быстрый веб-фреймворк для создания API с Python 3.6+ основанный на стандартных Python type hints.

**Почему FastAPI?**
```python
from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel, Field
import asyncio

app = FastAPI(title="Fraud Detection API", version="2.0.0")

class TransactionRequest(BaseModel):
    transaction_id: str = Field(..., example="TXN_123")
    user_id: str = Field(..., example="USER_456")
    amount: float = Field(..., gt=0, example=150.0)
    merchant_category: str = Field(..., example="grocery")
    
    class Config:
        schema_extra = {
            "example": {
                "transaction_id": "TXN_20240115_123456",
                "user_id": "USER_000123",
                "amount": 1500.00,
                "merchant_category": "electronics"
            }
        }

@app.post("/predict", response_model=PredictionResponse)
async def predict_fraud(
    transaction: TransactionRequest,
    model_service: ModelService = Depends(get_model_service),
    feature_store: FeatureStore = Depends(get_feature_store)
):
    """
    Predict fraud probability for a transaction.
    
    - **transaction_id**: Unique transaction identifier
    - **user_id**: User identifier
    - **amount**: Transaction amount in USD
    - **merchant_category**: Category of merchant
    """
    # Async feature retrieval
    features = await feature_store.get_features_async(transaction.user_id)
    
    # Model prediction
    prediction = await model_service.predict_async(transaction, features)
    
    return PredictionResponse(**prediction)

# Автоматическая документация
# http://localhost:8001/docs - Swagger UI
# http://localhost:8001/redoc - ReDoc
```

### Docker & Kubernetes

**Что это?**
Docker - платформа контейнеризации, Kubernetes - оркестратор контейнеров.

**Почему Docker + Kubernetes?**
```yaml
# Multi-stage Dockerfile для оптимизации
FROM python:3.10-slim as builder
WORKDIR /app
COPY requirements.txt .
RUN pip wheel --no-cache-dir --no-deps --wheel-dir /app/wheels -r requirements.txt

FROM python:3.10-slim
WORKDIR /app
COPY --from=builder /app/wheels /wheels
RUN pip install --no-cache /wheels/*

# Kubernetes deployment
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fraud-api
spec:
  replicas: 3
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 1
      maxUnavailable: 0
  template:
    spec:
      containers:
      - name: fraud-api
        image: fraud-detection:v2.3.1
        resources:
          requests:
            memory: "1Gi"
            cpu: "500m"
          limits:
            memory: "2Gi"
            cpu: "1000m"
        livenessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /ready
            port: 8000
          initialDelaySeconds: 5
          periodSeconds: 5
```

### Prometheus & Grafana

**Что это?**
Prometheus - система мониторинга и alerting, Grafana - платформа визуализации.

**Почему Prometheus + Grafana?**
```python
# Интеграция метрик в FastAPI
from prometheus_client import Counter, Histogram, Gauge

# Определение метрик
fraud_predictions_total = Counter(
    'fraud_predictions_total', 
    'Total number of fraud predictions',
    ['result', 'model_version']
)

prediction_duration = Histogram(
    'prediction_duration_seconds',
    'Time spent processing prediction',
    buckets=[0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0]
)

model_accuracy = Gauge(
    'model_accuracy',
    'Current model accuracy score'
)

# Использование в коде
@prediction_duration.time()
async def predict():
    result = await model.predict(data)
    fraud_predictions_total.labels(
        result=result['is_fraud'],
        model_version=model.version
    ).inc()
    return result

# Grafana Dashboard JSON
{
  "dashboard": {
    "title": "Fraud Detection Monitoring",
    "panels": [
      {
        "title": "Predictions per Second",
        "targets": [
          {
            "expr": "rate(fraud_predictions_total[5m])"
          }
        ]
      },
      {
        "title": "Fraud Detection Rate",
        "targets": [
          {
            "expr": "sum(rate(fraud_predictions_total{result=\"true\"}[5m])) / sum(rate(fraud_predictions_total[5m]))"
          }
        ]
      }
    ]
  }
}
```

## 🎯 Итоговая матрица технологий

| Слой | Технология | Альтернативы | Почему выбрана |
|------|-----------|--------------|----------------|
| **Streaming** | Kafka | Pulsar, RabbitMQ | Industry standard, экосистема |
| **Processing** | Spark + Flink | Storm, Samza | Batch + Stream, low latency |
| **Database** | PostgreSQL | MySQL, MongoDB | Расширяемость, JSON, performance |
| **Object Store** | MinIO | Ceph, GlusterFS | S3 API, производительность |
| **Cache** | Redis | Memcached, Hazelcast | Структуры данных, persistence |
| **Orchestration** | Airflow | Prefect, Dagster | Зрелость, community |
| **Transform** | dbt | Dataform, SQLMesh | SQL-first, тестирование |
| **ML Platform** | MLflow | W&B, Neptune | Open source, интеграции |
| **Feature Store** | Feast | Tecton, Hopsworks | Open source, простота |
| **API** | FastAPI | Flask, Django | Performance, async, types |
| **Container** | Docker | Podman | Стандарт индустрии |
| **Orchestrator** | Kubernetes | Nomad, Swarm | Экосистема, масштаб |
| **Monitoring** | Prometheus | Datadog, New Relic | Open source, flexibility |
| **Visualization** | Grafana | Kibana, Tableau | Интеграции, customization |

---

*Следующий раздел: [Установка и настройка →](./04_installation.md)*
