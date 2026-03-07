# 🏗️ Архитектура системы

## Общий обзор архитектуры

### Философия проектирования

Архитектура платформы построена на следующих принципах:
- **Микросервисная архитектура**: Каждый компонент изолирован и может масштабироваться независимо
- **Event-driven подход**: Асинхронная обработка через message brokers
- **Cloud-native**: Готовность к deployment в любом облаке
- **Data mesh**: Децентрализованное владение данными
- **GitOps**: Инфраструктура как код

### Высокоуровневая архитектура

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              EXTERNAL LAYER                                      │
├─────────────────────────────────────────────────────────────────────────────────┤
│  Banking APIs │ Static Datasets │ Synthetic Generators │ User Applications      │
└───────┬───────┴─────────┬───────┴──────────┬──────────┴──────────┬─────────────┘
        │                 │                   │                      │
┌───────▼─────────────────▼───────────────────▼──────────────────────▼─────────────┐
│                           INGESTION & STREAMING LAYER                             │
├───────────────────────────────────────────────────────────────────────────────────┤
│                                                                                   │
│  ┌─────────────┐    ┌──────────────────┐    ┌─────────────────┐                │
│  │   Apache    │    │  Apache Spark    │    │  Apache Flink   │                │
│  │   Kafka     │◄──►│  Streaming       │◄──►│  (Low Latency)  │                │
│  │  (Topics)   │    │  (Micro-batch)   │    │  (True Stream)  │                │
│  └─────────────┘    └──────────────────┘    └─────────────────┘                │
│         │                    │                        │                          │
└─────────┼────────────────────┼────────────────────────┼──────────────────────────┘
          │                    │                        │
┌─────────▼────────────────────▼────────────────────────▼──────────────────────────┐
│                            STORAGE & PROCESSING LAYER                             │
├───────────────────────────────────────────────────────────────────────────────────┤
│                                                                                   │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │ PostgreSQL   │  │    MinIO     │  │    Redis     │  │     HDFS     │       │
│  │   (OLTP)     │  │  (Objects)   │  │   (Cache)    │  │ (Big Files)  │       │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘       │
│         │                  │                  │                  │               │
│  ┌──────▼──────────────────▼──────────────────▼──────────────────▼───┐          │
│  │                    Apache Airflow (Orchestration)                 │          │
│  └────────────────────────────────────────────────────────────────────┘          │
│                                                                                   │
└───────────────────────────────────────────────────────────────────────────────────┘
                                      │
┌─────────────────────────────────────▼─────────────────────────────────────────────┐
│                               ML & ANALYTICS LAYER                                │
├───────────────────────────────────────────────────────────────────────────────────┤
│                                                                                   │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │    Feast     │  │    MLflow    │  │     dbt      │  │   Jupyter    │       │
│  │(Feature Store)│  │(Experiments) │  │(Transforms)  │  │  (Research)  │       │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘       │
│         │                  │                  │                  │               │
│  ┌──────▼──────────────────▼──────────────────▼──────────────────▼───┐          │
│  │                        Model Training & Serving                   │          │
│  └────────────────────────────────────────────────────────────────────┘          │
│                                                                                   │
└───────────────────────────────────────────────────────────────────────────────────┘
                                      │
┌─────────────────────────────────────▼─────────────────────────────────────────────┐
│                            APPLICATION & API LAYER                                │
├───────────────────────────────────────────────────────────────────────────────────┤
│                                                                                   │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │  Fraud API   │  │   Admin UI   │  │  Dashboards  │  │   Webhooks   │       │
│  │  (FastAPI)   │  │  (Streamlit) │  │  (Grafana)   │  │  (External)  │       │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘       │
│                                                                                   │
└───────────────────────────────────────────────────────────────────────────────────┘
                                      │
┌─────────────────────────────────────▼─────────────────────────────────────────────┐
│                          MONITORING & OBSERVABILITY                               │
├───────────────────────────────────────────────────────────────────────────────────┤
│  Prometheus │ Grafana │ ELK Stack │ Jaeger │ Alert Manager │ Custom Metrics     │
└───────────────────────────────────────────────────────────────────────────────────┘
```

## 📦 Компонентная архитектура

### Data Sources Layer

#### Banking APIs Integration
```python
# Архитектурный паттерн: Adapter Pattern
class BankingAPIAdapter:
    """Унифицированный интерфейс для различных banking APIs"""
    
    def __init__(self, api_type: str):
        self.adapter = self._get_adapter(api_type)
    
    def _get_adapter(self, api_type: str):
        adapters = {
            'plaid': PlaidAdapter(),
            'stripe': StripeAdapter(),
            'paypal': PayPalAdapter()
        }
        return adapters.get(api_type)
    
    async def fetch_transactions(self, params: Dict) -> List[Transaction]:
        """Полиморфный метод для получения транзакций"""
        return await self.adapter.fetch_transactions(params)
```

**Почему Adapter Pattern?**
- Унификация различных API интерфейсов
- Легкое добавление новых источников
- Изоляция изменений в external API

#### Data Generators
**Архитектура генераторов:**
```
FraudTransactionGenerator
├── UserProfileGenerator      # Генерация профилей пользователей
├── MerchantProfileGenerator   # Генерация профилей мерчантов
├── TransactionGenerator       # Основная генерация транзакций
│   ├── NormalTransactions    # 98% нормальных транзакций
│   └── FraudTransactions     # 2% мошеннических транзакций
└── StreamEmitter             # Отправка в Kafka
```

### Streaming Layer

#### Kafka Architecture
```yaml
# Топология Kafka кластера
Kafka Cluster:
  Brokers: 3  # Минимум для отказоустойчивости
  Zookeeper: 3  # Координация кластера
  
Topics:
  fraud-transactions:
    partitions: 10
    replication-factor: 3
    retention: 7 days
    compression: snappy
    
  fraud-predictions:
    partitions: 5
    replication-factor: 3
    retention: 30 days
    
  fraud-alerts:
    partitions: 3
    replication-factor: 3
    retention: 90 days
    
Consumer Groups:
  - spark-streaming-consumer
  - flink-consumer
  - audit-consumer
  - analytics-consumer
```

**Дизайн решения:**
- **Partitioning by user_id**: Гарантирует порядок для пользователя
- **Replication factor 3**: Выживание при падении 2 brokers
- **Compression**: Снижение network I/O на 70%

#### Spark Streaming Architecture
```scala
// Архитектура Spark Streaming Job
StreamingContext
├── Input DStream (Kafka)
├── Transformations
│   ├── Parse JSON
│   ├── Enrich with Features
│   ├── Apply ML Model
│   └── Calculate Risk Score
├── State Management
│   ├── updateStateByKey (user stats)
│   └── mapWithState (session tracking)
└── Output Operations
    ├── Save to PostgreSQL
    ├── Write to MinIO
    └── Send Alerts to Kafka
```

**Checkpointing стратегия:**
```python
# Надежное восстановление после сбоев
spark.conf.set("spark.streaming.checkpoint.interval", "30s")
spark.conf.set("spark.streaming.backpressure.enabled", "true")
spark.conf.set("spark.streaming.kafka.maxRatePerPartition", "1000")
```

#### Flink Architecture
```java
// Event-time processing для корректной обработки late data
DataStream<Transaction> transactions = env
    .addSource(new FlinkKafkaConsumer<>(...))
    .assignTimestampsAndWatermarks(
        WatermarkStrategy
            .<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(20))
            .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
    );

// Stateful processing
transactions
    .keyBy(Transaction::getUserId)
    .process(new FraudDetectionFunction())
    .addSink(new FlinkKafkaProducer<>(...));
```

**State Backend Configuration:**
```yaml
state.backend: rocksdb  # Для больших состояний
state.checkpoints.dir: s3://fraud-detection/flink-checkpoints
state.backend.incremental: true  # Инкрементальные checkpoints
```

### Storage Layer

#### PostgreSQL Schema Design
```sql
-- Оптимизированная схема для fraud detection
CREATE SCHEMA fraud_detection;

-- Основная таблица транзакций с партиционированием
CREATE TABLE fraud_detection.transactions (
    transaction_id UUID PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    user_id VARCHAR(50) NOT NULL,
    merchant_id VARCHAR(50) NOT NULL,
    amount DECIMAL(12,2) NOT NULL,
    currency VARCHAR(3) DEFAULT 'USD',
    merchant_category VARCHAR(50),
    channel VARCHAR(20),
    location JSONB,
    is_fraud BOOLEAN DEFAULT FALSE,
    fraud_score DECIMAL(5,4),
    model_version VARCHAR(20),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
) PARTITION BY RANGE (timestamp);

-- Индексы для оптимизации запросов
CREATE INDEX idx_user_timestamp ON fraud_detection.transactions(user_id, timestamp DESC);
CREATE INDEX idx_merchant_timestamp ON fraud_detection.transactions(merchant_id, timestamp DESC);
CREATE INDEX idx_fraud_transactions ON fraud_detection.transactions(timestamp DESC) 
    WHERE is_fraud = TRUE;
CREATE INDEX idx_location_gin ON fraud_detection.transactions USING GIN(location);

-- Материализованные представления для аналитики
CREATE MATERIALIZED VIEW fraud_detection.user_risk_profiles AS
SELECT 
    user_id,
    COUNT(*) as total_transactions,
    SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) as fraud_count,
    AVG(amount) as avg_transaction_amount,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY amount) as p95_amount,
    MAX(timestamp) as last_transaction_time
FROM fraud_detection.transactions
WHERE timestamp > CURRENT_TIMESTAMP - INTERVAL '90 days'
GROUP BY user_id;

CREATE INDEX idx_user_risk_profile ON fraud_detection.user_risk_profiles(user_id);
```

#### MinIO/S3 Storage Structure
```
s3://fraud-detection/
├── raw-data/
│   ├── transactions/
│   │   └── year=2024/month=01/day=15/
│   │       ├── part-00000.parquet
│   │       └── part-00001.parquet
│   └── external-data/
├── processed-data/
│   ├── features/
│   └── aggregates/
├── models/
│   ├── production/
│   │   └── v2.3.1/
│   │       ├── model.pkl
│   │       └── metadata.json
│   └── experiments/
├── checkpoints/
│   ├── spark/
│   └── flink/
└── backups/
    └── postgres/
```

#### Redis Architecture
```python
# Redis использование для различных целей
# 1. Feature Cache (Hash)
HSET user_features:USER_123 transaction_count_1h 5 avg_amount 125.50

# 2. Rate Limiting (Sorted Set)
ZADD rate_limit:api 1642332761 "client_ip:192.168.1.1"

# 3. Real-time Aggregates (HyperLogLog)
PFADD unique_users:2024-01-15 USER_123 USER_456

# 4. Session Storage (String with TTL)
SETEX session:abc123 3600 '{"user_id": "USER_123", "permissions": [...]}'

# 5. Pub/Sub for events
PUBLISH fraud_alerts '{"transaction_id": "TXN_789", "risk_score": 0.95}'
```

### Processing Layer

#### Airflow DAG Architecture
```python
# Модульная архитектура DAGs
dags/
├── fraud_detection/
│   ├── __init__.py
│   ├── daily_pipeline.py       # Основной ежедневный pipeline
│   ├── feature_engineering.py  # Feature generation DAG
│   ├── model_training.py       # ML training DAG
│   └── data_quality.py         # Data quality checks DAG
├── common/
│   ├── operators/              # Custom operators
│   ├── sensors/                # Custom sensors
│   └── hooks/                  # External system hooks
└── config/
    └── dag_config.yaml         # Централизованная конфигурация
```

**DAG Dependencies:**
```python
# Использование ExternalTaskSensor для зависимостей между DAGs
wait_for_data = ExternalTaskSensor(
    task_id='wait_for_data_pipeline',
    external_dag_id='data_ingestion',
    external_task_id='validate_data',
    allowed_states=['success'],
    failed_states=['failed', 'skipped']
)
```

### ML Layer

#### MLflow Architecture
```
MLflow Components:
├── Tracking Server
│   ├── Experiments/
│   ├── Runs/
│   └── Metrics & Parameters
├── Model Registry
│   ├── Model Versions
│   ├── Stage Transitions
│   └── Model Lineage
├── Projects
│   ├── Conda Environments
│   └── Docker Containers
└── Models
    ├── Artifacts Storage (MinIO)
    └── Deployment Targets
```

#### Feature Store (Feast) Architecture
```python
# Feature Store Design
@feature_view(
    entities=["user_id"],
    ttl=Duration(hours=24),
    online=True,
    batch_source=BigQuerySource(...),
    stream_source=KafkaSource(...)
)
class UserTransactionFeatures(FeatureView):
    """Признаки пользовательских транзакций"""
    
    transaction_count_1h = Field(dtype=Int64)
    transaction_count_24h = Field(dtype=Int64)
    unique_merchants_7d = Field(dtype=Int64)
    avg_transaction_amount = Field(dtype=Float64)
    
    @transformation
    def compute_velocity_features(df: DataFrame) -> DataFrame:
        """Вычисление velocity features"""
        return df.groupby("user_id").agg(...)
```

### API Layer

#### FastAPI Application Structure
```python
# Модульная структура API
app/
├── api/
│   ├── v1/
│   │   ├── endpoints/
│   │   │   ├── predict.py      # Prediction endpoints
│   │   │   ├── health.py       # Health checks
│   │   │   └── admin.py        # Admin operations
│   │   └── dependencies.py     # Shared dependencies
├── core/
│   ├── config.py               # Configuration
│   ├── security.py             # Authentication/Authorization
│   └── exceptions.py           # Custom exceptions
├── models/
│   ├── domain.py               # Domain models
│   └── schemas.py              # Pydantic schemas
├── services/
│   ├── prediction.py           # Business logic
│   ├── feature_store.py        # Feature retrieval
│   └── model_loader.py         # Model management
└── middleware/
    ├── logging.py              # Request logging
    ├── metrics.py              # Prometheus metrics
    └── rate_limit.py           # Rate limiting
```

### Monitoring Layer

#### Observability Stack
```yaml
# Полный observability stack
Metrics:
  Prometheus:
    - Node Exporter (system metrics)
    - Kafka Exporter
    - Postgres Exporter
    - Custom Application Metrics

Logs:
  ELK Stack:
    - Elasticsearch (storage)
    - Logstash (processing)
    - Kibana (visualization)
    - Filebeat (collection)

Traces:
  Jaeger:
    - Distributed tracing
    - Service dependencies
    - Latency analysis

Dashboards:
  Grafana:
    - System Dashboard
    - Business Metrics
    - ML Model Performance
    - Fraud Detection KPIs
```

## 🔄 Паттерны взаимодействия

### Синхронные взаимодействия
```
Client -> API Gateway -> Load Balancer -> API Service -> Response
                                              │
                                              ├─> Feature Store
                                              ├─> Model Service
                                              └─> Cache
```

### Асинхронные взаимодействия
```
Producer -> Kafka Topic -> Consumer Group -> Processing -> Storage
                               │                  │
                               │                  └─> Notifications
                               └─> Dead Letter Queue
```

### Batch Processing Pattern
```
Scheduler (Airflow) -> Extract -> Transform -> Load -> Validate
                          │           │          │         │
                          S3       Spark    PostgreSQL  G.E.
```

## 🔐 Безопасность архитектуры

### Network Security
```yaml
# Сетевая изоляция
Networks:
  - Public Subnet: API Gateway, Load Balancer
  - Private Subnet: Application Services
  - Data Subnet: Databases, Kafka
  - Management Subnet: Monitoring, Admin tools
  
Security Groups:
  - Ingress rules: Whitelist based
  - Egress rules: Least privilege
```

### Data Security
- **Encryption at rest**: AES-256
- **Encryption in transit**: TLS 1.3
- **Key management**: HashiCorp Vault
- **Access control**: RBAC + ABAC

## 🚀 Масштабирование

### Horizontal Scaling Strategy
```yaml
# Auto-scaling policies
API Services:
  min_replicas: 3
  max_replicas: 20
  target_cpu: 70%
  target_memory: 80%
  
Kafka Partitions:
  initial: 10
  max: 100
  rebalance_strategy: cooperative-sticky
  
Database:
  read_replicas: 3
  connection_pool: 100
  pgbouncer: enabled
```

### Vertical Scaling Limits
| Component | Min Resources | Max Resources | Scaling Trigger |
|-----------|--------------|---------------|-----------------|
| API Pod | 1 CPU, 2GB RAM | 4 CPU, 8GB RAM | Request latency |
| Spark Executor | 2 CPU, 4GB RAM | 8 CPU, 32GB RAM | Processing time |
| Kafka Broker | 4 CPU, 16GB RAM | 16 CPU, 64GB RAM | Throughput |
| PostgreSQL | 8 CPU, 32GB RAM | 32 CPU, 256GB RAM | Connections |

---

*Следующий раздел: [Технологический стек →](./03_technology_stack.md)*
