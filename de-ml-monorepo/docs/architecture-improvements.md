# Архитектурные улучшения для масштабируемости и отказоустойчивости

## 1. Масштабируемость

### Текущие ограничения
- Single-node PostgreSQL может стать узким местом
- Отсутствие автомасштабирования для сервисов
- Нет load balancing для API endpoints
- LocalExecutor в Airflow ограничивает параллелизм

### Рекомендуемые улучшения

#### 1.1 База данных
```yaml
# Добавить PostgreSQL с репликацией
postgres-primary:
  image: bitnami/postgresql:15
  environment:
    POSTGRESQL_REPLICATION_MODE: master
    POSTGRESQL_REPLICATION_USER: replicator
    POSTGRESQL_REPLICATION_PASSWORD: ${REPLICATION_PASSWORD}

postgres-replica:
  image: bitnami/postgresql:15
  environment:
    POSTGRESQL_REPLICATION_MODE: slave
    POSTGRESQL_MASTER_HOST: postgres-primary
  deploy:
    replicas: 2
```

#### 1.2 Kubernetes для production
```yaml
# k8s/fraud-detection-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fraud-detection-api
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
        resources:
          requests:
            memory: "512Mi"
            cpu: "500m"
          limits:
            memory: "1Gi"
            cpu: "1000m"
---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: fraud-api-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: fraud-detection-api
  minReplicas: 3
  maxReplicas: 10
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  - type: Resource
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: 80
```

#### 1.3 Кеширование и CDN
```python
# Redis Cluster для распределенного кеша
REDIS_CLUSTER_CONFIG = {
    'startup_nodes': [
        {'host': 'redis-1', 'port': 6379},
        {'host': 'redis-2', 'port': 6379},
        {'host': 'redis-3', 'port': 6379}
    ],
    'skip_full_coverage_check': True
}

# Добавить слой кеширования для feature store
@cache.memoize(timeout=300)  # 5 минут
def get_user_features(user_id: str):
    return feature_store.get_online_features(
        entity_rows=[{'user_id': user_id}]
    )
```

## 2. Отказоустойчивость

### Текущие проблемы
- Нет multi-region backup
- Отсутствует disaster recovery план
- Single point of failure для некоторых сервисов
- Нет автоматического failover

### Рекомендуемые улучшения

#### 2.1 Circuit Breaker Pattern
```python
# fraud_detection/utils/circuit_breaker.py
from pybreaker import CircuitBreaker

# Конфигурация circuit breaker
db_breaker = CircuitBreaker(
    fail_max=5,
    reset_timeout=60,
    exclude=[KeyError]
)

@db_breaker
def get_user_transaction_history(user_id: str):
    """Защищенный вызов к базе данных"""
    return db.query(
        "SELECT * FROM transactions WHERE user_id = %s",
        (user_id,)
    )
```

#### 2.2 Retry механизм с backoff
```python
from tenacity import retry, stop_after_attempt, wait_exponential

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10)
)
def call_external_api(transaction_data):
    """Вызов внешнего API с retry логикой"""
    response = requests.post(
        EXTERNAL_API_URL,
        json=transaction_data,
        timeout=5
    )
    response.raise_for_status()
    return response.json()
```

#### 2.3 Health Check улучшения
```python
# fraud_detection/api/health.py
from fastapi import APIRouter, Response
import asyncio

router = APIRouter()

@router.get("/health/live")
async def liveness():
    """Проверка что сервис жив"""
    return {"status": "alive"}

@router.get("/health/ready")
async def readiness():
    """Проверка готовности всех зависимостей"""
    checks = await asyncio.gather(
        check_database(),
        check_redis(),
        check_kafka(),
        check_model_loaded(),
        return_exceptions=True
    )
    
    if any(isinstance(check, Exception) for check in checks):
        return Response(status_code=503)
    
    return {"status": "ready", "checks": checks}
```

#### 2.4 Backup и Recovery
```yaml
# backup-cronjob.yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: postgres-backup
spec:
  schedule: "0 2 * * *"  # Ежедневно в 2 AM
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: postgres-backup
            image: postgres:15
            command:
            - /bin/bash
            - -c
            - |
              pg_dump -h postgres-primary -U $POSTGRES_USER \
                $POSTGRES_DB | gzip > /backups/backup-$(date +%Y%m%d).sql.gz
              # Upload to S3
              aws s3 cp /backups/backup-$(date +%Y%m%d).sql.gz \
                s3://fraud-detection-backups/postgres/
```

## 3. Мониторинг и Observability

### 3.1 Distributed Tracing
```yaml
# Jaeger для трассировки
jaeger:
  image: jaegertracing/all-in-one:latest
  ports:
    - "16686:16686"  # UI
    - "14268:14268"  # Collector
  environment:
    COLLECTOR_ZIPKIN_HTTP_PORT: 9411
```

### 3.2 Метрики производительности
```python
# Prometheus метрики
from prometheus_client import Counter, Histogram, Gauge

# Счетчики
fraud_predictions = Counter(
    'fraud_predictions_total',
    'Total fraud predictions',
    ['result', 'model_version']
)

# Гистограмма латентности
prediction_latency = Histogram(
    'prediction_latency_seconds',
    'Prediction latency in seconds',
    buckets=[0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]
)

# Текущее состояние
model_accuracy = Gauge(
    'model_accuracy_score',
    'Current model accuracy'
)
```

## 4. Безопасность

### 4.1 Secrets Management
```yaml
# Использование HashiCorp Vault
vault:
  image: vault:latest
  cap_add:
    - IPC_LOCK
  environment:
    VAULT_DEV_ROOT_TOKEN_ID: myroot
  ports:
    - "8200:8200"
```

### 4.2 Network Policies
```yaml
# k8s/network-policy.yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: fraud-api-network-policy
spec:
  podSelector:
    matchLabels:
      app: fraud-api
  policyTypes:
  - Ingress
  - Egress
  ingress:
  - from:
    - podSelector:
        matchLabels:
          app: nginx-ingress
    ports:
    - protocol: TCP
      port: 8000
  egress:
  - to:
    - podSelector:
        matchLabels:
          app: postgres
    ports:
    - protocol: TCP
      port: 5432
```

## 5. Тестирование устойчивости

### 5.1 Chaos Engineering
```yaml
# Chaos Mesh для тестирования отказов
chaos-experiment:
  apiVersion: chaos-mesh.org/v1alpha1
  kind: PodChaos
  metadata:
    name: fraud-api-failure
  spec:
    action: pod-failure
    mode: one
    duration: "30s"
    selector:
      labelSelectors:
        app: fraud-api
```

### 5.2 Load Testing
```python
# locustfile.py для нагрузочного тестирования
from locust import HttpUser, task, between

class FraudDetectionUser(HttpUser):
    wait_time = between(0.1, 0.5)
    
    @task(10)
    def predict_fraud(self):
        self.client.post("/predict", json={
            "transaction_id": fake.uuid4(),
            "amount": random.uniform(10, 1000),
            "merchant_category": random.choice(["grocery", "gas", "online"]),
            "user_id": f"USER_{random.randint(1, 10000):06d}"
        })
    
    @task(1)
    def health_check(self):
        self.client.get("/health/ready")
```

## 6. Оптимизация производительности

### 6.1 Batch Inference
```python
# Батчевая обработка для высокой пропускной способности
async def batch_predict(transactions: List[Transaction]):
    """Обработка batch транзакций"""
    # Группировка в батчи
    batch_size = 100
    results = []
    
    for i in range(0, len(transactions), batch_size):
        batch = transactions[i:i + batch_size]
        
        # Параллельное получение фичей
        features = await asyncio.gather(*[
            get_features_async(t) for t in batch
        ])
        
        # Batch prediction
        predictions = model.predict_batch(features)
        results.extend(predictions)
    
    return results
```

### 6.2 GPU Acceleration
```dockerfile
# Dockerfile.gpu для GPU inference
FROM nvidia/cuda:11.8.0-runtime-ubuntu22.04

# Install Python and dependencies
RUN apt-get update && apt-get install -y python3-pip

# Install GPU-enabled libraries
RUN pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu118
RUN pip install cupy-cuda118 rapids-cudf

# Enable GPU for XGBoost
ENV USE_CUDA=1
```

## 7. Compliance и Audit

### 7.1 Audit Logging
```python
# Логирование всех операций
from fraud_detection.utils.audit import audit_log

@audit_log(action="predict_fraud")
async def predict(transaction: Transaction, user: User):
    """Prediction с audit trail"""
    result = await model.predict(transaction)
    
    # Сохранение в audit log
    await save_audit_record({
        "user_id": user.id,
        "transaction_id": transaction.id,
        "prediction": result,
        "model_version": model.version,
        "timestamp": datetime.utcnow()
    })
    
    return result
```

## Приоритеты внедрения

1. **Немедленно**: Circuit breakers, retry логика, улучшенный health checks
2. **Краткосрочно**: Redis cluster, PostgreSQL репликация, backup стратегия
3. **Среднесрочно**: Kubernetes migration, auto-scaling, distributed tracing
4. **Долгосрочно**: Multi-region deployment, chaos engineering, GPU acceleration
