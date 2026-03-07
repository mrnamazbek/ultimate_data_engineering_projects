# 💡 Best Practices и примеры использования

## Обзор Best Practices

Этот раздел содержит проверенные практики и рекомендации для эффективной работы с платформой fraud detection, основанные на опыте production использования.

## 📋 Содержание

1. [Разработка и архитектура](#разработка-и-архитектура)
2. [Data Engineering практики](#data-engineering-практики)
3. [Machine Learning практики](#machine-learning-практики)
4. [API и микросервисы](#api-и-микросервисы)
5. [Мониторинг и отладка](#мониторинг-и-отладка)
6. [Безопасность](#безопасность)
7. [Производительность](#производительность)
8. [Примеры использования](#примеры-использования)

---

## 🏗️ Разработка и архитектура

### 1. Микросервисная архитектура

✅ **DO:**
```python
# Каждый сервис отвечает за одну область
class FraudDetectionService:
    """Только fraud detection логика"""
    async def predict(self, transaction: Transaction) -> Prediction:
        features = await self.feature_service.get_features(transaction)
        prediction = await self.model_service.predict(features)
        await self.monitoring_service.log_prediction(prediction)
        return prediction

class FeatureService:
    """Только работа с признаками"""
    async def get_features(self, transaction: Transaction) -> Features:
        # Изолированная логика feature engineering
        pass
```

❌ **DON'T:**
```python
# Монолитный подход - все в одном классе
class FraudSystem:
    def do_everything(self, transaction):
        # Feature engineering
        # Model prediction
        # Monitoring
        # Database updates
        # API responses
        # Все смешано в одном месте
```

### 2. Dependency Injection

✅ **DO:**
```python
from typing import Protocol

class FeatureStoreProtocol(Protocol):
    async def get_features(self, entity_id: str) -> dict:
        ...

class FraudDetectionService:
    def __init__(self, feature_store: FeatureStoreProtocol):
        self.feature_store = feature_store  # Инжектируем зависимость
    
    async def process(self, transaction: Transaction):
        features = await self.feature_store.get_features(transaction.user_id)
        # Легко тестировать с mock объектами
```

### 3. Configuration Management

✅ **DO:**
```python
# config/settings.py
from pydantic import BaseSettings, Field

class Settings(BaseSettings):
    # Типизированные настройки с валидацией
    kafka_brokers: list[str] = Field(..., env="KAFKA_BROKERS")
    model_threshold: float = Field(0.7, ge=0.0, le=1.0)
    redis_url: str = Field(..., env="REDIS_URL")
    
    class Config:
        env_file = ".env"
        case_sensitive = False

settings = Settings()
```

### 4. Error Handling

✅ **DO:**
```python
class FraudDetectionError(Exception):
    """Базовый класс для ошибок fraud detection"""
    pass

class FeatureExtractionError(FraudDetectionError):
    """Ошибка при извлечении признаков"""
    pass

async def predict_fraud(transaction: Transaction) -> Result:
    try:
        features = await extract_features(transaction)
    except FeatureExtractionError as e:
        logger.error(f"Feature extraction failed", extra={
            "transaction_id": transaction.id,
            "error": str(e)
        })
        # Fallback на baseline модель
        return await baseline_predict(transaction)
    except Exception as e:
        # Неожиданная ошибка
        await alert_manager.send_critical(
            f"Unexpected error in fraud detection: {e}"
        )
        raise
```

---

## 📊 Data Engineering практики

### 1. Schema Evolution

✅ **DO:**
```python
# Использование Apache Avro для schema evolution
from confluent_kafka import avro

transaction_schema = avro.loads('''
{
    "type": "record",
    "name": "Transaction",
    "fields": [
        {"name": "transaction_id", "type": "string"},
        {"name": "amount", "type": "double"},
        {"name": "merchant_category", "type": ["null", "string"], "default": null},
        {"name": "risk_score", "type": ["null", "double"], "default": null}
    ]
}
''')

# Backward compatible изменения
# - Добавление полей с default значениями
# - Использование union types для optional полей
```

### 2. Data Quality Checks

✅ **DO:**
```python
# data_quality/validators.py
from great_expectations.dataset import PandasDataset

def validate_transaction_data(df: pd.DataFrame) -> bool:
    """Валидация качества данных перед обработкой"""
    ge_df = PandasDataset(df)
    
    # Обязательные проверки
    results = []
    results.append(ge_df.expect_column_to_exist("transaction_id"))
    results.append(ge_df.expect_column_values_to_be_unique("transaction_id"))
    results.append(ge_df.expect_column_values_to_not_be_null("amount"))
    results.append(ge_df.expect_column_values_to_be_between(
        "amount", min_value=0.01, max_value=1000000
    ))
    
    # Логирование проблем
    failures = [r for r in results if not r.success]
    if failures:
        logger.warning(f"Data quality issues found: {len(failures)}")
        for failure in failures:
            logger.warning(f"Check failed: {failure}")
    
    return all(r.success for r in results)
```

### 3. Идемпотентность операций

✅ **DO:**
```python
# Идемпотентная обработка с использованием transaction_id
async def process_transaction(transaction: Transaction):
    # Проверка, не обработана ли транзакция
    if await redis_client.exists(f"processed:{transaction.id}"):
        logger.info(f"Transaction {transaction.id} already processed")
        return await get_existing_result(transaction.id)
    
    # Обработка
    result = await predict_fraud(transaction)
    
    # Сохранение результата с TTL
    await redis_client.setex(
        f"processed:{transaction.id}",
        86400,  # 24 часа
        json.dumps(result)
    )
    
    return result
```

### 4. Partitioning Strategy

✅ **DO:**
```sql
-- Партиционирование по времени для эффективного архивирования
CREATE TABLE transactions (
    transaction_id UUID,
    timestamp TIMESTAMPTZ,
    amount DECIMAL(12,2),
    -- другие поля
) PARTITION BY RANGE (timestamp);

-- Автоматическое создание партиций
CREATE OR REPLACE FUNCTION create_monthly_partitions()
RETURNS void AS $$
DECLARE
    start_date date;
    end_date date;
BEGIN
    FOR i IN 0..2 LOOP  -- Создать партиции на 3 месяца вперед
        start_date := date_trunc('month', CURRENT_DATE + (i || ' months')::interval);
        end_date := start_date + interval '1 month';
        
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS transactions_%s PARTITION OF transactions FOR VALUES FROM (%L) TO (%L)',
            to_char(start_date, 'YYYY_MM'),
            start_date,
            end_date
        );
    END LOOP;
END;
$$ LANGUAGE plpgsql;
```

---

## 🤖 Machine Learning практики

### 1. Feature Store Management

✅ **DO:**
```python
# features/feature_store_manager.py
class FeatureStoreManager:
    """Централизованное управление признаками"""
    
    def __init__(self):
        self.feature_definitions = {}
        self.feature_versions = {}
    
    def register_feature(self, 
                        name: str, 
                        computation: Callable,
                        dependencies: List[str],
                        ttl: timedelta):
        """Регистрация нового признака"""
        self.feature_definitions[name] = {
            'computation': computation,
            'dependencies': dependencies,
            'ttl': ttl,
            'version': hashlib.md5(
                str(computation.__code__.co_code).encode()
            ).hexdigest()
        }
    
    async def get_features(self, entity_id: str, feature_names: List[str]) -> dict:
        """Получение признаков с автоматическим вычислением"""
        features = {}
        
        for name in feature_names:
            # Проверка кеша
            cached = await self.get_from_cache(entity_id, name)
            if cached is not None:
                features[name] = cached
                continue
            
            # Вычисление
            feature_def = self.feature_definitions[name]
            dependencies = await self.get_features(
                entity_id, 
                feature_def['dependencies']
            )
            
            value = await feature_def['computation'](entity_id, dependencies)
            features[name] = value
            
            # Сохранение в кеш
            await self.save_to_cache(entity_id, name, value, feature_def['ttl'])
        
        return features
```

### 2. Model Versioning

✅ **DO:**
```python
# models/versioning.py
class ModelVersion:
    def __init__(self, version: str, model_path: str):
        self.version = version
        self.model_path = model_path
        self.loaded_at = None
        self.metrics = {}
    
    def to_dict(self) -> dict:
        return {
            'version': self.version,
            'model_path': self.model_path,
            'loaded_at': self.loaded_at.isoformat() if self.loaded_at else None,
            'metrics': self.metrics,
            'git_commit': os.getenv('GIT_COMMIT', 'unknown'),
            'training_date': self._get_training_date()
        }

class ModelManager:
    def __init__(self):
        self.models = {}
        self.active_model = None
    
    async def deploy_model(self, version: str, canary_percent: float = 0.0):
        """Развертывание модели с canary deployment"""
        new_model = await self.load_model(version)
        
        if canary_percent > 0:
            # Canary deployment
            self.models['canary'] = new_model
            self.canary_percent = canary_percent
            logger.info(f"Deployed {version} as canary ({canary_percent}%)")
        else:
            # Full deployment
            self.models['previous'] = self.active_model
            self.active_model = new_model
            logger.info(f"Deployed {version} as primary model")
    
    async def predict(self, features: pd.DataFrame) -> dict:
        """Предсказание с учетом canary"""
        if 'canary' in self.models and random.random() < self.canary_percent:
            model = self.models['canary']
            model_version = 'canary'
        else:
            model = self.active_model
            model_version = 'primary'
        
        prediction = await model.predict(features)
        prediction['model_version'] = model_version
        
        return prediction
```

### 3. Experiment Tracking

✅ **DO:**
```python
# experiments/tracker.py
import mlflow
from typing import Dict, Any

class ExperimentTracker:
    def __init__(self, experiment_name: str):
        self.experiment_name = experiment_name
        mlflow.set_experiment(experiment_name)
    
    def track_experiment(self, 
                        params: Dict[str, Any],
                        metrics: Dict[str, float],
                        artifacts: Dict[str, str],
                        tags: Dict[str, str] = None):
        """Полное отслеживание эксперимента"""
        with mlflow.start_run():
            # Параметры
            mlflow.log_params(params)
            
            # Метрики
            mlflow.log_metrics(metrics)
            
            # Артефакты
            for name, path in artifacts.items():
                mlflow.log_artifact(path, name)
            
            # Теги
            if tags:
                mlflow.set_tags(tags)
            
            # Системная информация
            mlflow.set_tags({
                'platform': platform.system(),
                'python_version': platform.python_version(),
                'user': os.getenv('USER', 'unknown'),
                'git_commit': self._get_git_commit()
            })
            
            # Сохранение кода
            mlflow.log_artifact(__file__, "code")
```

### 4. Model Monitoring

✅ **DO:**
```python
# monitoring/model_monitor.py
class ModelMonitor:
    def __init__(self, baseline_metrics: Dict[str, float]):
        self.baseline_metrics = baseline_metrics
        self.alert_thresholds = {
            'accuracy': 0.05,  # 5% degradation
            'precision': 0.10,  # 10% degradation
            'recall': 0.10,
            'f1_score': 0.07
        }
    
    async def monitor_predictions(self, 
                                 predictions: pd.DataFrame,
                                 actuals: pd.DataFrame):
        """Мониторинг качества предсказаний"""
        # Вычисление текущих метрик
        current_metrics = self.calculate_metrics(predictions, actuals)
        
        # Сравнение с baseline
        degradations = {}
        for metric, value in current_metrics.items():
            baseline = self.baseline_metrics.get(metric, value)
            degradation = (baseline - value) / baseline
            
            if degradation > self.alert_thresholds.get(metric, 0.1):
                degradations[metric] = {
                    'baseline': baseline,
                    'current': value,
                    'degradation_percent': degradation * 100
                }
        
        # Отправка алертов
        if degradations:
            await self.send_degradation_alert(degradations)
        
        # Логирование метрик
        for metric, value in current_metrics.items():
            model_accuracy.labels(metric_name=metric).set(value)
        
        return current_metrics
```

---

## 🌐 API и микросервисы

### 1. API Design

✅ **DO:**
```python
# api/design.py
from fastapi import APIRouter, Query, Path
from typing import Optional
from datetime import datetime

router = APIRouter(prefix="/api/v1", tags=["fraud-detection"])

@router.post("/transactions/{transaction_id}/predict")
async def predict_fraud(
    transaction_id: str = Path(..., description="Unique transaction ID"),
    request: TransactionRequest,
    include_explanation: bool = Query(False, description="Include SHAP explanation"),
    webhook_url: Optional[str] = Query(None, description="Webhook for async response")
):
    """
    Predict fraud for a transaction
    
    - **transaction_id**: Unique identifier for idempotency
    - **request**: Transaction details
    - **include_explanation**: Whether to include model explanation
    - **webhook_url**: URL for async callback (for long-running predictions)
    """
    # Версионирование API через prefix
    # Понятные параметры пути
    # Опциональные query параметры для расширенной функциональности
    pass
```

### 2. Rate Limiting

✅ **DO:**
```python
# middleware/rate_limiter.py
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

# Создание limiter с кастомной функцией для получения ключа
def get_rate_limit_key(request: Request) -> str:
    # Приоритет: API key > User ID > IP address
    if api_key := request.headers.get("X-API-Key"):
        return f"api_key:{api_key}"
    elif user_id := request.headers.get("X-User-ID"):
        return f"user:{user_id}"
    else:
        return f"ip:{get_remote_address(request)}"

limiter = Limiter(key_func=get_rate_limit_key)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

@app.post("/predict")
@limiter.limit("100/minute")  # Базовый лимит
async def predict(request: Request):
    pass

@app.post("/predict/batch")
@limiter.limit("10/minute")  # Более строгий лимит для batch
async def predict_batch(request: Request):
    pass
```

### 3. Circuit Breaker Pattern

✅ **DO:**
```python
# patterns/circuit_breaker.py
from pybreaker import CircuitBreaker
import asyncio

class AsyncCircuitBreaker:
    def __init__(self, 
                 failure_threshold: int = 5,
                 recovery_timeout: int = 60,
                 expected_exception: type = Exception):
        self.breaker = CircuitBreaker(
            fail_max=failure_threshold,
            reset_timeout=recovery_timeout,
            exclude=[KeyError]  # Не считать за failures
        )
        self.expected_exception = expected_exception
    
    def __call__(self, func):
        async def wrapper(*args, **kwargs):
            # Проверка состояния breaker
            if self.breaker.current_state == 'open':
                raise Exception(f"Circuit breaker is open for {func.__name__}")
            
            try:
                result = await func(*args, **kwargs)
                self.breaker.call_succeeded()
                return result
            
            except self.expected_exception as e:
                self.breaker.call_failed()
                raise
            
        return wrapper

# Использование
@AsyncCircuitBreaker(failure_threshold=5, recovery_timeout=60)
async def call_external_api(data: dict) -> dict:
    """Вызов внешнего API с защитой circuit breaker"""
    async with aiohttp.ClientSession() as session:
        async with session.post(API_URL, json=data) as response:
            if response.status >= 500:
                raise ExternalAPIError(f"API returned {response.status}")
            return await response.json()
```

### 4. Service Discovery

✅ **DO:**
```python
# discovery/consul_client.py
import consul
import random
from typing import List, Optional

class ServiceDiscovery:
    def __init__(self, consul_host: str = "consul", consul_port: int = 8500):
        self.consul = consul.Consul(host=consul_host, port=consul_port)
        self._service_cache = {}
        self._cache_ttl = 60  # seconds
    
    async def get_service(self, 
                         service_name: str, 
                         tag: Optional[str] = None) -> str:
        """Получение адреса сервиса с load balancing"""
        cache_key = f"{service_name}:{tag}"
        
        # Проверка кеша
        if cache_key in self._service_cache:
            cached_data, timestamp = self._service_cache[cache_key]
            if time.time() - timestamp < self._cache_ttl:
                return random.choice(cached_data)
        
        # Запрос к Consul
        _, services = self.consul.health.service(
            service_name, 
            tag=tag,
            passing=True  # Только здоровые сервисы
        )
        
        if not services:
            raise ServiceNotFoundError(f"No healthy instances of {service_name}")
        
        # Формирование адресов
        addresses = [
            f"{s['Service']['Address']}:{s['Service']['Port']}"
            for s in services
        ]
        
        # Обновление кеша
        self._service_cache[cache_key] = (addresses, time.time())
        
        # Random load balancing
        return random.choice(addresses)
    
    def register_service(self, 
                        name: str, 
                        address: str, 
                        port: int,
                        tags: List[str] = None,
                        check_interval: str = "10s"):
        """Регистрация сервиса в Consul"""
        self.consul.agent.service.register(
            name=name,
            service_id=f"{name}-{address}-{port}",
            address=address,
            port=port,
            tags=tags or [],
            check=consul.Check.http(
                f"http://{address}:{port}/health",
                interval=check_interval
            )
        )
```

---

## 🔍 Мониторинг и отладка

### 1. Structured Logging

✅ **DO:**
```python
# logging/structured.py
import structlog
from typing import Any, Dict

# Конфигурация structlog
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()

# Использование
class TransactionProcessor:
    def __init__(self):
        self.logger = logger.bind(component="transaction_processor")
    
    async def process(self, transaction: Transaction):
        log = self.logger.bind(
            transaction_id=transaction.id,
            user_id=transaction.user_id,
            amount=transaction.amount
        )
        
        log.info("processing_started")
        
        try:
            features = await self.extract_features(transaction)
            log.info("features_extracted", feature_count=len(features))
            
            prediction = await self.predict(features)
            log.info("prediction_made", 
                    is_fraud=prediction.is_fraud,
                    probability=prediction.probability,
                    processing_time_ms=prediction.processing_time)
            
            return prediction
            
        except Exception as e:
            log.error("processing_failed", 
                     error_type=type(e).__name__,
                     error_message=str(e),
                     exc_info=True)
            raise
```

### 2. Distributed Tracing

✅ **DO:**
```python
# tracing/instrumentation.py
from opentelemetry import trace
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor

# Настройка трейсинга
def setup_tracing(service_name: str):
    # Создание провайдера
    trace.set_tracer_provider(TracerProvider())
    tracer_provider = trace.get_tracer_provider()
    
    # Jaeger exporter
    jaeger_exporter = JaegerExporter(
        agent_host_name="jaeger",
        agent_port=6831,
    )
    
    # Batch processor для производительности
    span_processor = BatchSpanProcessor(jaeger_exporter)
    tracer_provider.add_span_processor(span_processor)
    
    # Автоматическая инструментация
    FastAPIInstrumentor.instrument()
    RequestsInstrumentor.instrument()
    
    return trace.get_tracer(service_name)

# Использование
tracer = setup_tracing("fraud-detection-api")

async def process_transaction(transaction: Transaction):
    with tracer.start_as_current_span(
        "process_transaction",
        attributes={
            "transaction.id": transaction.id,
            "transaction.amount": transaction.amount,
            "transaction.merchant_category": transaction.merchant_category
        }
    ) as span:
        # Feature extraction
        with tracer.start_as_current_span("extract_features"):
            features = await extract_features(transaction)
            span.set_attribute("feature.count", len(features))
        
        # Prediction
        with tracer.start_as_current_span("predict_fraud"):
            prediction = await predict(features)
            span.set_attribute("prediction.is_fraud", prediction.is_fraud)
            span.set_attribute("prediction.probability", prediction.probability)
        
        return prediction
```

### 3. Performance Profiling

✅ **DO:**
```python
# profiling/performance.py
import asyncio
import time
from functools import wraps
from typing import Callable, Dict
import cProfile
import pstats
import io

class PerformanceProfiler:
    def __init__(self):
        self.timings: Dict[str, List[float]] = {}
        self.profiler = cProfile.Profile()
    
    def time_it(self, name: str = None):
        """Декоратор для измерения времени выполнения"""
        def decorator(func: Callable):
            func_name = name or func.__name__
            
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                start = time.perf_counter()
                try:
                    result = await func(*args, **kwargs)
                    return result
                finally:
                    duration = time.perf_counter() - start
                    if func_name not in self.timings:
                        self.timings[func_name] = []
                    self.timings[func_name].append(duration)
                    
                    # Логирование медленных операций
                    if duration > 1.0:  # 1 секунда
                        logger.warning(f"Slow operation: {func_name}",
                                     duration_seconds=duration)
            
            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                start = time.perf_counter()
                try:
                    result = func(*args, **kwargs)
                    return result
                finally:
                    duration = time.perf_counter() - start
                    if func_name not in self.timings:
                        self.timings[func_name] = []
                    self.timings[func_name].append(duration)
            
            return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
        
        return decorator
    
    def get_stats(self) -> Dict[str, Dict[str, float]]:
        """Получение статистики производительности"""
        stats = {}
        for name, timings in self.timings.items():
            if timings:
                stats[name] = {
                    'count': len(timings),
                    'total': sum(timings),
                    'mean': np.mean(timings),
                    'median': np.median(timings),
                    'p95': np.percentile(timings, 95),
                    'p99': np.percentile(timings, 99),
                    'max': max(timings)
                }
        return stats

# Глобальный профайлер
profiler = PerformanceProfiler()

# Использование
@profiler.time_it("feature_extraction")
async def extract_features(transaction: Transaction) -> pd.DataFrame:
    # Извлечение признаков
    pass

@profiler.time_it("model_prediction")
async def predict(features: pd.DataFrame) -> Prediction:
    # Предсказание
    pass
```

---

## 🔐 Безопасность

### 1. API Security

✅ **DO:**
```python
# security/api_security.py
from fastapi import Security, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import jwt
from datetime import datetime, timedelta

class SecurityManager:
    def __init__(self, secret_key: str, algorithm: str = "HS256"):
        self.secret_key = secret_key
        self.algorithm = algorithm
        self.security = HTTPBearer()
    
    def create_access_token(self, 
                          subject: str, 
                          scopes: List[str] = None,
                          expires_delta: timedelta = None) -> str:
        """Создание JWT токена"""
        if expires_delta:
            expire = datetime.utcnow() + expires_delta
        else:
            expire = datetime.utcnow() + timedelta(minutes=15)
        
        to_encode = {
            "exp": expire,
            "sub": subject,
            "scopes": scopes or [],
            "iat": datetime.utcnow(),
            "iss": "fraud-detection-api"
        }
        
        encoded_jwt = jwt.encode(to_encode, self.secret_key, algorithm=self.algorithm)
        return encoded_jwt
    
    async def verify_token(self, 
                          credentials: HTTPAuthorizationCredentials = Security(security),
                          required_scopes: List[str] = None) -> Dict:
        """Верификация токена и проверка scopes"""
        token = credentials.credentials
        
        try:
            payload = jwt.decode(
                token, 
                self.secret_key, 
                algorithms=[self.algorithm],
                options={"verify_exp": True, "verify_iat": True}
            )
            
            # Проверка scopes
            if required_scopes:
                token_scopes = payload.get("scopes", [])
                for scope in required_scopes:
                    if scope not in token_scopes:
                        raise HTTPException(
                            status_code=status.HTTP_403_FORBIDDEN,
                            detail=f"Not enough permissions. Required scope: {scope}"
                        )
            
            return payload
            
        except jwt.ExpiredSignatureError:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token has expired"
            )
        except jwt.JWTError:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Could not validate credentials"
            )

# Использование
security_manager = SecurityManager(secret_key=settings.SECRET_KEY)

@app.post("/admin/reload-model")
async def reload_model(
    token_payload: Dict = Depends(security_manager.verify_token(required_scopes=["admin"]))
):
    """Endpoint требующий admin scope"""
    pass
```

### 2. Data Encryption

✅ **DO:**
```python
# security/encryption.py
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import base64

class DataEncryption:
    def __init__(self, master_key: str):
        # Деривация ключа из мастер-ключа
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=b'stable_salt',  # В production использовать случайную соль
            iterations=100000,
        )
        key = base64.urlsafe_b64encode(kdf.derive(master_key.encode()))
        self.cipher_suite = Fernet(key)
    
    def encrypt_sensitive_data(self, data: Dict) -> Dict:
        """Шифрование чувствительных полей"""
        encrypted_data = data.copy()
        
        # Список полей для шифрования
        sensitive_fields = ['credit_card_number', 'ssn', 'phone_number']
        
        for field in sensitive_fields:
            if field in encrypted_data and encrypted_data[field]:
                encrypted_value = self.cipher_suite.encrypt(
                    str(encrypted_data[field]).encode()
                )
                encrypted_data[field] = encrypted_value.decode()
        
        return encrypted_data
    
    def decrypt_sensitive_data(self, encrypted_data: Dict) -> Dict:
        """Расшифровка чувствительных полей"""
        decrypted_data = encrypted_data.copy()
        
        sensitive_fields = ['credit_card_number', 'ssn', 'phone_number']
        
        for field in sensitive_fields:
            if field in decrypted_data and decrypted_data[field]:
                decrypted_value = self.cipher_suite.decrypt(
                    decrypted_data[field].encode()
                )
                decrypted_data[field] = decrypted_value.decode()
        
        return decrypted_data
```

---

## ⚡ Производительность

### 1. Caching Strategy

✅ **DO:**
```python
# caching/strategy.py
from functools import lru_cache
import hashlib
import pickle
from typing import Any, Optional

class CacheManager:
    def __init__(self, redis_client):
        self.redis = redis_client
        self.local_cache = {}  # L1 cache
        self.cache_stats = {
            'hits': 0,
            'misses': 0,
            'errors': 0
        }
    
    def cache_key(self, prefix: str, *args, **kwargs) -> str:
        """Генерация стабильного ключа кеша"""
        # Сортировка kwargs для консистентности
        sorted_kwargs = sorted(kwargs.items())
        cache_data = (args, sorted_kwargs)
        
        # Хеширование для компактности
        hash_digest = hashlib.md5(
            pickle.dumps(cache_data)
        ).hexdigest()
        
        return f"{prefix}:{hash_digest}"
    
    async def get_or_compute(self,
                           key_prefix: str,
                           compute_func: Callable,
                           ttl: int = 3600,
                           *args,
                           **kwargs) -> Any:
        """Получение из кеша или вычисление"""
        cache_key = self.cache_key(key_prefix, *args, **kwargs)
        
        # L1 cache (in-memory)
        if cache_key in self.local_cache:
            self.cache_stats['hits'] += 1
            return self.local_cache[cache_key]
        
        # L2 cache (Redis)
        try:
            cached_value = await self.redis.get(cache_key)
            if cached_value:
                value = pickle.loads(cached_value)
                self.local_cache[cache_key] = value  # Populate L1
                self.cache_stats['hits'] += 1
                return value
        except Exception as e:
            logger.warning(f"Cache read error: {e}")
            self.cache_stats['errors'] += 1
        
        # Cache miss - compute
        self.cache_stats['misses'] += 1
        value = await compute_func(*args, **kwargs)
        
        # Save to cache
        try:
            await self.redis.setex(
                cache_key,
                ttl,
                pickle.dumps(value)
            )
            self.local_cache[cache_key] = value
        except Exception as e:
            logger.warning(f"Cache write error: {e}")
            self.cache_stats['errors'] += 1
        
        return value
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Получение статистики кеша"""
        total_requests = (
            self.cache_stats['hits'] + 
            self.cache_stats['misses']
        )
        hit_rate = (
            self.cache_stats['hits'] / total_requests 
            if total_requests > 0 else 0
        )
        
        return {
            **self.cache_stats,
            'hit_rate': hit_rate,
            'l1_cache_size': len(self.local_cache)
        }
```

### 2. Batch Processing

✅ **DO:**
```python
# processing/batch_processor.py
import asyncio
from typing import List, Any, Callable
from collections import defaultdict
import time

class BatchProcessor:
    def __init__(self,
                 process_func: Callable,
                 batch_size: int = 100,
                 batch_timeout: float = 1.0):
        self.process_func = process_func
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self.pending_items = []
        self.pending_futures = []
        self.lock = asyncio.Lock()
        self.processing_task = None
    
    async def add_item(self, item: Any) -> Any:
        """Добавление элемента для batch обработки"""
        future = asyncio.Future()
        
        async with self.lock:
            self.pending_items.append(item)
            self.pending_futures.append(future)
            
            # Запуск обработки если достигнут размер batch
            if len(self.pending_items) >= self.batch_size:
                await self._process_batch()
            elif self.processing_task is None or self.processing_task.done():
                # Запуск таймера для обработки по timeout
                self.processing_task = asyncio.create_task(
                    self._process_with_timeout()
                )
        
        return await future
    
    async def _process_with_timeout(self):
        """Обработка batch по таймауту"""
        await asyncio.sleep(self.batch_timeout)
        async with self.lock:
            if self.pending_items:
                await self._process_batch()
    
    async def _process_batch(self):
        """Обработка накопленного batch"""
        # Копирование текущего batch
        items = self.pending_items[:]
        futures = self.pending_futures[:]
        
        self.pending_items.clear()
        self.pending_futures.clear()
        
        try:
            # Batch обработка
            results = await self.process_func(items)
            
            # Разрешение futures
            for future, result in zip(futures, results):
                future.set_result(result)
                
        except Exception as e:
            # Ошибка для всех futures
            for future in futures:
                future.set_exception(e)

# Использование
async def batch_predict(transactions: List[Transaction]) -> List[Prediction]:
    """Batch предсказание для эффективности"""
    # Преобразование в DataFrame
    df = pd.DataFrame([t.dict() for t in transactions])
    
    # Feature engineering
    features = feature_pipeline.transform(df)
    
    # Batch prediction
    predictions = model.predict_proba(features)
    
    # Формирование результатов
    results = []
    for i, (idx, transaction) in enumerate(transactions):
        results.append(Prediction(
            transaction_id=transaction.id,
            is_fraud=predictions[i][1] > 0.5,
            probability=predictions[i][1]
        ))
    
    return results

# Создание batch процессора
batch_processor = BatchProcessor(
    process_func=batch_predict,
    batch_size=50,
    batch_timeout=0.1  # 100ms
)

# API endpoint использующий batching
@app.post("/predict")
async def predict(transaction: Transaction):
    # Автоматическое batching запросов
    result = await batch_processor.add_item(transaction)
    return result
```

### 3. Connection Pooling

✅ **DO:**
```python
# database/connection_pool.py
from asyncpg import create_pool
from contextlib import asynccontextmanager
import aioredis

class ConnectionManager:
    def __init__(self):
        self.pg_pool = None
        self.redis_pool = None
        
    async def init_pools(self):
        """Инициализация connection pools"""
        # PostgreSQL pool
        self.pg_pool = await create_pool(
            host=settings.DB_HOST,
            port=settings.DB_PORT,
            user=settings.DB_USER,
            password=settings.DB_PASSWORD,
            database=settings.DB_NAME,
            min_size=10,
            max_size=20,
            max_queries=50000,
            max_inactive_connection_lifetime=300,
            command_timeout=10
        )
        
        # Redis pool
        self.redis_pool = await aioredis.create_redis_pool(
            f'redis://{settings.REDIS_HOST}:{settings.REDIS_PORT}',
            minsize=5,
            maxsize=10,
            encoding='utf-8'
        )
    
    @asynccontextmanager
    async def get_db_connection(self):
        """Получение соединения из пула"""
        async with self.pg_pool.acquire() as connection:
            async with connection.transaction():
                yield connection
    
    @asynccontextmanager
    async def get_redis_connection(self):
        """Получение Redis соединения из пула"""
        conn = await self.redis_pool.acquire()
        try:
            yield conn
        finally:
            self.redis_pool.release(conn)
    
    async def close_pools(self):
        """Закрытие пулов при shutdown"""
        if self.pg_pool:
            await self.pg_pool.close()
        if self.redis_pool:
            self.redis_pool.close()
            await self.redis_pool.wait_closed()

# Глобальный connection manager
conn_manager = ConnectionManager()

# Startup/shutdown events
@app.on_event("startup")
async def startup():
    await conn_manager.init_pools()

@app.on_event("shutdown")
async def shutdown():
    await conn_manager.close_pools()

# Использование
async def get_user_features(user_id: str) -> dict:
    async with conn_manager.get_db_connection() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM user_features WHERE user_id = $1",
            user_id
        )
        return dict(row) if row else {}
```

---

## 💻 Примеры использования

### 1. Complete End-to-End Example

```python
# examples/complete_example.py
import asyncio
from datetime import datetime
from typing import Optional

# Инициализация компонентов
app = FastAPI(title="Fraud Detection API")
security_manager = SecurityManager(settings.SECRET_KEY)
cache_manager = CacheManager(redis_client)
batch_processor = BatchProcessor(batch_predict)
profiler = PerformanceProfiler()
logger = structlog.get_logger()

# Data Models
class TransactionRequest(BaseModel):
    user_id: str
    merchant_id: str
    amount: float
    merchant_category: str
    channel: str = "online"
    location: Optional[Location] = None

class PredictionResponse(BaseModel):
    transaction_id: str
    is_fraud: bool
    fraud_probability: float
    risk_factors: List[str]
    processing_time_ms: float
    cached: bool = False

# API Endpoint
@app.post("/api/v1/predict", response_model=PredictionResponse)
@limiter.limit("100/minute")
@profiler.time_it("predict_endpoint")
async def predict_fraud(
    request: TransactionRequest,
    token_payload: Dict = Depends(security_manager.verify_token()),
    x_request_id: str = Header(default=None)
):
    """
    Real-time fraud prediction endpoint
    """
    # Создание уникального ID для идемпотентности
    transaction_id = x_request_id or str(uuid.uuid4())
    
    # Структурированное логирование
    log = logger.bind(
        transaction_id=transaction_id,
        user_id=request.user_id,
        amount=request.amount,
        endpoint="predict"
    )
    
    log.info("prediction_request_received")
    
    start_time = time.time()
    
    try:
        # Проверка кеша
        cache_key = f"prediction:{transaction_id}"
        cached_result = await cache_manager.get_or_compute(
            cache_key,
            lambda: None,  # Не вычислять если нет в кеше
            ttl=300
        )
        
        if cached_result:
            log.info("prediction_cache_hit")
            cached_result['cached'] = True
            return PredictionResponse(**cached_result)
        
        # Feature extraction с профилированием
        with tracer.start_as_current_span("extract_features"):
            features = await extract_features_for_transaction(request)
            log.info("features_extracted", feature_count=len(features))
        
        # Batch prediction
        with tracer.start_as_current_span("model_prediction"):
            prediction = await batch_processor.add_item(
                (transaction_id, features)
            )
        
        # Risk analysis
        risk_factors = analyze_risk_factors(request, features, prediction)
        
        # Подготовка ответа
        response_data = {
            "transaction_id": transaction_id,
            "is_fraud": prediction['is_fraud'],
            "fraud_probability": prediction['probability'],
            "risk_factors": risk_factors,
            "processing_time_ms": (time.time() - start_time) * 1000,
            "cached": False
        }
        
        # Сохранение в кеш
        await cache_manager.redis.setex(
            cache_key,
            300,
            json.dumps(response_data)
        )
        
        # Отправка в monitoring
        transaction_counter.labels(
            status="completed",
            fraud=str(prediction['is_fraud']),
            channel=request.channel
        ).inc()
        
        prediction_duration.observe(time.time() - start_time)
        
        log.info("prediction_completed",
                is_fraud=prediction['is_fraud'],
                probability=prediction['probability'],
                processing_time_ms=response_data['processing_time_ms'])
        
        return PredictionResponse(**response_data)
        
    except Exception as e:
        log.error("prediction_failed",
                 error_type=type(e).__name__,
                 error_message=str(e),
                 exc_info=True)
        
        prediction_errors.labels(
            error_type=type(e).__name__,
            service='api'
        ).inc()
        
        # Fallback на простую модель
        try:
            simple_prediction = await simple_fraud_check(request)
            return PredictionResponse(
                transaction_id=transaction_id,
                is_fraud=simple_prediction['is_fraud'],
                fraud_probability=simple_prediction['probability'],
                risk_factors=["fallback_model_used"],
                processing_time_ms=(time.time() - start_time) * 1000
            )
        except:
            raise HTTPException(
                status_code=500,
                detail="Prediction service temporarily unavailable"
            )

# Helper functions
async def extract_features_for_transaction(
    request: TransactionRequest
) -> pd.DataFrame:
    """Extract features for a transaction"""
    # Получение исторических фичей
    user_features = await cache_manager.get_or_compute(
        f"user_features:{request.user_id}",
        lambda: fetch_user_features_from_db(request.user_id),
        ttl=3600
    )
    
    merchant_features = await cache_manager.get_or_compute(
        f"merchant_features:{request.merchant_id}",
        lambda: fetch_merchant_features_from_db(request.merchant_id),
        ttl=3600
    )
    
    # Создание DataFrame
    features = pd.DataFrame([{
        'amount': request.amount,
        'merchant_category': request.merchant_category,
        'channel': request.channel,
        'hour_of_day': datetime.now().hour,
        'day_of_week': datetime.now().weekday(),
        **user_features,
        **merchant_features
    }])
    
    return features

def analyze_risk_factors(
    request: TransactionRequest,
    features: pd.DataFrame,
    prediction: Dict
) -> List[str]:
    """Analyze risk factors for a transaction"""
    risk_factors = []
    
    # Amount-based risks
    if request.amount > 5000:
        risk_factors.append("high_amount")
    
    if features['amount_zscore'].iloc[0] > 3:
        risk_factors.append("unusual_amount_for_user")
    
    # Behavioral risks
    if features['transaction_count_1h'].iloc[0] > 10:
        risk_factors.append("high_velocity")
    
    # Merchant risks
    if request.merchant_category in ['jewelry', 'casino', 'crypto']:
        risk_factors.append("high_risk_merchant_category")
    
    # Time-based risks
    if datetime.now().hour in [0, 1, 2, 3, 4, 5]:
        risk_factors.append("unusual_time")
    
    # ML model confidence
    if prediction['probability'] > 0.9:
        risk_factors.append("very_high_ml_confidence")
    elif prediction['probability'] > 0.7:
        risk_factors.append("high_ml_confidence")
    
    return risk_factors

# Simple fallback model
async def simple_fraud_check(request: TransactionRequest) -> Dict:
    """Simple rule-based fraud check as fallback"""
    fraud_score = 0.0
    
    # Simple rules
    if request.amount > 10000:
        fraud_score += 0.5
    
    if request.merchant_category in ['jewelry', 'casino']:
        fraud_score += 0.3
    
    if datetime.now().hour < 6:
        fraud_score += 0.2
    
    return {
        'is_fraud': fraud_score >= 0.5,
        'probability': min(fraud_score, 1.0)
    }

# Health check endpoint
@app.get("/health")
async def health_check():
    """Comprehensive health check"""
    checks = {}
    
    # Database check
    try:
        async with conn_manager.get_db_connection() as conn:
            await conn.fetchval("SELECT 1")
        checks['database'] = "healthy"
    except Exception as e:
        checks['database'] = f"unhealthy: {str(e)}"
    
    # Redis check
    try:
        await cache_manager.redis.ping()
        checks['redis'] = "healthy"
    except Exception as e:
        checks['redis'] = f"unhealthy: {str(e)}"
    
    # Model check
    checks['model'] = "healthy" if model_manager.active_model else "unhealthy: no model loaded"
    
    # Overall status
    all_healthy = all(v == "healthy" for v in checks.values())
    
    return {
        "status": "healthy" if all_healthy else "degraded",
        "checks": checks,
        "timestamp": datetime.utcnow().isoformat(),
        "version": "2.0.0"
    }

# Metrics endpoint
@app.get("/metrics")
async def get_metrics():
    """Prometheus metrics endpoint"""
    # Performance stats
    perf_stats = profiler.get_stats()
    
    # Cache stats
    cache_stats = cache_manager.get_cache_stats()
    
    # Custom metrics
    for endpoint, stats in perf_stats.items():
        for metric, value in stats.items():
            if isinstance(value, (int, float)):
                custom_metrics_gauge.labels(
                    endpoint=endpoint,
                    metric=metric
                ).set(value)
    
    # Return Prometheus format
    return Response(
        generate_latest(),
        media_type="text/plain"
    )

# Startup event
@app.on_event("startup")
async def startup_event():
    """Initialize services on startup"""
    # Database pools
    await conn_manager.init_pools()
    
    # Load ML model
    await model_manager.load_latest_model()
    
    # Initialize tracer
    setup_tracing("fraud-detection-api")
    
    # Start background tasks
    asyncio.create_task(monitor_model_performance())
    
    logger.info("fraud_detection_api_started", version="2.0.0")

# Shutdown event  
@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    await conn_manager.close_pools()
    await batch_processor.shutdown()
    
    logger.info("fraud_detection_api_stopped")

# Background monitoring task
async def monitor_model_performance():
    """Continuously monitor model performance"""
    while True:
        try:
            # Get recent predictions and actuals
            recent_predictions = await get_recent_predictions(hours=1)
            
            if recent_predictions:
                # Calculate metrics
                metrics = calculate_model_metrics(recent_predictions)
                
                # Update Prometheus gauges
                for metric_name, value in metrics.items():
                    model_performance_gauge.labels(
                        metric=metric_name
                    ).set(value)
                
                # Check for degradation
                if metrics['precision'] < 0.9 or metrics['recall'] < 0.8:
                    await alert_manager.send_alert(
                        "model_performance_degradation",
                        severity="warning",
                        metrics=metrics
                    )
            
            # Sleep for 5 minutes
            await asyncio.sleep(300)
            
        except Exception as e:
            logger.error("monitoring_error", error=str(e))
            await asyncio.sleep(60)  # Retry after 1 minute

# Main entry point
if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8001,
        log_config={
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "default": {
                    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                },
                "json": {
                    "()": "pythonjsonlogger.jsonlogger.JsonFormatter",
                    "format": "%(asctime)s %(name)s %(levelname)s %(message)s"
                }
            },
            "handlers": {
                "default": {
                    "formatter": "json",
                    "class": "logging.StreamHandler",
                    "stream": "ext://sys.stdout"
                }
            },
            "root": {
                "level": "INFO",
                "handlers": ["default"]
            }
        }
    )
```

### 2. Testing Example

```python
# tests/test_fraud_detection.py
import pytest
from httpx import AsyncClient
from unittest.mock import Mock, patch
import asyncio

@pytest.fixture
async def client():
    """Test client fixture"""
    async with AsyncClient(app=app, base_url="http://test") as ac:
        yield ac

@pytest.fixture
def mock_model():
    """Mock model fixture"""
    mock = Mock()
    mock.predict_proba.return_value = [[0.1, 0.9]]  # High fraud probability
    return mock

class TestFraudDetectionAPI:
    """Test suite for fraud detection API"""
    
    @pytest.mark.asyncio
    async def test_predict_endpoint_success(self, client, mock_model):
        """Test successful prediction"""
        # Mock dependencies
        with patch('app.model_manager.active_model', mock_model):
            # Test request
            response = await client.post(
                "/api/v1/predict",
                json={
                    "user_id": "USER_123",
                    "merchant_id": "MERCH_456",
                    "amount": 5000.0,
                    "merchant_category": "jewelry"
                },
                headers={
                    "Authorization": "Bearer test-token",
                    "X-Request-ID": "TEST_001"
                }
            )
            
            assert response.status_code == 200
            data = response.json()
            
            assert data["transaction_id"] == "TEST_001"
            assert data["is_fraud"] == True
            assert data["fraud_probability"] >= 0.9
            assert "high_amount" in data["risk_factors"]
            assert "processing_time_ms"] > 0
    
    @pytest.mark.asyncio
    async def test_predict_with_caching(self, client):
        """Test caching behavior"""
        # First request
        response1 = await client.post(
            "/api/v1/predict",
            json={
                "user_id": "USER_123",
                "merchant_id": "MERCH_456",
                "amount": 100.0,
                "merchant_category": "grocery"
            },
            headers={"X-Request-ID": "CACHE_TEST_001"}
        )
        
        # Second request with same ID
        response2 = await client.post(
            "/api/v1/predict",
            json={
                "user_id": "USER_123",
                "merchant_id": "MERCH_456",
                "amount": 100.0,
                "merchant_category": "grocery"
            },
            headers={"X-Request-ID": "CACHE_TEST_001"}
        )
        
        assert response1.status_code == 200
        assert response2.status_code == 200
        
        # Second response should be cached
        assert response2.json()["cached"] == True
        assert response1.json()["fraud_probability"] == response2.json()["fraud_probability"]
    
    @pytest.mark.asyncio
    async def test_rate_limiting(self, client):
        """Test rate limiting"""
        # Send many requests quickly
        tasks = []
        for i in range(150):  # Over the 100/minute limit
            task = client.post(
                "/api/v1/predict",
                json={
                    "user_id": f"USER_{i}",
                    "merchant_id": "MERCH_001",
                    "amount": 100.0,
                    "merchant_category": "online"
                }
            )
            tasks.append(task)
        
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Some requests should be rate limited
        status_codes = [r.status_code for r in responses if hasattr(r, 'status_code')]
        assert 429 in status_codes  # Too Many Requests
    
    @pytest.mark.asyncio
    async def test_health_check(self, client):
        """Test health check endpoint"""
        response = await client.get("/health")
        
        assert response.status_code == 200
        data = response.json()
        
        assert "status" in data
        assert "checks" in data
        assert "timestamp" in data
        
        # Verify all checks are present
        assert "database" in data["checks"]
        assert "redis" in data["checks"]
        assert "model" in data["checks"]

# Performance test
@pytest.mark.asyncio
async def test_performance():
    """Test API performance"""
    async with AsyncClient(app=app, base_url="http://test") as client:
        # Warm up
        await client.post(
            "/api/v1/predict",
            json={
                "user_id": "PERF_USER",
                "merchant_id": "PERF_MERCH",
                "amount": 100.0,
                "merchant_category": "online"
            }
        )
        
        # Performance test
        start_time = time.time()
        tasks = []
        
        for i in range(100):
            task = client.post(
                "/api/v1/predict",
                json={
                    "user_id": f"PERF_USER_{i}",
                    "merchant_id": f"PERF_MERCH_{i % 10}",
                    "amount": random.uniform(10, 1000),
                    "merchant_category": random.choice(["online", "grocery", "gas"])
                }
            )
            tasks.append(task)
        
        responses = await asyncio.gather(*tasks)
        duration = time.time() - start_time
        
        # All requests should succeed
        assert all(r.status_code == 200 for r in responses)
        
        # Performance assertions
        avg_time = duration / len(responses)
        assert avg_time < 0.1  # Average < 100ms
        
        # Check individual response times
        response_times = [r.json()["processing_time_ms"] for r in responses]
        p95_time = np.percentile(response_times, 95)
        assert p95_time < 100  # P95 < 100ms
```

---

## 🎯 Итоговые рекомендации

### Ключевые принципы

1. **Простота важнее сложности** - Начинайте с простого решения и усложняйте по мере необходимости
2. **Мониторинг с первого дня** - Нельзя улучшить то, что не измеряешь
3. **Автоматизация всего** - От тестов до деплоя
4. **Документирование решений** - Код должен быть самодокументируемым, но архитектурные решения требуют объяснений
5. **Security by design** - Безопасность не должна быть afterthought

### Checklist для production

- [ ] Все endpoints имеют rate limiting
- [ ] Настроен structured logging
- [ ] Реализован health check
- [ ] Есть метрики для всех критических операций
- [ ] Настроен distributed tracing
- [ ] Реализована graceful shutdown
- [ ] Есть circuit breakers для внешних сервисов
- [ ] Настроен connection pooling
- [ ] Реализована идемпотентность операций
- [ ] Есть retry логика с exponential backoff
- [ ] Все sensitive данные зашифрованы
- [ ] Настроен мониторинг и алертинг
- [ ] Есть runbook для основных проблем
- [ ] Проведено load testing
- [ ] Настроен автоматический backup

### Полезные ресурсы

- [The Twelve-Factor App](https://12factor.net/)
- [Martin Fowler's Microservices](https://martinfowler.com/microservices/)
- [Google SRE Book](https://sre.google/books/)
- [High Performance Python](https://www.oreilly.com/library/view/high-performance-python/9781492055013/)
- [Designing Data-Intensive Applications](https://dataintensive.net/)

---

*Следующий раздел: [Troubleshooting Guide →](./10_troubleshooting.md)*
