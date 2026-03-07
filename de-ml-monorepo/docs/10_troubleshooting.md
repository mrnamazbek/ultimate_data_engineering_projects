# 🔧 Troubleshooting Guide

## Обзор

Этот документ содержит решения для наиболее распространенных проблем, которые могут возникнуть при работе с платформой fraud detection. Каждая проблема включает симптомы, диагностику и шаги по решению.

## 📋 Содержание

1. [Общие проблемы](#общие-проблемы)
2. [Проблемы с установкой](#проблемы-с-установкой)
3. [Проблемы с производительностью](#проблемы-с-производительностью)
4. [Проблемы с моделями ML](#проблемы-с-моделями-ml)
5. [Проблемы с данными](#проблемы-с-данными)
6. [Проблемы с API](#проблемы-с-api)
7. [Проблемы с инфраструктурой](#проблемы-с-инфраструктурой)
8. [Проблемы с мониторингом](#проблемы-с-мониторингом)

---

## 🚨 Общие проблемы

### Сервис не запускается

**Симптомы:**
- Docker контейнер падает сразу после запуска
- Ошибки в логах типа "Connection refused"
- Health check failing

**Диагностика:**
```bash
# Проверка логов контейнера
docker-compose logs fraud-api -f --tail=100

# Проверка статуса всех сервисов
docker-compose ps

# Проверка сети Docker
docker network ls
docker network inspect de-ml-monorepo_default
```

**Решение:**
```bash
# 1. Очистка и перезапуск
docker-compose down -v
docker-compose up -d

# 2. Проверка зависимостей
# Убедитесь, что все зависимые сервисы запущены
docker-compose up -d postgres redis kafka

# Подождите инициализации
sleep 30

# Запустите основной сервис
docker-compose up -d fraud-api

# 3. Проверка конфигурации
# Убедитесь, что .env файл содержит все необходимые переменные
cat .env | grep -E "(POSTGRES|REDIS|KAFKA)"

# 4. Проверка портов
netstat -tulpn | grep -E "(5432|6379|9092|8001)"
```

### Out of Memory (OOM) ошибки

**Симптомы:**
- Контейнер killed с exit code 137
- Сообщения "Killed" в логах
- Система становится медленной

**Диагностика:**
```bash
# Проверка памяти системы
free -h

# Проверка памяти контейнеров
docker stats --no-stream

# Проверка OOM killer в системных логах
dmesg | grep -i "killed process"
journalctl -u docker | grep -i "oom"
```

**Решение:**
```yaml
# docker-compose.yml - установка лимитов памяти
services:
  fraud-api:
    deploy:
      resources:
        limits:
          memory: 4G
        reservations:
          memory: 2G
    environment:
      - PYTHON_MAX_MEMORY=3G
      - JAVA_OPTS=-Xmx2g -Xms1g
```

```python
# Оптимизация памяти в коде
# 1. Использование генераторов вместо списков
def process_large_dataset(file_path):
    # Плохо - загружает все в память
    # data = pd.read_csv(file_path)
    
    # Хорошо - читает по частям
    for chunk in pd.read_csv(file_path, chunksize=10000):
        yield process_chunk(chunk)

# 2. Очистка памяти после использования
import gc

def process_batch():
    large_data = load_large_dataset()
    result = process(large_data)
    
    # Явная очистка
    del large_data
    gc.collect()
    
    return result
```

### Высокая нагрузка на CPU

**Симптомы:**
- CPU использование постоянно > 90%
- Медленный отклик API
- Timeouts

**Диагностика:**
```bash
# Top процессы по CPU
htop
docker stats

# Профилирование Python процесса
py-spy top --pid $(docker inspect -f '{{.State.Pid}}' fraud-api)

# Анализ горячих точек
py-spy record -o profile.svg --pid $(docker inspect -f '{{.State.Pid}}' fraud-api)
```

**Решение:**
```python
# 1. Оптимизация вычислений
import numpy as np
from numba import jit

# Использование векторизации
def calculate_features_optimized(df):
    # Плохо - цикл по строкам
    # for idx, row in df.iterrows():
    #     df.at[idx, 'feature'] = complex_calculation(row)
    
    # Хорошо - векторизация
    df['feature'] = np.vectorize(complex_calculation)(df['values'])

# 2. Использование JIT компиляции
@jit(nopython=True)
def fast_calculation(values):
    result = 0.0
    for val in values:
        result += val ** 2 + np.sin(val)
    return result

# 3. Параллельная обработка
from multiprocessing import Pool
from concurrent.futures import ProcessPoolExecutor

def parallel_process(data_chunks):
    with ProcessPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(process_chunk, data_chunks))
    return results
```

---

## 🛠️ Проблемы с установкой

### Docker Compose не находит сервисы

**Симптомы:**
```
ERROR: Service 'fraud-api' depends on service 'postgres' which is undefined.
```

**Решение:**
```bash
# Убедитесь, что используете правильные файлы compose
docker-compose -f docker-compose.yml -f docker-compose.fraud.yml up -d

# Или создайте alias
alias dc-fraud='docker-compose -f docker-compose.yml -f docker-compose.fraud.yml'
dc-fraud up -d
```

### Ошибки при сборке Docker образов

**Симптомы:**
```
ERROR: failed to solve: rpc error: code = Unknown desc = failed to compute cache key
```

**Решение:**
```bash
# 1. Очистка Docker cache
docker system prune -a

# 2. Rebuild без cache
docker-compose build --no-cache fraud-api

# 3. Проверка Dockerfile
# Убедитесь, что все COPY команды указывают на существующие файлы
ls -la projects/fraud-detection/

# 4. Использование .dockerignore
cat > .dockerignore << EOF
**/__pycache__
**/*.pyc
.git
.env
*.log
EOF
```

### Проблемы с правами доступа

**Симптомы:**
```
Permission denied: '/app/logs/fraud-detection.log'
```

**Решение:**
```bash
# 1. Проверка прав на хосте
ls -la $(pwd)/logs

# 2. Установка правильных прав
sudo chown -R $USER:$USER logs/
chmod -R 755 logs/

# 3. Использование user в Dockerfile
# Dockerfile
FROM python:3.10-slim
RUN useradd -m -u 1000 appuser
USER appuser

# 4. Volume permissions в docker-compose
volumes:
  - ./logs:/app/logs:rw
```

---

## ⚡ Проблемы с производительностью

### Медленные предсказания

**Симптомы:**
- API latency > 500ms
- Timeouts при batch предсказаниях
- Queue overflow

**Диагностика:**
```python
# Профилирование endpoint
import time
import cProfile
import pstats

def profile_prediction():
    profiler = cProfile.Profile()
    profiler.enable()
    
    # Ваш код предсказания
    start = time.time()
    result = predict(transaction)
    duration = time.time() - start
    
    profiler.disable()
    
    # Анализ
    stats = pstats.Stats(profiler)
    stats.sort_stats('cumulative')
    stats.print_stats(10)
    
    print(f"Total time: {duration}s")
```

**Решение:**
```python
# 1. Кеширование features
@lru_cache(maxsize=10000)
def get_user_features(user_id: str):
    return fetch_from_database(user_id)

# 2. Batch processing
class BatchPredictor:
    def __init__(self, batch_size=50, timeout=0.1):
        self.batch = []
        self.batch_size = batch_size
        self.timeout = timeout
        
    async def predict(self, transaction):
        self.batch.append(transaction)
        
        if len(self.batch) >= self.batch_size:
            return await self._process_batch()
        
        # Ждем больше транзакций или timeout
        await asyncio.sleep(self.timeout)
        if self.batch:
            return await self._process_batch()

# 3. Model optimization
# Конвертация в ONNX для ускорения
import onnx
import onnxruntime as ort

def optimize_model(sklearn_model):
    # Конвертация в ONNX
    from skl2onnx import convert_sklearn
    onnx_model = convert_sklearn(
        sklearn_model,
        initial_types=[('input', FloatTensorType([None, n_features]))]
    )
    
    # Создание inference session
    session = ort.InferenceSession(onnx_model.SerializeToString())
    
    def predict_optimized(X):
        input_name = session.get_inputs()[0].name
        return session.run(None, {input_name: X.astype(np.float32)})[0]
    
    return predict_optimized
```

### Database connection pool exhausted

**Симптомы:**
```
psycopg2.pool.PoolError: connection pool exhausted
```

**Решение:**
```python
# 1. Увеличение размера pool
from sqlalchemy.pool import QueuePool

engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,
    pool_size=20,          # Увеличить с 5 до 20
    max_overflow=40,       # Увеличить с 10 до 40
    pool_pre_ping=True,    # Проверка соединений
    pool_recycle=3600      # Обновление соединений каждый час
)

# 2. Правильное использование соединений
# Плохо - соединение не закрывается
def bad_query():
    conn = engine.connect()
    result = conn.execute("SELECT * FROM users")
    return result

# Хорошо - автоматическое закрытие
def good_query():
    with engine.connect() as conn:
        result = conn.execute("SELECT * FROM users")
        return result.fetchall()

# 3. Мониторинг pool
def check_pool_status():
    pool = engine.pool
    print(f"Size: {pool.size()}")
    print(f"Checked in: {pool.checkedin()}")
    print(f"Overflow: {pool.overflow()}")
    print(f"Total: {pool.total()}")
```

---

## 🤖 Проблемы с моделями ML

### Model accuracy degradation

**Симптомы:**
- Снижение метрик качества
- Увеличение false positives
- Жалобы пользователей

**Диагностика:**
```python
# Скрипт для проверки model drift
import pandas as pd
from scipy import stats

def check_model_drift(reference_data, current_data, features):
    drift_report = {}
    
    for feature in features:
        # Kolmogorov-Smirnov test
        ks_stat, p_value = stats.ks_2samp(
            reference_data[feature],
            current_data[feature]
        )
        
        drift_report[feature] = {
            'ks_statistic': ks_stat,
            'p_value': p_value,
            'drift_detected': p_value < 0.05
        }
    
    return drift_report

# Проверка дрифта
drift = check_model_drift(training_data, production_data, feature_columns)
print(f"Features with drift: {sum(1 for f in drift.values() if f['drift_detected'])}")
```

**Решение:**
```python
# 1. Автоматическое переобучение
class ModelRetrainer:
    def __init__(self, drift_threshold=0.1):
        self.drift_threshold = drift_threshold
        
    async def check_and_retrain(self):
        # Получение свежих данных
        recent_data = await get_recent_predictions(days=7)
        
        # Проверка качества
        current_metrics = calculate_metrics(recent_data)
        
        if self.needs_retraining(current_metrics):
            # Переобучение
            new_model = await self.retrain_model(recent_data)
            
            # A/B testing
            await deploy_as_canary(new_model, traffic_percent=10)
            
            # Мониторинг
            await monitor_canary_performance(hours=24)

# 2. Feature monitoring
def monitor_feature_importance():
    # Сравнение важности признаков
    current_importance = model.feature_importances_
    baseline_importance = load_baseline_importance()
    
    # Проверка изменений
    for i, (feature, importance) in enumerate(zip(features, current_importance)):
        baseline = baseline_importance[i]
        change = abs(importance - baseline) / baseline
        
        if change > 0.3:  # 30% изменение
            logger.warning(f"Significant change in {feature} importance: {change:.1%}")
```

### Model loading failures

**Симптомы:**
```
Error: Unable to load model from MLflow
ModuleNotFoundError: No module named 'xgboost'
```

**Решение:**
```python
# 1. Dependency management
# requirements-model.txt
xgboost==1.7.0
lightgbm==3.3.3
scikit-learn==1.1.3
mlflow==2.0.1

# 2. Model packaging с dependencies
import mlflow

# При сохранении модели
with mlflow.start_run():
    mlflow.sklearn.log_model(
        model,
        "model",
        conda_env={
            'channels': ['defaults'],
            'dependencies': [
                'python=3.10',
                'pip',
                {'pip': [
                    f'xgboost=={xgboost.__version__}',
                    f'scikit-learn=={sklearn.__version__}',
                    'pandas',
                    'numpy'
                ]}
            ]
        }
    )

# 3. Fallback механизм
class ModelLoader:
    def __init__(self):
        self.primary_model = None
        self.fallback_model = None
        
    async def load_model(self):
        try:
            # Загрузка основной модели
            self.primary_model = mlflow.pyfunc.load_model(
                "models:/fraud-detection/Production"
            )
        except Exception as e:
            logger.error(f"Failed to load primary model: {e}")
            
            # Загрузка fallback модели
            try:
                self.fallback_model = joblib.load("models/fallback_model.pkl")
                logger.warning("Using fallback model")
            except:
                # Критическая ошибка
                raise RuntimeError("No models available")
```

---

## 📊 Проблемы с данными

### Data quality issues

**Симптомы:**
- Много null значений
- Неправильные типы данных
- Дубликаты

**Диагностика:**
```python
# Скрипт проверки качества данных
def diagnose_data_quality(df):
    report = {
        'total_rows': len(df),
        'total_columns': len(df.columns),
        'duplicates': df.duplicated().sum(),
        'null_counts': df.isnull().sum().to_dict(),
        'dtypes': df.dtypes.to_dict()
    }
    
    # Проверка на аномалии
    for col in df.select_dtypes(include=[np.number]).columns:
        report[f'{col}_stats'] = {
            'mean': df[col].mean(),
            'std': df[col].std(),
            'min': df[col].min(),
            'max': df[col].max(),
            'outliers': len(df[df[col] > df[col].quantile(0.99)])
        }
    
    return report
```

**Решение:**
```python
# 1. Автоматическая очистка данных
class DataCleaner:
    def __init__(self, config):
        self.config = config
        
    def clean(self, df):
        # Удаление дубликатов
        df = df.drop_duplicates(subset=['transaction_id'])
        
        # Заполнение пропусков
        for col, strategy in self.config['fill_na'].items():
            if strategy == 'mean':
                df[col].fillna(df[col].mean(), inplace=True)
            elif strategy == 'mode':
                df[col].fillna(df[col].mode()[0], inplace=True)
            elif strategy == 'zero':
                df[col].fillna(0, inplace=True)
        
        # Приведение типов
        for col, dtype in self.config['dtypes'].items():
            df[col] = df[col].astype(dtype)
        
        # Валидация
        self._validate(df)
        
        return df
    
    def _validate(self, df):
        # Проверка обязательных полей
        required = ['transaction_id', 'user_id', 'amount']
        missing = [col for col in required if col not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns: {missing}")
        
        # Проверка на null в критических полях
        null_counts = df[required].isnull().sum()
        if null_counts.any():
            raise ValueError(f"Nulls in required fields: {null_counts[null_counts > 0]}")

# 2. Конфигурация очистки
cleaning_config = {
    'fill_na': {
        'merchant_category': 'mode',
        'location_lat': 'mean',
        'location_lon': 'mean'
    },
    'dtypes': {
        'amount': 'float64',
        'user_id': 'str',
        'merchant_id': 'str'
    }
}
```

### Kafka lag issues

**Симптомы:**
- Consumer lag постоянно растет
- Старые сообщения в топиках
- Задержки в обработке

**Решение:**
```python
# 1. Оптимизация consumer
consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'fraud-detection',
    'enable.auto.commit': False,
    'max.poll.records': 1000,        # Увеличить batch size
    'fetch.min.bytes': 1024 * 1024,  # 1MB minimum
    'fetch.max.wait.ms': 100,        # Снизить latency
}

# 2. Параллельные consumers
class ParallelKafkaConsumer:
    def __init__(self, topic, num_consumers=4):
        self.consumers = []
        for i in range(num_consumers):
            consumer = Consumer({
                **consumer_config,
                'group.instance.id': f'consumer-{i}'
            })
            consumer.subscribe([topic])
            self.consumers.append(consumer)
    
    async def consume_parallel(self):
        tasks = []
        for consumer in self.consumers:
            task = asyncio.create_task(self._consume_messages(consumer))
            tasks.append(task)
        
        await asyncio.gather(*tasks)
    
    async def _consume_messages(self, consumer):
        while True:
            messages = consumer.consume(num_messages=100, timeout=1.0)
            if messages:
                await self._process_batch(messages)
                consumer.commit()

# 3. Мониторинг lag
def monitor_kafka_lag():
    admin = AdminClient({'bootstrap.servers': 'localhost:9092'})
    
    # Получение consumer groups
    groups = admin.list_consumer_groups()
    
    for group in groups:
        # Получение lag для группы
        partitions = admin.list_consumer_group_offsets(group)
        
        for topic_partition, offset in partitions.items():
            # Получение high water mark
            high_water_mark = get_partition_metadata(
                topic_partition.topic,
                topic_partition.partition
            ).high_water_mark
            
            lag = high_water_mark - offset.offset
            
            if lag > 10000:  # Alert threshold
                alert(f"High Kafka lag: {lag} for {topic_partition}")
```

---

## 🌐 Проблемы с API

### 429 Too Many Requests

**Симптомы:**
- Клиенты получают 429 ошибки
- Rate limit exceeded

**Решение:**
```python
# 1. Настройка rate limiting
from slowapi import Limiter
from slowapi.util import get_remote_address

# Кастомная функция для получения ключа
def get_rate_limit_key(request):
    # Приоритет: API key > User ID > IP
    if api_key := request.headers.get("X-API-Key"):
        return f"api:{api_key}"
    elif user_id := request.headers.get("X-User-ID"):
        return f"user:{user_id}"
    else:
        return f"ip:{get_remote_address(request)}"

limiter = Limiter(
    key_func=get_rate_limit_key,
    default_limits=["100 per minute", "1000 per hour"]
)

# 2. Разные лимиты для разных endpoints
@app.post("/predict")
@limiter.limit("100/minute")
async def predict_single():
    pass

@app.post("/predict/batch")
@limiter.limit("10/minute")  # Более строгий для batch
async def predict_batch():
    pass

# 3. Premium limits для определенных API keys
PREMIUM_API_KEYS = {"premium_key_1", "premium_key_2"}

def get_limit_for_key(request):
    api_key = request.headers.get("X-API-Key")
    if api_key in PREMIUM_API_KEYS:
        return "1000/minute"
    return "100/minute"

@app.post("/predict")
@limiter.limit(get_limit_for_key)
async def predict():
    pass
```

### Connection timeout errors

**Симптомы:**
```
httpx.ConnectTimeout: timed out
```

**Решение:**
```python
# 1. Увеличение timeouts
import httpx

# Client с retry и backoff
class ResilientClient:
    def __init__(self, base_url, max_retries=3):
        self.base_url = base_url
        self.max_retries = max_retries
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(
                connect=5.0,      # Connection timeout
                read=30.0,        # Read timeout
                write=10.0,       # Write timeout
                pool=5.0          # Pool timeout
            ),
            limits=httpx.Limits(
                max_keepalive_connections=20,
                max_connections=100,
                keepalive_expiry=30.0
            )
        )
    
    async def request_with_retry(self, method, url, **kwargs):
        last_exception = None
        
        for attempt in range(self.max_retries):
            try:
                response = await self.client.request(method, url, **kwargs)
                response.raise_for_status()
                return response
                
            except (httpx.TimeoutException, httpx.NetworkError) as e:
                last_exception = e
                wait_time = 2 ** attempt  # Exponential backoff
                logger.warning(f"Request failed, retry {attempt + 1}/{self.max_retries} after {wait_time}s")
                await asyncio.sleep(wait_time)
                
        raise last_exception

# 2. Circuit breaker
from pybreaker import CircuitBreaker

db_breaker = CircuitBreaker(fail_max=5, reset_timeout=60)

@db_breaker
async def get_from_database(query):
    async with get_connection() as conn:
        return await conn.fetch(query)

# 3. Graceful degradation
async def get_user_data(user_id):
    try:
        # Попытка получить из основного источника
        return await get_from_database(f"SELECT * FROM users WHERE id = {user_id}")
    except Exception as e:
        logger.warning(f"Database unavailable: {e}")
        
        # Fallback на кеш
        cached = await redis.get(f"user:{user_id}")
        if cached:
            return json.loads(cached)
        
        # Последний fallback - дефолтные данные
        return {"user_id": user_id, "risk_score": 0.5}
```

---

## 🏗️ Проблемы с инфраструктурой

### Docker контейнеры постоянно перезапускаются

**Симптомы:**
- Container restarting loop
- Exit codes 1, 125, 126, 127

**Диагностика:**
```bash
# Проверка логов
docker logs fraud-api --tail 50

# Проверка exit code
docker inspect fraud-api --format='{{.State.ExitCode}}'

# События Docker
docker events --filter container=fraud-api --since 10m
```

**Решение:**
```yaml
# docker-compose.yml
services:
  fraud-api:
    # Правильная стратегия перезапуска
    restart: unless-stopped  # Не on-failure для отладки
    
    # Health check
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 60s  # Время на инициализацию
    
    # Зависимости
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_started
      kafka:
        condition: service_started
```

```dockerfile
# Dockerfile оптимизации
# Правильная обработка сигналов
FROM python:3.10-slim

# Установка tini для правильной обработки сигналов
RUN apt-get update && apt-get install -y tini
ENTRYPOINT ["/usr/bin/tini", "--"]

# Non-root user
RUN useradd -m -u 1000 appuser
USER appuser

# Graceful shutdown
STOPSIGNAL SIGTERM

CMD ["python", "-u", "app.py"]  # -u для unbuffered output
```

### Disk space issues

**Симптомы:**
```
No space left on device
Cannot create container for service fraud-api: Error response from daemon: devmapper
```

**Решение:**
```bash
# 1. Очистка Docker
# Удаление stopped контейнеров
docker container prune -f

# Удаление unused образов
docker image prune -a -f

# Удаление unused volumes
docker volume prune -f

# Полная очистка (осторожно!)
docker system prune -a --volumes -f

# 2. Ротация логов
# docker-compose.yml
services:
  fraud-api:
    logging:
      driver: "json-file"
      options:
        max-size: "10m"
        max-file: "3"

# 3. Очистка application логов
# logrotate конфигурация
cat > /etc/logrotate.d/fraud-detection << EOF
/var/log/fraud-detection/*.log {
    daily
    rotate 7
    maxsize 100M
    compress
    delaycompress
    notifempty
    create 0644 appuser appuser
    sharedscripts
    postrotate
        docker exec fraud-api kill -USR1 1
    endscript
}
EOF

# 4. Мониторинг disk usage
#!/bin/bash
THRESHOLD=90
USAGE=$(df -h / | awk 'NR==2 {print $5}' | sed 's/%//')

if [ $USAGE -gt $THRESHOLD ]; then
    echo "Disk usage critical: ${USAGE}%"
    # Автоматическая очистка
    docker system prune -f
    # Алерт
    curl -X POST $SLACK_WEBHOOK -d '{"text":"Disk usage critical: '${USAGE}'%"}'
fi
```

### Network connectivity issues

**Симптомы:**
- Cannot connect to database
- Name resolution failures
- Connection refused между контейнерами

**Решение:**
```bash
# 1. Проверка Docker network
docker network ls
docker network inspect de-ml-monorepo_default

# 2. Проверка DNS в контейнере
docker exec fraud-api nslookup postgres
docker exec fraud-api ping -c 3 postgres

# 3. Использование правильных имен хостов
# В коде используйте имена сервисов из docker-compose
DATABASE_URL = "postgresql://user:pass@postgres:5432/db"  # НЕ localhost!
REDIS_URL = "redis://redis:6379"
KAFKA_BROKERS = "kafka:9092"

# 4. Explicit network configuration
# docker-compose.yml
networks:
  fraud-net:
    driver: bridge
    ipam:
      config:
        - subnet: 172.20.0.0/16

services:
  fraud-api:
    networks:
      - fraud-net
    extra_hosts:
      - "host.docker.internal:host-gateway"  # Для доступа к хосту

# 5. Retry логика для соединений
import backoff

@backoff.on_exception(
    backoff.expo,
    (ConnectionError, TimeoutError),
    max_tries=5,
    max_time=30
)
async def connect_to_database():
    return await asyncpg.connect(DATABASE_URL)
```

---

## 📊 Проблемы с мониторингом

### Prometheus не собирает метрики

**Симптомы:**
- No data в Grafana
- Targets showing as DOWN в Prometheus

**Решение:**
```yaml
# 1. Правильная конфигурация prometheus.yml
scrape_configs:
  - job_name: 'fraud-detection'
    static_configs:
      - targets: ['fraud-api:8000']  # Используйте имя сервиса, не localhost
    metrics_path: '/metrics'
    scrape_interval: 15s
    scrape_timeout: 10s

# 2. Проверка доступности metrics endpoint
docker exec prometheus wget -O- http://fraud-api:8000/metrics

# 3. Правильный экспорт метрик в приложении
from prometheus_client import Counter, Histogram, generate_latest, REGISTRY
from prometheus_client.multiprocess import MultiProcessCollector

# Для многопроцессных приложений
if 'prometheus_multiproc_dir' in os.environ:
    REGISTRY = CollectorRegistry()
    MultiProcessCollector(REGISTRY)

# Метрики
prediction_counter = Counter('predictions_total', 'Total predictions')
prediction_histogram = Histogram('prediction_duration_seconds', 'Prediction duration')

@app.get("/metrics")
async def metrics():
    return Response(generate_latest(REGISTRY), media_type="text/plain")
```

### Grafana dashboards не работают

**Симптомы:**
- No data points
- Panel errors
- Wrong visualization

**Решение:**
```json
// 1. Проверка datasource
{
  "name": "Prometheus",
  "type": "prometheus",
  "url": "http://prometheus:9090",
  "access": "proxy",
  "isDefault": true
}

// 2. Правильные запросы
// Используйте label selectors
rate(predictions_total{job="fraud-detection"}[5m])

// Не забывайте про time ranges
increase(predictions_total[1h])

// 3. Import готовых dashboards
// grafana/dashboards/fraud-detection.json
{
  "dashboard": {
    "title": "Fraud Detection Monitoring",
    "panels": [
      {
        "targets": [
          {
            "expr": "rate(predictions_total{job=\"fraud-detection\"}[5m])",
            "legendFormat": "Predictions/sec"
          }
        ]
      }
    ],
    "time": {
      "from": "now-6h",
      "to": "now"
    },
    "refresh": "30s"
  }
}
```

### Logs не появляются в Elasticsearch

**Симптомы:**
- Kibana не показывает логи
- Index pattern отсутствует

**Решение:**
```yaml
# 1. Проверка Logstash pipeline
# logstash/pipeline/logstash.conf
input {
  beats {
    port => 5044
  }
  
  # Для Docker logs
  syslog {
    port => 5514
    type => "docker"
  }
}

filter {
  if [type] == "docker" {
    grok {
      match => { 
        "message" => "%{TIMESTAMP_ISO8601:timestamp} %{LOGLEVEL:level} %{GREEDYDATA:message}" 
      }
    }
  }
  
  # Парсинг JSON логов
  if [message] =~ /^\{/ {
    json {
      source => "message"
      target => "parsed"
    }
  }
}

output {
  elasticsearch {
    hosts => ["elasticsearch:9200"]
    index => "fraud-detection-%{+YYYY.MM.dd}"
  }
}

# 2. Docker logging driver
services:
  fraud-api:
    logging:
      driver: "syslog"
      options:
        syslog-address: "tcp://logstash:5514"
        syslog-format: "rfc3164"
        tag: "fraud-api"

# 3. Проверка Elasticsearch
curl -X GET "localhost:9200/_cat/indices?v"
curl -X GET "localhost:9200/fraud-detection-*/_search?pretty"
```

---

## 🆘 Emergency Procedures

### Полный сбой системы

**Шаги восстановления:**

```bash
#!/bin/bash
# emergency-recovery.sh

echo "Starting emergency recovery..."

# 1. Остановка всех сервисов
docker-compose down

# 2. Backup текущего состояния
mkdir -p backups/emergency-$(date +%Y%m%d_%H%M%S)
docker-compose ps -a > backups/emergency-$(date +%Y%m%d_%H%M%S)/containers.txt
docker logs fraud-api > backups/emergency-$(date +%Y%m%d_%H%M%S)/fraud-api.log 2>&1

# 3. Очистка и перезапуск инфраструктуры
docker system prune -f
docker-compose up -d postgres redis kafka

# 4. Ожидание готовности
echo "Waiting for infrastructure..."
sleep 60

# 5. Проверка здоровья базовых сервисов
for service in postgres redis kafka; do
    if docker-compose ps $service | grep -q "Up"; then
        echo "✓ $service is running"
    else
        echo "✗ $service failed to start"
        exit 1
    fi
done

# 6. Восстановление базы данных из backup
if [ -f "backups/latest/postgres_backup.sql.gz" ]; then
    echo "Restoring database..."
    gunzip < backups/latest/postgres_backup.sql.gz | docker exec -i postgres psql -U deuser fraud_detection
fi

# 7. Запуск приложения
docker-compose up -d fraud-api

# 8. Проверка работоспособности
sleep 30
if curl -f http://localhost:8001/health; then
    echo "✓ System recovered successfully"
else
    echo "✗ System recovery failed"
    exit 1
fi
```

### Data corruption recovery

```python
# scripts/data_recovery.py
import pandas as pd
import psycopg2
from datetime import datetime, timedelta

class DataRecovery:
    def __init__(self, db_config):
        self.conn = psycopg2.connect(**db_config)
        
    def check_data_integrity(self):
        """Проверка целостности данных"""
        issues = []
        
        # Проверка дубликатов
        cur = self.conn.cursor()
        cur.execute("""
            SELECT transaction_id, COUNT(*)
            FROM transactions
            GROUP BY transaction_id
            HAVING COUNT(*) > 1
        """)
        
        duplicates = cur.fetchall()
        if duplicates:
            issues.append(f"Found {len(duplicates)} duplicate transactions")
        
        # Проверка orphaned records
        cur.execute("""
            SELECT COUNT(*)
            FROM predictions p
            LEFT JOIN transactions t ON p.transaction_id = t.transaction_id
            WHERE t.transaction_id IS NULL
        """)
        
        orphaned = cur.fetchone()[0]
        if orphaned > 0:
            issues.append(f"Found {orphaned} orphaned predictions")
        
        return issues
    
    def repair_data(self):
        """Восстановление данных"""
        # Удаление дубликатов (оставляем последний)
        self.conn.execute("""
            DELETE FROM transactions t1
            USING transactions t2
            WHERE t1.ctid < t2.ctid
            AND t1.transaction_id = t2.transaction_id
        """)
        
        # Восстановление из backup партиций
        self.restore_missing_partitions()
        
        self.conn.commit()
    
    def restore_missing_partitions(self):
        """Восстановление отсутствующих партиций"""
        # Проверка пропущенных дат
        cur = self.conn.cursor()
        cur.execute("""
            SELECT date_trunc('day', timestamp) as day, COUNT(*) as cnt
            FROM transactions
            WHERE timestamp >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY day
            ORDER BY day
        """)
        
        daily_counts = cur.fetchall()
        
        # Поиск дней с аномально низким количеством транзакций
        avg_count = sum(cnt for _, cnt in daily_counts) / len(daily_counts)
        
        for day, count in daily_counts:
            if count < avg_count * 0.1:  # Менее 10% от среднего
                logger.warning(f"Low transaction count on {day}: {count}")
                # Восстановление из S3/backup
                self.restore_from_backup(day)
```

---

## 📚 Полезные команды для отладки

### Docker команды

```bash
# Просмотр использования ресурсов
docker stats --no-stream

# Exec в контейнер для отладки
docker exec -it fraud-api /bin/bash

# Копирование файлов из контейнера
docker cp fraud-api:/app/logs/error.log ./error.log

# Просмотр изменений в контейнере
docker diff fraud-api

# Export контейнера для анализа
docker export fraud-api | tar -tv
```

### Database команды

```sql
-- Проверка активных соединений
SELECT pid, usename, application_name, client_addr, state
FROM pg_stat_activity
WHERE datname = 'fraud_detection';

-- Kill зависших запросов
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE state = 'idle in transaction'
AND state_change < current_timestamp - interval '5 minutes';

-- Анализ медленных запросов
SELECT query, mean_exec_time, calls
FROM pg_stat_statements
ORDER BY mean_exec_time DESC
LIMIT 10;

-- Проверка размера таблиц
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE schemaname = 'fraud'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
```

### Performance анализ

```bash
# CPU profiling
perf record -F 99 -p $(pgrep -f fraud-api) -g -- sleep 30
perf report

# Memory profiling
valgrind --tool=massif --massif-out-file=massif.out python app.py
ms_print massif.out

# Network analysis
tcpdump -i any -w capture.pcap host postgres
wireshark capture.pcap

# strace для системных вызовов
strace -c -p $(pgrep -f fraud-api)
```

---

## 🏁 Заключение

Этот troubleshooting guide покрывает наиболее распространенные проблемы, с которыми вы можете столкнуться при работе с платформой fraud detection. Помните:

1. **Всегда начинайте с логов** - они содержат ценную информацию о проблеме
2. **Проверяйте мониторинг** - метрики могут показать корень проблемы
3. **Используйте изоляцию** - тестируйте компоненты по отдельности
4. **Документируйте решения** - добавляйте новые случаи в этот guide
5. **Автоматизируйте восстановление** - создавайте скрипты для типовых проблем

При возникновении новых проблем:
- Проверьте [GitHub Issues](https://github.com/yourusername/fraud-detection/issues)
- Обратитесь в [Slack канал](https://fraud-detection.slack.com)
- Создайте тикет в системе поддержки

---

*Вернуться к [оглавлению](./COMPLETE_DOCUMENTATION.md)*
