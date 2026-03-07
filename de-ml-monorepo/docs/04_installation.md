# 📦 Установка и настройка

## Требования к системе

### Минимальные требования
| Компонент | Минимум | Рекомендуется | Для production |
|-----------|---------|---------------|----------------|
| **CPU** | 4 cores | 8 cores | 16+ cores |
| **RAM** | 16 GB | 32 GB | 64+ GB |
| **Storage** | 50 GB SSD | 200 GB SSD | 500+ GB NVMe |
| **OS** | Ubuntu 20.04+ / macOS 11+ | Ubuntu 22.04 | Ubuntu 22.04 LTS |
| **Docker** | 20.10+ | 24.0+ | 24.0+ |
| **Docker Compose** | 2.0+ | 2.20+ | 2.20+ |

### Проверка системы
```bash
# Проверка версий
docker --version
docker-compose --version
python3 --version
git --version

# Проверка ресурсов
free -h  # RAM
df -h    # Disk space
nproc    # CPU cores
```

## 🚀 Быстрая установка (5 минут)

### Шаг 1: Клонирование репозитория
```bash
# Клонирование
git clone https://github.com/yourusername/ultimate_data_engineering_projects.git
cd ultimate_data_engineering_projects/de-ml-monorepo

# Создание веток для разработки
git checkout -b feature/fraud-detection
```

### Шаг 2: Настройка окружения
```bash
# Копирование примера конфигурации
cp .env.example .env

# Создание необходимых директорий
make setup

# Или вручную:
mkdir -p projects/fraud-detection/{models,data,experiments,feature_store,api}
mkdir -p data/{raw,processed,external}
mkdir -p logs/{airflow,spark,kafka}
mkdir -p backups
```

### Шаг 3: Настройка переменных окружения
```bash
# Редактирование .env файла
nano .env
```

Обязательные переменные:
```env
# Database
POSTGRES_USER=deuser
POSTGRES_PASSWORD=YourStrongPassword123!
POSTGRES_DB=fraud_detection

# Kafka
KAFKA_BROKER_ID=1
KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181
KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092

# MinIO
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=YourMinioPassword123!
MINIO_DEFAULT_BUCKETS=fraud-detection,models,checkpoints

# Airflow
AIRFLOW__CORE__FERNET_KEY=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
AIRFLOW__WEBSERVER__SECRET_KEY=$(openssl rand -hex 32)

# Fraud Detection
FRAUD_RATE=0.02
TRANSACTION_VOLUME=10000

# External APIs (опционально)
PLAID_CLIENT_ID=your_plaid_client_id
PLAID_SECRET=your_plaid_secret
PLAID_ENV=sandbox
```

### Шаг 4: Запуск базовой инфраструктуры
```bash
# Запуск core services
docker-compose up -d postgres kafka zookeeper minio redis

# Ожидание готовности
echo "Waiting for services to be ready..."
sleep 30

# Проверка статуса
docker-compose ps
```

### Шаг 5: Запуск всей платформы
```bash
# Запуск всех сервисов включая fraud detection
make fraud-start

# Или пошагово:
docker-compose up -d
docker-compose -f docker-compose.yml -f docker-compose.fraud.yml --profile fraud up -d
```

## 📋 Детальная установка

### 1. Подготовка системы

#### Ubuntu/Debian
```bash
# Обновление системы
sudo apt update && sudo apt upgrade -y

# Установка зависимостей
sudo apt install -y \
    curl \
    wget \
    git \
    build-essential \
    python3-pip \
    python3-venv \
    postgresql-client \
    redis-tools \
    jq

# Установка Docker
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker $USER
newgrp docker

# Установка Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/download/v2.20.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
```

#### macOS
```bash
# Установка Homebrew (если нет)
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Установка зависимостей
brew install \
    git \
    python3 \
    postgresql \
    redis \
    jq \
    wget

# Установка Docker Desktop
brew install --cask docker

# Запуск Docker Desktop
open /Applications/Docker.app
```

### 2. Конфигурация Docker

#### Настройка ресурсов Docker
```json
// Docker Desktop -> Settings -> Resources
{
  "cpus": 6,
  "memory": 24,  // GB
  "swap": 4,     // GB
  "disk": 100    // GB
}
```

#### Docker daemon configuration
```json
// /etc/docker/daemon.json или ~/.docker/daemon.json
{
  "log-driver": "json-file",
  "log-opts": {
    "max-size": "10m",
    "max-file": "3"
  },
  "storage-driver": "overlay2",
  "default-ulimits": {
    "nofile": {
      "Hard": 64000,
      "Soft": 64000
    }
  }
}
```

### 3. Инициализация баз данных

#### PostgreSQL setup
```bash
# Создание базы данных и схем
docker exec -it postgres psql -U deuser -d postgres <<EOF
CREATE DATABASE fraud_detection;
\c fraud_detection;

-- Создание схем
CREATE SCHEMA IF NOT EXISTS fraud;
CREATE SCHEMA IF NOT EXISTS features;
CREATE SCHEMA IF NOT EXISTS ml;

-- Создание расширений
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- Создание основных таблиц
CREATE TABLE fraud.transactions (
    transaction_id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
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
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
) PARTITION BY RANGE (timestamp);

-- Создание партиций на текущий месяц
CREATE TABLE fraud.transactions_2024_01 PARTITION OF fraud.transactions
FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

-- Индексы
CREATE INDEX idx_transactions_user_time ON fraud.transactions(user_id, timestamp DESC);
CREATE INDEX idx_transactions_merchant_time ON fraud.transactions(merchant_id, timestamp DESC);
CREATE INDEX idx_transactions_fraud ON fraud.transactions(timestamp DESC) WHERE is_fraud = TRUE;
EOF

echo "PostgreSQL initialized successfully!"
```

#### Создание Kafka топиков
```bash
# Создание топиков
docker exec -it kafka kafka-topics.sh --create \
    --bootstrap-server localhost:9092 \
    --topic fraud-transactions \
    --partitions 10 \
    --replication-factor 1 \
    --config retention.ms=604800000 \
    --config compression.type=snappy

docker exec -it kafka kafka-topics.sh --create \
    --bootstrap-server localhost:9092 \
    --topic fraud-predictions \
    --partitions 5 \
    --replication-factor 1

docker exec -it kafka kafka-topics.sh --create \
    --bootstrap-server localhost:9092 \
    --topic fraud-alerts \
    --partitions 3 \
    --replication-factor 1

# Проверка топиков
docker exec -it kafka kafka-topics.sh --list --bootstrap-server localhost:9092
```

### 4. Установка Airflow

```bash
# Инициализация Airflow базы данных
docker-compose run --rm airflow-webserver airflow db init

# Создание admin пользователя
docker-compose run --rm airflow-webserver airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@fraud-detection.com \
    --password admin123

# Создание connection для PostgreSQL
docker-compose run --rm airflow-webserver airflow connections add \
    'postgres_fraud' \
    --conn-type 'postgres' \
    --conn-host 'postgres' \
    --conn-schema 'fraud_detection' \
    --conn-login 'deuser' \
    --conn-password 'YourStrongPassword123!' \
    --conn-port 5432

# Запуск Airflow сервисов
docker-compose up -d airflow-webserver airflow-scheduler
```

### 5. Настройка MinIO

```bash
# Создание buckets
docker exec -it minio mc alias set local http://localhost:9000 minioadmin YourMinioPassword123!

docker exec -it minio mc mb local/fraud-detection
docker exec -it minio mc mb local/models
docker exec -it minio mc mb local/checkpoints
docker exec -it minio mc mb local/data-lake

# Настройка политик доступа
docker exec -it minio mc anonymous set download local/models
```

### 6. Инициализация Feature Store (Feast)

```bash
# Создание Feast проекта
docker exec -it feast-feature-store bash -c "
cd /feast
feast init fraud_detection
cd fraud_detection
"

# Копирование конфигурации
cat > projects/fraud-detection/feature_store/feature_store.yaml <<EOF
project: fraud_detection
registry: data/registry.db
provider: local
online_store:
  type: redis
  connection_string: redis:6379
offline_store:
  type: file
entity_key_serialization_version: 2
EOF

# Применение feature definitions
docker exec -it feast-feature-store feast apply
```

### 7. Установка ML компонентов

#### MLflow setup
```bash
# Создание default experiment
docker exec -it mlflow bash -c "
python -c \"
import mlflow
mlflow.set_tracking_uri('http://mlflow:5000')
mlflow.create_experiment('fraud_detection_baseline')
mlflow.create_experiment('fraud_detection_production')
\"
"
```

#### Загрузка начальных данных
```bash
# Загрузка датасетов
python3 scripts/download_datasets.py

# Или вручную:
mkdir -p data/raw
cd data/raw

# IEEE-CIS Fraud Detection
wget https://path-to-dataset/ieee-cis-fraud-detection.zip
unzip ieee-cis-fraud-detection.zip

# Credit Card Fraud Dataset
wget https://path-to-dataset/creditcard.csv

cd ../..
```

### 8. Проверка установки

#### Automated health check
```bash
# Создание скрипта проверки
cat > scripts/health_check.py <<'EOF'
import requests
import sys

services = {
    "PostgreSQL": {"url": "http://localhost:5432", "type": "tcp"},
    "Kafka": {"url": "http://localhost:9092", "type": "tcp"},
    "Airflow": {"url": "http://localhost:8085/health", "type": "http"},
    "MLflow": {"url": "http://localhost:5000", "type": "http"},
    "Fraud API": {"url": "http://localhost:8001/health", "type": "http"},
    "Grafana": {"url": "http://localhost:3000", "type": "http"},
    "MinIO": {"url": "http://localhost:9001", "type": "http"},
}

all_healthy = True

for service, config in services.items():
    try:
        if config["type"] == "http":
            response = requests.get(config["url"], timeout=5)
            status = "✅ Healthy" if response.status_code == 200 else f"⚠️  Unhealthy ({response.status_code})"
        else:
            # TCP check would go here
            status = "✅ Healthy"
    except Exception as e:
        status = f"❌ Down ({str(e)})"
        all_healthy = False
    
    print(f"{service:15} {status}")

sys.exit(0 if all_healthy else 1)
EOF

python3 scripts/health_check.py
```

#### Manual verification
```bash
# Проверка всех URL
make show-urls

# Тестовая транзакция
curl -X POST http://localhost:8001/predict \
  -H "Content-Type: application/json" \
  -d '{
    "transaction_id": "TEST001",
    "user_id": "USER_000001",
    "merchant_id": "MERCH_00001",
    "amount": 150.00,
    "timestamp": "2024-01-15T10:30:00Z",
    "merchant_category": "grocery"
  }'
```

## 🔧 Конфигурация для различных окружений

### Development
```yaml
# docker-compose.override.yml
version: '3.9'
services:
  fraud-api:
    environment:
      - DEBUG=true
      - LOG_LEVEL=debug
    volumes:
      - ./projects/fraud-detection/api:/app
    command: uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### Staging
```yaml
# docker-compose.staging.yml
version: '3.9'
services:
  fraud-api:
    deploy:
      replicas: 2
    environment:
      - ENV=staging
      - LOG_LEVEL=info
```

### Production
```yaml
# docker-compose.prod.yml
version: '3.9'
services:
  fraud-api:
    deploy:
      replicas: 5
      resources:
        limits:
          cpus: '2'
          memory: 4G
        reservations:
          cpus: '1'
          memory: 2G
    environment:
      - ENV=production
      - LOG_LEVEL=warning
```

## 🐳 Docker оптимизации

### Multi-stage builds
```dockerfile
# Dockerfile.optimized
# Stage 1: Dependencies
FROM python:3.10-slim as deps
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Stage 2: Build
FROM python:3.10-slim as build
WORKDIR /app
COPY --from=deps /usr/local/lib/python3.10/site-packages /usr/local/lib/python3.10/site-packages
COPY . .
RUN python -m compileall .

# Stage 3: Runtime
FROM python:3.10-slim
WORKDIR /app
COPY --from=build /app /app
COPY --from=deps /usr/local/lib/python3.10/site-packages /usr/local/lib/python3.10/site-packages
USER nobody
CMD ["python", "main.py"]
```

### Docker Compose best practices
```yaml
# Использование .env файлов
env_file:
  - .env
  - .env.local

# Health checks
healthcheck:
  test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
  interval: 30s
  timeout: 10s
  retries: 3
  start_period: 40s

# Restart policies
restart: unless-stopped

# Logging
logging:
  driver: "json-file"
  options:
    max-size: "10m"
    max-file: "3"
```

## 📝 Post-installation задачи

### 1. Настройка мониторинга
```bash
# Импорт Grafana dashboards
./scripts/import_dashboards.sh

# Настройка alerting
./scripts/setup_alerts.sh
```

### 2. Загрузка тестовых данных
```bash
# Генерация синтетических данных
docker exec fraud-generator python generate_test_data.py \
    --days 30 \
    --daily-volume 100000 \
    --output /data/test_data.parquet
```

### 3. Обучение baseline модели
```bash
# Запуск обучения
docker exec fraud-ml-trainer python train_baseline.py
```

### 4. Настройка backup
```bash
# Добавление в crontab
0 2 * * * /path/to/project/scripts/backup.sh
```

## ⚡ Полезные команды

```bash
# Просмотр логов
docker-compose logs -f fraud-api

# Перезапуск сервиса
docker-compose restart fraud-api

# Очистка всего
docker-compose down -v
docker system prune -a

# Backup базы данных
docker exec postgres pg_dump -U deuser fraud_detection | gzip > backup.sql.gz

# Восстановление из backup
gunzip -c backup.sql.gz | docker exec -i postgres psql -U deuser fraud_detection
```

---

*Следующий раздел: [Работа с данными →](./05_data_pipeline.md)*
