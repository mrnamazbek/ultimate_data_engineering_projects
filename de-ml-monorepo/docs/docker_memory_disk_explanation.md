# 🐳 Docker: Полное объяснение работы памяти и диска

## 📋 Содержание

1. [Где выполняются Docker контейнеры](#где-выполняются-docker-контейнеры)
2. [Использование оперативной памяти](#использование-оперативной-памяти)
3. [Использование дискового пространства](#использование-дискового-пространства)
4. [Docker Storage Driver](#docker-storage-driver)
5. [Практические примеры с вашим проектом](#практические-примеры-с-вашим-проектом)
6. [Мониторинг использования ресурсов](#мониторинг-использования-ресурсов)

---

## 🖥️ Где выполняются Docker контейнеры

### Основной принцип
**Docker ВСЕГДА работает локально на ВАШЕМ компьютере**, используя ВАШУ оперативную память и ВАШЕ дисковое пространство.

```
┌─────────────────── ВАШ КОМПЬЮТЕР ────────────────────┐
│                                                       │
│  ┌─────────────────── RAM (16GB) ─────────────────┐ │
│  │                                                 │ │
│  │  ┌─── macOS/Windows/Linux (Система) ───┐      │ │
│  │  │  • Системные процессы                │      │ │
│  │  │  • Браузер, IDE                      │ 8GB  │ │
│  │  └──────────────────────────────────────┘      │ │
│  │                                                 │ │
│  │  ┌─── Docker Desktop ──────────────────┐      │ │
│  │  │  • Docker daemon                     │      │ │
│  │  │  • Контейнеры:                       │      │ │
│  │  │    - postgres (512MB)                │      │ │
│  │  │    - kafka (1GB)                     │ 6GB  │ │
│  │  │    - spark (2GB)                     │      │ │
│  │  │    - fraud-api (512MB)               │      │ │
│  │  └──────────────────────────────────────┘      │ │
│  │                                                 │ │
│  │  Свободно: 2GB                                 │ │
│  └─────────────────────────────────────────────────┘ │
│                                                       │
│  ┌─────────────────── DISK (512GB) ───────────────┐ │
│  │                                                 │ │
│  │  /var/lib/docker/ (или Docker.raw на Mac)      │ │
│  │  └── containers/    # Данные контейнеров       │ │
│  │  └── images/        # Docker образы            │ │
│  │  └── volumes/       # Docker volumes           │ │
│  │  └── overlay2/      # Файловая система         │ │
│  │                                                 │ │
│  └─────────────────────────────────────────────────┘ │
└───────────────────────────────────────────────────────┘
```

---

## 💾 Использование оперативной памяти

### 1. Как Docker использует RAM

Когда вы запускаете `docker-compose up`, происходит следующее:

```yaml
# docker-compose.yml
services:
  postgres:
    image: postgres:14
    deploy:
      resources:
        limits:
          memory: 1G        # Максимум 1GB RAM
        reservations:
          memory: 512M      # Минимум 512MB RAM
```

### 2. Распределение памяти

```python
# Пример расчета использования памяти
def calculate_memory_usage():
    containers = {
        'postgres': 512,        # MB
        'redis': 256,          # MB
        'kafka': 1024,         # MB
        'zookeeper': 512,      # MB
        'spark-master': 2048,  # MB
        'spark-worker': 2048,  # MB
        'fraud-api': 512,      # MB
        'minio': 512,          # MB
        'airflow': 1024,       # MB
    }
    
    total_mb = sum(containers.values())
    total_gb = total_mb / 1024
    
    print(f"Общее использование RAM: {total_gb:.2f} GB")
    print("\nДетализация по контейнерам:")
    for name, mb in containers.items():
        print(f"  {name:15} : {mb:5} MB ({mb/1024:.2f} GB)")
    
    return total_gb

# Результат:
# Общее использование RAM: 8.31 GB
```

### 3. Как контейнеры делят память

```bash
# Проверка текущего использования памяти
docker stats --no-stream

# Вывод:
# CONTAINER ID   NAME            CPU %   MEM USAGE / LIMIT   MEM %   
# 8a5d4f3c9b2e   postgres        0.5%    423.2MiB / 1GiB     41.3%
# 3f2e1d9c8b7a   kafka           2.1%    892.1MiB / 2GiB     43.6%
# 9b7c6d5e4a3f   spark-master    5.3%    1.7GiB / 2GiB       85.0%
```

---

## 💿 Использование дискового пространства

### 1. Где Docker хранит данные

#### На macOS:
```bash
# Docker Desktop создает виртуальную машину
~/Library/Containers/com.docker.docker/Data/vms/0/data/Docker.raw

# Размер можно увидеть:
ls -lh ~/Library/Containers/com.docker.docker/Data/vms/0/data/
# -rw-r--r--  1 user  staff   64G  Docker.raw
```

#### На Linux:
```bash
# Прямое хранение в файловой системе
/var/lib/docker/
├── containers/     # Метаданные контейнеров
├── images/         # Слои образов
├── volumes/        # Docker volumes
├── overlay2/       # Union filesystem для контейнеров
└── tmp/           # Временные файлы
```

#### На Windows:
```powershell
# WSL2 виртуальная машина
C:\Users\%USERNAME%\AppData\Local\Docker\wsl\data\ext4.vhdx
```

### 2. Структура хранения данных

```
Docker Storage Structure
├── Images (Образы) - READ ONLY
│   ├── postgres:14 (400MB)
│   │   ├── Layer 1: Ubuntu base (80MB)
│   │   ├── Layer 2: PostgreSQL binaries (250MB)
│   │   └── Layer 3: Config files (70MB)
│   └── python:3.10 (900MB)
│       ├── Layer 1: Debian base (120MB)
│       ├── Layer 2: Python runtime (600MB)
│       └── Layer 3: pip packages (180MB)
│
├── Containers (Контейнеры) - READ/WRITE Layer
│   ├── postgres_container_1
│   │   └── Writable layer (изменения от образа)
│   └── fraud_api_container_1
│       └── Writable layer
│
└── Volumes (Тома) - Persistent Storage
    ├── postgres_data/ (5GB с данными БД)
    ├── kafka_data/ (2GB с сообщениями)
    └── minio_data/ (10GB с файлами)
```

### 3. Copy-on-Write механизм

```python
# Демонстрация Copy-on-Write
"""
1. Образ postgres:14 занимает 400MB (read-only)
2. Запускаем 3 контейнера из этого образа
3. Каждый контейнер создает только свой writable layer

Итого на диске:
- 400MB для образа (shared)
- 50MB x 3 для writable layers = 150MB
- Всего: 550MB вместо 1200MB (400MB x 3)
"""
```

---

## 🗄️ Docker Storage Driver

### Union Filesystem (overlay2)

```bash
# Проверка storage driver
docker info | grep "Storage Driver"
# Storage Driver: overlay2

# Как работает overlay2:
/var/lib/docker/overlay2/
├── l/          # Символические ссылки на слои
├── <layer-id>/
│   ├── diff/   # Файлы этого слоя
│   ├── link    # Короткий идентификатор
│   ├── lower   # Ссылка на нижние слои
│   └── work/   # Рабочая директория
```

### Пример многослойности:

```dockerfile
# Dockerfile создает слои
FROM python:3.10           # Слой 1: 900MB
WORKDIR /app              # Слой 2: metadata only
COPY requirements.txt .   # Слой 3: 1KB
RUN pip install -r requirements.txt  # Слой 4: 200MB
COPY . .                  # Слой 5: 50MB
CMD ["python", "app.py"]  # Слой 6: metadata only

# Итого образ: ~1.15GB
```

---

## 📊 Практические примеры с вашим проектом

### 1. Запуск вашего docker-compose

```bash
# При выполнении команды:
docker-compose -f docker-compose.yml -f docker-compose.fraud.yml up -d

# Происходит следующее:
```

#### Шаг 1: Загрузка образов
```bash
# Docker проверяет локальные образы
docker images

# Если образа нет - загружает с Docker Hub
# Downloading postgres:14... (400MB) → сохраняется на диск
# Downloading apache/kafka:3.5.0... (800MB) → сохраняется на диск
# Downloading apache/spark:3.4.1... (600MB) → сохраняется на диск
```

#### Шаг 2: Создание контейнеров
```bash
# Для каждого сервиса создается контейнер
# Container: postgres_1
#   - Base image layers: 400MB (shared, read-only)
#   - Container layer: 0MB (пока пустой, read-write)
#   - Выделено RAM: 512MB-1GB
```

#### Шаг 3: Volumes монтирование
```yaml
volumes:
  postgres_data:
    driver: local
  kafka_data:
    driver: local
  
# Создаются директории:
# macOS: ~/Library/Containers/com.docker.docker/Data/vms/0/data/
# Linux: /var/lib/docker/volumes/
```

### 2. Реальные объемы использования

```python
# monitoring_script.py
import subprocess
import json

def get_docker_stats():
    # Получаем статистику контейнеров
    cmd = ["docker", "stats", "--no-stream", "--format", "json"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    total_memory = 0
    total_disk = 0
    
    print("=== ИСПОЛЬЗОВАНИЕ РЕСУРСОВ DOCKER ===\n")
    print("КОНТЕЙНЕР          | RAM USAGE  | DISK I/O")
    print("-" * 50)
    
    for line in result.stdout.strip().split('\n'):
        if line:
            stats = json.loads(line)
            name = stats['Name']
            mem = stats['MemUsage'].split('/')[0]
            
            print(f"{name:18} | {mem:10} | {stats.get('BlockIO', 'N/A')}")
    
    # Disk usage
    disk_cmd = ["docker", "system", "df"]
    disk_result = subprocess.run(disk_cmd, capture_output=True, text=True)
    print("\n=== ИСПОЛЬЗОВАНИЕ ДИСКА ===")
    print(disk_result.stdout)

# Пример вывода:
"""
=== ИСПОЛЬЗОВАНИЕ РЕСУРСОВ DOCKER ===

КОНТЕЙНЕР          | RAM USAGE  | DISK I/O
--------------------------------------------------
postgres           | 423.2MiB   | 124MB / 89MB
kafka              | 892.1MiB   | 45MB / 234MB
spark-master       | 1.7GiB     | 12MB / 5MB
minio              | 234.5MiB   | 1.2GB / 890MB
redis              | 45.2MiB    | 0B / 0B

=== ИСПОЛЬЗОВАНИЕ ДИСКА ===
TYPE                TOTAL     ACTIVE    SIZE      RECLAIMABLE
Images              5.23GB    4.89GB    3.45GB    340MB (6%)
Containers          1.24GB    1.24GB    892MB     0B (0%)
Local Volumes       12.4GB    11.2GB    8.9GB     1.2GB (9%)
Build Cache         234MB     0B        234MB     234MB
"""
```

### 3. Оптимизация использования ресурсов

```yaml
# docker-compose.yml с оптимизацией
services:
  postgres:
    image: postgres:14-alpine  # Alpine версия меньше на 300MB
    deploy:
      resources:
        limits:
          cpus: '2'
          memory: 1G
        reservations:
          cpus: '0.5'
          memory: 512M
    volumes:
      - postgres_data:/var/lib/postgresql/data
    # tmpfs для временных файлов в RAM
    tmpfs:
      - /tmp
      - /run/postgresql
```

---

## 🔍 Мониторинг использования ресурсов

### 1. Команды для мониторинга

```bash
# Real-time статистика
docker stats

# Использование диска
docker system df

# Детальная информация о контейнере
docker inspect <container_name> | jq '.[0].HostConfig.Memory'

# Логи использования ресурсов
docker events --filter event=oom  # Out of Memory events
```

### 2. Grafana Dashboard для мониторинга

```yaml
# docker-compose.monitoring.yml
services:
  prometheus:
    image: prom/prometheus
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
    
  node-exporter:
    image: prom/node-exporter
    
  cadvisor:
    image: gcr.io/cadvisor/cadvisor
    volumes:
      - /:/rootfs:ro
      - /var/run:/var/run:ro
      - /sys:/sys:ro
      - /var/lib/docker/:/var/lib/docker:ro
```

### 3. Скрипт для автоматической очистки

```bash
#!/bin/bash
# cleanup_docker.sh

echo "=== Docker Cleanup Script ==="

# 1. Остановка неиспользуемых контейнеров
echo "Stopping unused containers..."
docker container prune -f

# 2. Удаление неиспользуемых образов
echo "Removing unused images..."
docker image prune -a -f

# 3. Очистка volumes
echo "Cleaning volumes..."
docker volume prune -f

# 4. Очистка build cache
echo "Cleaning build cache..."
docker buildx prune -f

# 5. Полная система очистка
echo "System cleanup..."
docker system prune -a --volumes -f

# Показать освобожденное место
docker system df
```

---

## 🎯 Ключевые выводы

1. **Docker работает ЛОКАЛЬНО** - все процессы выполняются на вашем компьютере
2. **RAM делится** между хост-системой и контейнерами
3. **Диск используется эффективно** благодаря слоям и Copy-on-Write
4. **Volumes хранят данные** отдельно от контейнеров
5. **Мониторинг важен** для контроля ресурсов

### Рекомендации для вашего проекта:

- Выделите Docker Desktop минимум 8GB RAM
- Резервируйте 50GB на диске для Docker
- Регулярно очищайте неиспользуемые образы
- Используйте `.dockerignore` для уменьшения размера образов
- Мониторьте использование ресурсов через `docker stats`
