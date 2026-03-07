# 🎯 Унифицированная организация рабочего пространства

## Структура проекта для магистерской диссертации

```
ultimate_data_engineering_projects/
├── de-ml-monorepo/                    # Основная платформа
│   ├── projects/
│   │   ├── fraud-detection/           # Модуль для диссертации
│   │   │   ├── research/              # Исследовательские notebook'и
│   │   │   ├── experiments/           # MLflow эксперименты
│   │   │   ├── models/                # Обученные модели
│   │   │   ├── data/                  # Локальные данные
│   │   │   └── api/                   # Production API
│   │   └── data-sources/
│   │       └── generators/            # Включая fraud_transaction_generator.py
│   ├── docker-compose.yml             # Основные сервисы
│   ├── docker-compose.fraud.yml       # Fraud detection сервисы
│   └── Makefile                       # Автоматизация команд
├── thesis/                            # Материалы диссертации
│   ├── papers/                        # Научные статьи
│   ├── notebooks/                     # Jupyter notebooks для анализа
│   ├── results/                       # Результаты экспериментов
│   └── manuscript/                    # Текст диссертации
└── scripts/                           # Утилиты и скрипты
```

## 🚀 Makefile для управления всем проектом

```makefile
# ~/projects/de-ml-monorepo/Makefile

.PHONY: help setup start-all stop-all fraud-start fraud-stop notebook mlflow clean

# Цветной вывод
GREEN  := \033[0;32m
YELLOW := \033[0;33m
RED    := \033[0;31m
NC     := \033[0m

help: ## Показать эту справку
	@echo "$(GREEN)Управление проектом DE-ML с Fraud Detection$(NC)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "$(YELLOW)%-20s$(NC) %s\n", $$1, $$2}'

# === БАЗОВЫЕ КОМАНДЫ ===

setup: ## Первоначальная настройка проекта
	@echo "$(GREEN)Setting up project...$(NC)"
	cp .env.example .env
	mkdir -p projects/fraud-detection/{models,data,experiments,research}
	mkdir -p thesis/{papers,notebooks,results,manuscript}
	docker network create de-ml-network 2>/dev/null || true
	@echo "$(GREEN)✓ Setup completed$(NC)"

start-all: ## Запустить всю платформу
	docker-compose up -d
	@echo "$(GREEN)✓ Base platform started$(NC)"
	@sleep 5
	@make show-urls

stop-all: ## Остановить всю платформу
	docker-compose down
	@echo "$(YELLOW)✓ Platform stopped$(NC)"

# === FRAUD DETECTION ===

fraud-start: ## Запустить fraud detection сервисы
	docker-compose -f docker-compose.yml -f docker-compose.fraud.yml --profile fraud up -d
	@echo "$(GREEN)✓ Fraud detection services started$(NC)"
	@make fraud-status

fraud-stop: ## Остановить fraud detection сервисы
	docker-compose -f docker-compose.yml -f docker-compose.fraud.yml --profile fraud down
	@echo "$(YELLOW)✓ Fraud detection services stopped$(NC)"

fraud-logs: ## Показать логи fraud detection
	docker-compose -f docker-compose.yml -f docker-compose.fraud.yml logs -f fraud-generator fraud-api

fraud-status: ## Статус fraud detection сервисов
	@echo "$(GREEN)Fraud Detection Services:$(NC)"
	@echo "  Fraud API: http://localhost:8001/docs"
	@echo "  Feast UI: http://localhost:6566"
	@echo "  Dashboard: http://localhost:8502"

# === ИССЛЕДОВАНИЯ ===

notebook: ## Запустить Jupyter Lab для исследований
	@echo "$(GREEN)Starting Jupyter Lab...$(NC)"
	docker exec -it de-ml-monorepo-jupyter-1 jupyter lab --ip=0.0.0.0 --allow-root

mlflow: ## Открыть MLflow UI
	@echo "$(GREEN)Opening MLflow UI...$(NC)"
	open http://localhost:5000 || xdg-open http://localhost:5000

tensorboard: ## Запустить TensorBoard
	docker run -d --rm -p 6006:6006 --name tensorboard \
		-v $(PWD)/projects/fraud-detection/experiments:/logs \
		tensorflow/tensorflow:latest tensorboard --logdir=/logs --bind_all

# === ДАННЫЕ ===

download-datasets: ## Скачать датасеты для исследования
	@echo "$(GREEN)Downloading datasets...$(NC)"
	python scripts/download_fraud_datasets.py

generate-data: ## Генерировать синтетические данные
	docker exec fraud-generator python /app/generators/fraud_transaction_generator.py \
		--mode batch --output /data/synthetic_fraud_data.parquet

stream-data: ## Запустить стриминг данных в Kafka
	docker exec -d fraud-generator python /app/generators/fraud_transaction_generator.py \
		--mode stream --rate 100

# === ЭКСПЕРИМЕНТЫ ===

train-baseline: ## Обучить baseline модель
	docker exec fraud-ml-trainer python train.py --model baseline --experiment thesis_baseline

train-ensemble: ## Обучить ensemble модель
	docker exec fraud-ml-trainer python train.py --model ensemble --experiment thesis_ensemble

evaluate: ## Оценить все модели
	docker exec fraud-ml-trainer python evaluate.py --compare-all

# === МОНИТОРИНГ ===

monitor: ## Открыть все дашборды мониторинга
	@echo "$(GREEN)Opening monitoring dashboards...$(NC)"
	open http://localhost:3000 || xdg-open http://localhost:3000  # Grafana
	open http://localhost:8088 || xdg-open http://localhost:8088  # Superset
	open http://localhost:8502 || xdg-open http://localhost:8502  # Fraud Dashboard

show-urls: ## Показать все URL сервисов
	@echo "$(GREEN)Service URLs:$(NC)"
	@echo "$(YELLOW)Core Services:$(NC)"
	@echo "  Airflow:    http://localhost:8085 (admin/admin)"
	@echo "  Superset:   http://localhost:8088 (admin/admin)"
	@echo "  MinIO:      http://localhost:9001 (check .env file)"
	@echo "  MLflow:     http://localhost:5000"
	@echo ""
	@echo "$(YELLOW)Fraud Detection:$(NC)"
	@echo "  API:        http://localhost:8001/docs"
	@echo "  Feast:      http://localhost:6566"
	@echo "  Dashboard:  http://localhost:8502"
	@echo ""
	@echo "$(YELLOW)Monitoring:$(NC)"
	@echo "  Grafana:    http://localhost:3000 (admin/admin)"
	@echo "  Prometheus: http://localhost:9090"

# === ТЕСТИРОВАНИЕ ===

test-fraud-api: ## Тестировать Fraud Detection API
	@echo "$(GREEN)Testing Fraud API...$(NC)"
	curl -X POST http://localhost:8001/predict \
		-H "Content-Type: application/json" \
		-d '{"transaction_id":"TEST123","amount":150.00,"merchant_category":"grocery","user_id":"USER_001"}'

load-test: ## Нагрузочное тестирование
	docker run --rm -v $(PWD)/tests:/tests \
		locustio/locust -f /tests/load_test.py \
		--host=http://fraud-api:8000 --users 100 --spawn-rate 10

# === УТИЛИТЫ ===

clean: ## Очистить временные файлы и кеши
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	docker system prune -f

backup: ## Создать backup данных
	@echo "$(GREEN)Creating backup...$(NC)"
	mkdir -p backups/$(shell date +%Y%m%d)
	docker exec postgres pg_dump -U ${POSTGRES_USER} ${POSTGRES_DB} | gzip > backups/$(shell date +%Y%m%d)/postgres.sql.gz
	docker exec fraud-ml-trainer tar -czf - /app/models | gzip > backups/$(shell date +%Y%m%d)/models.tar.gz

reset-all: ## Полный сброс (ОСТОРОЖНО!)
	@echo "$(RED)This will delete all data! Are you sure? [y/N]$(NC)"
	@read ans && [ $${ans} = y ] && docker-compose down -v && rm -rf projects/fraud-detection/data/* || echo "Cancelled"
```

## 🎛️ Централизованная панель управления

### Веб-интерфейс для управления всеми сервисами

```python
# ~/projects/de-ml-monorepo/scripts/control_panel.py

import streamlit as st
import docker
import requests
import pandas as pd
from datetime import datetime
import subprocess
import json

st.set_page_config(
    page_title="DE-ML Control Panel",
    page_icon="🎛️",
    layout="wide"
)

# Docker client
client = docker.from_env()

# Sidebar
st.sidebar.title("🎛️ Control Panel")
section = st.sidebar.radio(
    "Section",
    ["Overview", "Services", "Data Pipeline", "ML Experiments", "Monitoring"]
)

if section == "Overview":
    st.title("🚀 DE-ML Platform Overview")
    
    col1, col2, col3, col4 = st.columns(4)
    
    # Service status
    with col1:
        running_containers = len([c for c in client.containers.list() if c.status == 'running'])
        st.metric("Running Services", running_containers)
    
    with col2:
        # Check Kafka lag
        try:
            kafka_healthy = requests.get("http://localhost:9092/health", timeout=2).ok
        except:
            kafka_healthy = False
        st.metric("Kafka Status", "✅ Healthy" if kafka_healthy else "❌ Down")
    
    with col3:
        # Check fraud API
        try:
            api_response = requests.get("http://localhost:8001/health", timeout=2)
            api_healthy = api_response.status_code == 200
        except:
            api_healthy = False
        st.metric("Fraud API", "✅ Running" if api_healthy else "❌ Down")
    
    with col4:
        # Database connections
        try:
            db_info = subprocess.check_output(
                ["docker", "exec", "postgres", "psql", "-U", os.getenv("POSTGRES_USER", "postgres"), "-c", 
                 "SELECT count(*) FROM pg_stat_activity;"]
            )
            db_connections = int(db_info.decode().split()[2])
        except:
            db_connections = 0
        st.metric("DB Connections", db_connections)
    
    # Quick actions
    st.subheader("Quick Actions")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("🚀 Start All Services"):
            subprocess.run(["make", "start-all"])
            st.success("Services started!")
    
    with col2:
        if st.button("🛑 Stop All Services"):
            subprocess.run(["make", "stop-all"])
            st.warning("Services stopped!")
    
    with col3:
        if st.button("📊 Open MLflow"):
            st.write("Opening MLflow at http://localhost:5000")

elif section == "Services":
    st.title("🔧 Service Management")
    
    # List all containers
    containers = client.containers.list(all=True)
    
    service_data = []
    for container in containers:
        service_data.append({
            "Name": container.name,
            "Status": container.status,
            "Image": container.image.tags[0] if container.image.tags else "N/A",
            "Ports": ", ".join([f"{p['HostPort']}->{p['ContainerPort']}" 
                              for p in container.attrs['NetworkSettings']['Ports'].values() 
                              if p and p[0]['HostPort']])
        })
    
    df = pd.DataFrame(service_data)
    st.dataframe(df, use_container_width=True)
    
    # Service controls
    st.subheader("Service Controls")
    selected_service = st.selectbox("Select Service", [c.name for c in containers])
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        if st.button("▶️ Start"):
            container = client.containers.get(selected_service)
            container.start()
            st.success(f"{selected_service} started!")
    
    with col2:
        if st.button("⏸️ Stop"):
            container = client.containers.get(selected_service)
            container.stop()
            st.warning(f"{selected_service} stopped!")
    
    with col3:
        if st.button("🔄 Restart"):
            container = client.containers.get(selected_service)
            container.restart()
            st.info(f"{selected_service} restarted!")
    
    with col4:
        if st.button("📜 Logs"):
            container = client.containers.get(selected_service)
            logs = container.logs(tail=50).decode()
            st.text_area("Logs", logs, height=300)

elif section == "Data Pipeline":
    st.title("📊 Data Pipeline Management")
    
    # Pipeline status
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🔄 Data Generation")
        
        if st.button("Generate Batch Data"):
            result = subprocess.run(["make", "generate-data"], capture_output=True)
            if result.returncode == 0:
                st.success("Batch data generated!")
            else:
                st.error("Generation failed!")
        
        if st.button("Start Streaming"):
            subprocess.run(["make", "stream-data"])
            st.success("Streaming started!")
        
        # Streaming rate control
        rate = st.slider("Transactions per second", 1, 1000, 100)
        if st.button("Update Rate"):
            # Update streaming rate logic here
            st.info(f"Rate updated to {rate} TPS")
    
    with col2:
        st.subheader("📈 Pipeline Metrics")
        
        # Mock metrics - replace with actual data
        metrics = {
            "Total Transactions": "1,234,567",
            "Fraud Rate": "2.1%",
            "Processing Latency": "45ms",
            "Kafka Lag": "0"
        }
        
        for metric, value in metrics.items():
            st.metric(metric, value)

elif section == "ML Experiments":
    st.title("🧪 ML Experiments")
    
    # MLflow integration
    st.subheader("Recent Experiments")
    
    try:
        # Connect to MLflow
        import mlflow
        mlflow.set_tracking_uri("http://localhost:5000")
        
        experiments = mlflow.search_experiments()
        exp_data = []
        
        for exp in experiments:
            runs = mlflow.search_runs(experiment_ids=[exp.experiment_id], max_results=5)
            for _, run in runs.iterrows():
                exp_data.append({
                    "Experiment": exp.name,
                    "Run ID": run["run_id"][:8],
                    "Model": run.get("params.model_type", "N/A"),
                    "AUC-ROC": round(run.get("metrics.auc_roc", 0), 3),
                    "Status": run["status"],
                    "Duration": f"{run.get('metrics.training_time', 0):.1f}s"
                })
        
        if exp_data:
            df = pd.DataFrame(exp_data)
            st.dataframe(df, use_container_width=True)
        else:
            st.info("No experiments found")
    except Exception as e:
        st.error(f"Could not connect to MLflow: {e}")
    
    # Training controls
    st.subheader("Model Training")
    
    col1, col2 = st.columns(2)
    with col1:
        model_type = st.selectbox("Model Type", ["baseline", "xgboost", "lstm", "ensemble"])
    with col2:
        experiment_name = st.text_input("Experiment Name", f"thesis_{datetime.now().strftime('%Y%m%d_%H%M')}")
    
    if st.button("🚀 Start Training"):
        cmd = f"docker exec fraud-ml-trainer python train.py --model {model_type} --experiment {experiment_name}"
        subprocess.Popen(cmd, shell=True)
        st.success(f"Training started for {model_type} model!")

elif section == "Monitoring":
    st.title("📊 System Monitoring")
    
    # Resource usage
    st.subheader("Resource Usage")
    
    col1, col2, col3 = st.columns(3)
    
    # Docker stats
    stats = {}
    for container in client.containers.list():
        stats[container.name] = container.stats(stream=False)
    
    # Calculate metrics
    total_cpu = sum(s.get('cpu_stats', {}).get('cpu_usage', {}).get('total_usage', 0) for s in stats.values())
    total_memory = sum(s.get('memory_stats', {}).get('usage', 0) for s in stats.values())
    
    with col1:
        st.metric("Total CPU Usage", f"{total_cpu / 1e9:.2f} CPU seconds")
    with col2:
        st.metric("Total Memory", f"{total_memory / 1e9:.2f} GB")
    with col3:
        st.metric("Active Containers", len(stats))
    
    # Service health
    st.subheader("Service Health")
    
    health_data = []
    services = {
        "PostgreSQL": "http://localhost:5432",
        "Kafka": "http://localhost:9092",
        "Fraud API": "http://localhost:8001/health",
        "MLflow": "http://localhost:5000",
        "Airflow": "http://localhost:8085/health",
        "Grafana": "http://localhost:3000/api/health"
    }
    
    for service, url in services.items():
        try:
            if service == "PostgreSQL":
                # Special check for Postgres
                subprocess.check_output(["docker", "exec", "postgres", "pg_isready"])
                status = "✅ Healthy"
            else:
                response = requests.get(url, timeout=2)
                status = "✅ Healthy" if response.status_code == 200 else "⚠️ Degraded"
        except:
            status = "❌ Down"
        
        health_data.append({"Service": service, "Status": status, "Endpoint": url})
    
    health_df = pd.DataFrame(health_data)
    st.dataframe(health_df, use_container_width=True)
    
    # Quick links
    st.subheader("Quick Links")
    
    links = {
        "Grafana Dashboard": "http://localhost:3000",
        "Prometheus": "http://localhost:9090",
        "Airflow UI": "http://localhost:8085",
        "Superset": "http://localhost:8088",
        "Jupyter Lab": "http://localhost:8888"
    }
    
    cols = st.columns(len(links))
    for i, (name, url) in enumerate(links.items()):
        with cols[i]:
            st.markdown(f"[{name}]({url})")

# Footer
st.sidebar.markdown("---")
st.sidebar.info(
    "DE-ML Platform Control Panel\n"
    "For fraud detection thesis research"
)

if __name__ == "__main__":
    # Auto-refresh every 5 seconds for monitoring
    if section == "Monitoring":
        st_autorefresh(interval=5000)
```

## 🔧 VS Code конфигурация для удобной работы

```json
// .vscode/settings.json
{
    "python.defaultInterpreterPath": "${workspaceFolder}/venv/bin/python",
    "python.linting.enabled": true,
    "python.linting.pylintEnabled": true,
    "python.formatting.provider": "black",
    "files.associations": {
        "*.sql": "sql",
        "Dockerfile*": "dockerfile"
    },
    "terminal.integrated.profiles.linux": {
        "bash": {
            "path": "bash",
            "icon": "terminal-bash"
        },
        "make": {
            "path": "bash",
            "args": ["-c", "make help"],
            "icon": "tools"
        }
    },
    "docker.containers.sortBy": "Status",
    "docker.containers.groupBy": "Compose Project Name"
}

// .vscode/tasks.json
{
    "version": "2.0.0",
    "tasks": [
        {
            "label": "Start Platform",
            "type": "shell",
            "command": "make start-all",
            "group": "build",
            "presentation": {
                "reveal": "always",
                "panel": "new"
            }
        },
        {
            "label": "Open Jupyter",
            "type": "shell",
            "command": "make notebook",
            "group": "test"
        },
        {
            "label": "View Logs",
            "type": "shell",
            "command": "docker-compose logs -f",
            "presentation": {
                "reveal": "always",
                "panel": "dedicated"
            }
        }
    ]
}
```

## 🚀 Запуск централизованной панели управления

```bash
# Установка зависимостей для панели
pip install streamlit docker pandas requests

# Запуск панели управления
streamlit run scripts/control_panel.py --server.port 8505

# Или добавить в Makefile:
control-panel: ## Запустить панель управления
	streamlit run scripts/control_panel.py --server.port 8505
```

## 📱 Мобильный мониторинг через Telegram Bot

```python
# scripts/telegram_monitor.py
import telebot
import docker
import subprocess
from datetime import datetime

BOT_TOKEN = 'YOUR_TELEGRAM_BOT_TOKEN'
bot = telebot.TeleBot(BOT_TOKEN)
client = docker.from_env()

@bot.message_handler(commands=['status'])
def send_status(message):
    """Отправить статус всех сервисов"""
    containers = client.containers.list()
    status_text = f"🚀 Platform Status ({datetime.now().strftime('%H:%M:%S')})\n\n"
    
    for container in containers:
        emoji = "✅" if container.status == "running" else "❌"
        status_text += f"{emoji} {container.name}: {container.status}\n"
    
    bot.reply_to(message, status_text)

@bot.message_handler(commands=['start_fraud'])
def start_fraud(message):
    """Запустить fraud detection"""
    result = subprocess.run(["make", "fraud-start"], capture_output=True)
    if result.returncode == 0:
        bot.reply_to(message, "✅ Fraud detection services started!")
    else:
        bot.reply_to(message, "❌ Failed to start services")

@bot.message_handler(commands=['metrics'])
def send_metrics(message):
    """Отправить ключевые метрики"""
    # Здесь подключение к Prometheus/Grafana для получения метрик
    metrics = """
📊 Current Metrics:
• Fraud Detection Rate: 97.3%
• False Positive Rate: 1.8%
• API Latency: 43ms (p99)
• Model Version: v2.3.1
• Transactions/sec: 127
    """
    bot.reply_to(message, metrics)

if __name__ == "__main__":
    bot.polling()
```

## 🎯 Best Practices для организации

1. **Используйте Makefile** для всех частых операций
2. **Документируйте каждый сервис** в отдельном README
3. **Версионируйте конфигурации** через Git
4. **Автоматизируйте backup** данных и моделей
5. **Мониторьте ресурсы** через Grafana dashboards
6. **Используйте tmux/screen** для long-running процессов
7. **Настройте CI/CD** через GitHub Actions

С этой организацией вы сможете эффективно управлять всеми компонентами вашего проекта из одного места!
