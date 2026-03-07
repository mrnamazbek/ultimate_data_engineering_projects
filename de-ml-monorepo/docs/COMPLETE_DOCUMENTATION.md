# 📚 Полная документация DE-ML платформы с Fraud Detection

## 📋 Оглавление

### Основная документация
1. [Введение и обзор проекта](./01_introduction.md)
2. [Архитектура системы](./02_architecture.md)
3. [Технологический стек](./03_technology_stack.md)
4. [Установка и настройка](./04_installation.md)
5. [Работа с данными](./05_data_pipeline.md)
6. [Machine Learning Pipeline](./06_ml_pipeline.md)
7. [API и интерфейсы](./07_api_interfaces.md)
8. [Мониторинг и отладка](./08_monitoring.md)
9. [Best Practices](./09_best_practices.md)
10. [Troubleshooting](./10_troubleshooting.md)

### Специализированные разделы
- [Fraud Detection Module](./fraud-detection-guide.md)
- [Production Deployment](./production-deployment.md)
- [Performance Optimization](./performance-guide.md)
- [Security Guidelines](./security-guide.md)

---

## 🎯 Быстрый старт

### Минимальные требования
- Docker 20.10+
- Docker Compose 2.0+
- 16GB RAM
- 50GB свободного места

### Установка за 5 минут
```bash
# 1. Клонирование репозитория
git clone https://github.com/yourusername/ultimate_data_engineering_projects.git
cd ultimate_data_engineering_projects/de-ml-monorepo

# 2. Настройка окружения
cp .env.example .env
make setup

# 3. Запуск платформы
make start-all

# 4. Проверка статуса
make show-urls
```

### Основные команды
```bash
make fraud-start    # Запустить fraud detection
make notebook       # Открыть Jupyter Lab
make mlflow        # Открыть MLflow UI
make monitor       # Открыть dashboards
make stop-all      # Остановить все сервисы
```

---

## 🏗️ Обзор архитектуры

### Слои архитектуры

```
┌─────────────────────────────────────────────────┐
│            DATA SOURCES LAYER                   │
│  • Banking APIs  • Generators  • Datasets       │
└─────────────────────────────────────────────────┘
                        │
┌─────────────────────────────────────────────────┐
│           STREAMING & INGESTION                 │
│  • Kafka  • Spark Streaming  • Flink            │
└─────────────────────────────────────────────────┘
                        │
┌─────────────────────────────────────────────────┐
│              STORAGE LAYER                      │
│  • PostgreSQL  • MinIO  • Redis  • HDFS         │
└─────────────────────────────────────────────────┘
                        │
┌─────────────────────────────────────────────────┐
│            PROCESSING LAYER                     │
│  • Airflow  • dbt  • Feature Store (Feast)      │
└─────────────────────────────────────────────────┘
                        │
┌─────────────────────────────────────────────────┐
│               ML LAYER                          │
│  • MLflow  • Model Training  • Model Serving    │
└─────────────────────────────────────────────────┘
                        │
┌─────────────────────────────────────────────────┐
│            MONITORING LAYER                     │
│  • Prometheus  • Grafana  • Custom Dashboards   │
└─────────────────────────────────────────────────┘
```

### Ключевые компоненты

| Компонент | Назначение | Порт |
|-----------|------------|------|
| PostgreSQL | Основная БД | 5432 |
| Kafka | Streaming платформа | 9092 |
| Spark | Batch/Stream обработка | 8080 |
| Flink | Low-latency streaming | 8081 |
| Airflow | Оркестрация | 8085 |
| MLflow | ML эксперименты | 5000 |
| Fraud API | Predictions API | 8001 |
| Grafana | Мониторинг | 3000 |
| Superset | BI Analytics | 8088 |
| MinIO | Object Storage | 9001 |

---

## 🚀 Основные возможности

### Data Engineering
- ✅ Real-time data processing (Kafka + Spark/Flink)
- ✅ Batch processing (Airflow + dbt)
- ✅ Data quality checks (Great Expectations)
- ✅ Scalable storage (MinIO + PostgreSQL)

### Machine Learning
- ✅ Automated ML pipeline (MLflow)
- ✅ Feature Store (Feast)
- ✅ Model serving (FastAPI)
- ✅ A/B testing framework

### Fraud Detection
- ✅ Real-time fraud scoring
- ✅ Multiple fraud patterns detection
- ✅ Explainable AI (SHAP)
- ✅ Daily model retraining

### Monitoring
- ✅ System metrics (Prometheus)
- ✅ Business dashboards (Grafana)
- ✅ Data quality monitoring
- ✅ Model performance tracking

---

## 📊 Проект для магистерской диссертации

### Тема исследования
**"Разработка интеллектуальной системы обнаружения мошенничества в транзакциях по кредитным картам с использованием методов машинного и глубокого обучения"**

### Используемые датасеты
1. **IEEE-CIS Fraud Detection** - 590K транзакций
2. **Credit Card Fraud Dataset** - 284K транзакций
3. **PaySim** - 6M+ синтетических транзакций
4. **Real-time data** - Plaid Sandbox API

### ML модели
- XGBoost (baseline)
- LSTM Neural Networks
- Isolation Forest
- Graph Neural Networks
- Ensemble методы

### Метрики производительности
- **Latency**: < 100ms для онлайн предсказаний
- **Throughput**: 10,000 TPS
- **Accuracy**: ROC-AUC > 0.97
- **False Positive Rate**: < 2%

---

## 📁 Структура проекта

```
de-ml-monorepo/
├── projects/
│   ├── fraud-detection/        # Модуль для диссертации
│   │   ├── models/            # Обученные модели
│   │   ├── api/               # REST API
│   │   ├── feature_store/     # Feast конфигурация
│   │   └── experiments/       # MLflow эксперименты
│   ├── data-sources/          # Генераторы данных
│   ├── project1-kafka-spark/  # Streaming pipeline
│   ├── project2-dw-dbt-airflow/ # Data Warehouse
│   └── ...
├── docker-compose.yml         # Основные сервисы
├── docker-compose.fraud.yml   # Fraud detection
├── Makefile                   # Автоматизация
└── docs/                      # Документация
```

---

## 🛠️ Технологии и обоснование выбора

### Почему эти инструменты?

| Технология | Причина выбора | Альтернативы |
|------------|----------------|--------------|
| **Kafka** | Industry standard для streaming, высокая надежность | RabbitMQ, Pulsar |
| **Spark** | Унифицированный batch/stream processing | Flink only |
| **PostgreSQL** | Надежность, расширяемость, JSON support | MySQL, MongoDB |
| **Airflow** | Гибкая оркестрация, Python-based | Prefect, Dagster |
| **MLflow** | Полный ML lifecycle management | Weights&Biases, Neptune |
| **FastAPI** | Высокая производительность, async support | Flask, Django |
| **Docker** | Изоляция, портативность, легкий deployment | VMs, bare metal |

### Интеграция с исследованием

Платформа специально адаптирована для исследовательских задач:
- Легкое добавление новых ML моделей
- Автоматический сбор метрик для сравнения
- Воспроизводимость экспериментов
- Поддержка различных источников данных

---

## 💡 Полезные ссылки

### Внутренние ресурсы
- [Makefile команды](./Makefile) - все доступные команды
- [Environment настройка](./.env.example) - переменные окружения
- [Docker конфигурация](./docker-compose.yml) - сервисы и их настройки

### Внешние ресурсы
- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [Spark Streaming Guide](https://spark.apache.org/docs/latest/streaming-programming-guide.html)
- [MLflow Documentation](https://mlflow.org/docs/latest/index.html)
- [Feast Documentation](https://docs.feast.dev/)

---

## 📞 Поддержка

- **Email**: contact@example.com
- **GitHub Issues**: [Create Issue](https://github.com/yourusername/project/issues)
- **Documentation**: Эта папка `/docs`

---

*Последнее обновление: Январь 2024*
