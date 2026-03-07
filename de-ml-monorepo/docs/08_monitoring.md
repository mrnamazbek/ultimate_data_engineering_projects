# 📊 Мониторинг и отладка

## Обзор системы мониторинга

### Архитектура мониторинга

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         MONITORING ARCHITECTURE                          │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐            │
│  │   Metrics    │    │     Logs     │    │    Traces    │            │
│  │ Collection   │    │  Collection  │    │  Collection  │            │
│  │ (Prometheus) │    │  (Fluentd)   │    │   (Jaeger)   │            │
│  └──────┬───────┘    └──────┬───────┘    └──────┬───────┘            │
│         │                    │                    │                     │
│         ▼                    ▼                    ▼                     │
│  ┌─────────────────────────────────────────────────────┐              │
│  │                   Storage Layer                      │              │
│  │  • Prometheus TSDB  • Elasticsearch  • Jaeger DB    │              │
│  └─────────────────┬───────────────────────────────────┘              │
│                    │                                                   │
│                    ▼                                                   │
│  ┌─────────────────────────────────────────────────────┐              │
│  │              Visualization & Analysis                │              │
│  │  • Grafana    • Kibana    • Jaeger UI   • Alerts   │              │
│  └─────────────────────────────────────────────────────┘              │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Компоненты мониторинга

| Компонент | Назначение | Доступ |
|-----------|------------|--------|
| **Prometheus** | Сбор метрик | http://localhost:9090 |
| **Grafana** | Визуализация метрик | http://localhost:3000 |
| **Elasticsearch** | Хранение логов | http://localhost:9200 |
| **Kibana** | Анализ логов | http://localhost:5601 |
| **Jaeger** | Distributed tracing | http://localhost:16686 |
| **AlertManager** | Управление алертами | http://localhost:9093 |

## 📈 Метрики и KPI

### Business Metrics

```yaml
# Ключевые бизнес-метрики для fraud detection
fraud_detection_metrics:
  - name: fraud_detection_rate
    description: Процент обнаруженных мошеннических транзакций
    formula: true_positives / (true_positives + false_negatives)
    target: "> 95%"
    
  - name: false_positive_rate
    description: Процент ложных срабатываний
    formula: false_positives / (false_positives + true_negatives)
    target: "< 2%"
    
  - name: average_detection_time
    description: Среднее время обнаружения fraud
    unit: milliseconds
    target: "< 100ms"
    
  - name: daily_transaction_volume
    description: Количество обработанных транзакций в день
    unit: transactions
    
  - name: prevented_fraud_amount
    description: Сумма предотвращенного мошенничества
    unit: USD
```

### Technical Metrics

```python
# monitoring/metrics.py
from prometheus_client import Counter, Histogram, Gauge, Summary
import time

# Счетчики
transaction_counter = Counter(
    'fraud_detection_transactions_total',
    'Total number of processed transactions',
    ['status', 'fraud_type', 'channel']
)

prediction_errors = Counter(
    'fraud_detection_errors_total',
    'Total number of prediction errors',
    ['error_type', 'service']
)

# Гистограммы
prediction_duration = Histogram(
    'fraud_detection_prediction_duration_seconds',
    'Time spent processing fraud prediction',
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5]
)

feature_fetch_duration = Histogram(
    'fraud_detection_feature_fetch_duration_seconds',
    'Time spent fetching features',
    ['source']  # 'cache', 'database', 'api'
)

# Gauges
active_models = Gauge(
    'fraud_detection_active_models',
    'Number of active ML models',
    ['model_type', 'version']
)

model_accuracy = Gauge(
    'fraud_detection_model_accuracy',
    'Current model accuracy score',
    ['model_name']
)

queue_size = Gauge(
    'fraud_detection_queue_size',
    'Number of transactions in processing queue',
    ['queue_name']
)

# Summary
api_request_duration = Summary(
    'fraud_detection_api_request_duration_seconds',
    'API request duration',
    ['method', 'endpoint']
)

# Использование метрик в коде
class MetricsMiddleware:
    """Middleware для сбора метрик"""
    
    async def __call__(self, request, call_next):
        start_time = time.time()
        
        # Labels для метрик
        method = request.method
        endpoint = request.url.path
        
        try:
            response = await call_next(request)
            
            # Успешный запрос
            duration = time.time() - start_time
            api_request_duration.labels(
                method=method,
                endpoint=endpoint
            ).observe(duration)
            
            return response
            
        except Exception as e:
            # Ошибка
            prediction_errors.labels(
                error_type=type(e).__name__,
                service='api'
            ).inc()
            raise
```

## 🔍 Prometheus Configuration

### prometheus.yml

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s
  external_labels:
    cluster: 'fraud-detection-prod'
    region: 'us-east-1'

# Alerting configuration
alerting:
  alertmanagers:
    - static_configs:
        - targets:
          - alertmanager:9093

# Rules files
rule_files:
  - "alerts/*.yml"

# Scrape configurations
scrape_configs:
  # Fraud Detection API
  - job_name: 'fraud-detection-api'
    static_configs:
      - targets: ['fraud-api:8001']
    metrics_path: '/metrics'
    
  # PostgreSQL
  - job_name: 'postgres'
    static_configs:
      - targets: ['postgres-exporter:9187']
      
  # Kafka
  - job_name: 'kafka'
    static_configs:
      - targets: ['kafka-exporter:9308']
      
  # Redis
  - job_name: 'redis'
    static_configs:
      - targets: ['redis-exporter:9121']
      
  # Node Exporter (system metrics)
  - job_name: 'node'
    static_configs:
      - targets: ['node-exporter:9100']
      
  # Cadvisor (container metrics)
  - job_name: 'cadvisor'
    static_configs:
      - targets: ['cadvisor:8080']
```

### Alert Rules

```yaml
# alerts/fraud_detection_alerts.yml
groups:
  - name: fraud_detection
    interval: 30s
    rules:
      # High error rate
      - alert: HighErrorRate
        expr: |
          rate(fraud_detection_errors_total[5m]) > 0.05
        for: 5m
        labels:
          severity: critical
          team: fraud-detection
        annotations:
          summary: "High error rate detected"
          description: "Error rate is {{ $value }} errors per second"
          
      # Low fraud detection rate
      - alert: LowFraudDetectionRate
        expr: |
          fraud_detection_rate < 0.9
        for: 10m
        labels:
          severity: warning
          team: fraud-detection
        annotations:
          summary: "Fraud detection rate below threshold"
          description: "Current rate: {{ $value }}"
          
      # High latency
      - alert: HighPredictionLatency
        expr: |
          histogram_quantile(0.95, 
            rate(fraud_detection_prediction_duration_seconds_bucket[5m])
          ) > 0.1
        for: 5m
        labels:
          severity: warning
          team: fraud-detection
        annotations:
          summary: "High prediction latency"
          description: "95th percentile latency is {{ $value }}s"
          
      # Model accuracy degradation
      - alert: ModelAccuracyDegradation
        expr: |
          fraud_detection_model_accuracy < 0.9
        for: 30m
        labels:
          severity: critical
          team: ml-team
        annotations:
          summary: "Model accuracy below threshold"
          description: "Model {{ $labels.model_name }} accuracy: {{ $value }}"
```

## 📊 Grafana Dashboards

### Fraud Detection Overview Dashboard

```json
{
  "dashboard": {
    "title": "Fraud Detection Overview",
    "panels": [
      {
        "title": "Transaction Volume",
        "gridPos": {"h": 8, "w": 12, "x": 0, "y": 0},
        "targets": [
          {
            "expr": "sum(rate(fraud_detection_transactions_total[5m])) by (status)",
            "legendFormat": "{{ status }}"
          }
        ],
        "type": "graph"
      },
      {
        "title": "Fraud Detection Rate",
        "gridPos": {"h": 8, "w": 12, "x": 12, "y": 0},
        "targets": [
          {
            "expr": "fraud_detection_rate",
            "legendFormat": "Detection Rate"
          }
        ],
        "type": "gauge",
        "options": {
          "thresholds": {
            "steps": [
              {"color": "red", "value": 0},
              {"color": "yellow", "value": 0.9},
              {"color": "green", "value": 0.95}
            ]
          }
        }
      },
      {
        "title": "Prediction Latency (p50, p95, p99)",
        "gridPos": {"h": 8, "w": 24, "x": 0, "y": 8},
        "targets": [
          {
            "expr": "histogram_quantile(0.50, rate(fraud_detection_prediction_duration_seconds_bucket[5m]))",
            "legendFormat": "p50"
          },
          {
            "expr": "histogram_quantile(0.95, rate(fraud_detection_prediction_duration_seconds_bucket[5m]))",
            "legendFormat": "p95"
          },
          {
            "expr": "histogram_quantile(0.99, rate(fraud_detection_prediction_duration_seconds_bucket[5m]))",
            "legendFormat": "p99"
          }
        ],
        "type": "graph",
        "yaxes": [{"format": "s"}]
      },
      {
        "title": "Error Rate",
        "gridPos": {"h": 8, "w": 12, "x": 0, "y": 16},
        "targets": [
          {
            "expr": "sum(rate(fraud_detection_errors_total[5m])) by (error_type)",
            "legendFormat": "{{ error_type }}"
          }
        ],
        "type": "graph"
      },
      {
        "title": "Model Performance",
        "gridPos": {"h": 8, "w": 12, "x": 12, "y": 16},
        "targets": [
          {
            "expr": "fraud_detection_model_accuracy",
            "legendFormat": "{{ model_name }}"
          }
        ],
        "type": "graph"
      }
    ]
  }
}
```

### Custom Dashboard Code

```python
# dashboards/fraud_detection_dashboard.py
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
import pandas as pd
from prometheus_api_client import PrometheusConnect
from datetime import datetime, timedelta

class FraudDetectionDashboard:
    def __init__(self, prometheus_url='http://localhost:9090'):
        self.prom = PrometheusConnect(url=prometheus_url)
        
    def render(self):
        st.set_page_config(
            page_title="Fraud Detection Monitoring",
            page_icon="🚨",
            layout="wide"
        )
        
        st.title("🚨 Fraud Detection Real-time Monitoring")
        
        # Время обновления
        col1, col2, col3 = st.columns([1, 1, 2])
        with col1:
            if st.button("🔄 Refresh"):
                st.rerun()
        with col2:
            auto_refresh = st.checkbox("Auto-refresh (5s)", value=True)
        with col3:
            st.write(f"Last update: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        if auto_refresh:
            st_autorefresh(interval=5000, key="datarefresh")
        
        # Основные метрики
        self._render_key_metrics()
        
        # Графики
        col1, col2 = st.columns(2)
        with col1:
            self._render_transaction_volume()
        with col2:
            self._render_fraud_detection_rate()
        
        # Latency и errors
        self._render_latency_chart()
        self._render_error_analysis()
        
        # Детальная информация
        with st.expander("📊 Detailed Metrics"):
            self._render_detailed_metrics()
    
    def _render_key_metrics(self):
        """Отображение ключевых метрик"""
        col1, col2, col3, col4 = st.columns(4)
        
        # Transactions per second
        with col1:
            tps = self._query_prometheus(
                'sum(rate(fraud_detection_transactions_total[1m]))'
            )
            st.metric(
                "Transactions/sec",
                f"{tps:.1f}",
                delta=f"{tps - 100:.1f}"
            )
        
        # Fraud detection rate
        with col2:
            detection_rate = self._query_prometheus('fraud_detection_rate')
            st.metric(
                "Detection Rate",
                f"{detection_rate:.1%}",
                delta=f"{(detection_rate - 0.95):.1%}"
            )
        
        # Average latency
        with col3:
            latency = self._query_prometheus(
                'histogram_quantile(0.95, rate(fraud_detection_prediction_duration_seconds_bucket[5m]))'
            )
            st.metric(
                "P95 Latency",
                f"{latency*1000:.0f}ms",
                delta=f"{(latency*1000 - 100):.0f}ms"
            )
        
        # Error rate
        with col4:
            error_rate = self._query_prometheus(
                'sum(rate(fraud_detection_errors_total[5m]))'
            )
            st.metric(
                "Error Rate",
                f"{error_rate:.3f}/s",
                delta=f"{error_rate:.3f}"
            )
    
    def _query_prometheus(self, query):
        """Выполнение Prometheus запроса"""
        result = self.prom.custom_query(query)
        if result:
            return float(result[0]['value'][1])
        return 0
```

## 📝 Логирование

### Structured Logging

```python
# logging/structured_logger.py
import logging
import json
from datetime import datetime
from typing import Dict, Any
import traceback
from pythonjsonlogger import jsonlogger

class StructuredLogger:
    """Структурированное логирование для лучшего анализа"""
    
    def __init__(self, name: str, level=logging.INFO):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        
        # JSON formatter
        formatter = jsonlogger.JsonFormatter(
            fmt='%(timestamp)s %(level)s %(name)s %(message)s'
        )
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        
        # File handler для production
        file_handler = logging.FileHandler('/logs/fraud-detection.json')
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)
    
    def log_transaction(self, transaction_id: str, event: str, **kwargs):
        """Логирование событий транзакции"""
        self.logger.info(
            event,
            extra={
                'timestamp': datetime.utcnow().isoformat(),
                'transaction_id': transaction_id,
                'event_type': 'transaction',
                **kwargs
            }
        )
    
    def log_prediction(self, transaction_id: str, prediction: Dict[str, Any]):
        """Логирование предсказания"""
        self.logger.info(
            "Fraud prediction made",
            extra={
                'timestamp': datetime.utcnow().isoformat(),
                'transaction_id': transaction_id,
                'event_type': 'prediction',
                'is_fraud': prediction['is_fraud'],
                'probability': prediction['probability'],
                'risk_factors': prediction.get('risk_factors', []),
                'model_version': prediction.get('model_version'),
                'processing_time_ms': prediction.get('processing_time_ms')
            }
        )
    
    def log_error(self, error: Exception, context: Dict[str, Any] = None):
        """Логирование ошибок"""
        self.logger.error(
            f"Error: {str(error)}",
            extra={
                'timestamp': datetime.utcnow().isoformat(),
                'event_type': 'error',
                'error_type': type(error).__name__,
                'error_message': str(error),
                'stack_trace': traceback.format_exc(),
                'context': context or {}
            }
        )
    
    def log_performance(self, operation: str, duration_ms: float, **kwargs):
        """Логирование производительности"""
        self.logger.info(
            f"Performance: {operation}",
            extra={
                'timestamp': datetime.utcnow().isoformat(),
                'event_type': 'performance',
                'operation': operation,
                'duration_ms': duration_ms,
                **kwargs
            }
        )

# Использование
logger = StructuredLogger('fraud-detection')

# Пример логирования
logger.log_transaction(
    transaction_id="TXN_123",
    event="Transaction received",
    amount=1500.00,
    merchant_category="online",
    user_id="USER_456"
)

logger.log_prediction(
    transaction_id="TXN_123",
    prediction={
        'is_fraud': True,
        'probability': 0.89,
        'risk_factors': ['high_amount', 'unusual_time'],
        'model_version': 'v2.3.1',
        'processing_time_ms': 45.2
    }
)
```

### Log Aggregation с ELK

```yaml
# logstash/pipeline.conf
input {
  # Сбор логов из файлов
  file {
    path => "/logs/*.json"
    codec => "json"
    type => "fraud-detection"
    start_position => "beginning"
  }
  
  # Сбор из Kafka
  kafka {
    bootstrap_servers => "kafka:9092"
    topics => ["application-logs"]
    codec => "json"
  }
}

filter {
  # Парсинг timestamp
  date {
    match => [ "timestamp", "ISO8601" ]
    target => "@timestamp"
  }
  
  # Обогащение данными
  if [transaction_id] {
    elasticsearch {
      hosts => ["elasticsearch:9200"]
      index => "transactions"
      query => "transaction_id:%{[transaction_id]}"
      fields => {
        "user_id" => "enriched_user_id"
        "merchant_id" => "enriched_merchant_id"
      }
    }
  }
  
  # Классификация логов
  if [event_type] == "error" {
    mutate {
      add_field => { "alert_required" => true }
    }
  }
  
  # Метрики из логов
  if [event_type] == "prediction" {
    metrics {
      meter => "fraud_predictions"
      add_tag => "metric"
    }
  }
}

output {
  # Основной вывод в Elasticsearch
  elasticsearch {
    hosts => ["elasticsearch:9200"]
    index => "fraud-detection-%{+YYYY.MM.dd}"
    template_name => "fraud-detection"
    template => "/etc/logstash/templates/fraud-detection.json"
  }
  
  # Критические ошибки в отдельный индекс
  if [alert_required] {
    elasticsearch {
      hosts => ["elasticsearch:9200"]
      index => "fraud-detection-alerts-%{+YYYY.MM.dd}"
    }
    
    # Отправка в систему алертов
    http {
      url => "http://alert-manager:9093/api/v1/alerts"
      http_method => "post"
      format => "json"
    }
  }
}
```

## 🔎 Distributed Tracing

### Jaeger Integration

```python
# tracing/tracer_setup.py
from jaeger_client import Config
from opentracing.scope_managers.contextvars import ContextVarsScopeManager
import logging

def init_tracer(service_name='fraud-detection-api'):
    """Инициализация Jaeger tracer"""
    
    config = Config(
        config={
            'sampler': {
                'type': 'probabilistic',
                'param': 0.1,  # Sample 10% of traces
            },
            'local_agent': {
                'reporting_host': 'jaeger',
                'reporting_port': '6831',
            },
            'logging': True,
            'propagation': 'b3',  # Use B3 propagation for compatibility
        },
        service_name=service_name,
        scope_manager=ContextVarsScopeManager(),
        validate=True,
    )
    
    return config.initialize_tracer()

# Использование в API
from fastapi import FastAPI, Request
from opentracing import tracer
import opentracing

app = FastAPI()
tracer = init_tracer()

@app.middleware("http")
async def tracing_middleware(request: Request, call_next):
    """Middleware для трассировки запросов"""
    
    # Извлечение span context из headers
    span_context = tracer.extract(
        format=opentracing.Format.HTTP_HEADERS,
        carrier=dict(request.headers)
    )
    
    # Создание нового span
    with tracer.start_span(
        f"{request.method} {request.url.path}",
        child_of=span_context
    ) as span:
        # Добавление тегов
        span.set_tag("http.method", request.method)
        span.set_tag("http.url", str(request.url))
        span.set_tag("user.id", request.headers.get("X-User-ID"))
        
        try:
            response = await call_next(request)
            span.set_tag("http.status_code", response.status_code)
            return response
            
        except Exception as e:
            span.set_tag("error", True)
            span.log_kv({
                "event": "error",
                "message": str(e),
                "error.kind": type(e).__name__
            })
            raise

# Трассировка функций
def trace_function(operation_name: str):
    """Декоратор для трассировки функций"""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            with tracer.start_span(operation_name) as span:
                try:
                    # Добавление параметров как тегов
                    for key, value in kwargs.items():
                        if isinstance(value, (str, int, float, bool)):
                            span.set_tag(f"param.{key}", value)
                    
                    result = await func(*args, **kwargs)
                    return result
                    
                except Exception as e:
                    span.set_tag("error", True)
                    span.log_kv({
                        "event": "error",
                        "message": str(e)
                    })
                    raise
        return wrapper
    return decorator

# Пример использования
@trace_function("fetch_user_features")
async def fetch_user_features(user_id: str):
    """Получение фичей пользователя с трассировкой"""
    with tracer.start_span("redis_lookup") as span:
        # Redis lookup
        features = await redis_client.get(f"features:{user_id}")
        
    if not features:
        with tracer.start_span("database_query") as span:
            # Database query
            features = await db.fetch_user_features(user_id)
            
    return features
```

## 🚨 Alerting

### AlertManager Configuration

```yaml
# alertmanager/alertmanager.yml
global:
  resolve_timeout: 5m
  slack_api_url: 'YOUR_SLACK_WEBHOOK_URL'
  pagerduty_url: 'https://events.pagerduty.com/v2/enqueue'

route:
  group_by: ['alertname', 'cluster', 'service']
  group_wait: 10s
  group_interval: 10s
  repeat_interval: 12h
  receiver: 'default'
  
  routes:
    # Critical alerts
    - match:
        severity: critical
      receiver: pagerduty
      continue: true
      
    # ML team alerts
    - match:
        team: ml-team
      receiver: ml-slack
      
    # Fraud detection specific
    - match:
        service: fraud-detection
      receiver: fraud-team

receivers:
  - name: 'default'
    slack_configs:
      - channel: '#alerts'
        title: 'Alert: {{ .GroupLabels.alertname }}'
        text: '{{ range .Alerts }}{{ .Annotations.description }}{{ end }}'
        
  - name: 'pagerduty'
    pagerduty_configs:
      - service_key: 'YOUR_PAGERDUTY_SERVICE_KEY'
        description: '{{ .GroupLabels.alertname }}'
        
  - name: 'fraud-team'
    slack_configs:
      - channel: '#fraud-detection-alerts'
        title: 'Fraud Detection Alert'
        text: |
          *Alert:* {{ .GroupLabels.alertname }}
          *Severity:* {{ .CommonLabels.severity }}
          *Description:* {{ .CommonAnnotations.description }}
          *Dashboard:* <http://grafana:3000/d/fraud-detection|View Dashboard>
        send_resolved: true
        
  - name: 'ml-slack'
    slack_configs:
      - channel: '#ml-team'
        title: 'ML Model Alert'

inhibit_rules:
  - source_match:
      severity: 'critical'
    target_match:
      severity: 'warning'
    equal: ['alertname', 'cluster', 'service']
```

### Custom Alert Implementation

```python
# alerting/alert_manager.py
import aiohttp
import asyncio
from datetime import datetime
from typing import Dict, List, Optional
import json

class AlertManager:
    """Кастомный менеджер алертов"""
    
    def __init__(self, webhook_urls: Dict[str, str]):
        self.webhook_urls = webhook_urls
        self.alert_history = []
        
    async def send_alert(self, 
                        alert_type: str,
                        severity: str,
                        title: str,
                        description: str,
                        metadata: Optional[Dict] = None):
        """Отправка алерта"""
        
        alert = {
            "alert_type": alert_type,
            "severity": severity,
            "title": title,
            "description": description,
            "timestamp": datetime.utcnow().isoformat(),
            "metadata": metadata or {}
        }
        
        # Сохранение в историю
        self.alert_history.append(alert)
        
        # Отправка в различные каналы
        tasks = []
        
        if severity == "critical":
            tasks.append(self._send_to_pagerduty(alert))
            
        tasks.append(self._send_to_slack(alert))
        
        if alert_type == "model_degradation":
            tasks.append(self._send_to_ml_team(alert))
            
        await asyncio.gather(*tasks)
    
    async def _send_to_slack(self, alert: Dict):
        """Отправка в Slack"""
        slack_message = {
            "text": f"🚨 *{alert['title']}*",
            "attachments": [{
                "color": self._get_severity_color(alert['severity']),
                "fields": [
                    {
                        "title": "Severity",
                        "value": alert['severity'].upper(),
                        "short": True
                    },
                    {
                        "title": "Time",
                        "value": alert['timestamp'],
                        "short": True
                    },
                    {
                        "title": "Description",
                        "value": alert['description'],
                        "short": False
                    }
                ],
                "footer": "Fraud Detection System"
            }]
        }
        
        async with aiohttp.ClientSession() as session:
            await session.post(
                self.webhook_urls['slack'],
                json=slack_message
            )
    
    async def _send_to_pagerduty(self, alert: Dict):
        """Отправка в PagerDuty для критических алертов"""
        pagerduty_event = {
            "routing_key": self.webhook_urls['pagerduty_key'],
            "event_action": "trigger",
            "payload": {
                "summary": alert['title'],
                "severity": "error",
                "source": "fraud-detection",
                "custom_details": alert
            }
        }
        
        async with aiohttp.ClientSession() as session:
            await session.post(
                "https://events.pagerduty.com/v2/enqueue",
                json=pagerduty_event
            )
    
    def _get_severity_color(self, severity: str) -> str:
        """Цвет для Slack сообщения"""
        colors = {
            "critical": "#FF0000",
            "warning": "#FFA500",
            "info": "#0000FF"
        }
        return colors.get(severity, "#808080")

# Использование
alert_manager = AlertManager({
    'slack': 'https://hooks.slack.com/services/YOUR/WEBHOOK/URL',
    'pagerduty_key': 'YOUR_PAGERDUTY_INTEGRATION_KEY'
})

# Отправка алерта
await alert_manager.send_alert(
    alert_type='high_fraud_rate',
    severity='critical',
    title='High Fraud Rate Detected',
    description=f'Fraud rate increased to 5.2% (threshold: 3%)',
    metadata={
        'current_rate': 0.052,
        'threshold': 0.03,
        'affected_users': 150
    }
)
```

## 📈 Performance Monitoring

### Application Performance Monitoring (APM)

```python
# apm/performance_monitor.py
import time
import psutil
import asyncio
from typing import Dict, Callable
import numpy as np

class PerformanceMonitor:
    """Мониторинг производительности приложения"""
    
    def __init__(self):
        self.metrics = {
            'request_times': [],
            'memory_usage': [],
            'cpu_usage': [],
            'active_connections': 0
        }
        self.start_background_monitoring()
    
    def start_background_monitoring(self):
        """Запуск фонового мониторинга"""
        asyncio.create_task(self._monitor_system_resources())
    
    async def _monitor_system_resources(self):
        """Мониторинг системных ресурсов"""
        while True:
            # CPU usage
            cpu_percent = psutil.cpu_percent(interval=1)
            self.metrics['cpu_usage'].append(cpu_percent)
            
            # Memory usage
            memory = psutil.virtual_memory()
            self.metrics['memory_usage'].append(memory.percent)
            
            # Отправка метрик в Prometheus
            cpu_usage_gauge.set(cpu_percent)
            memory_usage_gauge.set(memory.percent)
            
            # Очистка старых метрик (хранить последний час)
            if len(self.metrics['cpu_usage']) > 3600:
                self.metrics['cpu_usage'] = self.metrics['cpu_usage'][-3600:]
                self.metrics['memory_usage'] = self.metrics['memory_usage'][-3600:]
            
            await asyncio.sleep(1)
    
    def measure_performance(self, func: Callable):
        """Декоратор для измерения производительности"""
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            
            try:
                result = await func(*args, **kwargs)
                duration = time.time() - start_time
                
                # Сохранение метрики
                self.metrics['request_times'].append(duration)
                
                # Prometheus метрика
                prediction_duration.observe(duration)
                
                # Логирование медленных запросов
                if duration > 1.0:  # 1 second threshold
                    logger.warning(
                        f"Slow request detected",
                        extra={
                            'function': func.__name__,
                            'duration_seconds': duration,
                            'args': str(args)[:100]
                        }
                    )
                
                return result
                
            except Exception as e:
                prediction_errors.labels(
                    error_type=type(e).__name__,
                    service=func.__name__
                ).inc()
                raise
        
        return wrapper
    
    def get_performance_stats(self) -> Dict:
        """Получение статистики производительности"""
        if self.metrics['request_times']:
            request_times = np.array(self.metrics['request_times'][-1000:])  # Last 1000 requests
            
            return {
                'avg_response_time': np.mean(request_times),
                'p50_response_time': np.percentile(request_times, 50),
                'p95_response_time': np.percentile(request_times, 95),
                'p99_response_time': np.percentile(request_times, 99),
                'max_response_time': np.max(request_times),
                'total_requests': len(self.metrics['request_times']),
                'avg_cpu_usage': np.mean(self.metrics['cpu_usage'][-60:]),  # Last minute
                'avg_memory_usage': np.mean(self.metrics['memory_usage'][-60:]),
                'active_connections': self.metrics['active_connections']
            }
        return {}
```

## 🔧 Debugging Tools

### Debug Endpoints

```python
# debug/debug_endpoints.py
from fastapi import APIRouter, HTTPException, Query
import gc
import objgraph
import tracemalloc
import sys
from datetime import datetime
import threading

router = APIRouter(prefix="/debug", tags=["debug"])

# Начало трассировки памяти
tracemalloc.start()

@router.get("/health/detailed")
async def detailed_health_check():
    """Детальная проверка здоровья системы"""
    
    health_status = {
        "timestamp": datetime.utcnow().isoformat(),
        "service": "fraud-detection-api",
        "version": "2.0.0",
        "status": "healthy",
        "checks": {}
    }
    
    # Database check
    try:
        await db.execute("SELECT 1")
        health_status["checks"]["database"] = {
            "status": "healthy",
            "response_time_ms": 5.2
        }
    except Exception as e:
        health_status["checks"]["database"] = {
            "status": "unhealthy",
            "error": str(e)
        }
        health_status["status"] = "degraded"
    
    # Redis check
    try:
        await redis_client.ping()
        health_status["checks"]["redis"] = {
            "status": "healthy",
            "response_time_ms": 1.1
        }
    except Exception as e:
        health_status["checks"]["redis"] = {
            "status": "unhealthy",
            "error": str(e)
        }
        health_status["status"] = "degraded"
    
    # Model check
    try:
        if model_server.model is not None:
            health_status["checks"]["ml_model"] = {
                "status": "healthy",
                "model_version": model_server.model_version,
                "loaded_at": model_server.loaded_at
            }
        else:
            health_status["checks"]["ml_model"] = {
                "status": "unhealthy",
                "error": "Model not loaded"
            }
    except Exception as e:
        health_status["checks"]["ml_model"] = {
            "status": "unhealthy",
            "error": str(e)
        }
    
    return health_status

@router.get("/memory/stats")
async def memory_stats():
    """Статистика использования памяти"""
    
    # Сборка мусора
    gc.collect()
    
    # Текущее использование памяти
    current, peak = tracemalloc.get_traced_memory()
    
    # Top memory allocations
    snapshot = tracemalloc.take_snapshot()
    top_stats = snapshot.statistics('lineno')[:10]
    
    return {
        "current_memory_mb": current / 1024 / 1024,
        "peak_memory_mb": peak / 1024 / 1024,
        "gc_stats": gc.get_stats(),
        "object_counts": {
            "total": len(gc.get_objects()),
            "dict": len([o for o in gc.get_objects() if isinstance(o, dict)]),
            "list": len([o for o in gc.get_objects() if isinstance(o, list)]),
            "tuple": len([o for o in gc.get_objects() if isinstance(o, tuple)])
        },
        "top_allocations": [
            {
                "file": stat.filename,
                "line": stat.lineno,
                "size_mb": stat.size / 1024 / 1024,
                "count": stat.count
            }
            for stat in top_stats
        ]
    }

@router.get("/threads")
async def thread_info():
    """Информация о потоках"""
    
    threads_info = []
    for thread in threading.enumerate():
        threads_info.append({
            "name": thread.name,
            "daemon": thread.daemon,
            "alive": thread.is_alive(),
            "ident": thread.ident
        })
    
    return {
        "active_threads": threading.active_count(),
        "threads": threads_info
    }

@router.get("/config")
async def get_config(include_secrets: bool = Query(False)):
    """Получение текущей конфигурации"""
    
    config = {
        "environment": os.getenv("ENVIRONMENT", "development"),
        "debug_mode": settings.DEBUG,
        "log_level": settings.LOG_LEVEL,
        "model": {
            "name": settings.MODEL_NAME,
            "version": model_server.model_version,
            "batch_size": settings.BATCH_SIZE
        },
        "database": {
            "host": settings.DB_HOST,
            "port": settings.DB_PORT,
            "name": settings.DB_NAME
        },
        "redis": {
            "host": settings.REDIS_HOST,
            "port": settings.REDIS_PORT
        },
        "kafka": {
            "brokers": settings.KAFKA_BROKERS,
            "topics": settings.KAFKA_TOPICS
        }
    }
    
    if include_secrets:
        # Только для authorized users
        pass
    
    return config

@router.post("/test/predict")
async def test_prediction(
    transaction_type: str = Query("normal", enum=["normal", "suspicious", "fraud"])
):
    """Тестовое предсказание для отладки"""
    
    test_transactions = {
        "normal": {
            "transaction_id": "TEST_NORMAL_001",
            "user_id": "USER_TEST",
            "merchant_id": "MERCH_GROCERY",
            "amount": 50.00,
            "merchant_category": "grocery"
        },
        "suspicious": {
            "transaction_id": "TEST_SUSPICIOUS_001",
            "user_id": "USER_TEST",
            "merchant_id": "MERCH_JEWELRY",
            "amount": 5000.00,
            "merchant_category": "jewelry"
        },
        "fraud": {
            "transaction_id": "TEST_FRAUD_001",
            "user_id": "USER_TEST",
            "merchant_id": "MERCH_CASINO",
            "amount": 10000.00,
            "merchant_category": "casino",
            "location": {
                "lat": 0,
                "lon": 0,
                "country": "XX"
            }
        }
    }
    
    transaction = test_transactions[transaction_type]
    
    # Выполнение предсказания с отладочной информацией
    start_time = time.time()
    
    # Feature extraction
    features = await feature_pipeline.extract_features(transaction)
    feature_time = time.time() - start_time
    
    # Model prediction
    prediction = await model_server.predict(features)
    total_time = time.time() - start_time
    
    return {
        "test_type": transaction_type,
        "transaction": transaction,
        "features": features.to_dict() if hasattr(features, 'to_dict') else features,
        "prediction": prediction,
        "timing": {
            "feature_extraction_ms": feature_time * 1000,
            "total_ms": total_time * 1000
        },
        "debug_info": {
            "model_version": model_server.model_version,
            "feature_count": len(features.columns) if hasattr(features, 'columns') else 0
        }
    }
```

### Performance Profiling

```python
# profiling/profiler.py
import cProfile
import pstats
import io
from memory_profiler import profile
import line_profiler
import asyncio

class Profiler:
    """Инструменты для профилирования"""
    
    def __init__(self):
        self.cpu_profiler = cProfile.Profile()
        self.line_profiler = line_profiler.LineProfiler()
    
    def profile_function(self, func):
        """Декоратор для CPU профилирования"""
        def wrapper(*args, **kwargs):
            self.cpu_profiler.enable()
            result = func(*args, **kwargs)
            self.cpu_profiler.disable()
            
            # Вывод статистики
            s = io.StringIO()
            ps = pstats.Stats(self.cpu_profiler, stream=s).sort_stats('cumulative')
            ps.print_stats(20)  # Top 20 functions
            
            print(s.getvalue())
            return result
        return wrapper
    
    @profile  # memory_profiler decorator
    async def profile_prediction_memory(self, transaction):
        """Профилирование использования памяти при предсказании"""
        # Feature extraction
        features = await self.feature_pipeline.extract_features(transaction)
        
        # Model loading (if needed)
        if not hasattr(self, 'model'):
            self.model = await load_model()
        
        # Prediction
        prediction = await self.model.predict(features)
        
        return prediction
    
    def analyze_bottlenecks(self):
        """Анализ узких мест производительности"""
        stats = pstats.Stats(self.cpu_profiler)
        
        # Top 10 по времени выполнения
        print("\n=== Top 10 by cumulative time ===")
        stats.sort_stats('cumulative').print_stats(10)
        
        # Top 10 по количеству вызовов
        print("\n=== Top 10 by call count ===")
        stats.sort_stats('calls').print_stats(10)
        
        # Функции с наибольшим временем на вызов
        print("\n=== Top 10 by time per call ===")
        stats.sort_stats('tottime').print_stats(10)
```

## 📋 Monitoring Checklist

### Daily Monitoring Tasks

- [ ] Проверка error rate (< 0.1%)
- [ ] Проверка latency P95 (< 100ms)
- [ ] Проверка fraud detection rate (> 95%)
- [ ] Проверка false positive rate (< 2%)
- [ ] Просмотр critical alerts
- [ ] Проверка disk space
- [ ] Проверка memory usage
- [ ] Проверка model performance

### Weekly Tasks

- [ ] Анализ slow queries
- [ ] Проверка data quality metrics
- [ ] Review security logs
- [ ] Capacity planning
- [ ] Performance optimization
- [ ] Update monitoring dashboards

### Monthly Tasks

- [ ] Full system health check
- [ ] Disaster recovery test
- [ ] Security audit
- [ ] Performance benchmarking
- [ ] Cost optimization review

---

*Следующий раздел: [Best Practices →](./09_best_practices.md)*
