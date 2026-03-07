# 🔌 API и интерфейсы

## Обзор API архитектуры

### API Gateway Pattern

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   API Clients   │────▶│   API Gateway   │────▶│  Microservices  │
│  • Web App      │     │  • Auth         │     │  • Fraud API    │
│  • Mobile App   │     │  • Rate Limit   │     │  • Feature API  │
│  • Partners     │     │  • Load Balance │     │  • Admin API    │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

### Доступные API Endpoints

| Service | Base URL | Описание |
|---------|----------|----------|
| **Fraud Detection API** | http://localhost:8001 | Основной API для fraud detection |
| **Feature Store API** | http://localhost:6566 | Feast UI и API |
| **Admin API** | http://localhost:8002 | Административные функции |
| **Monitoring API** | http://localhost:9090 | Prometheus metrics |
| **ML Model API** | http://localhost:8003 | MLflow model serving |

## 🚀 Fraud Detection API

### OpenAPI Specification

```yaml
openapi: 3.0.0
info:
  title: Fraud Detection API
  version: 2.0.0
  description: API для обнаружения мошенничества в кредитных транзакциях

servers:
  - url: http://localhost:8001
    description: Development server
  - url: https://api.fraud-detection.com
    description: Production server

paths:
  /predict:
    post:
      summary: Предсказание мошенничества для одной транзакции
      operationId: predictFraud
      requestBody:
        required: true
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/TransactionRequest'
      responses:
        '200':
          description: Успешное предсказание
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/PredictionResponse'
        '400':
          description: Некорректный запрос
        '500':
          description: Внутренняя ошибка сервера

  /predict/batch:
    post:
      summary: Batch предсказания
      operationId: predictBatch
      requestBody:
        required: true
        content:
          application/json:
            schema:
              type: array
              items:
                $ref: '#/components/schemas/TransactionRequest'
              maxItems: 1000
      responses:
        '200':
          description: Успешные предсказания
          content:
            application/json:
              schema:
                type: array
                items:
                  $ref: '#/components/schemas/PredictionResponse'

  /health:
    get:
      summary: Health check
      operationId: healthCheck
      responses:
        '200':
          description: Сервис работает нормально
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/HealthResponse'

  /metrics:
    get:
      summary: Prometheus метрики
      operationId: getMetrics
      responses:
        '200':
          description: Метрики в формате Prometheus
          content:
            text/plain:
              schema:
                type: string

components:
  schemas:
    TransactionRequest:
      type: object
      required:
        - transaction_id
        - user_id
        - merchant_id
        - amount
        - merchant_category
      properties:
        transaction_id:
          type: string
          format: uuid
          description: Уникальный ID транзакции
        user_id:
          type: string
          description: ID пользователя
        merchant_id:
          type: string
          description: ID мерчанта
        amount:
          type: number
          format: double
          minimum: 0.01
          description: Сумма транзакции
        merchant_category:
          type: string
          enum: [grocery, gas, restaurant, online, travel, entertainment, other]
          description: Категория мерчанта
        timestamp:
          type: string
          format: date-time
          description: Время транзакции
        location:
          $ref: '#/components/schemas/Location'
        channel:
          type: string
          enum: [online, chip, swipe, contactless]
          default: online
          description: Канал транзакции

    PredictionResponse:
      type: object
      properties:
        transaction_id:
          type: string
          format: uuid
        is_fraud:
          type: boolean
          description: Является ли транзакция мошеннической
        fraud_probability:
          type: number
          format: double
          minimum: 0
          maximum: 1
          description: Вероятность мошенничества
        risk_score:
          type: number
          format: double
          minimum: 0
          maximum: 1
          description: Общий риск-скор
        risk_factors:
          type: array
          items:
            type: string
          description: Список факторов риска
        model_version:
          type: string
          description: Версия используемой модели
        recommendation:
          type: string
          enum: [approve, review, block]
          description: Рекомендация по транзакции
        processing_time_ms:
          type: number
          format: double
          description: Время обработки в миллисекундах

    Location:
      type: object
      properties:
        lat:
          type: number
          format: double
          minimum: -90
          maximum: 90
        lon:
          type: number
          format: double
          minimum: -180
          maximum: 180
        country:
          type: string
          maxLength: 2
          description: ISO код страны

    HealthResponse:
      type: object
      properties:
        status:
          type: string
          enum: [healthy, degraded, unhealthy]
        model_loaded:
          type: boolean
        dependencies:
          type: object
          properties:
            database:
              type: boolean
            redis:
              type: boolean
            feature_store:
              type: boolean
        timestamp:
          type: string
          format: date-time

  securitySchemes:
    ApiKeyAuth:
      type: apiKey
      in: header
      name: X-API-Key
    OAuth2:
      type: oauth2
      flows:
        clientCredentials:
          tokenUrl: /oauth/token
          scopes:
            read: Read access
            write: Write access
            admin: Admin access

security:
  - ApiKeyAuth: []
  - OAuth2: [read, write]
```

### Примеры использования API

#### Python Client

```python
# clients/python/fraud_detection_client.py
import requests
from typing import Dict, List, Optional
from datetime import datetime
import json
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class FraudDetectionClient:
    """
    Python клиент для Fraud Detection API
    """
    
    def __init__(self, base_url: str = "http://localhost:8001", 
                 api_key: Optional[str] = None,
                 timeout: int = 10):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.timeout = timeout
        
        # Настройка retry стратегии
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Headers
        self.headers = {"Content-Type": "application/json"}
        if api_key:
            self.headers["X-API-Key"] = api_key
    
    def predict(self, transaction: Dict) -> Dict:
        """
        Предсказание для одной транзакции
        
        Args:
            transaction: Словарь с данными транзакции
            
        Returns:
            Словарь с результатом предсказания
            
        Example:
            >>> client = FraudDetectionClient()
            >>> result = client.predict({
            ...     "transaction_id": "TXN_123",
            ...     "user_id": "USER_456",
            ...     "merchant_id": "MERCH_789",
            ...     "amount": 150.00,
            ...     "merchant_category": "online"
            ... })
            >>> print(f"Is fraud: {result['is_fraud']}")
        """
        url = f"{self.base_url}/predict"
        
        # Добавление timestamp если не указан
        if 'timestamp' not in transaction:
            transaction['timestamp'] = datetime.now().isoformat()
        
        response = self.session.post(
            url,
            headers=self.headers,
            json=transaction,
            timeout=self.timeout
        )
        
        response.raise_for_status()
        return response.json()
    
    def predict_batch(self, transactions: List[Dict]) -> List[Dict]:
        """
        Batch предсказания
        
        Args:
            transactions: Список транзакций (максимум 1000)
            
        Returns:
            Список результатов предсказаний
        """
        if len(transactions) > 1000:
            raise ValueError("Batch size cannot exceed 1000 transactions")
        
        url = f"{self.base_url}/predict/batch"
        
        # Добавление timestamps
        for tx in transactions:
            if 'timestamp' not in tx:
                tx['timestamp'] = datetime.now().isoformat()
        
        response = self.session.post(
            url,
            headers=self.headers,
            json=transactions,
            timeout=self.timeout * 2  # Увеличенный timeout для batch
        )
        
        response.raise_for_status()
        return response.json()
    
    def health_check(self) -> Dict:
        """
        Проверка состояния сервиса
        
        Returns:
            Словарь со статусом сервиса
        """
        url = f"{self.base_url}/health"
        response = self.session.get(url, timeout=5)
        response.raise_for_status()
        return response.json()
    
    def get_metrics(self) -> str:
        """
        Получение метрик Prometheus
        
        Returns:
            Строка с метриками в формате Prometheus
        """
        url = f"{self.base_url}/metrics"
        response = self.session.get(url, timeout=5)
        response.raise_for_status()
        return response.text

# Пример использования
if __name__ == "__main__":
    # Инициализация клиента
    client = FraudDetectionClient(
        base_url="http://localhost:8001",
        api_key="your-api-key-here"
    )
    
    # Проверка здоровья сервиса
    health = client.health_check()
    print(f"Service status: {health['status']}")
    
    # Одиночное предсказание
    transaction = {
        "transaction_id": "TXN_20240115_001",
        "user_id": "USER_123456",
        "merchant_id": "MERCH_AMAZON",
        "amount": 1500.00,
        "merchant_category": "online",
        "location": {
            "lat": 40.7128,
            "lon": -74.0060,
            "country": "US"
        },
        "channel": "online"
    }
    
    result = client.predict(transaction)
    print(f"Transaction {transaction['transaction_id']}:")
    print(f"  Is fraud: {result['is_fraud']}")
    print(f"  Probability: {result['fraud_probability']:.2%}")
    print(f"  Risk factors: {', '.join(result['risk_factors'])}")
    print(f"  Recommendation: {result['recommendation']}")
```

#### JavaScript/TypeScript Client

```typescript
// clients/typescript/fraud-detection-client.ts
import axios, { AxiosInstance } from 'axios';

interface TransactionRequest {
  transaction_id: string;
  user_id: string;
  merchant_id: string;
  amount: number;
  merchant_category: 'grocery' | 'gas' | 'restaurant' | 'online' | 'travel' | 'entertainment' | 'other';
  timestamp?: string;
  location?: {
    lat: number;
    lon: number;
    country: string;
  };
  channel?: 'online' | 'chip' | 'swipe' | 'contactless';
}

interface PredictionResponse {
  transaction_id: string;
  is_fraud: boolean;
  fraud_probability: number;
  risk_score: number;
  risk_factors: string[];
  model_version: string;
  recommendation: 'approve' | 'review' | 'block';
  processing_time_ms: number;
}

export class FraudDetectionClient {
  private client: AxiosInstance;

  constructor(
    private baseUrl: string = 'http://localhost:8001',
    private apiKey?: string
  ) {
    this.client = axios.create({
      baseURL: this.baseUrl,
      timeout: 10000,
      headers: {
        'Content-Type': 'application/json',
        ...(apiKey && { 'X-API-Key': apiKey })
      }
    });

    // Interceptors для retry логики
    this.client.interceptors.response.use(
      response => response,
      async error => {
        const config = error.config;
        if (!config || !config.retry) {
          config.retry = 0;
        }
        
        config.retry += 1;
        
        if (config.retry > 3) {
          return Promise.reject(error);
        }
        
        if (error.response?.status >= 500) {
          return new Promise(resolve => {
            setTimeout(() => resolve(this.client(config)), 1000 * config.retry);
          });
        }
        
        return Promise.reject(error);
      }
    );
  }

  async predict(transaction: TransactionRequest): Promise<PredictionResponse> {
    const { data } = await this.client.post<PredictionResponse>('/predict', {
      ...transaction,
      timestamp: transaction.timestamp || new Date().toISOString()
    });
    return data;
  }

  async predictBatch(transactions: TransactionRequest[]): Promise<PredictionResponse[]> {
    if (transactions.length > 1000) {
      throw new Error('Batch size cannot exceed 1000 transactions');
    }

    const transactionsWithTimestamp = transactions.map(tx => ({
      ...tx,
      timestamp: tx.timestamp || new Date().toISOString()
    }));

    const { data } = await this.client.post<PredictionResponse[]>(
      '/predict/batch',
      transactionsWithTimestamp
    );
    return data;
  }

  async healthCheck(): Promise<{
    status: 'healthy' | 'degraded' | 'unhealthy';
    model_loaded: boolean;
    dependencies: {
      database: boolean;
      redis: boolean;
      feature_store: boolean;
    };
    timestamp: string;
  }> {
    const { data } = await this.client.get('/health');
    return data;
  }

  async getMetrics(): Promise<string> {
    const { data } = await this.client.get('/metrics', {
      responseType: 'text'
    });
    return data;
  }
}

// Пример использования
async function example() {
  const client = new FraudDetectionClient(
    'http://localhost:8001',
    'your-api-key'
  );

  try {
    // Health check
    const health = await client.healthCheck();
    console.log(`Service status: ${health.status}`);

    // Предсказание
    const result = await client.predict({
      transaction_id: 'TXN_20240115_001',
      user_id: 'USER_123456',
      merchant_id: 'MERCH_AMAZON',
      amount: 1500.00,
      merchant_category: 'online',
      location: {
        lat: 40.7128,
        lon: -74.0060,
        country: 'US'
      }
    });

    console.log(`Is fraud: ${result.is_fraud}`);
    console.log(`Probability: ${(result.fraud_probability * 100).toFixed(2)}%`);
    console.log(`Recommendation: ${result.recommendation}`);
  } catch (error) {
    console.error('Error:', error);
  }
}
```

#### cURL Examples

```bash
# Health check
curl -X GET http://localhost:8001/health

# Одиночное предсказание
curl -X POST http://localhost:8001/predict \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-api-key" \
  -d '{
    "transaction_id": "TXN_20240115_001",
    "user_id": "USER_123456",
    "merchant_id": "MERCH_AMAZON",
    "amount": 1500.00,
    "merchant_category": "online",
    "channel": "online"
  }'

# Batch предсказания
curl -X POST http://localhost:8001/predict/batch \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-api-key" \
  -d '[
    {
      "transaction_id": "TXN_001",
      "user_id": "USER_123",
      "merchant_id": "MERCH_001",
      "amount": 100.00,
      "merchant_category": "grocery"
    },
    {
      "transaction_id": "TXN_002",
      "user_id": "USER_123",
      "merchant_id": "MERCH_002",
      "amount": 5000.00,
      "merchant_category": "jewelry"
    }
  ]'

# Получение метрик
curl -X GET http://localhost:8001/metrics
```

## 📊 Feature Store API

### Feast REST API

```python
# api/feature_store_api.py
from fastapi import FastAPI, HTTPException
from feast import FeatureStore
import pandas as pd
from typing import Dict, List, Optional
from datetime import datetime

app = FastAPI(title="Feature Store API")
store = FeatureStore(repo_path="/feast/fraud_detection")

@app.post("/get_online_features")
async def get_online_features(
    entity_rows: List[Dict[str, str]],
    features: List[str]
) -> Dict:
    """
    Получение онлайн-фичей из Feature Store
    
    Example request:
    {
        "entity_rows": [
            {"user_id": "USER_123", "merchant_id": "MERCH_456"}
        ],
        "features": [
            "user_stats:transaction_count_24h",
            "user_stats:avg_amount",
            "merchant_stats:fraud_rate"
        ]
    }
    """
    try:
        online_features = store.get_online_features(
            features=features,
            entity_rows=entity_rows
        ).to_dict()
        
        return online_features
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/get_historical_features")
async def get_historical_features(
    entity_df: Dict,  # Serialized DataFrame
    features: List[str],
    timestamp_column: str = "timestamp"
) -> Dict:
    """
    Получение исторических фичей для обучения
    """
    try:
        # Deserialize entity DataFrame
        df = pd.DataFrame(entity_df['data'])
        df[timestamp_column] = pd.to_datetime(df[timestamp_column])
        
        # Get historical features
        feature_df = store.get_historical_features(
            entity_df=df,
            features=features
        ).to_df()
        
        # Serialize result
        return {
            "features": feature_df.to_dict(orient='records'),
            "columns": feature_df.columns.tolist(),
            "shape": feature_df.shape
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/materialize")
async def materialize_features(
    start_date: datetime,
    end_date: datetime,
    feature_views: Optional[List[str]] = None
):
    """
    Материализация фичей в онлайн-стор
    """
    try:
        store.materialize(
            start_date=start_date,
            end_date=end_date,
            feature_views=feature_views
        )
        
        return {
            "status": "success",
            "message": f"Features materialized from {start_date} to {end_date}"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/list_feature_views")
async def list_feature_views():
    """
    Список доступных feature views
    """
    feature_views = store.list_feature_views()
    
    return {
        "feature_views": [
            {
                "name": fv.name,
                "entities": [e.name for e in fv.entities],
                "features": [f.name for f in fv.features],
                "online": fv.online,
                "ttl": str(fv.ttl) if fv.ttl else None
            }
            for fv in feature_views
        ]
    }
```

## 🛡️ Admin API

### Administrative Functions

```python
# api/admin_api.py
from fastapi import FastAPI, HTTPException, Depends, Security
from fastapi.security import APIKeyHeader
import mlflow
from typing import Dict, List, Optional
import pandas as pd
from datetime import datetime
import asyncio

app = FastAPI(title="Admin API")

# Security
api_key_header = APIKeyHeader(name="X-Admin-API-Key", auto_error=False)

async def verify_admin_key(api_key: str = Security(api_key_header)):
    if api_key != "admin-secret-key":  # В production из переменной окружения
        raise HTTPException(status_code=403, detail="Invalid API Key")
    return api_key

@app.post("/models/deploy", dependencies=[Depends(verify_admin_key)])
async def deploy_model(
    model_name: str,
    version: str,
    stage: str = "Production"
) -> Dict:
    """
    Деплой новой версии модели
    """
    try:
        client = mlflow.tracking.MlflowClient()
        
        # Переход модели в production
        client.transition_model_version_stage(
            name=model_name,
            version=version,
            stage=stage,
            archive_existing_versions=True
        )
        
        # Перезагрузка сервисов
        await reload_model_services()
        
        return {
            "status": "success",
            "model_name": model_name,
            "version": version,
            "stage": stage,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/models/rollback", dependencies=[Depends(verify_admin_key)])
async def rollback_model(
    model_name: str,
    to_version: str
) -> Dict:
    """
    Откат к предыдущей версии модели
    """
    try:
        client = mlflow.tracking.MlflowClient()
        
        # Получение текущей production версии
        current_version = client.get_latest_versions(
            model_name, 
            stages=["Production"]
        )[0]
        
        # Архивирование текущей версии
        client.transition_model_version_stage(
            name=model_name,
            version=current_version.version,
            stage="Archived"
        )
        
        # Восстановление предыдущей версии
        client.transition_model_version_stage(
            name=model_name,
            version=to_version,
            stage="Production"
        )
        
        await reload_model_services()
        
        return {
            "status": "success",
            "rolled_back_from": current_version.version,
            "rolled_back_to": to_version,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/system/status", dependencies=[Depends(verify_admin_key)])
async def system_status() -> Dict:
    """
    Полный статус системы
    """
    status = {
        "timestamp": datetime.now().isoformat(),
        "services": {},
        "metrics": {},
        "alerts": []
    }
    
    # Проверка сервисов
    services = [
        {"name": "fraud-api", "url": "http://fraud-api:8000/health"},
        {"name": "postgres", "check": check_postgres},
        {"name": "redis", "check": check_redis},
        {"name": "kafka", "check": check_kafka}
    ]
    
    for service in services:
        if "url" in service:
            status["services"][service["name"]] = await check_http_service(service["url"])
        else:
            status["services"][service["name"]] = await service["check"]()
    
    # Метрики
    status["metrics"] = await get_system_metrics()
    
    # Активные алерты
    status["alerts"] = await get_active_alerts()
    
    return status

@app.post("/data/reprocess", dependencies=[Depends(verify_admin_key)])
async def reprocess_data(
    start_date: str,
    end_date: str,
    data_type: str = "transactions"
) -> Dict:
    """
    Перезапуск обработки данных за период
    """
    try:
        # Запуск Airflow DAG с параметрами
        from airflow.api.client.local_client import Client
        
        client = Client(None, None)
        
        dag_run = client.trigger_dag(
            dag_id=f'reprocess_{data_type}',
            conf={
                'start_date': start_date,
                'end_date': end_date
            }
        )
        
        return {
            "status": "started",
            "dag_run_id": dag_run.run_id,
            "start_date": start_date,
            "end_date": end_date
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def reload_model_services():
    """Перезагрузка сервисов с моделями"""
    # Отправка сигнала на перезагрузку
    # В production через Kubernetes API или Docker API
    pass

async def check_http_service(url: str) -> bool:
    """Проверка HTTP сервиса"""
    import aiohttp
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=5) as response:
                return response.status == 200
    except:
        return False

async def check_postgres() -> bool:
    """Проверка PostgreSQL"""
    # Реализация проверки
    return True

async def check_redis() -> bool:
    """Проверка Redis"""
    # Реализация проверки
    return True

async def check_kafka() -> bool:
    """Проверка Kafka"""
    # Реализация проверки
    return True

async def get_system_metrics() -> Dict:
    """Получение системных метрик"""
    return {
        "transactions_per_second": 150,
        "average_latency_ms": 45,
        "error_rate": 0.001,
        "model_accuracy": 0.97
    }

async def get_active_alerts() -> List[Dict]:
    """Получение активных алертов"""
    return [
        {
            "id": "alert_001",
            "severity": "warning",
            "message": "High latency detected",
            "timestamp": "2024-01-15T10:30:00Z"
        }
    ]
```

## 🌐 GraphQL API

### GraphQL Schema

```graphql
# api/graphql/schema.graphql
type Query {
  # Получение транзакции по ID
  transaction(id: ID!): Transaction
  
  # Получение предсказаний пользователя
  userPredictions(userId: ID!, limit: Int = 10): [Prediction!]!
  
  # Статистика по мошенничеству
  fraudStatistics(
    startDate: DateTime!
    endDate: DateTime!
    groupBy: GroupByPeriod
  ): FraudStats!
  
  # Поиск транзакций
  searchTransactions(
    filter: TransactionFilter
    sort: TransactionSort
    pagination: PaginationInput
  ): TransactionConnection!
}

type Mutation {
  # Создание предсказания
  predictFraud(input: TransactionInput!): Prediction!
  
  # Обратная связь по предсказанию
  submitFeedback(
    transactionId: ID!
    actualFraud: Boolean!
  ): Feedback!
  
  # Обновление модели
  updateModel(
    modelName: String!
    version: String!
  ): ModelDeployment!
}

type Subscription {
  # Подписка на новые fraud алерты
  fraudAlerts(userId: ID): FraudAlert!
  
  # Подписка на метрики модели
  modelMetrics(modelName: String!): ModelMetric!
}

type Transaction {
  id: ID!
  userId: ID!
  merchantId: ID!
  amount: Float!
  merchantCategory: String!
  timestamp: DateTime!
  location: Location
  prediction: Prediction
}

type Prediction {
  transactionId: ID!
  isFraud: Boolean!
  fraudProbability: Float!
  riskScore: Float!
  riskFactors: [String!]!
  modelVersion: String!
  recommendation: Recommendation!
  createdAt: DateTime!
}

type Location {
  lat: Float!
  lon: Float!
  country: String!
  city: String
}

type FraudStats {
  totalTransactions: Int!
  fraudulentTransactions: Int!
  fraudRate: Float!
  totalAmount: Float!
  fraudulentAmount: Float!
  byCategory: [CategoryStats!]!
  byTime: [TimeStats!]!
}

type CategoryStats {
  category: String!
  transactions: Int!
  fraudCount: Int!
  fraudRate: Float!
}

type TimeStats {
  period: DateTime!
  transactions: Int!
  fraudCount: Int!
  fraudRate: Float!
}

enum Recommendation {
  APPROVE
  REVIEW
  BLOCK
}

enum GroupByPeriod {
  HOUR
  DAY
  WEEK
  MONTH
}

input TransactionInput {
  userId: ID!
  merchantId: ID!
  amount: Float!
  merchantCategory: String!
  location: LocationInput
  channel: String
}

input LocationInput {
  lat: Float!
  lon: Float!
  country: String!
}

input TransactionFilter {
  userId: ID
  merchantId: ID
  minAmount: Float
  maxAmount: Float
  startDate: DateTime
  endDate: DateTime
  isFraud: Boolean
}

input TransactionSort {
  field: TransactionSortField!
  direction: SortDirection!
}

enum TransactionSortField {
  AMOUNT
  TIMESTAMP
  RISK_SCORE
}

enum SortDirection {
  ASC
  DESC
}

input PaginationInput {
  limit: Int = 20
  offset: Int = 0
}

type TransactionConnection {
  edges: [TransactionEdge!]!
  pageInfo: PageInfo!
  totalCount: Int!
}

type TransactionEdge {
  node: Transaction!
  cursor: String!
}

type PageInfo {
  hasNextPage: Boolean!
  hasPreviousPage: Boolean!
  startCursor: String
  endCursor: String
}
```

### GraphQL Resolvers

```python
# api/graphql/resolvers.py
from ariadne import QueryType, MutationType, SubscriptionType, make_executable_schema
from ariadne.asgi import GraphQL
from datetime import datetime
import asyncio

query = QueryType()
mutation = MutationType()
subscription = SubscriptionType()

@query.field("transaction")
async def resolve_transaction(_, info, id):
    """Получение транзакции по ID"""
    # Логика получения транзакции
    return {
        "id": id,
        "userId": "USER_123",
        "merchantId": "MERCH_456",
        "amount": 150.00,
        "merchantCategory": "online",
        "timestamp": datetime.now()
    }

@query.field("fraudStatistics")
async def resolve_fraud_statistics(_, info, startDate, endDate, groupBy=None):
    """Статистика по мошенничеству"""
    # Логика получения статистики
    return {
        "totalTransactions": 10000,
        "fraudulentTransactions": 200,
        "fraudRate": 0.02,
        "totalAmount": 1500000.00,
        "fraudulentAmount": 50000.00,
        "byCategory": [
            {
                "category": "online",
                "transactions": 5000,
                "fraudCount": 150,
                "fraudRate": 0.03
            }
        ]
    }

@mutation.field("predictFraud")
async def resolve_predict_fraud(_, info, input):
    """Создание предсказания"""
    # Вызов ML модели
    prediction = await predict_fraud_transaction(input)
    
    return {
        "transactionId": "TXN_" + str(datetime.now().timestamp()),
        "isFraud": prediction["is_fraud"],
        "fraudProbability": prediction["probability"],
        "riskScore": prediction["risk_score"],
        "riskFactors": prediction["risk_factors"],
        "modelVersion": "2.0.0",
        "recommendation": prediction["recommendation"],
        "createdAt": datetime.now()
    }

@subscription.source("fraudAlerts")
async def generate_fraud_alerts(_, info, userId=None):
    """Генератор fraud алертов"""
    while True:
        await asyncio.sleep(5)  # Проверка каждые 5 секунд
        
        # Проверка новых fraud транзакций
        alert = await check_for_fraud_alerts(userId)
        if alert:
            yield alert

@subscription.field("fraudAlerts")
def resolve_fraud_alert(alert, info):
    """Резолвер для fraud алертов"""
    return alert

# Создание executable schema
with open("schema.graphql") as f:
    type_defs = f.read()

schema = make_executable_schema(type_defs, query, mutation, subscription)
app = GraphQL(schema, debug=True)
```

## 🔐 API Security

### Authentication & Authorization

```python
# security/auth.py
from fastapi import HTTPException, Security, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import jwt
from datetime import datetime, timedelta
from typing import Optional

class AuthHandler:
    security = HTTPBearer()
    secret = "your-secret-key"  # В production из переменной окружения
    
    def encode_token(self, user_id: str, scopes: List[str] = None) -> str:
        """Создание JWT токена"""
        payload = {
            "exp": datetime.utcnow() + timedelta(days=1),
            "iat": datetime.utcnow(),
            "sub": user_id,
            "scopes": scopes or ["read"]
        }
        return jwt.encode(payload, self.secret, algorithm="HS256")
    
    def decode_token(self, token: str) -> dict:
        """Декодирование JWT токена"""
        try:
            payload = jwt.decode(token, self.secret, algorithms=["HS256"])
            return payload
        except jwt.ExpiredSignatureError:
            raise HTTPException(status_code=401, detail="Token expired")
        except jwt.InvalidTokenError:
            raise HTTPException(status_code=401, detail="Invalid token")
    
    def verify_token(self, auth: HTTPAuthorizationCredentials = Security(security)) -> dict:
        """Верификация токена"""
        return self.decode_token(auth.credentials)
    
    def check_scope(self, required_scope: str):
        """Проверка scope"""
        def scope_checker(payload: dict = Depends(self.verify_token)):
            if required_scope not in payload.get("scopes", []):
                raise HTTPException(
                    status_code=403,
                    detail=f"Scope '{required_scope}' required"
                )
            return payload
        return scope_checker

auth_handler = AuthHandler()
```

### Rate Limiting

```python
# middleware/rate_limiter.py
from fastapi import Request, HTTPException
import time
from typing import Dict
import redis

class RateLimiter:
    def __init__(self, redis_client, requests_per_minute: int = 60):
        self.redis_client = redis_client
        self.requests_per_minute = requests_per_minute
        self.window_size = 60  # seconds
    
    async def check_rate_limit(self, request: Request, key: str = None):
        """Проверка rate limit"""
        if not key:
            # Используем IP адрес как ключ
            key = request.client.host
        
        current_time = int(time.time())
        window_start = current_time - self.window_size
        
        # Уникальный ключ для пользователя
        redis_key = f"rate_limit:{key}"
        
        # Удаление старых записей
        self.redis_client.zremrangebyscore(redis_key, 0, window_start)
        
        # Подсчет запросов в текущем окне
        current_requests = self.redis_client.zcard(redis_key)
        
        if current_requests >= self.requests_per_minute:
            raise HTTPException(
                status_code=429,
                detail="Rate limit exceeded",
                headers={
                    "X-RateLimit-Limit": str(self.requests_per_minute),
                    "X-RateLimit-Remaining": "0",
                    "X-RateLimit-Reset": str(current_time + self.window_size)
                }
            )
        
        # Добавление текущего запроса
        self.redis_client.zadd(redis_key, {str(current_time): current_time})
        self.redis_client.expire(redis_key, self.window_size)
        
        # Добавление headers
        request.state.rate_limit_headers = {
            "X-RateLimit-Limit": str(self.requests_per_minute),
            "X-RateLimit-Remaining": str(self.requests_per_minute - current_requests - 1),
            "X-RateLimit-Reset": str(current_time + self.window_size)
        }
```

## 📡 Webhook Integration

### Webhook для уведомлений

```python
# webhooks/fraud_webhook.py
from fastapi import BackgroundTasks
import httpx
from typing import Dict, List
import asyncio
from datetime import datetime

class FraudWebhookManager:
    def __init__(self):
        self.subscribers = []
    
    def register_webhook(self, webhook_config: Dict):
        """Регистрация webhook"""
        self.subscribers.append({
            "url": webhook_config["url"],
            "events": webhook_config["events"],
            "secret": webhook_config.get("secret"),
            "active": True,
            "created_at": datetime.now()
        })
    
    async def send_webhook(self, event_type: str, payload: Dict):
        """Отправка webhook всем подписчикам"""
        tasks = []
        
        for subscriber in self.subscribers:
            if event_type in subscriber["events"] and subscriber["active"]:
                task = self._send_to_subscriber(subscriber, event_type, payload)
                tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return results
    
    async def _send_to_subscriber(self, subscriber: Dict, event_type: str, payload: Dict):
        """Отправка webhook конкретному подписчику"""
        webhook_payload = {
            "event": event_type,
            "timestamp": datetime.now().isoformat(),
            "data": payload
        }
        
        headers = {
            "Content-Type": "application/json",
            "X-Webhook-Event": event_type,
            "X-Webhook-Timestamp": str(datetime.now().timestamp())
        }
        
        if subscriber.get("secret"):
            # Добавление подписи для верификации
            signature = self._generate_signature(webhook_payload, subscriber["secret"])
            headers["X-Webhook-Signature"] = signature
        
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    subscriber["url"],
                    json=webhook_payload,
                    headers=headers,
                    timeout=10
                )
                
                if response.status_code >= 400:
                    # Логирование ошибки
                    print(f"Webhook failed: {response.status_code}")
                    
                return {
                    "subscriber": subscriber["url"],
                    "status_code": response.status_code,
                    "success": response.status_code < 400
                }
                
        except Exception as e:
            print(f"Webhook error: {e}")
            return {
                "subscriber": subscriber["url"],
                "error": str(e),
                "success": False
            }
    
    def _generate_signature(self, payload: Dict, secret: str) -> str:
        """Генерация подписи для webhook"""
        import hmac
        import hashlib
        import json
        
        payload_string = json.dumps(payload, sort_keys=True)
        signature = hmac.new(
            secret.encode(),
            payload_string.encode(),
            hashlib.sha256
        ).hexdigest()
        
        return f"sha256={signature}"

# Использование в API
webhook_manager = FraudWebhookManager()

@app.post("/webhooks/subscribe")
async def subscribe_webhook(
    url: str,
    events: List[str],
    secret: Optional[str] = None
):
    """Подписка на webhook уведомления"""
    webhook_manager.register_webhook({
        "url": url,
        "events": events,
        "secret": secret
    })
    
    return {
        "status": "subscribed",
        "url": url,
        "events": events
    }
```

---

*Следующий раздел: [Мониторинг и отладка →](./08_monitoring.md)*
