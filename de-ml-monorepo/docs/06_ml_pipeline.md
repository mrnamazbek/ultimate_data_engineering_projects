# 🤖 Machine Learning Pipeline

## Обзор ML архитектуры

### Философия ML подхода

Наш ML pipeline построен на следующих принципах:
- **Reproducibility**: Все эксперименты воспроизводимы
- **Scalability**: Обучение и inference масштабируются горизонтально
- **Explainability**: Прозрачность принятия решений
- **Continuous Learning**: Автоматическое переобучение на новых данных
- **A/B Testing**: Постепенный rollout новых моделей

### ML Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           ML PIPELINE OVERVIEW                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                │
│  │    Data      │───▶│   Feature    │───▶│   Training   │                │
│  │  Ingestion   │    │ Engineering  │    │   Pipeline   │                │
│  └──────────────┘    └──────────────┘    └──────────────┘                │
│         │                    │                    │                         │
│         ▼                    ▼                    ▼                         │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                │
│  │    Data      │    │   Feature    │    │    Model     │                │
│  │ Validation   │    │    Store     │    │  Registry    │                │
│  └──────────────┘    └──────────────┘    └──────────────┘                │
│                              │                    │                         │
│                              ▼                    ▼                         │
│                      ┌──────────────┐    ┌──────────────┐                │
│                      │   Online     │    │   Model      │                │
│                      │   Serving    │◀───│  Deployment  │                │
│                      └──────────────┘    └──────────────┘                │
│                              │                                             │
│                              ▼                                             │
│                      ┌──────────────┐    ┌──────────────┐                │
│                      │ Monitoring & │───▶│   Feedback   │                │
│                      │  Evaluation  │    │     Loop     │                │
│                      └──────────────┘    └──────────────┘                │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 📊 Feature Engineering

### Feature Categories для Fraud Detection

| Категория | Примеры признаков | Важность |
|-----------|-------------------|----------|
| **Транзакционные** | amount, merchant_category | Высокая |
| **Временные** | hour_of_day, day_of_week, is_weekend | Средняя |
| **Поведенческие** | transaction_velocity, unique_merchants | Высокая |
| **Исторические** | user_avg_amount, user_fraud_rate | Высокая |
| **Графовые** | user_centrality, community_id | Средняя |
| **Внешние** | merchant_risk_score, location_risk | Средняя |

### Key Features Implementation

```python
# features/core_features.py
import pandas as pd
import numpy as np
from typing import Dict, List

class FraudFeatureEngineering:
    """Основные признаки для fraud detection"""
    
    def create_transaction_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Базовые признаки транзакции"""
        # Логарифм суммы
        df['amount_log'] = np.log1p(df['amount'])
        
        # Категории сумм
        df['amount_category'] = pd.cut(
            df['amount'], 
            bins=[0, 50, 200, 1000, 5000, np.inf],
            labels=['very_low', 'low', 'medium', 'high', 'very_high']
        )
        
        # Округленные суммы
        df['is_round_amount'] = (df['amount'] % 10 == 0).astype(int)
        
        return df
    
    def create_temporal_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Временные признаки"""
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Базовые
        df['hour'] = df['timestamp'].dt.hour
        df['day_of_week'] = df['timestamp'].dt.dayofweek
        df['day_of_month'] = df['timestamp'].dt.day
        
        # Производные
        df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
        df['is_night'] = df['hour'].between(0, 6).astype(int)
        df['is_business_hours'] = (
            df['hour'].between(9, 17) & (~df['is_weekend'].astype(bool))
        ).astype(int)
        
        return df
    
    def create_velocity_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Скорость и частота транзакций"""
        # Сортировка по времени
        df = df.sort_values(['user_id', 'timestamp'])
        
        # Время с последней транзакции
        df['hours_since_last_transaction'] = (
            df.groupby('user_id')['timestamp']
            .diff()
            .dt.total_seconds() / 3600
        ).fillna(24)
        
        # Ускорение транзакций
        df['transaction_acceleration'] = (
            df.groupby('user_id')['hours_since_last_transaction']
            .diff()
        ).fillna(0)
        
        return df
    
    def create_user_behavior_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Поведенческие признаки пользователя"""
        # Расчет статистик по пользователю
        user_stats = df.groupby('user_id').agg({
            'amount': ['mean', 'std', 'min', 'max'],
            'merchant_id': 'nunique',
            'merchant_category': lambda x: x.mode()[0] if len(x) > 0 else 'unknown'
        })
        
        user_stats.columns = ['_'.join(col) for col in user_stats.columns]
        user_stats = user_stats.add_prefix('user_')
        
        # Присоединение к основному датафрейму
        df = df.merge(user_stats, left_on='user_id', right_index=True, how='left')
        
        # Z-score для суммы транзакции
        df['amount_zscore'] = (
            (df['amount'] - df['user_amount_mean']) / 
            df['user_amount_std'].clip(lower=1)
        )
        
        return df
```

## 🎯 Model Training

### Supported Models

1. **XGBoost** - Gradient Boosting для табличных данных
2. **LightGBM** - Быстрая альтернатива XGBoost
3. **Neural Networks** - Deep Learning подход
4. **LSTM** - Для последовательных паттернов
5. **Isolation Forest** - Anomaly detection
6. **Ensemble** - Комбинация моделей

### Training Pipeline с MLflow

```python
# training/train_model.py
import mlflow
import mlflow.sklearn
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, classification_report
import pandas as pd
import numpy as np

class ModelTrainer:
    """Обучение и оценка моделей"""
    
    def __init__(self, experiment_name: str):
        mlflow.set_experiment(experiment_name)
        self.experiment_name = experiment_name
    
    def train_xgboost(self, X: pd.DataFrame, y: pd.Series, params: dict = None):
        """Обучение XGBoost модели"""
        import xgboost as xgb
        
        # Разделение данных
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, stratify=y, random_state=42
        )
        
        with mlflow.start_run(run_name="xgboost_model"):
            # Параметры по умолчанию
            default_params = {
                'n_estimators': 1000,
                'max_depth': 10,
                'learning_rate': 0.01,
                'objective': 'binary:logistic',
                'scale_pos_weight': (y_train == 0).sum() / (y_train == 1).sum(),
                'eval_metric': 'auc',
                'early_stopping_rounds': 50,
                'random_state': 42
            }
            
            if params:
                default_params.update(params)
            
            # Обучение
            model = xgb.XGBClassifier(**default_params)
            
            eval_set = [(X_train, y_train), (X_test, y_test)]
            model.fit(
                X_train, y_train,
                eval_set=eval_set,
                verbose=False
            )
            
            # Оценка
            y_pred_proba = model.predict_proba(X_test)[:, 1]
            auc_score = roc_auc_score(y_test, y_pred_proba)
            
            # Логирование
            mlflow.log_params(default_params)
            mlflow.log_metric("test_auc", auc_score)
            mlflow.log_metric("best_iteration", model.best_iteration)
            
            # Сохранение модели
            mlflow.xgboost.log_model(model, "model")
            
            # Feature importance
            feature_importance = pd.DataFrame({
                'feature': X.columns,
                'importance': model.feature_importances_
            }).sort_values('importance', ascending=False)
            
            feature_importance.to_csv("feature_importance.csv", index=False)
            mlflow.log_artifact("feature_importance.csv")
            
            print(f"Model AUC: {auc_score:.4f}")
            
            return model, auc_score
    
    def train_neural_network(self, X: pd.DataFrame, y: pd.Series):
        """Обучение нейронной сети"""
        import tensorflow as tf
        from tensorflow import keras
        
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, stratify=y, random_state=42
        )
        
        with mlflow.start_run(run_name="neural_network_model"):
            # Архитектура сети
            model = keras.Sequential([
                keras.layers.Dense(256, activation='relu', input_shape=(X.shape[1],)),
                keras.layers.BatchNormalization(),
                keras.layers.Dropout(0.3),
                keras.layers.Dense(128, activation='relu'),
                keras.layers.BatchNormalization(),
                keras.layers.Dropout(0.3),
                keras.layers.Dense(64, activation='relu'),
                keras.layers.BatchNormalization(),
                keras.layers.Dropout(0.2),
                keras.layers.Dense(1, activation='sigmoid')
            ])
            
            # Компиляция
            model.compile(
                optimizer='adam',
                loss='binary_crossentropy',
                metrics=['AUC']
            )
            
            # Обучение
            history = model.fit(
                X_train, y_train,
                validation_data=(X_test, y_test),
                epochs=50,
                batch_size=256,
                class_weight={0: 1, 1: 50},  # Для несбалансированных классов
                callbacks=[
                    keras.callbacks.EarlyStopping(
                        monitor='val_auc',
                        patience=10,
                        restore_best_weights=True,
                        mode='max'
                    )
                ],
                verbose=0
            )
            
            # Оценка
            test_loss, test_auc = model.evaluate(X_test, y_test, verbose=0)
            
            # Логирование
            mlflow.log_metric("test_auc", test_auc)
            mlflow.log_metric("epochs", len(history.history['loss']))
            
            # Сохранение модели
            mlflow.tensorflow.log_model(model, "model")
            
            print(f"Neural Network AUC: {test_auc:.4f}")
            
            return model, test_auc
```

## 🚀 Model Deployment

### Serving Architecture

```python
# serving/model_service.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import mlflow
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Optional

class TransactionRequest(BaseModel):
    transaction_id: str
    user_id: str
    merchant_id: str
    amount: float
    merchant_category: str
    timestamp: Optional[datetime] = None

class PredictionResponse(BaseModel):
    transaction_id: str
    is_fraud: bool
    fraud_probability: float
    risk_factors: List[str]
    model_version: str

class FraudDetectionAPI:
    def __init__(self):
        self.app = FastAPI(title="Fraud Detection API")
        self.model = None
        self.feature_pipeline = None
        self.model_version = None
        self._load_model()
        self._setup_routes()
    
    def _load_model(self):
        """Загрузка модели из MLflow"""
        # Загрузка последней production модели
        model_name = "fraud_detection_model"
        stage = "Production"
        
        model_uri = f"models:/{model_name}/{stage}"
        self.model = mlflow.pyfunc.load_model(model_uri)
        
        # Загрузка feature pipeline
        self.feature_pipeline = FraudFeatureEngineering()
        
        print(f"Model loaded: {model_name} ({stage})")
    
    def _setup_routes(self):
        @self.app.post("/predict", response_model=PredictionResponse)
        async def predict(request: TransactionRequest):
            try:
                # Подготовка данных
                df = pd.DataFrame([request.dict()])
                
                # Feature engineering
                df = self.feature_pipeline.create_transaction_features(df)
                df = self.feature_pipeline.create_temporal_features(df)
                
                # Предсказание
                prediction_proba = self.model.predict(df)[0]
                is_fraud = prediction_proba > 0.5
                
                # Анализ факторов риска
                risk_factors = self._analyze_risk_factors(df, prediction_proba)
                
                return PredictionResponse(
                    transaction_id=request.transaction_id,
                    is_fraud=bool(is_fraud),
                    fraud_probability=float(prediction_proba),
                    risk_factors=risk_factors,
                    model_version="1.0.0"
                )
                
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.get("/health")
        async def health_check():
            return {
                "status": "healthy",
                "model_loaded": self.model is not None,
                "timestamp": datetime.now()
            }
    
    def _analyze_risk_factors(self, df: pd.DataFrame, probability: float) -> List[str]:
        """Анализ факторов риска"""
        risk_factors = []
        
        if df['amount'].iloc[0] > 5000:
            risk_factors.append("high_amount")
        
        if df.get('is_night', [False])[0]:
            risk_factors.append("unusual_time")
        
        if probability > 0.8:
            risk_factors.append("high_ml_score")
        
        return risk_factors

# Запуск API
if __name__ == "__main__":
    import uvicorn
    api = FraudDetectionAPI()
    uvicorn.run(api.app, host="0.0.0.0", port=8001)
```

## 🔄 Continuous Learning

### Model Monitoring

```python
# monitoring/model_monitor.py
import pandas as pd
import numpy as np
from typing import Dict
from datetime import datetime, timedelta

class ModelMonitor:
    """Мониторинг производительности модели"""
    
    def __init__(self, baseline_metrics: Dict[str, float]):
        self.baseline_metrics = baseline_metrics
        self.alerts = []
    
    def check_model_drift(self, current_metrics: Dict[str, float]) -> Dict:
        """Проверка drift модели"""
        drift_report = {
            "timestamp": datetime.now(),
            "drifts": []
        }
        
        for metric, baseline_value in self.baseline_metrics.items():
            current_value = current_metrics.get(metric, 0)
            drift_percentage = abs(current_value - baseline_value) / baseline_value
            
            if drift_percentage > 0.1:  # 10% drift threshold
                drift_report["drifts"].append({
                    "metric": metric,
                    "baseline": baseline_value,
                    "current": current_value,
                    "drift_percentage": drift_percentage * 100
                })
        
        return drift_report
    
    def check_data_quality(self, df: pd.DataFrame) -> Dict:
        """Проверка качества входных данных"""
        quality_report = {
            "total_records": len(df),
            "null_percentage": df.isnull().sum().sum() / (len(df) * len(df.columns)) * 100,
            "duplicate_percentage": df.duplicated().sum() / len(df) * 100,
            "timestamp": datetime.now()
        }
        
        # Проверка на аномальные значения
        if 'amount' in df.columns:
            quality_report["amount_outliers"] = (
                df['amount'] > df['amount'].quantile(0.99)
            ).sum()
        
        return quality_report
```

## 📈 Performance Metrics

### Key Metrics для Fraud Detection

| Метрика | Описание | Target |
|---------|----------|--------|
| **Precision** | Доля правильных fraud предсказаний | > 0.9 |
| **Recall** | Доля обнаруженных fraud | > 0.8 |
| **F1-Score** | Гармоническое среднее | > 0.85 |
| **AUC-ROC** | Площадь под ROC кривой | > 0.95 |
| **False Positive Rate** | Доля ложных срабатываний | < 0.05 |

### Model Performance Dashboard

```python
# visualization/performance_dashboard.py
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

def create_performance_dashboard(metrics_history: pd.DataFrame):
    """Создание dashboard с метриками модели"""
    
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=(
            'AUC-ROC Over Time',
            'Precision vs Recall',
            'Fraud Detection Rate',
            'False Positive Rate'
        )
    )
    
    # AUC-ROC trend
    fig.add_trace(
        go.Scatter(
            x=metrics_history['date'],
            y=metrics_history['auc_roc'],
            mode='lines+markers',
            name='AUC-ROC'
        ),
        row=1, col=1
    )
    
    # Precision vs Recall
    fig.add_trace(
        go.Scatter(
            x=metrics_history['recall'],
            y=metrics_history['precision'],
            mode='markers',
            text=metrics_history['date'],
            name='Daily Performance'
        ),
        row=1, col=2
    )
    
    # Fraud Detection Rate
    fig.add_trace(
        go.Bar(
            x=metrics_history['date'],
            y=metrics_history['fraud_catch_rate'],
            name='Detection Rate'
        ),
        row=2, col=1
    )
    
    # False Positive Rate
    fig.add_trace(
        go.Scatter(
            x=metrics_history['date'],
            y=metrics_history['false_positive_rate'],
            mode='lines',
            name='FP Rate',
            line=dict(color='red')
        ),
        row=2, col=2
    )
    
    fig.update_layout(height=800, showlegend=False)
    return fig
```

## 🎓 Best Practices для ML в Fraud Detection

### 1. **Handling Imbalanced Data**
- Использование class weights
- SMOTE для генерации синтетических примеров
- Ensemble методы с разными sampling стратегиями

### 2. **Feature Engineering**
- Создание velocity features
- Использование исторических агрегатов
- Graph-based features из транзакционной сети

### 3. **Model Selection**
- XGBoost/LightGBM для baseline
- Neural Networks для сложных паттернов
- Ensemble для максимальной точности

### 4. **Production Considerations**
- Low-latency inference (< 100ms)
- Feature caching для производительности
- A/B testing новых моделей
- Continuous monitoring

### 5. **Explainability**
- SHAP values для объяснения предсказаний
- Feature importance tracking
- Business-friendly risk factors

---

*Следующий раздел: [API и интерфейсы →](./07_api_interfaces.md)*
