# 📊 Работа с данными

## Обзор Data Pipeline

### Архитектура потоков данных

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Data Sources   │────▶│   Ingestion     │────▶│   Processing    │
│ • APIs          │     │ • Validation    │     │ • Enrichment    │
│ • Files         │     │ • Schema Check  │     │ • Aggregation   │
│ • Streams       │     │ • Deduplication │     │ • Feature Eng.  │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                                                          │
                        ┌─────────────────────────────────┘
                        ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│    Storage      │     │   Serving       │     │   Monitoring    │
│ • Data Lake     │◀────│ • Feature Store │────▶│ • Data Quality  │
│ • Data Warehouse│     │ • Cache         │     │ • Lineage       │
│ • Archives      │     │ • APIs          │     │ • Metrics       │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

### Типы данных в системе

| Тип данных | Источник | Формат | Объем | Обновление |
|------------|----------|--------|-------|------------|
| **Транзакции** | Banking APIs | JSON | ~10M/день | Real-time |
| **Исторические данные** | Datasets | CSV/Parquet | ~100GB | Batch |
| **User profiles** | Database | Relational | ~1M записей | Incremental |
| **Merchant data** | External APIs | JSON | ~100K записей | Daily |
| **ML Features** | Feature Store | Parquet | ~50GB | Hourly |

## 🔄 Real-time Data Ingestion

### Kafka Producer Configuration

```python
# producers/transaction_producer.py
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import logging
from typing import Dict, Optional
import time

class TransactionProducer:
    """
    Производитель транзакций для Kafka с retry логикой и monitoring
    """
    
    def __init__(self, bootstrap_servers: str = 'localhost:9092'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            
            # Производительность
            batch_size=16384,  # 16KB
            linger_ms=10,      # Ждать до 10ms для batching
            compression_type='snappy',
            
            # Надежность
            acks='all',        # Ждать подтверждения от всех replicas
            retries=3,
            max_in_flight_requests_per_connection=5,
            
            # Мониторинг
            metrics_num_samples=2,
            metrics_sample_window_ms=30000,
            metric_reporters=['prometheus_client.KafkaMetricsReporter']
        )
        
        self.logger = logging.getLogger(__name__)
        self.metrics = ProducerMetrics()
    
    def send_transaction(self, transaction: Dict, key: Optional[str] = None) -> bool:
        """
        Отправка транзакции в Kafka с обработкой ошибок
        """
        try:
            # Валидация
            if not self._validate_transaction(transaction):
                self.metrics.invalid_transactions.inc()
                return False
            
            # Обогащение метаданными
            transaction['producer_timestamp'] = time.time()
            transaction['producer_version'] = '1.0.0'
            
            # Отправка
            future = self.producer.send(
                topic='fraud-transactions',
                key=key or transaction.get('user_id'),
                value=transaction,
                timestamp_ms=int(time.time() * 1000)
            )
            
            # Синхронное ожидание для критических транзакций
            if transaction.get('amount', 0) > 10000:
                record_metadata = future.get(timeout=10)
                self.logger.info(f"High-value transaction sent: {record_metadata}")
            
            self.metrics.transactions_sent.inc()
            self.metrics.bytes_sent.inc(len(json.dumps(transaction)))
            
            return True
            
        except KafkaError as e:
            self.logger.error(f"Kafka error: {e}")
            self.metrics.send_errors.inc()
            
            # Отправка в DLQ
            self._send_to_dlq(transaction, str(e))
            return False
    
    def _validate_transaction(self, transaction: Dict) -> bool:
        """Валидация транзакции перед отправкой"""
        required_fields = ['transaction_id', 'user_id', 'amount', 'timestamp']
        
        for field in required_fields:
            if field not in transaction:
                self.logger.warning(f"Missing required field: {field}")
                return False
        
        # Проверка типов
        if not isinstance(transaction['amount'], (int, float)) or transaction['amount'] <= 0:
            return False
        
        return True
    
    def _send_to_dlq(self, transaction: Dict, error: str):
        """Отправка проблемных транзакций в Dead Letter Queue"""
        dlq_message = {
            'original_message': transaction,
            'error': error,
            'timestamp': time.time()
        }
        
        self.producer.send('fraud-transactions-dlq', value=dlq_message)
    
    def close(self):
        """Корректное закрытие producer"""
        self.producer.flush()
        self.producer.close()
        self.logger.info("Producer closed successfully")
```

### Spark Streaming Consumer

```python
# streaming/spark_consumer.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

class FraudStreamProcessor:
    """
    Обработка потока транзакций в реальном времени с Spark Streaming
    """
    
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("FraudDetectionStreaming") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.streaming.backpressure.enabled", "true") \
            .config("spark.sql.streaming.stateStore.providerClass", 
                    "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        self.logger = logging.getLogger(__name__)
    
    def create_stream(self):
        """Создание streaming dataframe из Kafka"""
        
        # Схема транзакций
        transaction_schema = StructType([
            StructField("transaction_id", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("user_id", StringType(), False),
            StructField("merchant_id", StringType(), False),
            StructField("amount", DoubleType(), False),
            StructField("currency", StringType(), True),
            StructField("merchant_category", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("location", StructType([
                StructField("lat", DoubleType(), True),
                StructField("lon", DoubleType(), True),
                StructField("country", StringType(), True)
            ]), True)
        ])
        
        # Чтение из Kafka
        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", "fraud-transactions") \
            .option("startingOffsets", "latest") \
            .option("maxOffsetsPerTrigger", 10000) \
            .option("kafka.consumer.commit.groupid", "spark-streaming") \
            .load()
        
        # Парсинг JSON
        transactions = df.select(
            col("key").cast("string").alias("kafka_key"),
            from_json(col("value").cast("string"), transaction_schema).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ).select("data.*", "kafka_timestamp")
        
        return transactions
    
    def enrich_with_features(self, df):
        """Обогащение транзакций фичами"""
        
        # Временные фичи
        enriched = df \
            .withColumn("hour_of_day", hour("timestamp")) \
            .withColumn("day_of_week", dayofweek("timestamp")) \
            .withColumn("is_weekend", col("day_of_week").isin([1, 7])) \
            .withColumn("is_night", col("hour_of_day").between(0, 6))
        
        # Категориальные фичи
        enriched = enriched \
            .withColumn("is_high_risk_category", 
                       col("merchant_category").isin(["jewelry", "casino", "crypto"])) \
            .withColumn("is_high_amount", col("amount") > 5000)
        
        # User velocity (streaming aggregation)
        user_stats = enriched \
            .withWatermark("timestamp", "1 hour") \
            .groupBy(
                col("user_id"),
                window(col("timestamp"), "1 hour", "10 minutes")
            ) \
            .agg(
                count("*").alias("transaction_count_1h"),
                sum("amount").alias("total_amount_1h"),
                avg("amount").alias("avg_amount_1h"),
                stddev("amount").alias("std_amount_1h"),
                collect_set("merchant_id").alias("unique_merchants_1h")
            )
        
        # Join с основным потоком
        enriched_with_stats = enriched.join(
            user_stats,
            ["user_id", enriched.timestamp == user_stats.window.start],
            "left"
        )
        
        return enriched_with_stats
    
    def detect_anomalies(self, df):
        """Обнаружение аномалий в потоке"""
        
        # Z-score для суммы транзакции
        with_zscore = df.withColumn(
            "amount_zscore",
            when(col("std_amount_1h") > 0,
                 (col("amount") - col("avg_amount_1h")) / col("std_amount_1h")
            ).otherwise(0)
        )
        
        # Простые правила для демонстрации
        with_rules = with_zscore \
            .withColumn("rule_high_amount", col("amount") > 10000) \
            .withColumn("rule_unusual_time", col("is_night") & (col("amount") > 1000)) \
            .withColumn("rule_rapid_transactions", col("transaction_count_1h") > 10) \
            .withColumn("rule_high_zscore", col("amount_zscore") > 3)
        
        # Комбинированный risk score
        with_risk_score = with_rules.withColumn(
            "risk_score",
            (
                col("rule_high_amount").cast("int") * 0.3 +
                col("rule_unusual_time").cast("int") * 0.2 +
                col("rule_rapid_transactions").cast("int") * 0.3 +
                col("rule_high_zscore").cast("int") * 0.2
            )
        )
        
        return with_risk_score
    
    def process_stream(self):
        """Основная логика обработки потока"""
        
        # Создание потока
        transactions = self.create_stream()
        
        # Обогащение
        enriched = self.enrich_with_features(transactions)
        
        # Детекция аномалий
        with_anomalies = self.detect_anomalies(enriched)
        
        # Фильтрация подозрительных транзакций
        suspicious = with_anomalies.filter(col("risk_score") > 0.5)
        
        # Запись в разные sinks
        
        # 1. Все транзакции в S3/MinIO (parquet)
        all_transactions_query = with_anomalies.writeStream \
            .outputMode("append") \
            .format("parquet") \
            .option("path", "s3a://fraud-detection/transactions/") \
            .option("checkpointLocation", "s3a://fraud-detection/checkpoints/all/") \
            .partitionBy("date", "hour_of_day") \
            .trigger(processingTime='1 minute') \
            .start()
        
        # 2. Подозрительные транзакции в Kafka для алертов
        suspicious_query = suspicious.selectExpr(
            "user_id as key",
            "to_json(struct(*)) as value"
        ).writeStream \
            .outputMode("append") \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("topic", "fraud-alerts") \
            .option("checkpointLocation", "s3a://fraud-detection/checkpoints/alerts/") \
            .start()
        
        # 3. Агрегаты в PostgreSQL
        def write_aggregates_to_postgres(df, epoch_id):
            """Запись агрегатов в PostgreSQL"""
            agg_df = df.groupBy(
                window(col("timestamp"), "5 minutes"),
                "merchant_category"
            ).agg(
                count("*").alias("transaction_count"),
                sum("amount").alias("total_amount"),
                avg("risk_score").alias("avg_risk_score")
            )
            
            agg_df.write \
                .mode("append") \
                .jdbc(
                    url="jdbc:postgresql://postgres:5432/fraud_detection",
                    table="streaming_aggregates",
                    properties={
                        "user": "namazbek",
                        "password": "YourStrongPassword123!",
                        "driver": "org.postgresql.Driver"
                    }
                )
        
        aggregates_query = with_anomalies.writeStream \
            .outputMode("update") \
            .foreachBatch(write_aggregates_to_postgres) \
            .trigger(processingTime='5 minutes') \
            .start()
        
        # Ожидание завершения
        self.spark.streams.awaitAnyTermination()
```

## 📁 Batch Processing

### Airflow DAG для ежедневной обработки

```python
# dags/daily_fraud_pipeline.py
from airflow import DAG
from airflow.decorators import task, task_group
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from datetime import datetime, timedelta
import pandas as pd

# Конфигурация DAG
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['data-team@fraud-detection.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    'fraud_detection_daily_pipeline',
    default_args=default_args,
    description='Daily fraud detection data pipeline',
    schedule='0 2 * * *',  # 2 AM daily
    catchup=False,
    tags=['fraud-detection', 'production']
)
def fraud_detection_pipeline():
    
    # Task 1: Проверка доступности сервисов
    check_postgres = HttpSensor(
        task_id='check_postgres',
        http_conn_id='postgres_health',
        endpoint='/health',
        timeout=20,
        poke_interval=5
    )
    
    @task_group(group_id='data_extraction')
    def extract_data():
        """Группа задач для извлечения данных"""
        
        @task
        def extract_from_apis():
            """Извлечение данных из внешних API"""
            from integrations.plaid_client import PlaidClient
            from integrations.stripe_client import StripeClient
            
            # Plaid transactions
            plaid = PlaidClient()
            plaid_data = plaid.get_transactions(
                start_date=datetime.now() - timedelta(days=1),
                end_date=datetime.now()
            )
            
            # Stripe transactions
            stripe = StripeClient()
            stripe_data = stripe.get_charges(
                created={'gte': int((datetime.now() - timedelta(days=1)).timestamp())}
            )
            
            # Сохранение в staging
            combined_df = pd.concat([plaid_data, stripe_data])
            combined_df.to_parquet('/data/staging/api_transactions.parquet')
            
            return {'records': len(combined_df)}
        
        @task
        def extract_from_kafka():
            """Извлечение накопленных данных из Kafka"""
            from kafka import KafkaConsumer
            import json
            
            consumer = KafkaConsumer(
                'fraud-transactions',
                bootstrap_servers=['kafka:9092'],
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                group_id='airflow-batch-consumer',
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            
            transactions = []
            for message in consumer:
                transactions.append(message.value)
                
                # Ограничение на batch size
                if len(transactions) >= 100000:
                    break
            
            df = pd.DataFrame(transactions)
            df.to_parquet('/data/staging/kafka_transactions.parquet')
            
            consumer.close()
            return {'records': len(df)}
        
        @task
        def extract_from_database():
            """Извлечение исторических данных из БД"""
            from sqlalchemy import create_engine
            
            engine = create_engine('postgresql://namazbek:bekzhanov@postgres/fraud_detection')
            
            query = """
            SELECT *
            FROM fraud.transactions
            WHERE created_at >= CURRENT_DATE - INTERVAL '1 day'
                AND created_at < CURRENT_DATE
            """
            
            df = pd.read_sql(query, engine)
            df.to_parquet('/data/staging/db_transactions.parquet')
            
            return {'records': len(df)}
        
        # Выполнение задач извлечения
        api_data = extract_from_apis()
        kafka_data = extract_from_kafka()
        db_data = extract_from_database()
        
        return [api_data, kafka_data, db_data]
    
    @task_group(group_id='data_validation')
    def validate_data():
        """Валидация качества данных с Great Expectations"""
        
        @task
        def validate_schema():
            """Проверка схемы данных"""
            import great_expectations as ge
            
            # Загрузка данных
            df = pd.read_parquet('/data/staging/api_transactions.parquet')
            ge_df = ge.from_pandas(df)
            
            # Expectations
            expectations = [
                ge_df.expect_column_to_exist('transaction_id'),
                ge_df.expect_column_to_exist('amount'),
                ge_df.expect_column_values_to_be_unique('transaction_id'),
                ge_df.expect_column_values_to_be_between('amount', 0.01, 1000000),
                ge_df.expect_column_values_to_not_be_null('user_id'),
                ge_df.expect_column_values_to_match_regex(
                    'transaction_id', 
                    r'^TXN_\d{8}_\d{6}$'
                )
            ]
            
            # Проверка результатов
            results = ge_df.validate()
            
            if not results['success']:
                failed = [e for e in results['results'] if not e['success']]
                raise ValueError(f"Data validation failed: {failed}")
            
            return {'status': 'validated'}
        
        @task
        def check_data_quality():
            """Проверка качества данных"""
            # Загрузка всех staging файлов
            dfs = []
            for file in ['api_transactions', 'kafka_transactions', 'db_transactions']:
                try:
                    df = pd.read_parquet(f'/data/staging/{file}.parquet')
                    dfs.append(df)
                except FileNotFoundError:
                    continue
            
            combined_df = pd.concat(dfs, ignore_index=True)
            
            # Качественные метрики
            quality_metrics = {
                'total_records': len(combined_df),
                'duplicate_rate': combined_df.duplicated('transaction_id').sum() / len(combined_df),
                'null_rate': combined_df.isnull().sum().sum() / (len(combined_df) * len(combined_df.columns)),
                'amount_outliers': len(combined_df[combined_df['amount'] > combined_df['amount'].quantile(0.99)]),
                'date_range': f"{combined_df['timestamp'].min()} to {combined_df['timestamp'].max()}"
            }
            
            # Проверка пороговых значений
            if quality_metrics['duplicate_rate'] > 0.01:
                raise ValueError(f"Duplicate rate too high: {quality_metrics['duplicate_rate']}")
            
            if quality_metrics['null_rate'] > 0.05:
                raise ValueError(f"Null rate too high: {quality_metrics['null_rate']}")
            
            return quality_metrics
        
        schema_validation = validate_schema()
        quality_check = check_data_quality()
        
        return [schema_validation, quality_check]
    
    @task_group(group_id='feature_engineering')
    def create_features():
        """Создание фичей для ML"""
        
        @task
        def generate_user_features():
            """Генерация user-level фичей"""
            # Загрузка данных
            df = pd.read_parquet('/data/staging/combined_transactions.parquet')
            
            # User aggregates
            user_features = df.groupby('user_id').agg({
                'amount': ['count', 'mean', 'std', 'min', 'max', 'sum'],
                'merchant_id': 'nunique',
                'merchant_category': lambda x: x.mode()[0] if len(x) > 0 else 'unknown',
                'timestamp': ['min', 'max']
            })
            
            user_features.columns = ['_'.join(col) for col in user_features.columns]
            
            # Дополнительные фичи
            user_features['days_since_first_transaction'] = (
                datetime.now() - pd.to_datetime(user_features['timestamp_min'])
            ).dt.days
            
            user_features['avg_days_between_transactions'] = (
                pd.to_datetime(user_features['timestamp_max']) - 
                pd.to_datetime(user_features['timestamp_min'])
            ).dt.days / user_features['amount_count']
            
            user_features.to_parquet('/data/features/user_features.parquet')
            
            return {'user_features': len(user_features)}
        
        @task
        def generate_merchant_features():
            """Генерация merchant-level фичей"""
            df = pd.read_parquet('/data/staging/combined_transactions.parquet')
            
            # Merchant risk scores
            merchant_features = df.groupby('merchant_id').agg({
                'is_fraud': ['sum', 'mean'],
                'amount': ['count', 'mean', 'std'],
                'user_id': 'nunique'
            })
            
            merchant_features.columns = ['_'.join(col) for col in merchant_features.columns]
            merchant_features['fraud_rate'] = merchant_features['is_fraud_mean']
            
            # Risk categories
            merchant_features['risk_category'] = pd.cut(
                merchant_features['fraud_rate'],
                bins=[0, 0.01, 0.05, 0.1, 1.0],
                labels=['low', 'medium', 'high', 'very_high']
            )
            
            merchant_features.to_parquet('/data/features/merchant_features.parquet')
            
            return {'merchant_features': len(merchant_features)}
        
        @task
        def generate_graph_features():
            """Генерация сетевых фичей"""
            import networkx as nx
            
            df = pd.read_parquet('/data/staging/combined_transactions.parquet')
            
            # Создание графа user-merchant
            G = nx.Graph()
            
            for _, row in df.iterrows():
                G.add_edge(
                    f"user_{row['user_id']}", 
                    f"merchant_{row['merchant_id']}",
                    weight=row['amount']
                )
            
            # Graph metrics
            centrality = nx.degree_centrality(G)
            betweenness = nx.betweenness_centrality(G, weight='weight', k=min(100, len(G)))
            
            # Сохранение метрик
            graph_features = pd.DataFrame({
                'node': list(centrality.keys()),
                'degree_centrality': list(centrality.values()),
                'betweenness_centrality': [betweenness.get(node, 0) for node in centrality.keys()]
            })
            
            graph_features.to_parquet('/data/features/graph_features.parquet')
            
            return {'nodes': len(G.nodes()), 'edges': len(G.edges())}
        
        user_feat = generate_user_features()
        merchant_feat = generate_merchant_features()
        graph_feat = generate_graph_features()
        
        return [user_feat, merchant_feat, graph_feat]
    
    @task_group(group_id='data_loading')
    def load_data():
        """Загрузка обработанных данных"""
        
        @task
        def update_feature_store():
            """Обновление Feast feature store"""
            from feast import FeatureStore
            
            store = FeatureStore(repo_path="/feast/fraud_detection")
            
            # Загрузка фичей
            user_features = pd.read_parquet('/data/features/user_features.parquet')
            user_features['event_timestamp'] = datetime.now()
            user_features['created'] = datetime.now()
            
            # Материализация в online store
            store.materialize(
                end_date=datetime.now(),
                feature_views=['user_transaction_features']
            )
            
            return {'features_updated': len(user_features)}
        
        @task
        def load_to_warehouse():
            """Загрузка в data warehouse"""
            from sqlalchemy import create_engine
            
            engine = create_engine('postgresql://namazbek:bekzhanov@postgres/fraud_detection')
            
            # Загрузка транзакций
            df = pd.read_parquet('/data/staging/combined_transactions.parquet')
            
            # Добавление партиции
            df['partition_date'] = pd.to_datetime(df['timestamp']).dt.date
            
            # Загрузка с заменой партиции
            for partition_date, partition_df in df.groupby('partition_date'):
                table_name = f"transactions_{partition_date.strftime('%Y%m%d')}"
                
                partition_df.to_sql(
                    table_name, 
                    engine, 
                    schema='fraud',
                    if_exists='replace',
                    index=False,
                    method='multi',
                    chunksize=10000
                )
            
            return {'partitions_loaded': df['partition_date'].nunique()}
        
        @task
        def export_to_s3():
            """Экспорт в S3/MinIO для долгосрочного хранения"""
            import boto3
            from io import BytesIO
            
            s3 = boto3.client(
                's3',
                endpoint_url='http://minio:9000',
                aws_access_key_id='namazbek',
                aws_secret_access_key='YourMinioPassword123!'
            )
            
            # Загрузка всех фичей
            features = {}
            for feature_type in ['user_features', 'merchant_features', 'graph_features']:
                df = pd.read_parquet(f'/data/features/{feature_type}.parquet')
                
                # Сохранение в S3
                buffer = BytesIO()
                df.to_parquet(buffer)
                buffer.seek(0)
                
                s3.put_object(
                    Bucket='fraud-detection',
                    Key=f'features/{datetime.now().strftime("%Y%m%d")}/{feature_type}.parquet',
                    Body=buffer.getvalue()
                )
                
                features[feature_type] = len(df)
            
            return features
        
        feast_update = update_feature_store()
        warehouse_load = load_to_warehouse()
        s3_export = export_to_s3()
        
        return [feast_update, warehouse_load, s3_export]
    
    # Очистка временных файлов
    @task
    def cleanup_staging():
        """Очистка staging области"""
        import shutil
        import os
        
        staging_dir = '/data/staging'
        if os.path.exists(staging_dir):
            shutil.rmtree(staging_dir)
            os.makedirs(staging_dir)
        
        return {'status': 'cleaned'}
    
    # Определение зависимостей DAG
    postgres_check = check_postgres
    extraction = extract_data()
    validation = validate_data()
    features = create_features()
    loading = load_data()
    cleanup = cleanup_staging()
    
    # Pipeline flow
    postgres_check >> extraction >> validation >> features >> loading >> cleanup

# Инстанциация DAG
dag = fraud_detection_pipeline()
```

## 🔄 Data Transformation с dbt

### dbt Project Structure

```yaml
# dbt_project.yml
name: 'fraud_detection'
version: '1.0.0'
config-version: 2

profile: 'fraud_detection'

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["data"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

target-path: "target"
clean-targets:
  - "target"
  - "dbt_packages"

models:
  fraud_detection:
    staging:
      +materialized: view
      +schema: staging
    intermediate:
      +materialized: table
      +schema: intermediate
    marts:
      +materialized: table
      +schema: marts
      +indexes:
        - columns: ['user_id']
        - columns: ['merchant_id']
        - columns: ['timestamp']

vars:
  # Переменные для настройки
  fraud_threshold: 0.7
  lookback_days: 90
  high_risk_categories: ['jewelry', 'casino', 'crypto', 'money_transfer']
```

### Staging Models

```sql
-- models/staging/stg_transactions.sql
{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    on_schema_change='fail',
    indexes=[
        {'columns': ['user_id', 'timestamp'], 'type': 'btree'},
        {'columns': ['merchant_id'], 'type': 'hash'}
    ]
) }}

WITH source_data AS (
    SELECT 
        transaction_id,
        timestamp,
        user_id,
        merchant_id,
        amount,
        currency,
        merchant_category,
        channel,
        location,
        is_fraud,
        fraud_score,
        model_version,
        created_at,
        _dbt_source_relation
    FROM {{ source('raw', 'transactions') }}
    
    {% if is_incremental() %}
        WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
    {% endif %}
),

cleaned AS (
    SELECT
        -- IDs
        transaction_id,
        user_id,
        merchant_id,
        
        -- Timestamps
        timestamp::TIMESTAMP AS transaction_timestamp,
        created_at AS ingestion_timestamp,
        
        -- Transaction details
        COALESCE(amount, 0) AS amount,
        COALESCE(currency, 'USD') AS currency,
        LOWER(TRIM(merchant_category)) AS merchant_category,
        LOWER(TRIM(channel)) AS channel,
        
        -- Location parsing
        CASE 
            WHEN location IS NOT NULL THEN location->>'country'
            ELSE 'unknown'
        END AS country,
        
        CASE 
            WHEN location IS NOT NULL THEN (location->>'lat')::FLOAT
            ELSE NULL
        END AS latitude,
        
        CASE 
            WHEN location IS NOT NULL THEN (location->>'lon')::FLOAT
            ELSE NULL
        END AS longitude,
        
        -- Fraud indicators
        COALESCE(is_fraud, FALSE) AS is_fraud,
        COALESCE(fraud_score, 0) AS fraud_score,
        COALESCE(model_version, 'unknown') AS model_version,
        
        -- Metadata
        _dbt_source_relation,
        CURRENT_TIMESTAMP AS dbt_updated_at
        
    FROM source_data
),

validated AS (
    SELECT *
    FROM cleaned
    WHERE 
        amount > 0
        AND amount < 1000000  -- Исключение явных ошибок
        AND transaction_timestamp >= '2020-01-01'
        AND transaction_timestamp <= CURRENT_TIMESTAMP + INTERVAL '1 day'
)

SELECT * FROM validated
```

### Intermediate Models

```sql
-- models/intermediate/int_user_transaction_patterns.sql
{{ config(
    materialized='table',
    post_hook="ANALYZE {{ this }}"
) }}

WITH user_transactions AS (
    SELECT
        user_id,
        transaction_timestamp,
        amount,
        merchant_category,
        is_fraud,
        fraud_score
    FROM {{ ref('stg_transactions') }}
    WHERE transaction_timestamp >= CURRENT_DATE - {{ var('lookback_days') }}
),

user_daily_stats AS (
    SELECT
        user_id,
        DATE(transaction_timestamp) AS transaction_date,
        COUNT(*) AS daily_transaction_count,
        SUM(amount) AS daily_amount,
        AVG(amount) AS daily_avg_amount,
        STDDEV(amount) AS daily_std_amount,
        COUNT(DISTINCT merchant_category) AS unique_categories,
        SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) AS fraud_count
    FROM user_transactions
    GROUP BY 1, 2
),

user_patterns AS (
    SELECT
        user_id,
        
        -- Общая статистика
        COUNT(DISTINCT transaction_date) AS active_days,
        SUM(daily_transaction_count) AS total_transactions,
        SUM(daily_amount) AS total_amount,
        AVG(daily_amount) AS avg_daily_amount,
        STDDEV(daily_amount) AS std_daily_amount,
        
        -- Паттерны активности
        MAX(daily_transaction_count) AS max_daily_transactions,
        AVG(daily_transaction_count) AS avg_daily_transactions,
        
        -- Разнообразие
        AVG(unique_categories) AS avg_daily_categories,
        
        -- Риск метрики
        SUM(fraud_count) AS total_fraud_count,
        SUM(fraud_count)::FLOAT / NULLIF(SUM(daily_transaction_count), 0) AS fraud_rate,
        
        -- Временные паттерны
        MIN(transaction_date) AS first_transaction_date,
        MAX(transaction_date) AS last_transaction_date,
        MAX(transaction_date) - MIN(transaction_date) AS days_active
        
    FROM user_daily_stats
    GROUP BY 1
)

SELECT 
    *,
    CASE 
        WHEN fraud_rate > 0.1 THEN 'very_high_risk'
        WHEN fraud_rate > 0.05 THEN 'high_risk'
        WHEN fraud_rate > 0.02 THEN 'medium_risk'
        ELSE 'low_risk'
    END AS risk_category
FROM user_patterns
```

### Mart Models

```sql
-- models/marts/fraud_risk_scoring.sql
{{ config(
    materialized='table',
    post_hook=[
        "CREATE INDEX idx_risk_score ON {{ this }} (risk_score DESC)",
        "CREATE INDEX idx_user_risk ON {{ this }} (user_id, risk_score DESC)"
    ]
) }}

WITH latest_transactions AS (
    SELECT
        t.*,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY transaction_timestamp DESC) AS rn
    FROM {{ ref('stg_transactions') }} t
    WHERE transaction_timestamp >= CURRENT_DATE - INTERVAL '24 hours'
),

recent_user_activity AS (
    SELECT
        user_id,
        COUNT(*) AS transactions_24h,
        SUM(amount) AS amount_24h,
        AVG(amount) AS avg_amount_24h,
        MAX(amount) AS max_amount_24h,
        COUNT(DISTINCT merchant_id) AS unique_merchants_24h,
        COUNT(DISTINCT merchant_category) AS unique_categories_24h
    FROM latest_transactions
    GROUP BY user_id
),

user_history AS (
    SELECT * FROM {{ ref('int_user_transaction_patterns') }}
),

risk_factors AS (
    SELECT
        lt.transaction_id,
        lt.user_id,
        lt.transaction_timestamp,
        lt.amount,
        lt.merchant_category,
        lt.is_fraud,
        
        -- Velocity риски
        CASE WHEN rua.transactions_24h > 50 THEN 1 ELSE 0 END AS high_velocity_risk,
        CASE WHEN lt.amount > uh.avg_daily_amount * 3 THEN 1 ELSE 0 END AS unusual_amount_risk,
        
        -- Категориальные риски
        CASE WHEN lt.merchant_category IN {{ var('high_risk_categories') }} THEN 1 ELSE 0 END AS high_risk_category,
        
        -- Поведенческие риски
        CASE WHEN rua.unique_categories_24h > uh.avg_daily_categories * 2 THEN 1 ELSE 0 END AS category_diversity_risk,
        
        -- Временные риски
        CASE WHEN EXTRACT(HOUR FROM lt.transaction_timestamp) BETWEEN 0 AND 6 THEN 1 ELSE 0 END AS unusual_time_risk,
        
        -- Исторические риски
        uh.fraud_rate AS historical_fraud_rate,
        uh.risk_category AS historical_risk_category
        
    FROM latest_transactions lt
    LEFT JOIN recent_user_activity rua ON lt.user_id = rua.user_id
    LEFT JOIN user_history uh ON lt.user_id = uh.user_id
    WHERE lt.rn = 1
),

risk_scoring AS (
    SELECT
        *,
        
        -- Взвешенный risk score
        (
            high_velocity_risk * 0.2 +
            unusual_amount_risk * 0.25 +
            high_risk_category * 0.15 +
            category_diversity_risk * 0.1 +
            unusual_time_risk * 0.1 +
            COALESCE(historical_fraud_rate, 0) * 0.2
        ) AS risk_score,
        
        -- Risk explanation
        ARRAY_REMOVE(ARRAY[
            CASE WHEN high_velocity_risk = 1 THEN 'high_velocity' END,
            CASE WHEN unusual_amount_risk = 1 THEN 'unusual_amount' END,
            CASE WHEN high_risk_category = 1 THEN 'risky_category' END,
            CASE WHEN category_diversity_risk = 1 THEN 'unusual_diversity' END,
            CASE WHEN unusual_time_risk = 1 THEN 'unusual_time' END,
            CASE WHEN historical_fraud_rate > 0.05 THEN 'fraud_history' END
        ], NULL) AS risk_factors_list
        
    FROM risk_factors
)

SELECT
    transaction_id,
    user_id,
    transaction_timestamp,
    amount,
    merchant_category,
    is_fraud,
    risk_score,
    risk_factors_list,
    
    -- Классификация риска
    CASE 
        WHEN risk_score >= {{ var('fraud_threshold') }} THEN 'high'
        WHEN risk_score >= 0.5 THEN 'medium'
        ELSE 'low'
    END AS risk_level,
    
    -- Рекомендация
    CASE 
        WHEN risk_score >= {{ var('fraud_threshold') }} THEN 'block'
        WHEN risk_score >= 0.5 THEN 'review'
        ELSE 'approve'
    END AS recommendation,
    
    CURRENT_TIMESTAMP AS scored_at
    
FROM risk_scoring
```

## 📊 Data Quality Monitoring

### Great Expectations Suite

```python
# great_expectations/expectations/fraud_transactions_suite.py
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.data_context.types.resource_identifiers import ExpectationSuiteIdentifier

def build_fraud_transaction_expectations():
    """
    Построение набора ожиданий для валидации транзакций
    """
    
    expectations = [
        # Структурные проверки
        ExpectationConfiguration(
            expectation_type="expect_table_row_count_to_be_between",
            kwargs={"min_value": 10000, "max_value": 10000000}
        ),
        
        ExpectationConfiguration(
            expectation_type="expect_table_columns_to_match_set",
            kwargs={
                "column_set": [
                    "transaction_id", "timestamp", "user_id", "merchant_id",
                    "amount", "currency", "merchant_category", "is_fraud"
                ]
            }
        ),
        
        # Уникальность
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_unique",
            kwargs={"column": "transaction_id"}
        ),
        
        # Null проверки
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "transaction_id"}
        ),
        
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "user_id"}
        ),
        
        # Диапазоны значений
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={
                "column": "amount",
                "min_value": 0.01,
                "max_value": 1000000,
                "mostly": 0.99
            }
        ),
        
        # Формат данных
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_match_regex",
            kwargs={
                "column": "transaction_id",
                "regex": r"^TXN_\d{8}_\d{6}$"
            }
        ),
        
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={
                "column": "currency",
                "value_set": ["USD", "EUR", "GBP", "JPY"]
            }
        ),
        
        # Статистические проверки
        ExpectationConfiguration(
            expectation_type="expect_column_mean_to_be_between",
            kwargs={
                "column": "amount",
                "min_value": 50,
                "max_value": 500
            }
        ),
        
        ExpectationConfiguration(
            expectation_type="expect_column_stdev_to_be_between",
            kwargs={
                "column": "amount",
                "min_value": 10,
                "max_value": 1000
            }
        ),
        
        # Распределение fraud
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={
                "column": "is_fraud",
                "value_set": [True, False]
            }
        ),
        
        ExpectationConfiguration(
            expectation_type="expect_column_mean_to_be_between",
            kwargs={
                "column": "is_fraud",
                "min_value": 0.001,
                "max_value": 0.05
            }
        )
    ]
    
    return expectations
```

## 🔍 Data Lineage и Documentation

### Lineage Tracking

```python
# lineage/data_lineage_tracker.py
from typing import Dict, List, Optional
import networkx as nx
import json
from datetime import datetime

class DataLineageTracker:
    """
    Отслеживание происхождения данных через pipeline
    """
    
    def __init__(self):
        self.graph = nx.DiGraph()
        self.metadata = {}
    
    def add_data_source(self, source_id: str, metadata: Dict):
        """Добавление источника данных"""
        self.graph.add_node(source_id, node_type='source', **metadata)
        self.metadata[source_id] = {
            'type': 'source',
            'created_at': datetime.now().isoformat(),
            **metadata
        }
    
    def add_transformation(self, 
                         transform_id: str, 
                         inputs: List[str], 
                         outputs: List[str],
                         metadata: Dict):
        """Добавление трансформации"""
        self.graph.add_node(transform_id, node_type='transformation', **metadata)
        
        # Добавление связей
        for input_node in inputs:
            self.graph.add_edge(input_node, transform_id)
        
        for output_node in outputs:
            self.graph.add_node(output_node, node_type='dataset')
            self.graph.add_edge(transform_id, output_node)
        
        self.metadata[transform_id] = {
            'type': 'transformation',
            'inputs': inputs,
            'outputs': outputs,
            'created_at': datetime.now().isoformat(),
            **metadata
        }
    
    def get_lineage(self, dataset_id: str) -> Dict:
        """Получение полного lineage для датасета"""
        ancestors = nx.ancestors(self.graph, dataset_id)
        subgraph = self.graph.subgraph(ancestors | {dataset_id})
        
        return {
            'dataset': dataset_id,
            'lineage_graph': nx.node_link_data(subgraph),
            'metadata': {node: self.metadata.get(node, {}) for node in subgraph.nodes()}
        }
    
    def generate_lineage_report(self) -> str:
        """Генерация отчета о lineage"""
        report = {
            'total_nodes': self.graph.number_of_nodes(),
            'total_edges': self.graph.number_of_edges(),
            'sources': [n for n, d in self.graph.nodes(data=True) if d.get('node_type') == 'source'],
            'transformations': [n for n, d in self.graph.nodes(data=True) if d.get('node_type') == 'transformation'],
            'datasets': [n for n, d in self.graph.nodes(data=True) if d.get('node_type') == 'dataset'],
            'generated_at': datetime.now().isoformat()
        }
        
        return json.dumps(report, indent=2)

# Использование
lineage_tracker = DataLineageTracker()

# Добавление источников
lineage_tracker.add_data_source('plaid_api', {
    'description': 'Banking transactions from Plaid API',
    'frequency': 'real-time',
    'schema': 'transaction_schema_v1'
})

lineage_tracker.add_data_source('kafka_stream', {
    'description': 'Real-time transaction stream',
    'topic': 'fraud-transactions',
    'format': 'json'
})

# Добавление трансформаций
lineage_tracker.add_transformation(
    'feature_engineering_job',
    inputs=['plaid_api', 'kafka_stream'],
    outputs=['user_features', 'merchant_features'],
    metadata={
        'job_type': 'spark',
        'schedule': 'hourly',
        'owner': 'data-team'
    }
)

# Получение lineage
lineage = lineage_tracker.get_lineage('user_features')
print(json.dumps(lineage, indent=2))
```

## 💾 Оптимизация хранения

### Partitioning Strategy

```sql
-- Автоматическое создание партиций
CREATE OR REPLACE FUNCTION create_monthly_partition()
RETURNS void AS $$
DECLARE
    partition_date date;
    partition_name text;
    start_date date;
    end_date date;
BEGIN
    -- Следующий месяц
    partition_date := date_trunc('month', CURRENT_DATE + interval '1 month');
    partition_name := 'transactions_' || to_char(partition_date, 'YYYY_MM');
    start_date := partition_date;
    end_date := partition_date + interval '1 month';
    
    -- Проверка существования
    IF NOT EXISTS (
        SELECT 1 FROM pg_tables 
        WHERE schemaname = 'fraud' 
        AND tablename = partition_name
    ) THEN
        -- Создание партиции
        EXECUTE format(
            'CREATE TABLE fraud.%I PARTITION OF fraud.transactions 
            FOR VALUES FROM (%L) TO (%L)',
            partition_name, start_date, end_date
        );
        
        -- Создание индексов
        EXECUTE format(
            'CREATE INDEX %I ON fraud.%I (user_id, timestamp)',
            'idx_' || partition_name || '_user_time',
            partition_name
        );
        
        RAISE NOTICE 'Created partition %', partition_name;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Scheduled job для создания партиций
SELECT cron.schedule(
    'create-monthly-partitions',
    '0 0 25 * *',  -- 25 числа каждого месяца
    'SELECT create_monthly_partition()'
);
```

### Compression и архивирование

```python
# compression/data_archiver.py
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from datetime import datetime, timedelta
import os

class DataArchiver:
    """
    Архивирование и сжатие старых данных
    """
    
    def __init__(self, retention_days: int = 90):
        self.retention_days = retention_days
        self.compression_codec = 'snappy'
    
    def archive_old_data(self, source_path: str, archive_path: str):
        """Архивирование данных старше retention period"""
        
        cutoff_date = datetime.now() - timedelta(days=self.retention_days)
        
        # Чтение данных
        df = pd.read_parquet(source_path)
        
        # Разделение на текущие и архивные
        current_data = df[df['timestamp'] >= cutoff_date]
        archive_data = df[df['timestamp'] < cutoff_date]
        
        if len(archive_data) > 0:
            # Сохранение архива с оптимальным сжатием
            table = pa.Table.from_pandas(archive_data)
            
            pq.write_table(
                table,
                archive_path,
                compression=self.compression_codec,
                use_dictionary=True,
                column_encoding={
                    'user_id': 'PLAIN_DICTIONARY',
                    'merchant_id': 'PLAIN_DICTIONARY',
                    'merchant_category': 'PLAIN_DICTIONARY'
                },
                row_group_size=100000
            )
            
            # Обновление текущих данных
            current_data.to_parquet(source_path, compression=self.compression_codec)
            
            print(f"Archived {len(archive_data)} records to {archive_path}")
            
            # Статистика сжатия
            original_size = os.path.getsize(source_path)
            archive_size = os.path.getsize(archive_path)
            compression_ratio = 1 - (archive_size / original_size)
            
            print(f"Compression ratio: {compression_ratio:.2%}")
```

---

*Следующий раздел: [Machine Learning Pipeline →](./06_ml_pipeline.md)*
