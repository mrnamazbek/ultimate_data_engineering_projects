"""
Centralized configuration management with validation
"""
from typing import List, Optional
from pydantic import BaseSettings, Field, validator
from functools import lru_cache


class PostgresConfig(BaseSettings):
    """PostgreSQL configuration"""
    host: str = Field(..., env="POSTGRES_HOST")
    port: int = Field(5432, env="POSTGRES_PORT")
    user: str = Field(..., env="POSTGRES_USER")
    password: str = Field(..., env="POSTGRES_PASSWORD")
    database: str = Field(..., env="POSTGRES_DB")
    
    @property
    def url(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
    
    class Config:
        env_prefix = "POSTGRES_"


class KafkaConfig(BaseSettings):
    """Kafka configuration"""
    brokers: List[str] = Field(..., env="KAFKA_BROKERS")
    topic_transactions: str = Field("fraud-transactions", env="KAFKA_TOPIC_TRANSACTIONS")
    topic_predictions: str = Field("fraud-predictions", env="KAFKA_TOPIC_PREDICTIONS")
    consumer_group: str = Field("fraud-detection-group", env="KAFKA_CONSUMER_GROUP")
    
    @validator("brokers", pre=True)
    def parse_brokers(cls, v):
        if isinstance(v, str):
            return v.split(",")
        return v
    
    class Config:
        env_prefix = "KAFKA_"


class RedisConfig(BaseSettings):
    """Redis configuration"""
    host: str = Field("localhost", env="REDIS_HOST")
    port: int = Field(6379, env="REDIS_PORT")
    password: Optional[str] = Field(None, env="REDIS_PASSWORD")
    db: int = Field(0, env="REDIS_DB")
    
    @property
    def url(self) -> str:
        if self.password:
            return f"redis://:{self.password}@{self.host}:{self.port}/{self.db}"
        return f"redis://{self.host}:{self.port}/{self.db}"
    
    class Config:
        env_prefix = "REDIS_"


class MLConfig(BaseSettings):
    """Machine Learning configuration"""
    model_path: str = Field("/models/fraud_detection_model.pkl", env="MODEL_PATH")
    feature_store_url: str = Field("feast://localhost:6566", env="FEATURE_STORE_URL")
    model_threshold: float = Field(0.7, ge=0.0, le=1.0, env="MODEL_THRESHOLD")
    batch_size: int = Field(100, env="MODEL_BATCH_SIZE")
    
    class Config:
        env_prefix = "ML_"


class APIConfig(BaseSettings):
    """API configuration"""
    host: str = Field("0.0.0.0", env="API_HOST")
    port: int = Field(8001, env="API_PORT")
    workers: int = Field(4, env="API_WORKERS")
    reload: bool = Field(False, env="API_RELOAD")
    log_level: str = Field("INFO", env="LOG_LEVEL")
    rate_limit: str = Field("100/minute", env="API_RATE_LIMIT")
    
    class Config:
        env_prefix = "API_"


class Settings(BaseSettings):
    """Main application settings"""
    app_name: str = Field("Fraud Detection Service", env="APP_NAME")
    environment: str = Field("development", env="ENVIRONMENT")
    debug: bool = Field(False, env="DEBUG")
    
    # Sub-configurations
    postgres: PostgresConfig = PostgresConfig()
    kafka: KafkaConfig = KafkaConfig()
    redis: RedisConfig = RedisConfig()
    ml: MLConfig = MLConfig()
    api: APIConfig = APIConfig()
    
    # Security
    secret_key: str = Field(..., env="SECRET_KEY")
    jwt_algorithm: str = Field("HS256", env="JWT_ALGORITHM")
    jwt_expiration_minutes: int = Field(30, env="JWT_EXPIRATION_MINUTES")
    
    class Config:
        env_file = ".env"
        case_sensitive = False


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance"""
    return Settings()
