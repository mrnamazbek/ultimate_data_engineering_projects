"""
Dependency injection container and providers
"""
from typing import AsyncGenerator, Optional
from contextlib import asynccontextmanager
import aioredis
import asyncpg
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
import mlflow
import joblib
from functools import lru_cache

from .config import get_settings, Settings


class Dependencies:
    """
    Central dependency container using dependency injection pattern
    """
    
    def __init__(self, settings: Optional[Settings] = None):
        self.settings = settings or get_settings()
        self._postgres_pool: Optional[asyncpg.Pool] = None
        self._redis_pool: Optional[aioredis.Redis] = None
        self._kafka_producer: Optional[AIOKafkaProducer] = None
        self._kafka_consumer: Optional[AIOKafkaConsumer] = None
        self._async_engine = None
        self._async_session_maker = None
        self._ml_model = None
    
    # Database Dependencies
    async def get_postgres_pool(self) -> asyncpg.Pool:
        """Get PostgreSQL connection pool"""
        if not self._postgres_pool:
            self._postgres_pool = await asyncpg.create_pool(
                host=self.settings.postgres.host,
                port=self.settings.postgres.port,
                user=self.settings.postgres.user,
                password=self.settings.postgres.password,
                database=self.settings.postgres.database,
                min_size=10,
                max_size=20,
                max_inactive_connection_lifetime=300
            )
        return self._postgres_pool
    
    @asynccontextmanager
    async def get_postgres_connection(self) -> AsyncGenerator[asyncpg.Connection, None]:
        """Get PostgreSQL connection from pool"""
        pool = await self.get_postgres_pool()
        async with pool.acquire() as connection:
            yield connection
    
    async def get_async_session_maker(self) -> async_sessionmaker:
        """Get SQLAlchemy async session maker"""
        if not self._async_session_maker:
            if not self._async_engine:
                self._async_engine = create_async_engine(
                    self.settings.postgres.url.replace('postgresql://', 'postgresql+asyncpg://'),
                    pool_pre_ping=True,
                    pool_size=20,
                    max_overflow=40
                )
            self._async_session_maker = async_sessionmaker(
                self._async_engine,
                class_=AsyncSession,
                expire_on_commit=False
            )
        return self._async_session_maker
    
    @asynccontextmanager
    async def get_db_session(self) -> AsyncGenerator[AsyncSession, None]:
        """Get SQLAlchemy async session"""
        session_maker = await self.get_async_session_maker()
        async with session_maker() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise
            finally:
                await session.close()
    
    # Redis Dependencies
    async def get_redis_pool(self) -> aioredis.Redis:
        """Get Redis connection pool"""
        if not self._redis_pool:
            self._redis_pool = await aioredis.create_redis_pool(
                self.settings.redis.url,
                minsize=5,
                maxsize=10,
                encoding='utf-8'
            )
        return self._redis_pool
    
    @asynccontextmanager
    async def get_redis_connection(self) -> AsyncGenerator[aioredis.Redis, None]:
        """Get Redis connection"""
        pool = await self.get_redis_pool()
        yield pool
    
    # Kafka Dependencies
    async def get_kafka_producer(self) -> AIOKafkaProducer:
        """Get Kafka producer"""
        if not self._kafka_producer:
            self._kafka_producer = AIOKafkaProducer(
                bootstrap_servers=self.settings.kafka.brokers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                compression_type="gzip",
                acks='all',
                retries=5
            )
            await self._kafka_producer.start()
        return self._kafka_producer
    
    async def get_kafka_consumer(self, topics: list[str]) -> AIOKafkaConsumer:
        """Get Kafka consumer"""
        consumer = AIOKafkaConsumer(
            *topics,
            bootstrap_servers=self.settings.kafka.brokers,
            group_id=self.settings.kafka.consumer_group,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=False
        )
        await consumer.start()
        return consumer
    
    # ML Model Dependencies
    async def get_ml_model(self):
        """Get ML model (cached)"""
        if not self._ml_model:
            try:
                # Try loading from MLflow
                self._ml_model = mlflow.pyfunc.load_model(
                    f"models:/{self.settings.ml.model_name}/Production"
                )
            except Exception:
                # Fallback to local file
                self._ml_model = joblib.load(self.settings.ml.model_path)
        return self._ml_model
    
    # Cleanup
    async def close(self):
        """Close all connections and cleanup resources"""
        if self._postgres_pool:
            await self._postgres_pool.close()
        
        if self._redis_pool:
            self._redis_pool.close()
            await self._redis_pool.wait_closed()
        
        if self._kafka_producer:
            await self._kafka_producer.stop()
        
        if self._kafka_consumer:
            await self._kafka_consumer.stop()
        
        if self._async_engine:
            await self._async_engine.dispose()


# Global dependency container instance
_dependencies: Optional[Dependencies] = None


def get_dependencies() -> Dependencies:
    """Get global dependencies instance"""
    global _dependencies
    if _dependencies is None:
        _dependencies = Dependencies()
    return _dependencies


async def init_dependencies(settings: Optional[Settings] = None):
    """Initialize dependencies with custom settings"""
    global _dependencies
    _dependencies = Dependencies(settings)
    return _dependencies


async def close_dependencies():
    """Close all dependencies"""
    if _dependencies:
        await _dependencies.close()
