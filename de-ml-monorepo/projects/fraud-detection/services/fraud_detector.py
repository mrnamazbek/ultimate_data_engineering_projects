"""
Fraud Detection Service with proper dependency injection
"""
from typing import Dict, Optional, Protocol
import pandas as pd
import numpy as np
from datetime import datetime
import structlog

from ..core.config import Settings
from ..core.dependencies import Dependencies


logger = structlog.get_logger()


class FeatureStoreProtocol(Protocol):
    """Protocol for feature store abstraction"""
    async def get_features(self, entity_id: str) -> Dict:
        ...


class ModelServerProtocol(Protocol):
    """Protocol for model server abstraction"""
    async def predict(self, features: pd.DataFrame) -> np.ndarray:
        ...


class CacheProtocol(Protocol):
    """Protocol for cache abstraction"""
    async def get(self, key: str) -> Optional[str]:
        ...
    
    async def set(self, key: str, value: str, ttl: int = 3600) -> None:
        ...


class FraudDetectorService:
    """
    Fraud detection service with dependency injection
    
    This service demonstrates proper DI patterns:
    - Dependencies injected through constructor
    - Uses protocols for abstraction
    - Easily testable with mocks
    - No direct creation of dependencies
    """
    
    def __init__(
        self,
        feature_store: FeatureStoreProtocol,
        model_server: ModelServerProtocol,
        cache: CacheProtocol,
        settings: Settings
    ):
        self.feature_store = feature_store
        self.model_server = model_server
        self.cache = cache
        self.settings = settings
        self.logger = logger.bind(service="fraud_detector")
    
    async def predict(self, transaction: Dict) -> Dict:
        """
        Predict fraud for a transaction
        
        Args:
            transaction: Transaction data dict
            
        Returns:
            Prediction result with fraud probability
        """
        transaction_id = transaction.get("transaction_id")
        
        # Check cache first
        cached_result = await self._check_cache(transaction_id)
        if cached_result:
            self.logger.info("cache_hit", transaction_id=transaction_id)
            return cached_result
        
        # Extract features
        features = await self._extract_features(transaction)
        
        # Get prediction
        prediction = await self._get_prediction(features)
        
        # Prepare result
        result = {
            "transaction_id": transaction_id,
            "is_fraud": prediction["probability"] > self.settings.ml.model_threshold,
            "fraud_probability": prediction["probability"],
            "risk_factors": self._analyze_risk_factors(transaction, features),
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Cache result
        await self._cache_result(transaction_id, result)
        
        self.logger.info(
            "prediction_complete",
            transaction_id=transaction_id,
            is_fraud=result["is_fraud"],
            probability=result["fraud_probability"]
        )
        
        return result
    
    async def _check_cache(self, transaction_id: str) -> Optional[Dict]:
        """Check cache for existing prediction"""
        try:
            cached = await self.cache.get(f"prediction:{transaction_id}")
            if cached:
                import json
                return json.loads(cached)
        except Exception as e:
            self.logger.warning("cache_error", error=str(e))
        return None
    
    async def _extract_features(self, transaction: Dict) -> pd.DataFrame:
        """Extract features for prediction"""
        # Get user features from feature store
        user_features = await self.feature_store.get_features(
            transaction["user_id"]
        )
        
        # Get merchant features
        merchant_features = await self.feature_store.get_features(
            transaction["merchant_id"]
        )
        
        # Combine with transaction features
        features = {
            **transaction,
            **user_features,
            **merchant_features,
            "hour_of_day": datetime.now().hour,
            "day_of_week": datetime.now().weekday()
        }
        
        return pd.DataFrame([features])
    
    async def _get_prediction(self, features: pd.DataFrame) -> Dict:
        """Get model prediction"""
        try:
            predictions = await self.model_server.predict(features)
            return {
                "probability": float(predictions[0][1]),
                "scores": predictions[0].tolist()
            }
        except Exception as e:
            self.logger.error("prediction_error", error=str(e))
            # Fallback to rule-based
            return self._rule_based_prediction(features)
    
    def _rule_based_prediction(self, features: pd.DataFrame) -> Dict:
        """Simple rule-based fallback"""
        row = features.iloc[0]
        score = 0.0
        
        # High amount
        if row.get("amount", 0) > 5000:
            score += 0.3
        
        # Unusual time
        if row.get("hour_of_day", 12) in [0, 1, 2, 3, 4, 5]:
            score += 0.2
        
        # High-risk merchant
        if row.get("merchant_category") in ["jewelry", "casino", "crypto"]:
            score += 0.3
        
        return {
            "probability": min(score, 1.0),
            "scores": [1 - score, score]
        }
    
    def _analyze_risk_factors(
        self, 
        transaction: Dict, 
        features: pd.DataFrame
    ) -> list[str]:
        """Analyze risk factors for explanation"""
        risk_factors = []
        
        if transaction.get("amount", 0) > 5000:
            risk_factors.append("high_amount")
        
        if features.iloc[0].get("hour_of_day") in range(0, 6):
            risk_factors.append("unusual_time")
        
        if transaction.get("merchant_category") in ["jewelry", "casino"]:
            risk_factors.append("high_risk_merchant")
        
        return risk_factors
    
    async def _cache_result(self, transaction_id: str, result: Dict) -> None:
        """Cache prediction result"""
        try:
            import json
            await self.cache.set(
                f"prediction:{transaction_id}",
                json.dumps(result),
                ttl=3600  # 1 hour
            )
        except Exception as e:
            self.logger.warning("cache_write_error", error=str(e))


# Factory function for creating service with dependencies
async def create_fraud_detector(deps: Dependencies) -> FraudDetectorService:
    """
    Factory function to create FraudDetectorService with all dependencies
    
    This demonstrates how to wire up dependencies from the container
    """
    from ..adapters.feature_store import FeatureStoreAdapter
    from ..adapters.model_server import ModelServerAdapter
    from ..adapters.cache import RedisCacheAdapter
    
    # Create adapters that implement the protocols
    feature_store = FeatureStoreAdapter(
        await deps.get_postgres_connection(),
        await deps.get_redis_connection()
    )
    
    model_server = ModelServerAdapter(
        await deps.get_ml_model()
    )
    
    cache = RedisCacheAdapter(
        await deps.get_redis_pool()
    )
    
    # Create service with injected dependencies
    return FraudDetectorService(
        feature_store=feature_store,
        model_server=model_server,
        cache=cache,
        settings=deps.settings
    )
