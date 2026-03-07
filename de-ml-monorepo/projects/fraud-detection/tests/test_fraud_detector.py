"""
Unit tests demonstrating how dependency injection makes testing easier
"""
import pytest
from unittest.mock import Mock, AsyncMock
import pandas as pd
import numpy as np
from datetime import datetime

from ..services.fraud_detector import FraudDetectorService
from ..core.config import Settings


class TestFraudDetector:
    """Test suite showing the benefits of dependency injection"""
    
    @pytest.fixture
    def mock_feature_store(self):
        """Mock feature store for testing"""
        mock = Mock()
        mock.get_features = AsyncMock(return_value={
            "user_avg_amount": 150.0,
            "user_transaction_count_1d": 5,
            "merchant_risk_score": 0.2
        })
        return mock
    
    @pytest.fixture
    def mock_model_server(self):
        """Mock model server for testing"""
        mock = Mock()
        mock.predict = AsyncMock(return_value=np.array([[0.3, 0.7]]))
        return mock
    
    @pytest.fixture
    def mock_cache(self):
        """Mock cache for testing"""
        mock = Mock()
        mock.get = AsyncMock(return_value=None)
        mock.set = AsyncMock()
        return mock
    
    @pytest.fixture
    def test_settings(self):
        """Test settings"""
        return Settings(
            ml={"model_threshold": 0.5},
            _env_file=None  # Don't load from .env in tests
        )
    
    @pytest.fixture
    def fraud_detector(self, mock_feature_store, mock_model_server, mock_cache, test_settings):
        """Create fraud detector with mocked dependencies"""
        return FraudDetectorService(
            feature_store=mock_feature_store,
            model_server=mock_model_server,
            cache=mock_cache,
            settings=test_settings
        )
    
    @pytest.mark.asyncio
    async def test_predict_fraud_transaction(self, fraud_detector):
        """Test fraud prediction for a fraudulent transaction"""
        # Arrange
        transaction = {
            "transaction_id": "TEST_001",
            "user_id": "USER_123",
            "merchant_id": "MERCH_456",
            "amount": 5000.0,
            "merchant_category": "jewelry"
        }
        
        # Act
        result = await fraud_detector.predict(transaction)
        
        # Assert
        assert result["transaction_id"] == "TEST_001"
        assert result["is_fraud"] is True  # 0.7 > 0.5 threshold
        assert result["fraud_probability"] == 0.7
        assert "high_amount" in result["risk_factors"]
        assert "high_risk_merchant" in result["risk_factors"]
    
    @pytest.mark.asyncio
    async def test_predict_with_cache_hit(self, fraud_detector, mock_cache):
        """Test prediction with cache hit"""
        # Arrange
        cached_result = {
            "transaction_id": "TEST_002",
            "is_fraud": False,
            "fraud_probability": 0.1,
            "risk_factors": []
        }
        mock_cache.get.return_value = '{"transaction_id": "TEST_002", "is_fraud": false, "fraud_probability": 0.1, "risk_factors": []}'
        
        transaction = {
            "transaction_id": "TEST_002",
            "user_id": "USER_456",
            "merchant_id": "MERCH_789",
            "amount": 50.0
        }
        
        # Act
        result = await fraud_detector.predict(transaction)
        
        # Assert
        assert result["transaction_id"] == "TEST_002"
        assert result["is_fraud"] is False
        # Model should not be called due to cache hit
        fraud_detector.model_server.predict.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_fallback_on_model_error(self, fraud_detector, mock_model_server):
        """Test fallback to rule-based when model fails"""
        # Arrange
        mock_model_server.predict.side_effect = Exception("Model error")
        
        transaction = {
            "transaction_id": "TEST_003",
            "user_id": "USER_789",
            "merchant_id": "MERCH_CASINO",
            "amount": 10000.0,
            "merchant_category": "casino"
        }
        
        # Act
        result = await fraud_detector.predict(transaction)
        
        # Assert
        assert result["transaction_id"] == "TEST_003"
        assert result["is_fraud"] is True  # Rule-based: high amount + casino
        assert result["fraud_probability"] >= 0.6
    
    @pytest.mark.asyncio
    async def test_feature_extraction(self, fraud_detector, mock_feature_store):
        """Test feature extraction combines all sources"""
        # Arrange
        transaction = {
            "transaction_id": "TEST_004",
            "user_id": "USER_999",
            "merchant_id": "MERCH_111",
            "amount": 100.0
        }
        
        # Act
        features = await fraud_detector._extract_features(transaction)
        
        # Assert
        assert isinstance(features, pd.DataFrame)
        assert len(features) == 1
        # Check transaction features
        assert features.iloc[0]["amount"] == 100.0
        # Check user features from mock
        assert features.iloc[0]["user_avg_amount"] == 150.0
        # Check added features
        assert "hour_of_day" in features.columns
        assert "day_of_week" in features.columns


class TestDependencyInjectionBenefits:
    """Tests demonstrating the benefits of DI for testing"""
    
    @pytest.mark.asyncio
    async def test_isolated_unit_testing(self):
        """Show how DI enables isolated unit testing"""
        # Create completely isolated mocks
        mock_feature_store = Mock()
        mock_feature_store.get_features = AsyncMock(return_value={})
        
        mock_model_server = Mock()
        mock_model_server.predict = AsyncMock(return_value=np.array([[0.9, 0.1]]))
        
        mock_cache = Mock()
        mock_cache.get = AsyncMock(return_value=None)
        mock_cache.set = AsyncMock()
        
        # Inject mocks - no real dependencies needed
        service = FraudDetectorService(
            feature_store=mock_feature_store,
            model_server=mock_model_server,
            cache=mock_cache,
            settings=Mock(ml=Mock(model_threshold=0.5))
        )
        
        # Test in complete isolation
        result = await service.predict({"transaction_id": "ISO_TEST", "user_id": "U1", "merchant_id": "M1"})
        
        assert result is not None
        assert mock_feature_store.get_features.called
        assert mock_model_server.predict.called
    
    def test_different_implementations(self):
        """Show how DI allows swapping implementations"""
        
        # Production implementation
        class RedisCache:
            async def get(self, key): 
                # Real Redis logic
                pass
        
        # Test implementation
        class InMemoryCache:
            def __init__(self):
                self.data = {}
            
            async def get(self, key):
                return self.data.get(key)
            
            async def set(self, key, value, ttl=None):
                self.data[key] = value
        
        # Can use either implementation
        test_cache = InMemoryCache()
        prod_cache = RedisCache()
        
        # Both work with the service
        service_test = FraudDetectorService(
            feature_store=Mock(),
            model_server=Mock(),
            cache=test_cache,  # In-memory for tests
            settings=Mock()
        )
        
        service_prod = FraudDetectorService(
            feature_store=Mock(),
            model_server=Mock(),
            cache=prod_cache,  # Redis for production
            settings=Mock()
        )
