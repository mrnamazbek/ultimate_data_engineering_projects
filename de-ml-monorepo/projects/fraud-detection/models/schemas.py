"""
Pydantic schemas for request/response validation
"""
from pydantic import BaseModel, Field, validator
from typing import List, Optional
from datetime import datetime


class TransactionRequest(BaseModel):
    """Transaction request schema with validation"""
    transaction_id: str = Field(..., description="Unique transaction ID")
    user_id: str = Field(..., description="User identifier")
    merchant_id: str = Field(..., description="Merchant identifier")
    amount: float = Field(..., gt=0, description="Transaction amount")
    merchant_category: str = Field(..., description="Merchant category code")
    channel: str = Field(default="online", description="Transaction channel")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    
    # Optional fields
    location_lat: Optional[float] = Field(None, ge=-90, le=90)
    location_lon: Optional[float] = Field(None, ge=-180, le=180)
    device_id: Optional[str] = None
    
    @validator("amount")
    def validate_amount(cls, v):
        if v > 1000000:  # $1M limit
            raise ValueError("Transaction amount exceeds maximum limit")
        return round(v, 2)
    
    class Config:
        schema_extra = {
            "example": {
                "transaction_id": "TXN_123456",
                "user_id": "USER_789",
                "merchant_id": "MERCH_456",
                "amount": 150.99,
                "merchant_category": "grocery",
                "channel": "online"
            }
        }


class PredictionResponse(BaseModel):
    """Fraud prediction response schema"""
    transaction_id: str
    is_fraud: bool
    fraud_probability: float = Field(..., ge=0.0, le=1.0)
    risk_factors: List[str] = Field(default_factory=list)
    timestamp: str
    error: Optional[str] = None
    
    class Config:
        schema_extra = {
            "example": {
                "transaction_id": "TXN_123456",
                "is_fraud": False,
                "fraud_probability": 0.15,
                "risk_factors": ["high_amount"],
                "timestamp": "2024-01-01T12:00:00Z"
            }
        }


class HealthResponse(BaseModel):
    """Health check response schema"""
    status: str = Field(..., description="Overall health status")
    version: str
    environment: str
    checks: dict = Field(..., description="Individual component health checks")
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class ErrorResponse(BaseModel):
    """Error response schema"""
    error: str
    message: str
    details: Optional[dict] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)
