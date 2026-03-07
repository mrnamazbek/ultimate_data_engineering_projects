"""
FastAPI application with proper dependency injection
"""
from fastapi import FastAPI, Depends, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from typing import Dict
import structlog

from ..core.config import get_settings, Settings
from ..core.dependencies import Dependencies, get_dependencies, init_dependencies, close_dependencies
from ..services.fraud_detector import FraudDetectorService, create_fraud_detector
from ..models.schemas import TransactionRequest, PredictionResponse


logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan manager
    Handles startup and shutdown events properly
    """
    logger.info("Starting Fraud Detection API")
    
    # Initialize dependencies
    settings = get_settings()
    await init_dependencies(settings)
    
    # Warm up connections
    deps = get_dependencies()
    await deps.get_postgres_pool()
    await deps.get_redis_pool()
    await deps.get_ml_model()
    
    logger.info("All dependencies initialized")
    
    yield
    
    # Cleanup
    logger.info("Shutting down Fraud Detection API")
    await close_dependencies()
    logger.info("All dependencies closed")


app = FastAPI(
    title="Fraud Detection API",
    version="2.0.0",
    lifespan=lifespan
)

# Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Dependency injection functions
async def get_current_dependencies() -> Dependencies:
    """Get current dependencies instance"""
    return get_dependencies()


async def get_fraud_detector(
    deps: Dependencies = Depends(get_current_dependencies)
) -> FraudDetectorService:
    """Get fraud detector service with dependencies injected"""
    return await create_fraud_detector(deps)


async def get_settings_dep() -> Settings:
    """Get settings dependency"""
    return get_settings()


# API Endpoints
@app.get("/health")
async def health_check(
    deps: Dependencies = Depends(get_current_dependencies),
    settings: Settings = Depends(get_settings_dep)
) -> Dict:
    """
    Health check endpoint
    Tests all critical dependencies
    """
    health_status = {
        "status": "healthy",
        "version": "2.0.0",
        "environment": settings.environment,
        "checks": {}
    }
    
    # Check PostgreSQL
    try:
        async with deps.get_postgres_connection() as conn:
            await conn.fetchval("SELECT 1")
        health_status["checks"]["postgres"] = "healthy"
    except Exception as e:
        health_status["checks"]["postgres"] = f"unhealthy: {str(e)}"
        health_status["status"] = "degraded"
    
    # Check Redis
    try:
        redis = await deps.get_redis_pool()
        await redis.ping()
        health_status["checks"]["redis"] = "healthy"
    except Exception as e:
        health_status["checks"]["redis"] = f"unhealthy: {str(e)}"
        health_status["status"] = "degraded"
    
    # Check ML Model
    try:
        model = await deps.get_ml_model()
        health_status["checks"]["ml_model"] = "healthy" if model else "not loaded"
    except Exception as e:
        health_status["checks"]["ml_model"] = f"unhealthy: {str(e)}"
        health_status["status"] = "degraded"
    
    return health_status


@app.post("/predict", response_model=PredictionResponse)
async def predict_fraud(
    request: TransactionRequest,
    fraud_detector: FraudDetectorService = Depends(get_fraud_detector)
) -> PredictionResponse:
    """
    Predict fraud for a transaction
    
    This endpoint demonstrates proper DI:
    - Service is injected, not created
    - All dependencies are managed by the container
    - Easy to test with mock dependencies
    """
    try:
        # Convert request to dict
        transaction_data = request.dict()
        
        # Get prediction
        result = await fraud_detector.predict(transaction_data)
        
        # Return response
        return PredictionResponse(**result)
        
    except Exception as e:
        logger.error("prediction_error", error=str(e), transaction_id=request.transaction_id)
        raise HTTPException(status_code=500, detail="Prediction failed")


@app.post("/predict/batch")
async def predict_batch(
    transactions: list[TransactionRequest],
    fraud_detector: FraudDetectorService = Depends(get_fraud_detector)
) -> list[PredictionResponse]:
    """Batch prediction endpoint"""
    results = []
    
    for transaction in transactions:
        try:
            result = await fraud_detector.predict(transaction.dict())
            results.append(PredictionResponse(**result))
        except Exception as e:
            logger.error("batch_prediction_error", error=str(e))
            # Return partial results
            results.append(PredictionResponse(
                transaction_id=transaction.transaction_id,
                is_fraud=False,
                fraud_probability=0.0,
                risk_factors=[],
                error=str(e)
            ))
    
    return results


# Example of injecting dependencies in background tasks
from fastapi import BackgroundTasks

@app.post("/transactions/{transaction_id}/report")
async def report_transaction(
    transaction_id: str,
    is_fraud: bool,
    background_tasks: BackgroundTasks,
    deps: Dependencies = Depends(get_current_dependencies)
):
    """
    Report transaction as fraud/legitimate
    Demonstrates background task with DI
    """
    async def update_model_feedback(transaction_id: str, is_fraud: bool, deps: Dependencies):
        """Background task to update model with feedback"""
        async with deps.get_postgres_connection() as conn:
            await conn.execute(
                """
                INSERT INTO fraud_feedback (transaction_id, is_fraud, reported_at)
                VALUES ($1, $2, NOW())
                ON CONFLICT (transaction_id) DO UPDATE
                SET is_fraud = $2, reported_at = NOW()
                """,
                transaction_id,
                is_fraud
            )
        
        # Invalidate cache
        redis = await deps.get_redis_pool()
        await redis.delete(f"prediction:{transaction_id}")
        
        logger.info("feedback_recorded", transaction_id=transaction_id, is_fraud=is_fraud)
    
    # Schedule background task
    background_tasks.add_task(
        update_model_feedback,
        transaction_id,
        is_fraud,
        deps
    )
    
    return {"status": "feedback_recorded", "transaction_id": transaction_id}


if __name__ == "__main__":
    import uvicorn
    
    settings = get_settings()
    
    uvicorn.run(
        "api.main:app",
        host=settings.api.host,
        port=settings.api.port,
        workers=settings.api.workers,
        reload=settings.api.reload,
        log_level=settings.api.log_level.lower()
    )
