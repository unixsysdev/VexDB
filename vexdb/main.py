"""
VexDB FastAPI Application Entry Point

This module contains the main FastAPI application for the VexDB system.
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import structlog

from .api.vectors import router as vectors_router
from .api.search import router as search_router
from .api.admin import router as admin_router
from .api.analytics import router as analytics_router
from .config.settings import get_settings

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan management for startup and shutdown tasks.
    """
    logger.info("Starting VexDB application")
    
    # Startup tasks
    settings = get_settings()
    logger.info("Application configuration loaded", env=settings.environment)
    
    # TODO: Initialize core components
    # - Data profiler
    # - Algorithm registry
    # - Optimization engine
    # - Query analytics
    # - Index manager
    
    yield
    
    # Shutdown tasks
    logger.info("Shutting down VexDB application")
    # TODO: Cleanup resources


def create_app() -> FastAPI:
    """
    Create and configure the FastAPI application.
    
    Returns:
        FastAPI: Configured application instance
    """
    settings = get_settings()
    
    app = FastAPI(
        title="VexDB - Adaptive Vector Database",
        description="An intelligent vector database that automatically optimizes indexing algorithms",
        version="0.1.0",
        lifespan=lifespan,
        docs_url="/docs" if settings.environment != "production" else None,
        redoc_url="/redoc" if settings.environment != "production" else None,
    )
    
    # Add CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.allowed_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    # Include API routers
    app.include_router(vectors_router, prefix="/api/v1", tags=["vectors"])
    app.include_router(search_router, prefix="/api/v1", tags=["search"])
    app.include_router(admin_router, prefix="/api/v1/admin", tags=["admin"])
    app.include_router(analytics_router, prefix="/api/v1/analytics", tags=["analytics"])
    
    @app.get("/")
    async def root():
        """Root endpoint with basic system information."""
        return {
            "name": "VexDB",
            "version": "0.1.0",
            "description": "Adaptive Vector Database System",
            "status": "operational"
        }
    
    @app.get("/health")
    async def health_check():
        """Health check endpoint for monitoring."""
        return {
            "status": "healthy",
            "timestamp": structlog.processors.TimeStamper()._make_stamper(fmt="iso")(),
            "environment": settings.environment
        }
    
    return app


# Create the application instance
app = create_app()


if __name__ == "__main__":
    import uvicorn
    settings = get_settings()
    
    uvicorn.run(
        "vexdb.main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.environment == "development",
        log_level=settings.log_level.lower(),
    )