"""
Configuration management for VexDB.
"""

from pydantic_settings import BaseSettings
from pydantic import Field
from typing import List, Optional
from functools import lru_cache
import os


class Settings(BaseSettings):
    """
    Application settings with environment variable support.
    """
    
    # Application settings
    environment: str = Field(default="development", env="VEXDB_ENV")
    host: str = Field(default="0.0.0.0", env="VEXDB_HOST")
    port: int = Field(default=8000, env="VEXDB_PORT")
    log_level: str = Field(default="INFO", env="VEXDB_LOG_LEVEL")
    
    # Security settings
    secret_key: str = Field(default="dev-secret-key-change-in-production", env="VEXDB_SECRET_KEY")
    allowed_origins: List[str] = Field(
        default=["http://localhost:3000", "http://localhost:8080"],
        env="VEXDB_ALLOWED_ORIGINS"
    )
    
    # Database settings
    database_url: str = Field(
        default="postgresql+asyncpg://vexdb:vexdb@localhost:5432/vexdb",
        env="VEXDB_DATABASE_URL"
    )
    database_pool_size: int = Field(default=10, env="VEXDB_DB_POOL_SIZE")
    database_max_overflow: int = Field(default=20, env="VEXDB_DB_MAX_OVERFLOW")
    
    # Redis settings
    redis_url: str = Field(default="redis://localhost:6379/0", env="VEXDB_REDIS_URL")
    redis_max_connections: int = Field(default=10, env="VEXDB_REDIS_MAX_CONNECTIONS")
    
    # Vector processing settings
    default_vector_dimension: int = Field(default=768, env="VEXDB_DEFAULT_VECTOR_DIM")
    max_vectors_per_batch: int = Field(default=1000, env="VEXDB_MAX_VECTORS_PER_BATCH")
    
    # Algorithm settings
    hnsw_m: int = Field(default=16, env="VEXDB_HNSW_M")
    hnsw_ef_construction: int = Field(default=200, env="VEXDB_HNSW_EF_CONSTRUCTION")
    hnsw_ef_search: int = Field(default=50, env="VEXDB_HNSW_EF_SEARCH")
    
    ivf_nlist: int = Field(default=100, env="VEXDB_IVF_NLIST")
    ivf_nprobe: int = Field(default=10, env="VEXDB_IVF_NPROBE")
    
    # Optimization settings
    profiling_sample_size: int = Field(default=10000, env="VEXDB_PROFILING_SAMPLE_SIZE")
    optimization_interval_seconds: int = Field(default=3600, env="VEXDB_OPTIMIZATION_INTERVAL")
    min_queries_for_optimization: int = Field(default=1000, env="VEXDB_MIN_QUERIES_FOR_OPT")
    
    # Monitoring settings
    metrics_enabled: bool = Field(default=True, env="VEXDB_METRICS_ENABLED")
    metrics_port: int = Field(default=9090, env="VEXDB_METRICS_PORT")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


class AlgorithmConfig(BaseSettings):
    """
    Algorithm-specific configuration settings.
    """
    
    # Available algorithms
    available_algorithms: List[str] = Field(
        default=["brute_force", "hnsw", "ivf", "lsh"],
        env="VEXDB_AVAILABLE_ALGORITHMS"
    )
    
    # Default algorithm selection strategy
    default_algorithm: str = Field(default="brute_force", env="VEXDB_DEFAULT_ALGORITHM")
    
    # Resource constraints
    max_memory_mb: int = Field(default=4096, env="VEXDB_MAX_MEMORY_MB")
    max_cpu_cores: int = Field(default=4, env="VEXDB_MAX_CPU_CORES")
    
    # Performance thresholds
    max_query_latency_ms: int = Field(default=100, env="VEXDB_MAX_QUERY_LATENCY_MS")
    min_accuracy_threshold: float = Field(default=0.95, env="VEXDB_MIN_ACCURACY_THRESHOLD")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache()
def get_settings() -> Settings:
    """
    Get application settings singleton.
    
    Returns:
        Settings: Application settings instance
    """
    return Settings()


@lru_cache()
def get_algorithm_config() -> AlgorithmConfig:
    """
    Get algorithm configuration singleton.
    
    Returns:
        AlgorithmConfig: Algorithm configuration instance
    """
    return AlgorithmConfig()