"""
Data models for profiling and analytics.
"""

from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from datetime import datetime
from enum import Enum
import uuid


class DataCharacteristic(str, Enum):
    """
    Enumeration of data characteristics that can be analyzed.
    """
    DIMENSIONALITY = "dimensionality"
    SPARSITY = "sparsity"
    CLUSTERING = "clustering"
    DISTRIBUTION = "distribution"
    CORRELATION = "correlation"


class DataProfile(BaseModel):
    """
    Statistical profile of vector data.
    """
    profile_id: str = Field(default_factory=lambda: str(uuid.uuid4()), description="Profile identifier")
    sample_size: int = Field(..., description="Number of vectors analyzed")
    dimensionality: int = Field(..., description="Vector dimensionality")
    
    # Statistical properties
    mean_vector: List[float] = Field(..., description="Mean vector across all dimensions")
    std_vector: List[float] = Field(..., description="Standard deviation per dimension")
    min_values: List[float] = Field(..., description="Minimum values per dimension")
    max_values: List[float] = Field(..., description="Maximum values per dimension")
    
    # Sparsity analysis
    sparsity_ratio: float = Field(..., ge=0.0, le=1.0, description="Ratio of zero values")
    effective_dimensions: int = Field(..., description="Number of non-zero dimensions")
    
    # Clustering properties
    has_clusters: bool = Field(..., description="Whether distinct clusters are detected")
    estimated_clusters: Optional[int] = Field(None, description="Estimated number of clusters")
    silhouette_score: Optional[float] = Field(None, description="Silhouette analysis score")
    
    # Distribution properties
    is_normalized: bool = Field(..., description="Whether vectors are normalized")
    distribution_type: str = Field(..., description="Detected distribution type")
    
    # Correlation analysis
    correlation_matrix_summary: Dict[str, float] = Field(
        default_factory=dict, description="Summary of correlation matrix"
    )
    
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Profile creation time")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class QueryPattern(BaseModel):
    """
    Analysis of query patterns and behavior.
    """
    pattern_id: str = Field(default_factory=lambda: str(uuid.uuid4()), description="Pattern identifier")
    time_window_start: datetime = Field(..., description="Analysis window start time")
    time_window_end: datetime = Field(..., description="Analysis window end time")
    
    # Query frequency
    total_queries: int = Field(..., description="Total queries in time window")
    queries_per_second: float = Field(..., description="Average queries per second")
    peak_qps: float = Field(..., description="Peak queries per second")
    
    # Query characteristics
    avg_k_value: float = Field(..., description="Average k value requested")
    common_k_values: List[int] = Field(..., description="Most common k values")
    similarity_threshold_usage: float = Field(..., description="Percentage using similarity threshold")
    
    # Temporal patterns
    hourly_distribution: Dict[int, int] = Field(..., description="Query count by hour of day")
    daily_distribution: Dict[str, int] = Field(..., description="Query count by day of week")
    
    # Performance patterns
    avg_latency_ms: float = Field(..., description="Average query latency")
    p95_latency_ms: float = Field(..., description="95th percentile latency")
    p99_latency_ms: float = Field(..., description="99th percentile latency")
    
    # Algorithm usage
    algorithm_usage: Dict[str, int] = Field(..., description="Usage count per algorithm")
    
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Pattern analysis time")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class PerformanceMetrics(BaseModel):
    """
    Performance metrics for algorithm evaluation.
    """
    metric_id: str = Field(default_factory=lambda: str(uuid.uuid4()), description="Metric identifier")
    algorithm_name: str = Field(..., description="Algorithm being measured")
    
    # Query performance
    avg_query_time_ms: float = Field(..., description="Average query time")
    median_query_time_ms: float = Field(..., description="Median query time")
    p95_query_time_ms: float = Field(..., description="95th percentile query time")
    p99_query_time_ms: float = Field(..., description="99th percentile query time")
    
    # Accuracy metrics
    recall_at_k: Dict[int, float] = Field(..., description="Recall at different k values")
    precision_at_k: Dict[int, float] = Field(..., description="Precision at different k values")
    mean_average_precision: float = Field(..., description="Mean average precision")
    
    # Resource usage
    memory_usage_mb: float = Field(..., description="Memory usage in MB")
    cpu_usage_percent: float = Field(..., description="CPU usage percentage")
    index_build_time_ms: Optional[float] = Field(None, description="Index construction time")
    
    # Throughput metrics
    queries_per_second: float = Field(..., description="Sustained queries per second")
    max_concurrent_queries: int = Field(..., description="Maximum concurrent queries handled")
    
    # Dataset characteristics
    dataset_size: int = Field(..., description="Number of vectors in dataset")
    vector_dimensionality: int = Field(..., description="Vector dimensionality")
    
    measurement_timestamp: datetime = Field(default_factory=datetime.utcnow, description="Measurement time")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class AlgorithmRecommendation(BaseModel):
    """
    Algorithm recommendation based on data characteristics and requirements.
    """
    recommendation_id: str = Field(default_factory=lambda: str(uuid.uuid4()), description="Recommendation ID")
    
    # Recommended algorithm
    algorithm_name: str = Field(..., description="Recommended algorithm")
    confidence_score: float = Field(..., ge=0.0, le=1.0, description="Confidence in recommendation")
    
    # Configuration parameters
    recommended_parameters: Dict[str, Any] = Field(..., description="Recommended algorithm parameters")
    
    # Reasoning
    reasoning: List[str] = Field(..., description="Reasons for this recommendation")
    data_characteristics_considered: List[DataCharacteristic] = Field(
        ..., description="Data characteristics that influenced the decision"
    )
    
    # Performance predictions
    predicted_query_time_ms: Optional[float] = Field(None, description="Predicted average query time")
    predicted_accuracy: Optional[float] = Field(None, description="Predicted accuracy")
    predicted_memory_usage_mb: Optional[float] = Field(None, description="Predicted memory usage")
    
    # Alternative options
    alternative_algorithms: List[Dict[str, Any]] = Field(
        default_factory=list, description="Alternative algorithm options"
    )
    
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Recommendation time")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }