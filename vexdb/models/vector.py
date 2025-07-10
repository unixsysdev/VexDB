"""
Core data models for VexDB.
"""

from pydantic import BaseModel, Field, validator
from typing import List, Dict, Any, Optional, Union
from datetime import datetime
import uuid


class VectorData(BaseModel):
    """
    Represents a vector with its metadata.
    """
    id: str = Field(default_factory=lambda: str(uuid.uuid4()), description="Unique identifier")
    embedding: List[float] = Field(..., description="Vector embedding values")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Associated metadata")
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Creation timestamp")
    updated_at: Optional[datetime] = Field(None, description="Last update timestamp")
    
    @validator('embedding')
    def validate_embedding(cls, v):
        if not v:
            raise ValueError("Embedding cannot be empty")
        if not all(isinstance(x, (int, float)) for x in v):
            raise ValueError("Embedding must contain only numeric values")
        return v
    
    @validator('id')
    def validate_id(cls, v):
        if not v or not v.strip():
            raise ValueError("ID cannot be empty")
        return v.strip()
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class QueryRequest(BaseModel):
    """
    Request model for vector similarity search.
    """
    query_vector: List[float] = Field(..., description="Query vector for similarity search")
    k: int = Field(default=10, ge=1, le=1000, description="Number of results to return")
    similarity_threshold: Optional[float] = Field(
        None, ge=0.0, le=1.0, description="Minimum similarity threshold"
    )
    metadata_filters: Dict[str, Any] = Field(
        default_factory=dict, description="Metadata filters to apply"
    )
    algorithm_hint: Optional[str] = Field(
        None, description="Preferred algorithm for this query"
    )
    
    @validator('query_vector')
    def validate_query_vector(cls, v):
        if not v:
            raise ValueError("Query vector cannot be empty")
        if not all(isinstance(x, (int, float)) for x in v):
            raise ValueError("Query vector must contain only numeric values")
        return v


class SearchResult(BaseModel):
    """
    Individual search result with similarity score.
    """
    vector_id: str = Field(..., description="ID of the matching vector")
    similarity_score: float = Field(..., description="Similarity score (0-1)")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Vector metadata")
    distance: float = Field(..., description="Distance metric value")


class QueryResponse(BaseModel):
    """
    Response model for vector similarity search.
    """
    query_id: str = Field(default_factory=lambda: str(uuid.uuid4()), description="Query identifier")
    results: List[SearchResult] = Field(..., description="Search results")
    total_results: int = Field(..., description="Total number of results found")
    query_time_ms: float = Field(..., description="Query execution time in milliseconds")
    algorithm_used: str = Field(..., description="Algorithm used for the search")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Response timestamp")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class BatchInsertRequest(BaseModel):
    """
    Request model for batch vector insertion.
    """
    vectors: List[VectorData] = Field(..., min_items=1, max_items=1000, description="Vectors to insert")
    
    @validator('vectors')
    def validate_vectors(cls, v):
        if not v:
            raise ValueError("Vectors list cannot be empty")
        
        # Check for duplicate IDs
        ids = [vector.id for vector in v]
        if len(ids) != len(set(ids)):
            raise ValueError("Duplicate vector IDs found in batch")
        
        # Check for consistent dimensionality
        dimensions = [len(vector.embedding) for vector in v]
        if len(set(dimensions)) > 1:
            raise ValueError("All vectors in batch must have the same dimensionality")
        
        return v


class BatchInsertResponse(BaseModel):
    """
    Response model for batch vector insertion.
    """
    inserted_count: int = Field(..., description="Number of vectors successfully inserted")
    failed_count: int = Field(default=0, description="Number of vectors that failed to insert")
    failed_ids: List[str] = Field(default_factory=list, description="IDs of vectors that failed")
    processing_time_ms: float = Field(..., description="Processing time in milliseconds")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Response timestamp")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class VectorUpdateRequest(BaseModel):
    """
    Request model for updating an existing vector.
    """
    embedding: Optional[List[float]] = Field(None, description="New embedding values")
    metadata: Optional[Dict[str, Any]] = Field(None, description="New metadata")
    
    @validator('embedding')
    def validate_embedding(cls, v):
        if v is not None:
            if not v:
                raise ValueError("Embedding cannot be empty if provided")
            if not all(isinstance(x, (int, float)) for x in v):
                raise ValueError("Embedding must contain only numeric values")
        return v


class SystemStatus(BaseModel):
    """
    System status information.
    """
    status: str = Field(..., description="Overall system status")
    version: str = Field(..., description="VexDB version")
    uptime_seconds: int = Field(..., description="System uptime in seconds")
    total_vectors: int = Field(..., description="Total number of vectors stored")
    active_algorithms: List[str] = Field(..., description="Currently active algorithms")
    memory_usage_mb: float = Field(..., description="Current memory usage in MB")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Status timestamp")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }