"""
Base algorithm interface for VexDB.

All vector indexing algorithms must implement this interface to ensure
consistent behavior and compatibility with the adaptive system.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import numpy as np

from ..models.vector import VectorData, QueryRequest, SearchResult
from ..models.profiling import PerformanceMetrics


class AlgorithmInterface(ABC):
    """
    Abstract base class for all vector indexing algorithms.
    
    This interface defines the contract that all algorithms must follow
    to be compatible with the VexDB adaptive system.
    """
    
    def __init__(self, name: str, config: Dict[str, Any]):
        """
        Initialize the algorithm with configuration.
        
        Args:
            name: Algorithm name/identifier
            config: Algorithm-specific configuration parameters
        """
        self.name = name
        self.config = config
        self.is_built = False
        self.vector_count = 0
        self.dimensionality = 0
        self.build_time_ms = 0.0
        self.last_build_timestamp: Optional[datetime] = None
    
    @abstractmethod
    async def build_index(self, vectors: List[VectorData]) -> None:
        """
        Build the index from a collection of vectors.
        
        Args:
            vectors: List of vectors to index
            
        Raises:
            ValueError: If vectors are invalid or incompatible
            RuntimeError: If index building fails
        """
        pass
    
    @abstractmethod
    async def insert_vector(self, vector: VectorData) -> None:
        """
        Insert a single vector into the existing index.
        
        Args:
            vector: Vector to insert
            
        Raises:
            ValueError: If vector is invalid or incompatible
            RuntimeError: If insertion fails
        """
        pass
    
    @abstractmethod
    async def insert_batch(self, vectors: List[VectorData]) -> None:
        """
        Insert multiple vectors into the index efficiently.
        
        Args:
            vectors: List of vectors to insert
            
        Raises:
            ValueError: If vectors are invalid or incompatible
            RuntimeError: If batch insertion fails
        """
        pass
    
    @abstractmethod
    async def search(self, query: QueryRequest) -> List[SearchResult]:
        """
        Perform similarity search.
        
        Args:
            query: Search query with parameters
            
        Returns:
            List[SearchResult]: Search results sorted by similarity
            
        Raises:
            ValueError: If query is invalid
            RuntimeError: If search fails
        """
        pass
    
    @abstractmethod
    async def update_vector(self, vector_id: str, new_vector: VectorData) -> None:
        """
        Update an existing vector in the index.
        
        Args:
            vector_id: ID of vector to update
            new_vector: New vector data
            
        Raises:
            ValueError: If vector is invalid or not found
            RuntimeError: If update fails
        """
        pass
    
    @abstractmethod
    async def delete_vector(self, vector_id: str) -> None:
        """
        Delete a vector from the index.
        
        Args:
            vector_id: ID of vector to delete
            
        Raises:
            ValueError: If vector ID not found
            RuntimeError: If deletion fails
        """
        pass
    
    @abstractmethod
    async def get_performance_metrics(self) -> PerformanceMetrics:
        """
        Get current performance metrics for this algorithm.
        
        Returns:
            PerformanceMetrics: Current performance statistics
        """
        pass
    
    @abstractmethod
    def get_memory_usage(self) -> float:
        """
        Get current memory usage in MB.
        
        Returns:
            float: Memory usage in megabytes
        """
        pass
    
    @abstractmethod
    def supports_updates(self) -> bool:
        """
        Check if algorithm supports online updates.
        
        Returns:
            bool: True if updates are supported
        """
        pass
    
    @abstractmethod
    def supports_deletes(self) -> bool:
        """
        Check if algorithm supports deletions.
        
        Returns:
            bool: True if deletions are supported
        """
        pass
    
    @abstractmethod
    def get_algorithm_info(self) -> Dict[str, Any]:
        """
        Get algorithm information and capabilities.
        
        Returns:
            Dict: Algorithm metadata and capabilities
        """
        pass
    
    def validate_vector(self, vector: VectorData) -> None:
        """
        Validate a vector for compatibility with this algorithm.
        
        Args:
            vector: Vector to validate
            
        Raises:
            ValueError: If vector is incompatible
        """
        if not vector.embedding:
            raise ValueError("Vector embedding cannot be empty")
        
        if self.is_built and len(vector.embedding) != self.dimensionality:
            raise ValueError(
                f"Vector dimensionality {len(vector.embedding)} "
                f"does not match index dimensionality {self.dimensionality}"
            )
        
        if not all(isinstance(x, (int, float)) for x in vector.embedding):
            raise ValueError("Vector embedding must contain only numeric values")
    
    def validate_query(self, query: QueryRequest) -> None:
        """
        Validate a query for compatibility with this algorithm.
        
        Args:
            query: Query to validate
            
        Raises:
            ValueError: If query is incompatible
        """
        if not query.query_vector:
            raise ValueError("Query vector cannot be empty")
        
        if self.is_built and len(query.query_vector) != self.dimensionality:
            raise ValueError(
                f"Query vector dimensionality {len(query.query_vector)} "
                f"does not match index dimensionality {self.dimensionality}"
            )
        
        if not all(isinstance(x, (int, float)) for x in query.query_vector):
            raise ValueError("Query vector must contain only numeric values")
        
        if query.k <= 0:
            raise ValueError("k must be positive")
    
    def _update_build_stats(self, vectors: List[VectorData], build_time_ms: float) -> None:
        """
        Update build statistics after index construction.
        
        Args:
            vectors: Vectors that were indexed
            build_time_ms: Time taken to build index
        """
        self.is_built = True
        self.vector_count = len(vectors)
        self.dimensionality = len(vectors[0].embedding) if vectors else 0
        self.build_time_ms = build_time_ms
        self.last_build_timestamp = datetime.utcnow()


class AlgorithmCapabilities:
    """
    Defines algorithm capabilities and characteristics.
    """
    
    def __init__(
        self,
        exact_search: bool = False,
        approximate_search: bool = True,
        supports_updates: bool = False,
        supports_deletes: bool = False,
        supports_batch_insert: bool = True,
        memory_efficient: bool = False,
        build_time_complexity: str = "O(n log n)",
        query_time_complexity: str = "O(log n)",
        space_complexity: str = "O(n)",
        best_use_cases: List[str] = None,
        limitations: List[str] = None
    ):
        """
        Initialize algorithm capabilities.
        
        Args:
            exact_search: Whether algorithm provides exact results
            approximate_search: Whether algorithm provides approximate results
            supports_updates: Whether online updates are supported
            supports_deletes: Whether deletions are supported
            supports_batch_insert: Whether batch insertion is supported
            memory_efficient: Whether algorithm is memory efficient
            build_time_complexity: Time complexity for index building
            query_time_complexity: Time complexity for queries
            space_complexity: Space complexity
            best_use_cases: List of best use cases
            limitations: List of algorithm limitations
        """
        self.exact_search = exact_search
        self.approximate_search = approximate_search
        self.supports_updates = supports_updates
        self.supports_deletes = supports_deletes
        self.supports_batch_insert = supports_batch_insert
        self.memory_efficient = memory_efficient
        self.build_time_complexity = build_time_complexity
        self.query_time_complexity = query_time_complexity
        self.space_complexity = space_complexity
        self.best_use_cases = best_use_cases or []
        self.limitations = limitations or []
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert capabilities to dictionary.
        
        Returns:
            Dict: Capabilities as dictionary
        """
        return {
            "exact_search": self.exact_search,
            "approximate_search": self.approximate_search,
            "supports_updates": self.supports_updates,
            "supports_deletes": self.supports_deletes,
            "supports_batch_insert": self.supports_batch_insert,
            "memory_efficient": self.memory_efficient,
            "build_time_complexity": self.build_time_complexity,
            "query_time_complexity": self.query_time_complexity,
            "space_complexity": self.space_complexity,
            "best_use_cases": self.best_use_cases,
            "limitations": self.limitations
        }