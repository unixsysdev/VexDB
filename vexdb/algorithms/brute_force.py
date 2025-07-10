"""
Brute force algorithm implementation for VexDB.

This provides exact similarity search with linear time complexity.
Used as a baseline and for small datasets where exact results are required.
"""

import time
from typing import List, Dict, Any
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity, euclidean_distances
import structlog

from .base import AlgorithmInterface, AlgorithmCapabilities
from ..models.vector import VectorData, QueryRequest, SearchResult
from ..models.profiling import PerformanceMetrics

logger = structlog.get_logger()


class BruteForceAlgorithm(AlgorithmInterface):
    """
    Brute force exact similarity search algorithm.
    
    This algorithm provides exact results by computing similarities
    to all vectors in the dataset. It's simple, accurate, but has
    O(n) query time complexity.
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize brute force algorithm.
        
        Args:
            config: Algorithm configuration
        """
        config = config or {}
        super().__init__("brute_force", config)
        
        # Configuration
        self.distance_metric = config.get("distance_metric", "cosine")
        self.normalize_vectors = config.get("normalize_vectors", True)
        
        # Internal storage
        self.vectors: List[VectorData] = []
        self.embeddings_matrix: np.ndarray = None
        self.vector_ids: List[str] = []
        
        # Performance tracking
        self.query_times: List[float] = []
        self.total_queries = 0
        
        logger.info("Brute force algorithm initialized", 
                   distance_metric=self.distance_metric,
                   normalize_vectors=self.normalize_vectors)
    
    async def build_index(self, vectors: List[VectorData]) -> None:
        """
        Build the brute force index (essentially store all vectors).
        
        Args:
            vectors: List of vectors to index
        """
        if not vectors:
            raise ValueError("Cannot build index with empty vector list")
        
        start_time = time.perf_counter()
        
        # Validate vectors
        for vector in vectors:
            self.validate_vector(vector)
        
        # Store vectors and create embeddings matrix
        self.vectors = vectors.copy()
        self.vector_ids = [v.id for v in vectors]
        
        # Create numpy matrix for efficient computation
        embeddings = [v.embedding for v in vectors]
        self.embeddings_matrix = np.array(embeddings, dtype=np.float32)
        
        # Normalize if requested
        if self.normalize_vectors:
            norms = np.linalg.norm(self.embeddings_matrix, axis=1, keepdims=True)
            norms[norms == 0] = 1  # Avoid division by zero
            self.embeddings_matrix = self.embeddings_matrix / norms
        
        build_time_ms = (time.perf_counter() - start_time) * 1000
        self._update_build_stats(vectors, build_time_ms)
        
        logger.info("Brute force index built successfully",
                   vector_count=len(vectors),
                   dimensionality=self.dimensionality,
                   build_time_ms=build_time_ms)
    
    async def insert_vector(self, vector: VectorData) -> None:
        """
        Insert a single vector into the index.
        
        Args:
            vector: Vector to insert
        """
        self.validate_vector(vector)
        
        # Add to vectors list
        self.vectors.append(vector)
        self.vector_ids.append(vector.id)
        
        # Update embeddings matrix
        embedding = np.array(vector.embedding, dtype=np.float32).reshape(1, -1)
        
        if self.normalize_vectors:
            norm = np.linalg.norm(embedding)
            if norm > 0:
                embedding = embedding / norm
        
        if self.embeddings_matrix is None:
            self.embeddings_matrix = embedding
        else:
            self.embeddings_matrix = np.vstack([self.embeddings_matrix, embedding])
        
        self.vector_count += 1
        
        logger.debug("Vector inserted", vector_id=vector.id, total_vectors=self.vector_count)
    
    async def insert_batch(self, vectors: List[VectorData]) -> None:
        """
        Insert multiple vectors efficiently.
        
        Args:
            vectors: List of vectors to insert
        """
        if not vectors:
            return
        
        # Validate all vectors first
        for vector in vectors:
            self.validate_vector(vector)
        
        # Add to vectors list
        self.vectors.extend(vectors)
        self.vector_ids.extend([v.id for v in vectors])
        
        # Create embeddings matrix for new vectors
        new_embeddings = np.array([v.embedding for v in vectors], dtype=np.float32)
        
        if self.normalize_vectors:
            norms = np.linalg.norm(new_embeddings, axis=1, keepdims=True)
            norms[norms == 0] = 1
            new_embeddings = new_embeddings / norms
        
        # Update main embeddings matrix
        if self.embeddings_matrix is None:
            self.embeddings_matrix = new_embeddings
        else:
            self.embeddings_matrix = np.vstack([self.embeddings_matrix, new_embeddings])
        
        self.vector_count += len(vectors)
        
        logger.info("Batch insertion completed", 
                   inserted_count=len(vectors), 
                   total_vectors=self.vector_count)
    
    async def search(self, query: QueryRequest) -> List[SearchResult]:
        """
        Perform brute force similarity search.
        
        Args:
            query: Search query parameters
            
        Returns:
            List[SearchResult]: Search results sorted by similarity
        """
        start_time = time.perf_counter()
        
        self.validate_query(query)
        
        if not self.is_built or self.embeddings_matrix is None:
            return []
        
        # Prepare query vector
        query_vector = np.array(query.query_vector, dtype=np.float32).reshape(1, -1)
        
        if self.normalize_vectors:
            norm = np.linalg.norm(query_vector)
            if norm > 0:
                query_vector = query_vector / norm
        
        # Compute similarities
        if self.distance_metric == "cosine":
            similarities = cosine_similarity(query_vector, self.embeddings_matrix)[0]
        elif self.distance_metric == "euclidean":
            distances = euclidean_distances(query_vector, self.embeddings_matrix)[0]
            # Convert distances to similarities (higher is better)
            max_distance = np.max(distances) if len(distances) > 0 else 1.0
            similarities = 1.0 - (distances / max_distance) if max_distance > 0 else np.ones_like(distances)
        else:
            raise ValueError(f"Unsupported distance metric: {self.distance_metric}")
        
        # Get top-k results
        top_indices = np.argsort(similarities)[::-1][:query.k]
        
        # Apply similarity threshold if specified
        if query.similarity_threshold is not None:
            top_indices = top_indices[similarities[top_indices] >= query.similarity_threshold]
        
        # Create search results
        results = []
        for idx in top_indices:
            similarity_score = float(similarities[idx])
            distance = 1.0 - similarity_score if self.distance_metric == "cosine" else float(euclidean_distances(query_vector, self.embeddings_matrix[idx:idx+1])[0][0])
            
            result = SearchResult(
                vector_id=self.vector_ids[idx],
                similarity_score=similarity_score,
                metadata=self.vectors[idx].metadata,
                distance=distance
            )
            results.append(result)
        
        # Track performance
        query_time = (time.perf_counter() - start_time) * 1000
        self.query_times.append(query_time)
        self.total_queries += 1
        
        logger.debug("Search completed",
                    results_count=len(results),
                    query_time_ms=query_time,
                    k=query.k)
        
        return results
    
    async def update_vector(self, vector_id: str, new_vector: VectorData) -> None:
        """
        Update an existing vector.
        
        Args:
            vector_id: ID of vector to update
            new_vector: New vector data
        """
        self.validate_vector(new_vector)
        
        try:
            idx = self.vector_ids.index(vector_id)
        except ValueError:
            raise ValueError(f"Vector {vector_id} not found in index")
        
        # Update vector data
        self.vectors[idx] = new_vector
        
        # Update embeddings matrix
        embedding = np.array(new_vector.embedding, dtype=np.float32)
        
        if self.normalize_vectors:
            norm = np.linalg.norm(embedding)
            if norm > 0:
                embedding = embedding / norm
        
        self.embeddings_matrix[idx] = embedding
        
        logger.debug("Vector updated", vector_id=vector_id)
    
    async def delete_vector(self, vector_id: str) -> None:
        """
        Delete a vector from the index.
        
        Args:
            vector_id: ID of vector to delete
        """
        try:
            idx = self.vector_ids.index(vector_id)
        except ValueError:
            raise ValueError(f"Vector {vector_id} not found in index")
        
        # Remove from all data structures
        del self.vectors[idx]
        del self.vector_ids[idx]
        
        if self.embeddings_matrix is not None:
            self.embeddings_matrix = np.delete(self.embeddings_matrix, idx, axis=0)
        
        self.vector_count -= 1
        
        logger.debug("Vector deleted", vector_id=vector_id, remaining_vectors=self.vector_count)
    
    async def get_performance_metrics(self) -> PerformanceMetrics:
        """
        Get performance metrics for this algorithm instance.
        
        Returns:
            PerformanceMetrics: Current performance statistics
        """
        if not self.query_times:
            avg_query_time = 0.0
            median_query_time = 0.0
            p95_query_time = 0.0
            p99_query_time = 0.0
        else:
            avg_query_time = float(np.mean(self.query_times))
            median_query_time = float(np.median(self.query_times))
            p95_query_time = float(np.percentile(self.query_times, 95))
            p99_query_time = float(np.percentile(self.query_times, 99))
        
        metrics = PerformanceMetrics(
            algorithm_name=self.name,
            avg_query_time_ms=avg_query_time,
            median_query_time_ms=median_query_time,
            p95_query_time_ms=p95_query_time,
            p99_query_time_ms=p99_query_time,
            recall_at_k={10: 1.0, 50: 1.0, 100: 1.0},  # Always exact for brute force
            precision_at_k={10: 1.0, 50: 1.0, 100: 1.0},
            mean_average_precision=1.0,
            memory_usage_mb=self.get_memory_usage(),
            cpu_usage_percent=0.0,  # TODO: Implement CPU monitoring
            index_build_time_ms=self.build_time_ms,
            queries_per_second=0.0,  # TODO: Calculate QPS
            max_concurrent_queries=1,  # Single-threaded for now
            dataset_size=self.vector_count,
            vector_dimensionality=self.dimensionality
        )
        
        return metrics
    
    def get_memory_usage(self) -> float:
        """
        Estimate memory usage in MB.
        
        Returns:
            float: Memory usage in megabytes
        """
        if self.embeddings_matrix is None:
            return 0.0
        
        # Estimate memory usage
        matrix_bytes = self.embeddings_matrix.nbytes
        vectors_bytes = sum(len(str(v)) for v in self.vectors) * 4  # Rough estimate
        ids_bytes = sum(len(id_) for id_ in self.vector_ids) * 4
        
        total_bytes = matrix_bytes + vectors_bytes + ids_bytes
        return total_bytes / (1024 * 1024)  # Convert to MB
    
    def supports_updates(self) -> bool:
        """Check if algorithm supports updates."""
        return True
    
    def supports_deletes(self) -> bool:
        """Check if algorithm supports deletions."""
        return True
    
    def get_algorithm_info(self) -> Dict[str, Any]:
        """Get algorithm information."""
        capabilities = AlgorithmCapabilities(
            exact_search=True,
            approximate_search=False,
            supports_updates=True,
            supports_deletes=True,
            supports_batch_insert=True,
            memory_efficient=False,
            build_time_complexity="O(n)",
            query_time_complexity="O(n)",
            space_complexity="O(n*d)",
            best_use_cases=["small_datasets", "exact_results_required", "baseline_comparison"],
            limitations=["poor_scalability", "high_query_latency_for_large_datasets"]
        )
        
        return {
            "name": self.name,
            "description": "Exact similarity search with linear query time",
            "version": "1.0.0",
            "capabilities": capabilities.to_dict(),
            "configuration": self.config,
            "status": {
                "is_built": self.is_built,
                "vector_count": self.vector_count,
                "dimensionality": self.dimensionality,
                "memory_usage_mb": self.get_memory_usage()
            }
        }