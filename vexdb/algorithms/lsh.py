"""
Locality Sensitive Hashing (LSH) algorithm implementation.

LSH is an approximation algorithm that uses hash functions to map similar
vectors to similar hash codes. It's particularly effective for high-dimensional
sparse data and when approximate results are acceptable.
"""

import asyncio
import time
from typing import Dict, List, Optional, Any, Tuple, Set
import numpy as np
from dataclasses import dataclass
import logging
from collections import defaultdict
import hashlib
import pickle

from vexdb.algorithms.base import AlgorithmInterface, AlgorithmCapabilities
from vexdb.models.vector import VectorData, SearchResult

logger = logging.getLogger(__name__)


@dataclass
class LSHHashTable:
    """Represents a single LSH hash table."""
    table_id: int
    hash_table: Dict[str, List[str]]  # hash -> list of vector_ids
    hyperplanes: np.ndarray
    
    
class LSHAlgorithm(AlgorithmInterface):
    """
    Locality Sensitive Hashing (LSH) algorithm implementation.
    
    LSH uses random hyperplanes to create hash functions that map similar
    vectors to the same hash buckets with high probability.
    """
    
    def __init__(self, **parameters):
        """Initialize LSH algorithm with configuration parameters."""
        self.num_hashtables = parameters.get('num_hashtables', 10)
        self.hash_size = parameters.get('hash_size', 20)  # Number of bits per hash
        self.num_hyperplanes = parameters.get('num_hyperplanes', None)  # Will be set to hash_size
        self.metric = parameters.get('metric', 'cosine')
        self.seed = parameters.get('seed', 42)
        self.max_candidates = parameters.get('max_candidates', 1000)
        
        # Internal state
        self.dimension = None
        self.hash_tables: List[LSHHashTable] = []
        self.vector_data: Dict[str, Dict[str, Any]] = {}  # vector_id -> {vector, metadata}
        self.is_initialized = False
        self.total_vectors = 0
        
        # Random number generator
        self.rng = np.random.RandomState(self.seed)
        
        # Validate parameters
        self._validate_parameters()
        
        logger.info("LSH algorithm initialized",
                   num_hashtables=self.num_hashtables,
                   hash_size=self.hash_size,
                   metric=self.metric)
    
    def _validate_parameters(self):
        """Validate algorithm parameters."""
        if self.num_hashtables < 1:
            raise ValueError("num_hashtables must be positive")
        
        if self.hash_size < 1:
            raise ValueError("hash_size must be positive")
        
        if self.metric not in ['cosine', 'euclidean', 'hamming']:
            raise ValueError("metric must be 'cosine', 'euclidean', or 'hamming'")
    
    @property
    def capabilities(self) -> AlgorithmCapabilities:
        """Return algorithm capabilities."""
        return AlgorithmCapabilities(
            supports_incremental_updates=True,
            supports_deletions=True,
            supports_metadata=True,
            optimal_dataset_size_range=(1000, 1000000),
            optimal_dimensionality_range=(100, 10000),
            memory_efficiency_score=0.8,
            query_speed_score=0.9,
            accuracy_score=0.7,  # Approximate algorithm
            build_speed_score=0.9
        )
    
    async def add_vectors(self, vectors: List[VectorData]) -> bool:
        """Add vectors to the index."""
        if not vectors:
            return True
        
        start_time = time.perf_counter()
        
        try:
            # Initialize if this is the first batch
            if not self.is_initialized:
                first_vector = vectors[0]
                self.dimension = len(first_vector.embedding)
                self.num_hyperplanes = self.num_hyperplanes or self.hash_size
                await self._initialize_hash_tables()
            
            # Add vectors to hash tables and storage
            for vector in vectors:
                await self._add_single_vector(vector)
            
            self.total_vectors += len(vectors)
            
            add_time = (time.perf_counter() - start_time) * 1000
            logger.info("Vectors added to LSH index",
                       vector_count=len(vectors),
                       total_vectors=self.total_vectors,
                       add_time_ms=add_time)
            
            return True
            
        except Exception as e:
            logger.error("Failed to add vectors to LSH index", error=str(e))
            return False
    
    async def _initialize_hash_tables(self):
        """Initialize LSH hash tables with random hyperplanes."""
        logger.info("Initializing LSH hash tables")
        
        self.hash_tables = []
        
        for table_id in range(self.num_hashtables):
            # Generate random hyperplanes for this table
            if self.metric == 'cosine':
                # For cosine similarity, use random hyperplanes from unit sphere
                hyperplanes = self.rng.randn(self.num_hyperplanes, self.dimension)
                hyperplanes = hyperplanes / np.linalg.norm(hyperplanes, axis=1, keepdims=True)
            else:
                # For euclidean distance, use random hyperplanes
                hyperplanes = self.rng.randn(self.num_hyperplanes, self.dimension)
            
            hash_table = LSHHashTable(
                table_id=table_id,
                hash_table={},
                hyperplanes=hyperplanes
            )
            
            self.hash_tables.append(hash_table)
        
        self.is_initialized = True
        logger.info("LSH hash tables initialized", num_tables=len(self.hash_tables))
    
    async def _add_single_vector(self, vector: VectorData):
        """Add a single vector to all hash tables."""
        vector_np = np.array(vector.embedding, dtype=np.float32)
        
        # Normalize vector if using cosine metric
        if self.metric == 'cosine':
            vector_np = self._normalize_vector(vector_np)
        
        # Store vector data
        self.vector_data[vector.id] = {
            'vector': vector_np,
            'metadata': vector.metadata or {}
        }
        
        # Add to each hash table
        for hash_table in self.hash_tables:
            hash_code = self._compute_hash(vector_np, hash_table.hyperplanes)
            
            if hash_code not in hash_table.hash_table:
                hash_table.hash_table[hash_code] = []
            
            hash_table.hash_table[hash_code].append(vector.id)
    
    def _compute_hash(self, vector: np.ndarray, hyperplanes: np.ndarray) -> str:
        """Compute LSH hash for a vector using given hyperplanes."""
        # Compute dot products with hyperplanes
        projections = np.dot(hyperplanes, vector)
        
        # Convert to binary hash
        binary_hash = (projections > 0).astype(int)
        
        # Convert to string representation
        hash_str = ''.join(map(str, binary_hash))
        
        return hash_str
    
    def _normalize_vector(self, vector: np.ndarray) -> np.ndarray:
        """Normalize vector to unit length."""
        norm = np.linalg.norm(vector)
        if norm == 0:
            return vector
        return vector / norm
    
    async def search(
        self, 
        query_vector: List[float], 
        k: int = 10, 
        **search_params
    ) -> List[SearchResult]:
        """Search for similar vectors."""
        if not self.is_initialized or not self.hash_tables:
            return []
        
        start_time = time.perf_counter()
        
        # Convert query to numpy array
        query_np = np.array(query_vector, dtype=np.float32)
        
        # Normalize if using cosine metric
        if self.metric == 'cosine':
            query_np = self._normalize_vector(query_np)
        
        # Collect candidate vectors from all hash tables
        candidate_ids = set()
        
        for hash_table in self.hash_tables:
            hash_code = self._compute_hash(query_np, hash_table.hyperplanes)
            
            # Get candidates from exact hash match
            if hash_code in hash_table.hash_table:
                candidate_ids.update(hash_table.hash_table[hash_code])
            
            # Optionally, get candidates from nearby hashes (Hamming distance 1-2)
            nearby_hashes = self._get_nearby_hashes(hash_code, max_distance=1)
            for nearby_hash in nearby_hashes:
                if nearby_hash in hash_table.hash_table:
                    candidate_ids.update(hash_table.hash_table[nearby_hash])
        
        # Limit number of candidates for performance
        if len(candidate_ids) > self.max_candidates:
            candidate_ids = set(list(candidate_ids)[:self.max_candidates])
        
        # Calculate exact distances for candidates
        candidates_with_scores = []
        
        for vector_id in candidate_ids:
            if vector_id in self.vector_data:
                candidate_vector = self.vector_data[vector_id]['vector']
                distance = self._calculate_distance(query_np, candidate_vector)
                
                candidates_with_scores.append({
                    'vector_id': vector_id,
                    'distance': distance,
                    'metadata': self.vector_data[vector_id]['metadata']
                })
        
        # Sort by distance and take top k
        candidates_with_scores.sort(key=lambda x: x['distance'])
        top_candidates = candidates_with_scores[:k]
        
        # Convert to SearchResult objects
        results = []
        for candidate in top_candidates:
            score = 1.0 / (1.0 + candidate['distance'])  # Convert distance to similarity score
            result = SearchResult(
                id=candidate['vector_id'],
                score=score,
                metadata=candidate['metadata']
            )
            results.append(result)
        
        search_time = (time.perf_counter() - start_time) * 1000
        logger.debug("LSH search completed",
                    query_time_ms=search_time,
                    candidates_found=len(candidate_ids),
                    results_returned=len(results))
        
        return results
    
    def _get_nearby_hashes(self, hash_code: str, max_distance: int = 1) -> List[str]:
        """Get hash codes within Hamming distance of the given hash."""
        nearby_hashes = []
        
        if max_distance == 0:
            return [hash_code]
        
        # Generate all hashes with Hamming distance 1
        for i in range(len(hash_code)):
            # Flip bit at position i
            nearby_hash = hash_code[:i] + ('0' if hash_code[i] == '1' else '1') + hash_code[i+1:]
            nearby_hashes.append(nearby_hash)
        
        return nearby_hashes
    
    def _calculate_distance(self, query: np.ndarray, vector: np.ndarray) -> float:
        """Calculate distance between query and vector."""
        if self.metric == 'cosine':
            # Cosine distance = 1 - cosine_similarity
            similarity = np.dot(query, vector)
            return 1.0 - similarity
        elif self.metric == 'euclidean':
            return float(np.linalg.norm(query - vector))
        elif self.metric == 'hamming':
            # For binary vectors
            return float(np.sum(query != vector))
        else:
            raise ValueError(f"Unsupported metric: {self.metric}")
    
    async def update_vector(self, vector_id: str, new_vector: VectorData) -> bool:
        """Update an existing vector."""
        # Remove old vector
        removed = await self.remove_vector(vector_id)
        if not removed:
            return False
        
        # Add new vector
        return await self.add_vectors([new_vector])
    
    async def remove_vector(self, vector_id: str) -> bool:
        """Remove a vector from the index."""
        try:
            if vector_id not in self.vector_data:
                logger.warning("Vector not found for removal", vector_id=vector_id)
                return False
            
            # Remove from vector storage
            vector_data = self.vector_data.pop(vector_id)
            
            # Remove from all hash tables
            for hash_table in self.hash_tables:
                for hash_code, vector_list in hash_table.hash_table.items():
                    if vector_id in vector_list:
                        vector_list.remove(vector_id)
                        
                        # Clean up empty buckets
                        if not vector_list:
                            del hash_table.hash_table[hash_code]
                        break
            
            self.total_vectors -= 1
            
            logger.debug("Vector removed from LSH index", vector_id=vector_id)
            return True
            
        except Exception as e:
            logger.error("Failed to remove vector from LSH index",
                        vector_id=vector_id,
                        error=str(e))
            return False
    
    async def get_vector_count(self) -> int:
        """Get the total number of vectors in the index."""
        return self.total_vectors
    
    async def get_memory_usage(self) -> Dict[str, Any]:
        """Get memory usage statistics."""
        vector_memory = 0
        metadata_memory = 0
        
        for vector_data in self.vector_data.values():
            vector_memory += vector_data['vector'].nbytes
            metadata_memory += len(str(vector_data['metadata']).encode('utf-8'))
        
        hash_table_memory = 0
        total_buckets = 0
        
        for hash_table in self.hash_tables:
            total_buckets += len(hash_table.hash_table)
            hash_table_memory += hash_table.hyperplanes.nbytes
            
            # Estimate memory for hash table structure
            for hash_code, vector_list in hash_table.hash_table.items():
                hash_table_memory += len(hash_code.encode('utf-8'))
                hash_table_memory += len(vector_list) * 50  # Rough estimate per vector ID
        
        return {
            'total_memory_bytes': vector_memory + metadata_memory + hash_table_memory,
            'vectors_memory_bytes': vector_memory,
            'metadata_memory_bytes': metadata_memory,
            'hash_tables_memory_bytes': hash_table_memory,
            'total_buckets': total_buckets,
            'avg_bucket_size': self.total_vectors / max(total_buckets, 1)
        }
    
    async def get_statistics(self) -> Dict[str, Any]:
        """Get comprehensive algorithm statistics."""
        bucket_stats = []
        
        for i, hash_table in enumerate(self.hash_tables):
            bucket_sizes = [len(vector_list) for vector_list in hash_table.hash_table.values()]
            
            bucket_stats.append({
                'table_id': i,
                'num_buckets': len(hash_table.hash_table),
                'avg_bucket_size': np.mean(bucket_sizes) if bucket_sizes else 0,
                'max_bucket_size': max(bucket_sizes) if bucket_sizes else 0,
                'min_bucket_size': min(bucket_sizes) if bucket_sizes else 0
            })
        
        return {
            'algorithm_name': 'LSH',
            'total_vectors': self.total_vectors,
            'num_hashtables': self.num_hashtables,
            'hash_size': self.hash_size,
            'metric': self.metric,
            'dimension': self.dimension,
            'is_initialized': self.is_initialized,
            'bucket_statistics': bucket_stats,
            'memory_usage': await self.get_memory_usage()
        }
    
    def save_index(self, filepath: str) -> bool:
        """Save the index to a file."""
        try:
            index_data = {
                'num_hashtables': self.num_hashtables,
                'hash_size': self.hash_size,
                'num_hyperplanes': self.num_hyperplanes,
                'metric': self.metric,
                'seed': self.seed,
                'max_candidates': self.max_candidates,
                'dimension': self.dimension,
                'hash_tables': self.hash_tables,
                'vector_data': self.vector_data,
                'is_initialized': self.is_initialized,
                'total_vectors': self.total_vectors
            }
            
            with open(filepath, 'wb') as f:
                pickle.dump(index_data, f)
            
            logger.info("LSH index saved successfully", filepath=filepath)
            return True
            
        except Exception as e:
            logger.error("Failed to save LSH index", filepath=filepath, error=str(e))
            return False
    
    def load_index(self, filepath: str) -> bool:
        """Load the index from a file."""
        try:
            with open(filepath, 'rb') as f:
                index_data = pickle.load(f)
            
            # Restore all attributes
            self.num_hashtables = index_data['num_hashtables']
            self.hash_size = index_data['hash_size']
            self.num_hyperplanes = index_data['num_hyperplanes']
            self.metric = index_data['metric']
            self.seed = index_data['seed']
            self.max_candidates = index_data['max_candidates']
            self.dimension = index_data['dimension']
            self.hash_tables = index_data['hash_tables']
            self.vector_data = index_data['vector_data']
            self.is_initialized = index_data['is_initialized']
            self.total_vectors = index_data['total_vectors']
            
            # Reinitialize RNG
            self.rng = np.random.RandomState(self.seed)
            
            logger.info("LSH index loaded successfully",
                       filepath=filepath,
                       total_vectors=self.total_vectors,
                       num_tables=len(self.hash_tables))
            return True
            
        except Exception as e:
            logger.error("Failed to load LSH index", filepath=filepath, error=str(e))
            return False
    
    def get_hash_distribution(self) -> Dict[str, Any]:
        """Get statistics about hash distribution across tables."""
        distribution_stats = {}
        
        for i, hash_table in enumerate(self.hash_tables):
            bucket_sizes = [len(vector_list) for vector_list in hash_table.hash_table.values()]
            
            distribution_stats[f"table_{i}"] = {
                'num_buckets': len(hash_table.hash_table),
                'total_vectors': sum(bucket_sizes),
                'bucket_size_distribution': {
                    'mean': np.mean(bucket_sizes) if bucket_sizes else 0,
                    'std': np.std(bucket_sizes) if bucket_sizes else 0,
                    'min': min(bucket_sizes) if bucket_sizes else 0,
                    'max': max(bucket_sizes) if bucket_sizes else 0
                }
            }
        
        return distribution_stats
