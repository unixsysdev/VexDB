"""
Inverted File (IVF) algorithm implementation.

IVF is a clustering-based algorithm that partitions the vector space using
k-means clustering and maintains inverted lists for each cluster. This
approach is particularly effective for large datasets with natural clustering.
"""

import asyncio
import time
from typing import Dict, List, Optional, Any, Tuple
import numpy as np
from dataclasses import dataclass
import logging
from concurrent.futures import ThreadPoolExecutor
import pickle

# Clustering imports
try:
    from sklearn.cluster import KMeans, MiniBatchKMeans
    from sklearn.preprocessing import StandardScaler
    from sklearn.metrics import pairwise_distances
except ImportError:
    KMeans = None
    MiniBatchKMeans = None
    StandardScaler = None
    pairwise_distances = None

from vexdb.algorithms.base import AlgorithmInterface, AlgorithmCapabilities
from vexdb.models.vector import VectorData, SearchResult

logger = logging.getLogger(__name__)


@dataclass
class IVFCluster:
    """Represents a single cluster in the IVF index."""
    cluster_id: int
    centroid: np.ndarray
    vector_ids: List[str]
    vectors: np.ndarray
    metadata: List[Dict[str, Any]]


class IVFAlgorithm(AlgorithmInterface):
    """
    Inverted File (IVF) algorithm implementation.
    
    IVF partitions the vector space using k-means clustering and maintains
    inverted lists for each cluster. Search is performed by selecting the
    most relevant clusters and searching within them.
    """
    
    def __init__(self, **parameters):
        """Initialize IVF algorithm with configuration parameters."""
        self.nlist = parameters.get('nlist', 100)  # Number of clusters
        self.nprobe = parameters.get('nprobe', 10)  # Number of clusters to search
        self.metric = parameters.get('metric', 'L2')  # Distance metric
        self.quantizer_type = parameters.get('quantizer_type', 'flat')
        self.max_vectors_per_cluster = parameters.get('max_vectors_per_cluster', 10000)
        self.use_minibatch_kmeans = parameters.get('use_minibatch_kmeans', True)
        
        # Internal state
        self.is_trained = False
        self.clusters: List[IVFCluster] = []
        self.centroids: Optional[np.ndarray] = None
        self.kmeans_model = None
        self.scaler = None
        self.dimension = None
        self.total_vectors = 0
        
        # Thread pool for parallel operations
        self.executor = ThreadPoolExecutor(max_workers=4)
        
        # Validate parameters
        self._validate_parameters()
        
        logger.info("IVF algorithm initialized", 
                   nlist=self.nlist, 
                   nprobe=self.nprobe, 
                   metric=self.metric)
    
    def _validate_parameters(self):
        """Validate algorithm parameters."""
        if self.nlist < 1:
            raise ValueError("nlist must be positive")
        
        if self.nprobe < 1 or self.nprobe > self.nlist:
            raise ValueError("nprobe must be between 1 and nlist")
        
        if self.metric not in ['L2', 'cosine', 'euclidean']:
            raise ValueError("metric must be 'L2', 'cosine', or 'euclidean'")
        
        if KMeans is None:
            raise RuntimeError("scikit-learn is required for IVF algorithm")
    
    @property
    def capabilities(self) -> AlgorithmCapabilities:
        """Return algorithm capabilities."""
        return AlgorithmCapabilities(
            supports_incremental_updates=True,
            supports_deletions=True,
            supports_metadata=True,
            optimal_dataset_size_range=(10000, 10000000),
            optimal_dimensionality_range=(50, 2000),
            memory_efficiency_score=0.7,
            query_speed_score=0.8,
            accuracy_score=0.85,
            build_speed_score=0.6
        )
    
    async def add_vectors(self, vectors: List[VectorData]) -> bool:
        """Add vectors to the index."""
        if not vectors:
            return True
        
        start_time = time.perf_counter()
        
        try:
            # Extract embeddings and metadata
            embeddings = np.array([v.embedding for v in vectors], dtype=np.float32)
            vector_ids = [v.id for v in vectors]
            metadata_list = [v.metadata or {} for v in vectors]
            
            # Initialize if this is the first batch
            if not self.is_trained:
                await self._initialize_index(embeddings)
            
            # Assign vectors to clusters
            await self._assign_vectors_to_clusters(embeddings, vector_ids, metadata_list)
            
            self.total_vectors += len(vectors)
            
            add_time = (time.perf_counter() - start_time) * 1000
            logger.info("Vectors added to IVF index", 
                       vector_count=len(vectors),
                       total_vectors=self.total_vectors,
                       add_time_ms=add_time)
            
            return True
            
        except Exception as e:
            logger.error("Failed to add vectors to IVF index", error=str(e))
            return False
    
    async def _initialize_index(self, initial_embeddings: np.ndarray):
        """Initialize the index with k-means clustering."""
        logger.info("Initializing IVF index with k-means clustering")
        
        if len(initial_embeddings) < self.nlist:
            # Adjust nlist if we don't have enough vectors
            self.nlist = max(1, len(initial_embeddings) // 2)
            logger.warning("Adjusted nlist due to insufficient vectors", new_nlist=self.nlist)
        
        # Set dimension
        self.dimension = initial_embeddings.shape[1]
        
        # Normalize embeddings if using cosine metric
        if self.metric == 'cosine':
            initial_embeddings = self._normalize_vectors(initial_embeddings)
        
        # Scale features for better clustering
        self.scaler = StandardScaler()
        scaled_embeddings = self.scaler.fit_transform(initial_embeddings)
        
        # Train k-means
        await self._train_kmeans(scaled_embeddings)
        
        # Initialize empty clusters
        self.clusters = []
        for i in range(self.nlist):
            cluster = IVFCluster(
                cluster_id=i,
                centroid=self.centroids[i],
                vector_ids=[],
                vectors=np.empty((0, self.dimension), dtype=np.float32),
                metadata=[]
            )
            self.clusters.append(cluster)
        
        self.is_trained = True
        logger.info("IVF index initialized successfully", nlist=self.nlist)
    
    async def _train_kmeans(self, embeddings: np.ndarray):
        """Train k-means clustering model."""
        def train_kmeans_sync():
            if self.use_minibatch_kmeans and len(embeddings) > 10000:
                # Use MiniBatchKMeans for large datasets
                self.kmeans_model = MiniBatchKMeans(
                    n_clusters=self.nlist,
                    random_state=42,
                    batch_size=min(1000, len(embeddings) // 10),
                    n_init=3
                )
            else:
                # Use regular KMeans for smaller datasets
                self.kmeans_model = KMeans(
                    n_clusters=self.nlist,
                    random_state=42,
                    n_init=10
                )
            
            self.kmeans_model.fit(embeddings)
            return self.kmeans_model.cluster_centers_
        
        # Run k-means in thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        self.centroids = await loop.run_in_executor(self.executor, train_kmeans_sync)
        
        logger.info("K-means training completed", clusters=len(self.centroids))
    
    async def _assign_vectors_to_clusters(
        self, 
        embeddings: np.ndarray, 
        vector_ids: List[str], 
        metadata_list: List[Dict[str, Any]]
    ):
        """Assign vectors to their nearest clusters."""
        if not self.is_trained:
            raise RuntimeError("Index not initialized")
        
        # Normalize if using cosine metric
        if self.metric == 'cosine':
            embeddings = self._normalize_vectors(embeddings)
        
        # Scale embeddings
        scaled_embeddings = self.scaler.transform(embeddings)
        
        # Predict cluster assignments
        cluster_assignments = self.kmeans_model.predict(scaled_embeddings)
        
        # Group vectors by cluster
        cluster_vectors = {}
        for i, cluster_id in enumerate(cluster_assignments):
            if cluster_id not in cluster_vectors:
                cluster_vectors[cluster_id] = {
                    'embeddings': [],
                    'ids': [],
                    'metadata': []
                }
            
            cluster_vectors[cluster_id]['embeddings'].append(embeddings[i])
            cluster_vectors[cluster_id]['ids'].append(vector_ids[i])
            cluster_vectors[cluster_id]['metadata'].append(metadata_list[i])
        
        # Add vectors to clusters
        for cluster_id, vectors_data in cluster_vectors.items():
            cluster = self.clusters[cluster_id]
            
            # Convert to numpy arrays
            new_embeddings = np.array(vectors_data['embeddings'], dtype=np.float32)
            
            # Append to cluster
            if len(cluster.vectors) == 0:
                cluster.vectors = new_embeddings
            else:
                cluster.vectors = np.vstack([cluster.vectors, new_embeddings])
            
            cluster.vector_ids.extend(vectors_data['ids'])
            cluster.metadata.extend(vectors_data['metadata'])
            
            logger.debug("Added vectors to cluster", 
                        cluster_id=cluster_id, 
                        new_vectors=len(vectors_data['ids']),
                        total_in_cluster=len(cluster.vector_ids))
    
    async def search(
        self, 
        query_vector: List[float], 
        k: int = 10, 
        **search_params
    ) -> List[SearchResult]:
        """Search for similar vectors."""
        if not self.is_trained or not self.clusters:
            return []
        
        start_time = time.perf_counter()
        
        # Convert query to numpy array
        query_np = np.array([query_vector], dtype=np.float32)
        
        # Normalize if using cosine metric
        if self.metric == 'cosine':
            query_np = self._normalize_vectors(query_np)
        
        # Scale query
        query_scaled = self.scaler.transform(query_np)
        
        # Find nearest clusters
        cluster_distances = self._calculate_cluster_distances(query_scaled[0])
        nearest_clusters = np.argsort(cluster_distances)[:self.nprobe]
        
        # Search within selected clusters
        all_candidates = []
        
        for cluster_id in nearest_clusters:
            cluster = self.clusters[cluster_id]
            if len(cluster.vectors) == 0:
                continue
            
            # Calculate distances within cluster
            distances = self._calculate_distances(query_np[0], cluster.vectors)
            
            # Add candidates with cluster info
            for i, distance in enumerate(distances):
                all_candidates.append({
                    'vector_id': cluster.vector_ids[i],
                    'distance': distance,
                    'metadata': cluster.metadata[i],
                    'cluster_id': cluster_id
                })
        
        # Sort by distance and take top k
        all_candidates.sort(key=lambda x: x['distance'])
        top_candidates = all_candidates[:k]
        
        # Convert to SearchResult objects
        results = []
        for candidate in top_candidates:
            result = SearchResult(
                id=candidate['vector_id'],
                score=1.0 / (1.0 + candidate['distance']),  # Convert distance to similarity score
                metadata=candidate['metadata']
            )
            results.append(result)
        
        search_time = (time.perf_counter() - start_time) * 1000
        logger.debug("IVF search completed", 
                    query_time_ms=search_time,
                    candidates_searched=len(all_candidates),
                    clusters_searched=self.nprobe,
                    results_returned=len(results))
        
        return results
    
    def _calculate_cluster_distances(self, query_scaled: np.ndarray) -> np.ndarray:
        """Calculate distances from query to all cluster centroids."""
        if self.metric in ['L2', 'euclidean']:
            distances = np.linalg.norm(self.centroids - query_scaled, axis=1)
        elif self.metric == 'cosine':
            # For cosine, we use dot product since vectors are normalized
            similarities = np.dot(self.centroids, query_scaled)
            distances = 1.0 - similarities  # Convert similarity to distance
        else:
            raise ValueError(f"Unsupported metric: {self.metric}")
        
        return distances
    
    def _calculate_distances(self, query: np.ndarray, vectors: np.ndarray) -> np.ndarray:
        """Calculate distances between query and a set of vectors."""
        if self.metric in ['L2', 'euclidean']:
            distances = np.linalg.norm(vectors - query, axis=1)
        elif self.metric == 'cosine':
            # Cosine distance = 1 - cosine_similarity
            similarities = np.dot(vectors, query)
            distances = 1.0 - similarities
        else:
            raise ValueError(f"Unsupported metric: {self.metric}")
        
        return distances
    
    def _normalize_vectors(self, vectors: np.ndarray) -> np.ndarray:
        """Normalize vectors to unit length."""
        norms = np.linalg.norm(vectors, axis=1, keepdims=True)
        norms[norms == 0] = 1  # Avoid division by zero
        return vectors / norms
    
    async def update_vector(self, vector_id: str, new_vector: VectorData) -> bool:
        """Update an existing vector."""
        # First, remove the old vector
        removed = await self.remove_vector(vector_id)
        if not removed:
            return False
        
        # Add the new vector
        return await self.add_vectors([new_vector])
    
    async def remove_vector(self, vector_id: str) -> bool:
        """Remove a vector from the index."""
        try:
            # Find and remove vector from appropriate cluster
            for cluster in self.clusters:
                if vector_id in cluster.vector_ids:
                    # Find index of vector
                    vector_index = cluster.vector_ids.index(vector_id)
                    
                    # Remove from all lists
                    cluster.vector_ids.pop(vector_index)
                    cluster.metadata.pop(vector_index)
                    
                    # Remove from vectors array
                    if len(cluster.vectors) > 0:
                        cluster.vectors = np.delete(cluster.vectors, vector_index, axis=0)
                    
                    self.total_vectors -= 1
                    
                    logger.debug("Vector removed from IVF index", 
                               vector_id=vector_id, 
                               cluster_id=cluster.cluster_id)
                    return True
            
            logger.warning("Vector not found for removal", vector_id=vector_id)
            return False
            
        except Exception as e:
            logger.error("Failed to remove vector from IVF index", 
                        vector_id=vector_id, 
                        error=str(e))
            return False
    
    async def get_vector_count(self) -> int:
        """Get the total number of vectors in the index."""
        return self.total_vectors
    
    async def get_memory_usage(self) -> Dict[str, Any]:
        """Get memory usage statistics."""
        total_vectors_memory = 0
        total_metadata_memory = 0
        
        for cluster in self.clusters:
            if len(cluster.vectors) > 0:
                total_vectors_memory += cluster.vectors.nbytes
            
            # Estimate metadata memory (rough approximation)
            total_metadata_memory += len(cluster.metadata) * 100  # ~100 bytes per metadata dict
        
        centroids_memory = self.centroids.nbytes if self.centroids is not None else 0
        
        return {
            'total_memory_bytes': total_vectors_memory + total_metadata_memory + centroids_memory,
            'vectors_memory_bytes': total_vectors_memory,
            'metadata_memory_bytes': total_metadata_memory,
            'centroids_memory_bytes': centroids_memory,
            'cluster_count': len(self.clusters),
            'vectors_per_cluster': [len(c.vector_ids) for c in self.clusters]
        }
    
    async def get_statistics(self) -> Dict[str, Any]:
        """Get comprehensive algorithm statistics."""
        cluster_stats = []
        for cluster in self.clusters:
            cluster_stats.append({
                'cluster_id': cluster.cluster_id,
                'vector_count': len(cluster.vector_ids),
                'centroid_norm': float(np.linalg.norm(cluster.centroid)) if cluster.centroid is not None else 0
            })
        
        return {
            'algorithm_name': 'IVF',
            'total_vectors': self.total_vectors,
            'nlist': self.nlist,
            'nprobe': self.nprobe,
            'metric': self.metric,
            'is_trained': self.is_trained,
            'dimension': self.dimension,
            'cluster_statistics': cluster_stats,
            'memory_usage': await self.get_memory_usage()
        }
    
    def save_index(self, filepath: str) -> bool:
        """Save the index to a file."""
        try:
            index_data = {
                'nlist': self.nlist,
                'nprobe': self.nprobe,
                'metric': self.metric,
                'quantizer_type': self.quantizer_type,
                'max_vectors_per_cluster': self.max_vectors_per_cluster,
                'use_minibatch_kmeans': self.use_minibatch_kmeans,
                'is_trained': self.is_trained,
                'dimension': self.dimension,
                'total_vectors': self.total_vectors,
                'centroids': self.centroids,
                'kmeans_model': self.kmeans_model,
                'scaler': self.scaler,
                'clusters': self.clusters
            }
            
            with open(filepath, 'wb') as f:
                pickle.dump(index_data, f)
            
            logger.info("IVF index saved successfully", filepath=filepath)
            return True
            
        except Exception as e:
            logger.error("Failed to save IVF index", filepath=filepath, error=str(e))
            return False
    
    def load_index(self, filepath: str) -> bool:
        """Load the index from a file."""
        try:
            with open(filepath, 'rb') as f:
                index_data = pickle.load(f)
            
            # Restore all attributes
            self.nlist = index_data['nlist']
            self.nprobe = index_data['nprobe']
            self.metric = index_data['metric']
            self.quantizer_type = index_data['quantizer_type']
            self.max_vectors_per_cluster = index_data['max_vectors_per_cluster']
            self.use_minibatch_kmeans = index_data['use_minibatch_kmeans']
            self.is_trained = index_data['is_trained']
            self.dimension = index_data['dimension']
            self.total_vectors = index_data['total_vectors']
            self.centroids = index_data['centroids']
            self.kmeans_model = index_data['kmeans_model']
            self.scaler = index_data['scaler']
            self.clusters = index_data['clusters']
            
            logger.info("IVF index loaded successfully", 
                       filepath=filepath,
                       total_vectors=self.total_vectors,
                       clusters=len(self.clusters))
            return True
            
        except Exception as e:
            logger.error("Failed to load IVF index", filepath=filepath, error=str(e))
            return False
    
    async def rebuild_index(self):
        """Rebuild the index with current vectors."""
        if not self.clusters:
            logger.warning("No clusters to rebuild")
            return False
        
        try:
            # Collect all vectors
            all_vectors = []
            all_ids = []
            all_metadata = []
            
            for cluster in self.clusters:
                if len(cluster.vectors) > 0:
                    all_vectors.extend(cluster.vectors.tolist())
                    all_ids.extend(cluster.vector_ids)
                    all_metadata.extend(cluster.metadata)
            
            if not all_vectors:
                logger.warning("No vectors found to rebuild")
                return False
            
            # Reset state
            self.is_trained = False
            self.clusters = []
            self.centroids = None
            self.kmeans_model = None
            self.scaler = None
            
            # Rebuild with all vectors
            embeddings = np.array(all_vectors, dtype=np.float32)
            await self._initialize_index(embeddings)
            await self._assign_vectors_to_clusters(embeddings, all_ids, all_metadata)
            
            logger.info("IVF index rebuilt successfully", total_vectors=len(all_vectors))
            return True
            
        except Exception as e:
            logger.error("Failed to rebuild IVF index", error=str(e))
            return False
    
    def get_cluster_info(self, cluster_id: int) -> Optional[Dict[str, Any]]:
        """Get detailed information about a specific cluster."""
        if cluster_id < 0 or cluster_id >= len(self.clusters):
            return None
        
        cluster = self.clusters[cluster_id]
        
        return {
            'cluster_id': cluster.cluster_id,
            'vector_count': len(cluster.vector_ids),
            'centroid': cluster.centroid.tolist() if cluster.centroid is not None else None,
            'centroid_norm': float(np.linalg.norm(cluster.centroid)) if cluster.centroid is not None else 0,
            'vector_ids': cluster.vector_ids[:10],  # Sample of IDs
            'memory_usage_bytes': cluster.vectors.nbytes if len(cluster.vectors) > 0 else 0
        }
    
    async def optimize_clusters(self):
        """Optimize cluster distribution by rebalancing if needed."""
        if not self.is_trained:
            return False
        
        try:
            # Check cluster balance
            cluster_sizes = [len(cluster.vector_ids) for cluster in self.clusters]
            mean_size = np.mean(cluster_sizes)
            std_size = np.std(cluster_sizes)
            
            # If clusters are very imbalanced, trigger rebuild
            if std_size > mean_size * 0.5:  # High variance indicates imbalance
                logger.info("Clusters are imbalanced, triggering rebuild",
                           mean_size=mean_size,
                           std_size=std_size)
                return await self.rebuild_index()
            
            logger.info("Clusters are well balanced", 
                       mean_size=mean_size,
                       std_size=std_size)
            return True
            
        except Exception as e:
            logger.error("Failed to optimize clusters", error=str(e))
            return False