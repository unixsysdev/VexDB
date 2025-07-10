"""
Data Profiler for VexDB.

This module analyzes vector data characteristics to inform algorithm selection
and optimization decisions. It performs statistical analysis, clustering detection,
distribution analysis, and correlation analysis.
"""

import asyncio
import time
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import structlog

from ..models.vector import VectorData
from ..models.profiling import DataProfile, DataCharacteristic
from ..config.settings import get_settings

logger = structlog.get_logger()


class DataProfiler:
    """
    Analyzes vector data characteristics to inform algorithm selection.
    
    The DataProfiler examines vector datasets to extract key characteristics
    that influence algorithm performance, including dimensionality, sparsity,
    clustering tendencies, and distribution properties.
    """
    
    def __init__(self):
        """Initialize the data profiler."""
        self.settings = get_settings()
        self.current_profile: Optional[DataProfile] = None
        self.last_analysis_time: Optional[datetime] = None
        
        # Analysis parameters
        self.min_sample_size = 100
        self.max_sample_size = self.settings.profiling_sample_size
        self.clustering_sample_size = min(1000, self.max_sample_size)
        
        logger.info("Data profiler initialized", 
                   min_sample_size=self.min_sample_size,
                   max_sample_size=self.max_sample_size)
    
    async def analyze_dataset(self, vectors: List[VectorData]) -> DataProfile:
        """
        Perform comprehensive analysis of a vector dataset.
        
        Args:
            vectors: List of vectors to analyze
            
        Returns:
            DataProfile: Comprehensive data profile
            
        Raises:
            ValueError: If dataset is too small or invalid
        """
        if not vectors:
            raise ValueError("Cannot analyze empty dataset")
        
        if len(vectors) < self.min_sample_size:
            logger.warning("Dataset smaller than recommended minimum", 
                          size=len(vectors), 
                          minimum=self.min_sample_size)
        
        start_time = time.perf_counter()
        logger.info("Starting dataset analysis", vector_count=len(vectors))
        
        # Sample if dataset is too large
        sample_vectors = self._sample_vectors(vectors)
        embeddings_matrix = np.array([v.embedding for v in sample_vectors], dtype=np.float32)
        
        # Perform all analyses
        profile = await self._perform_comprehensive_analysis(sample_vectors, embeddings_matrix)
        
        # Update internal state
        self.current_profile = profile
        self.last_analysis_time = datetime.utcnow()
        
        analysis_time = (time.perf_counter() - start_time) * 1000
        logger.info("Dataset analysis completed", 
                   analysis_time_ms=analysis_time,
                   sample_size=len(sample_vectors),
                   dimensionality=profile.dimensionality)
        
        return profile
    
    async def analyze_incremental(self, new_vectors: List[VectorData]) -> DataProfile:
        """
        Perform incremental analysis when new vectors are added.
        
        Args:
            new_vectors: New vectors to incorporate into analysis
            
        Returns:
            DataProfile: Updated data profile
        """
        if not new_vectors:
            return self.current_profile
        
        logger.info("Performing incremental analysis", new_vector_count=len(new_vectors))
        
        # For now, we'll do a simple approach - just analyze the new vectors
        # TODO: Implement true incremental analysis that updates existing statistics
        return await self.analyze_dataset(new_vectors)
    
    def get_current_profile(self) -> Optional[DataProfile]:
        """
        Get the current data profile.
        
        Returns:
            DataProfile: Current profile or None if no analysis performed
        """
        return self.current_profile
    
    def needs_reanalysis(self, vector_count_change: int = 0) -> bool:
        """
        Determine if dataset needs reanalysis.
        
        Args:
            vector_count_change: Number of vectors added/removed since last analysis
            
        Returns:
            bool: True if reanalysis is recommended
        """
        if self.current_profile is None:
            return True
        
        # Check if enough time has passed
        if self.last_analysis_time:
            time_since_analysis = datetime.utcnow() - self.last_analysis_time
            if time_since_analysis.total_seconds() > 3600:  # 1 hour
                return True
        
        # Check if significant data change
        if self.current_profile and vector_count_change > 0:
            change_ratio = vector_count_change / max(self.current_profile.sample_size, 1)
            if change_ratio > 0.1:  # 10% change
                return True
        
        return False
    
    async def _perform_comprehensive_analysis(
        self, 
        vectors: List[VectorData], 
        embeddings_matrix: np.ndarray
    ) -> DataProfile:
        """
        Perform all analysis components.
        
        Args:
            vectors: Vector data
            embeddings_matrix: NumPy matrix of embeddings
            
        Returns:
            DataProfile: Complete analysis results
        """
        # Basic statistics
        basic_stats = self._calculate_basic_statistics(embeddings_matrix)
        
        # Sparsity analysis
        sparsity_info = self._analyze_sparsity(embeddings_matrix)
        
        # Clustering analysis
        clustering_info = await self._analyze_clustering(embeddings_matrix)
        
        # Distribution analysis
        distribution_info = self._analyze_distribution(embeddings_matrix)
        
        # Correlation analysis
        correlation_info = self._analyze_correlation(embeddings_matrix)
        
        # Create profile
        profile = DataProfile(
            sample_size=len(vectors),
            dimensionality=embeddings_matrix.shape[1],
            mean_vector=basic_stats['mean'].tolist(),
            std_vector=basic_stats['std'].tolist(),
            min_values=basic_stats['min'].tolist(),
            max_values=basic_stats['max'].tolist(),
            sparsity_ratio=sparsity_info['sparsity_ratio'],
            effective_dimensions=sparsity_info['effective_dimensions'],
            has_clusters=clustering_info['has_clusters'],
            estimated_clusters=clustering_info['estimated_clusters'],
            silhouette_score=clustering_info['silhouette_score'],
            is_normalized=distribution_info['is_normalized'],
            distribution_type=distribution_info['distribution_type'],
            correlation_matrix_summary=correlation_info
        )
        
        return profile
    
    def _sample_vectors(self, vectors: List[VectorData]) -> List[VectorData]:
        """
        Sample vectors if dataset is too large.
        
        Args:
            vectors: Full vector list
            
        Returns:
            List[VectorData]: Sampled vectors
        """
        if len(vectors) <= self.max_sample_size:
            return vectors
        
        # Use systematic sampling for better representation
        step = len(vectors) // self.max_sample_size
        sampled = vectors[::step][:self.max_sample_size]
        
        logger.info("Sampled vectors for analysis", 
                   original_size=len(vectors), 
                   sampled_size=len(sampled))
        
        return sampled
    
    def _calculate_basic_statistics(self, embeddings: np.ndarray) -> Dict[str, np.ndarray]:
        """
        Calculate basic statistical properties.
        
        Args:
            embeddings: Vector embeddings matrix
            
        Returns:
            Dict: Basic statistics
        """
        return {
            'mean': np.mean(embeddings, axis=0),
            'std': np.std(embeddings, axis=0),
            'min': np.min(embeddings, axis=0),
            'max': np.max(embeddings, axis=0),
            'median': np.median(embeddings, axis=0)
        }
    
    def _analyze_sparsity(self, embeddings: np.ndarray) -> Dict[str, Any]:
        """
        Analyze sparsity characteristics.
        
        Args:
            embeddings: Vector embeddings matrix
            
        Returns:
            Dict: Sparsity analysis results
        """
        # Calculate sparsity ratio
        zero_count = np.count_nonzero(embeddings == 0)
        total_elements = embeddings.size
        sparsity_ratio = zero_count / total_elements if total_elements > 0 else 0.0
        
        # Calculate effective dimensions (dimensions with non-zero variance)
        variances = np.var(embeddings, axis=0)
        effective_dimensions = np.count_nonzero(variances > 1e-8)
        
        return {
            'sparsity_ratio': float(sparsity_ratio),
            'effective_dimensions': int(effective_dimensions),
            'zero_elements': int(zero_count),
            'total_elements': int(total_elements)
        }
    
    async def _analyze_clustering(self, embeddings: np.ndarray) -> Dict[str, Any]:
        """
        Analyze clustering tendencies.
        
        Args:
            embeddings: Vector embeddings matrix
            
        Returns:
            Dict: Clustering analysis results
        """
        if len(embeddings) < 10:
            return {
                'has_clusters': False,
                'estimated_clusters': None,
                'silhouette_score': None
            }
        
        # Sample for clustering analysis if needed
        sample_size = min(len(embeddings), self.clustering_sample_size)
        if sample_size < len(embeddings):
            indices = np.random.choice(len(embeddings), sample_size, replace=False)
            sample_embeddings = embeddings[indices]
        else:
            sample_embeddings = embeddings
        
        # Standardize features for clustering
        scaler = StandardScaler()
        scaled_embeddings = scaler.fit_transform(sample_embeddings)
        
        # Try different numbers of clusters
        best_silhouette = -1
        best_n_clusters = None
        max_clusters = min(10, len(sample_embeddings) // 2)
        
        for n_clusters in range(2, max_clusters + 1):
            try:
                kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
                cluster_labels = kmeans.fit_predict(scaled_embeddings)
                
                # Calculate silhouette score
                silhouette_avg = silhouette_score(scaled_embeddings, cluster_labels)
                
                if silhouette_avg > best_silhouette:
                    best_silhouette = silhouette_avg
                    best_n_clusters = n_clusters
                    
            except Exception as e:
                logger.debug("Clustering failed for n_clusters", n_clusters=n_clusters, error=str(e))
                continue
        
        # Determine if meaningful clusters exist
        has_clusters = best_silhouette > 0.3  # Threshold for meaningful clustering
        
        return {
            'has_clusters': has_clusters,
            'estimated_clusters': best_n_clusters if has_clusters else None,
            'silhouette_score': float(best_silhouette) if best_silhouette > -1 else None
        }
    
    def _analyze_distribution(self, embeddings: np.ndarray) -> Dict[str, Any]:
        """
        Analyze distribution characteristics.
        
        Args:
            embeddings: Vector embeddings matrix
            
        Returns:
            Dict: Distribution analysis results
        """
        # Check if vectors are normalized
        norms = np.linalg.norm(embeddings, axis=1)
        is_normalized = np.allclose(norms, 1.0, rtol=0.1)
        
        # Analyze distribution type
        # Check if data follows a normal distribution (simple test)
        flattened = embeddings.flatten()
        sample_size = min(len(flattened), 5000)  # Sample for efficiency
        sample_data = np.random.choice(flattened, sample_size, replace=False)
        
        # Simple distribution detection based on statistics
        mean_val = np.mean(sample_data)
        std_val = np.std(sample_data)
        skewness = self._calculate_skewness(sample_data)
        
        if abs(skewness) < 0.5 and abs(mean_val) < std_val:
            distribution_type = "normal"
        elif abs(skewness) > 1.0:
            distribution_type = "skewed"
        elif is_normalized:
            distribution_type = "uniform_sphere"
        else:
            distribution_type = "unknown"
        
        return {
            'is_normalized': bool(is_normalized),
            'distribution_type': distribution_type,
            'mean_norm': float(np.mean(norms)),
            'std_norm': float(np.std(norms)),
            'skewness': float(skewness)
        }
    
    def _analyze_correlation(self, embeddings: np.ndarray) -> Dict[str, float]:
        """
        Analyze correlation structure.
        
        Args:
            embeddings: Vector embeddings matrix
            
        Returns:
            Dict: Correlation analysis summary
        """
        if embeddings.shape[1] > 1000:
            # Use PCA for high-dimensional data
            pca = PCA(n_components=min(50, embeddings.shape[0] - 1))
            try:
                pca.fit(embeddings)
                explained_variance_ratio = pca.explained_variance_ratio_
                
                return {
                    'first_pc_variance': float(explained_variance_ratio[0]),
                    'top_5_pc_variance': float(np.sum(explained_variance_ratio[:5])),
                    'effective_rank': int(np.sum(np.cumsum(explained_variance_ratio) < 0.95)),
                    'intrinsic_dimensionality': int(np.sum(explained_variance_ratio > 0.01))
                }
            except Exception as e:
                logger.warning("PCA analysis failed", error=str(e))
                return {'analysis_failed': True}
        else:
            # Direct correlation analysis for lower dimensions
            try:
                corr_matrix = np.corrcoef(embeddings.T)
                
                # Remove diagonal and get absolute correlations
                mask = ~np.eye(corr_matrix.shape[0], dtype=bool)
                off_diagonal_corr = corr_matrix[mask]
                
                return {
                    'mean_correlation': float(np.mean(np.abs(off_diagonal_corr))),
                    'max_correlation': float(np.max(np.abs(off_diagonal_corr))),
                    'correlation_std': float(np.std(off_diagonal_corr))
                }
            except Exception as e:
                logger.warning("Correlation analysis failed", error=str(e))
                return {'analysis_failed': True}
    
    def _calculate_skewness(self, data: np.ndarray) -> float:
        """
        Calculate skewness of data.
        
        Args:
            data: Data array
            
        Returns:
            float: Skewness value
        """
        mean_val = np.mean(data)
        std_val = np.std(data)
        
        if std_val == 0:
            return 0.0
        
        skewness = np.mean(((data - mean_val) / std_val) ** 3)
        return float(skewness)
    
    def get_algorithm_hints(self, profile: DataProfile = None) -> Dict[str, float]:
        """
        Get algorithm performance hints based on data profile.
        
        Args:
            profile: Data profile to analyze (uses current if None)
            
        Returns:
            Dict: Algorithm suitability scores (0-1)
        """
        if profile is None:
            profile = self.current_profile
        
        if profile is None:
            return {}
        
        hints = {}
        
        # Brute force - always works but scales poorly
        hints['brute_force'] = 1.0 if profile.sample_size < 10000 else 0.1
        
        # HNSW - good for most cases, especially when clustering exists
        hnsw_score = 0.8
        if profile.has_clusters:
            hnsw_score += 0.1
        if profile.dimensionality > 100:
            hnsw_score += 0.1
        hints['hnsw'] = min(hnsw_score, 1.0)
        
        # IVF - good for large datasets with clusters
        ivf_score = 0.0
        if profile.sample_size > 50000:
            ivf_score += 0.4
        if profile.has_clusters and profile.estimated_clusters:
            ivf_score += 0.5
        if profile.dimensionality > 50:
            ivf_score += 0.1
        hints['ivf'] = min(ivf_score, 1.0)
        
        # LSH - good for high-dimensional sparse data
        lsh_score = 0.0
        if profile.dimensionality > 500:
            lsh_score += 0.4
        if profile.sparsity_ratio > 0.5:
            lsh_score += 0.4
        if profile.is_normalized:
            lsh_score += 0.2
        hints['lsh'] = min(lsh_score, 1.0)
        
        return hints