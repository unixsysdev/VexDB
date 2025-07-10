"""
Algorithm Registry for VexDB.

This module manages the registration, discovery, and lifecycle of vector
indexing algorithms. It provides a centralized way to register algorithms,
query their capabilities, and select appropriate algorithms based on requirements.
"""

import asyncio
from typing import Dict, List, Type, Optional, Any
from datetime import datetime
import structlog

from ..algorithms.base import AlgorithmInterface, AlgorithmCapabilities
from ..algorithms.brute_force import BruteForceAlgorithm
from ..models.profiling import DataProfile, AlgorithmRecommendation
from ..config.settings import get_algorithm_config

logger = structlog.get_logger()


class AlgorithmRegistry:
    """
    Central registry for managing vector indexing algorithms.
    
    The AlgorithmRegistry maintains a catalog of available algorithms,
    their capabilities, and performance characteristics. It provides
    methods for algorithm discovery, instantiation, and recommendation.
    """
    
    def __init__(self):
        """Initialize the algorithm registry."""
        self.config = get_algorithm_config()
        
        # Registry of available algorithm classes
        self._algorithm_classes: Dict[str, Type[AlgorithmInterface]] = {}
        
        # Registry of algorithm instances
        self._algorithm_instances: Dict[str, AlgorithmInterface] = {}
        
        # Algorithm capabilities cache
        self._capabilities_cache: Dict[str, AlgorithmCapabilities] = {}
        
        # Performance history
        self._performance_history: Dict[str, List[Dict[str, Any]]] = {}
        
        # Register built-in algorithms
        self._register_builtin_algorithms()
        
        logger.info("Algorithm registry initialized", 
                   available_algorithms=list(self._algorithm_classes.keys()))
    
    def register_algorithm(
        self, 
        name: str, 
        algorithm_class: Type[AlgorithmInterface],
        capabilities: Optional[AlgorithmCapabilities] = None
    ) -> None:
        """
        Register a new algorithm class.
        
        Args:
            name: Algorithm identifier
            algorithm_class: Algorithm class implementing AlgorithmInterface
            capabilities: Algorithm capabilities (auto-detected if None)
            
        Raises:
            ValueError: If algorithm name already exists or class is invalid
        """
        if not issubclass(algorithm_class, AlgorithmInterface):
            raise ValueError(f"Algorithm class must implement AlgorithmInterface")
        
        if name in self._algorithm_classes:
            logger.warning("Overwriting existing algorithm", algorithm_name=name)
        
        self._algorithm_classes[name] = algorithm_class
        
        # Cache capabilities
        if capabilities:
            self._capabilities_cache[name] = capabilities
        else:
            # Try to get capabilities from a temporary instance
            try:
                temp_instance = algorithm_class({})
                info = temp_instance.get_algorithm_info()
                if 'capabilities' in info:
                    caps_dict = info['capabilities']
                    self._capabilities_cache[name] = AlgorithmCapabilities(**caps_dict)
            except Exception as e:
                logger.warning("Could not determine algorithm capabilities", 
                             algorithm_name=name, error=str(e))
        
        logger.info("Algorithm registered", algorithm_name=name)
    
    def unregister_algorithm(self, name: str) -> None:
        """
        Unregister an algorithm.
        
        Args:
            name: Algorithm identifier
        """
        if name in self._algorithm_classes:
            del self._algorithm_classes[name]
        
        if name in self._algorithm_instances:
            del self._algorithm_instances[name]
        
        if name in self._capabilities_cache:
            del self._capabilities_cache[name]
        
        if name in self._performance_history:
            del self._performance_history[name]
        
        logger.info("Algorithm unregistered", algorithm_name=name)
    
    def get_available_algorithms(self) -> List[str]:
        """
        Get list of available algorithm names.
        
        Returns:
            List[str]: Available algorithm identifiers
        """
        return list(self._algorithm_classes.keys())
    
    def get_algorithm_info(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed information about an algorithm.
        
        Args:
            name: Algorithm identifier
            
        Returns:
            Dict: Algorithm information or None if not found
        """
        if name not in self._algorithm_classes:
            return None
        
        info = {
            'name': name,
            'class': self._algorithm_classes[name].__name__,
            'available': self._is_algorithm_available(name),
            'capabilities': None,
            'instance_active': name in self._algorithm_instances
        }
        
        # Add capabilities if available
        if name in self._capabilities_cache:
            info['capabilities'] = self._capabilities_cache[name].to_dict()
        
        # Add performance history summary
        if name in self._performance_history:
            history = self._performance_history[name]
            if history:
                recent_performance = history[-1]
                info['recent_performance'] = recent_performance
        
        return info
    
    async def create_algorithm_instance(
        self, 
        name: str, 
        config: Optional[Dict[str, Any]] = None
    ) -> AlgorithmInterface:
        """
        Create an instance of the specified algorithm.
        
        Args:
            name: Algorithm identifier
            config: Algorithm-specific configuration
            
        Returns:
            AlgorithmInterface: Algorithm instance
            
        Raises:
            ValueError: If algorithm not found or creation fails
        """
        if name not in self._algorithm_classes:
            raise ValueError(f"Algorithm '{name}' not found")
        
        if not self._is_algorithm_available(name):
            raise ValueError(f"Algorithm '{name}' is not available (missing dependencies)")
        
        algorithm_class = self._algorithm_classes[name]
        config = config or {}
        
        try:
            instance = algorithm_class(config)
            logger.info("Algorithm instance created", algorithm_name=name)
            return instance
        except Exception as e:
            logger.error("Failed to create algorithm instance", 
                        algorithm_name=name, error=str(e))
            raise ValueError(f"Failed to create algorithm instance: {str(e)}")
    
    async def get_or_create_instance(
        self, 
        name: str, 
        config: Optional[Dict[str, Any]] = None
    ) -> AlgorithmInterface:
        """
        Get existing instance or create a new one.
        
        Args:
            name: Algorithm identifier
            config: Algorithm-specific configuration
            
        Returns:
            AlgorithmInterface: Algorithm instance
        """
        # Check if instance already exists
        if name in self._algorithm_instances:
            return self._algorithm_instances[name]
        
        # Create new instance
        instance = await self.create_algorithm_instance(name, config)
        self._algorithm_instances[name] = instance
        
        return instance
    
    def get_algorithm_capabilities(self, name: str) -> Optional[AlgorithmCapabilities]:
        """
        Get algorithm capabilities.
        
        Args:
            name: Algorithm identifier
            
        Returns:
            AlgorithmCapabilities: Capabilities or None if not found
        """
        return self._capabilities_cache.get(name)
    
    def filter_algorithms_by_capabilities(
        self, 
        exact_search: Optional[bool] = None,
        supports_updates: Optional[bool] = None,
        supports_deletes: Optional[bool] = None,
        memory_efficient: Optional[bool] = None
    ) -> List[str]:
        """
        Filter algorithms by capability requirements.
        
        Args:
            exact_search: Require exact search capability
            supports_updates: Require update support
            supports_deletes: Require deletion support
            memory_efficient: Require memory efficiency
            
        Returns:
            List[str]: Filtered algorithm names
        """
        filtered = []
        
        for name in self._algorithm_classes:
            capabilities = self._capabilities_cache.get(name)
            if not capabilities:
                continue
            
            # Apply filters
            if exact_search is not None and capabilities.exact_search != exact_search:
                continue
            if supports_updates is not None and capabilities.supports_updates != supports_updates:
                continue
            if supports_deletes is not None and capabilities.supports_deletes != supports_deletes:
                continue
            if memory_efficient is not None and capabilities.memory_efficient != memory_efficient:
                continue
            
            filtered.append(name)
        
        return filtered
    
    def recommend_algorithms(
        self, 
        data_profile: Optional[DataProfile] = None,
        requirements: Optional[Dict[str, Any]] = None
    ) -> List[AlgorithmRecommendation]:
        """
        Recommend algorithms based on data profile and requirements.
        
        Args:
            data_profile: Data characteristics
            requirements: Performance and capability requirements
            
        Returns:
            List[AlgorithmRecommendation]: Ranked algorithm recommendations
        """
        requirements = requirements or {}
        recommendations = []
        
        # Get available algorithms
        available_algorithms = [name for name in self._algorithm_classes 
                              if self._is_algorithm_available(name)]
        
        for algorithm_name in available_algorithms:
            recommendation = self._evaluate_algorithm_for_data(
                algorithm_name, data_profile, requirements
            )
            if recommendation:
                recommendations.append(recommendation)
        
        # Sort by confidence score
        recommendations.sort(key=lambda x: x.confidence_score, reverse=True)
        
        logger.info("Generated algorithm recommendations", 
                   recommendation_count=len(recommendations))
        
        return recommendations
    
    def record_performance(self, algorithm_name: str, performance_data: Dict[str, Any]) -> None:
        """
        Record performance data for an algorithm.
        
        Args:
            algorithm_name: Algorithm identifier
            performance_data: Performance metrics
        """
        if algorithm_name not in self._performance_history:
            self._performance_history[algorithm_name] = []
        
        performance_entry = {
            'timestamp': datetime.utcnow().isoformat(),
            **performance_data
        }
        
        self._performance_history[algorithm_name].append(performance_entry)
        
        # Keep only recent history (last 100 entries)
        self._performance_history[algorithm_name] = \
            self._performance_history[algorithm_name][-100:]
        
        logger.debug("Performance recorded", algorithm_name=algorithm_name)
    
    def get_performance_history(self, algorithm_name: str) -> List[Dict[str, Any]]:
        """
        Get performance history for an algorithm.
        
        Args:
            algorithm_name: Algorithm identifier
            
        Returns:
            List[Dict]: Performance history entries
        """
        return self._performance_history.get(algorithm_name, [])
    
    def _register_builtin_algorithms(self) -> None:
        """Register built-in algorithms."""
        # Register brute force (always available)
        self.register_algorithm('brute_force', BruteForceAlgorithm)
        
        # TODO: Register other algorithms when available
        # try:
        #     from ..algorithms.hnsw import HNSWAlgorithm
        #     self.register_algorithm('hnsw', HNSWAlgorithm)
        # except ImportError:
        #     logger.info("HNSW algorithm not available (missing hnswlib)")
        
        # try:
        #     from ..algorithms.ivf import IVFAlgorithm
        #     self.register_algorithm('ivf', IVFAlgorithm)
        # except ImportError:
        #     logger.info("IVF algorithm not available (missing faiss)")
        
        # try:
        #     from ..algorithms.lsh import LSHAlgorithm
        #     self.register_algorithm('lsh', LSHAlgorithm)
        # except ImportError:
        #     logger.info("LSH algorithm not available (missing datasketch)")
    
    def _is_algorithm_available(self, name: str) -> bool:
        """
        Check if algorithm is available (dependencies installed).
        
        Args:
            name: Algorithm identifier
            
        Returns:
            bool: True if algorithm is available
        """
        if name == 'brute_force':
            return True
        
        # TODO: Check dependencies for other algorithms
        # For now, only brute force is available
        return False
    
    def _evaluate_algorithm_for_data(
        self,
        algorithm_name: str,
        data_profile: Optional[DataProfile],
        requirements: Dict[str, Any]
    ) -> Optional[AlgorithmRecommendation]:
        """
        Evaluate how well an algorithm fits the data and requirements.
        
        Args:
            algorithm_name: Algorithm to evaluate
            data_profile: Data characteristics
            requirements: Performance requirements
            
        Returns:
            AlgorithmRecommendation: Recommendation or None
        """
        capabilities = self._capabilities_cache.get(algorithm_name)
        if not capabilities:
            return None
        
        # Base confidence score
        confidence = 0.5
        reasoning = []
        data_characteristics = []
        
        # Evaluate based on data profile
        if data_profile:
            # Dataset size considerations
            if data_profile.sample_size < 1000:
                if algorithm_name == 'brute_force':
                    confidence += 0.3
                    reasoning.append("Small dataset favors exact search")
                else:
                    confidence -= 0.2
                    reasoning.append("Large-scale algorithms may be overkill for small datasets")
            
            elif data_profile.sample_size > 50000:
                if algorithm_name == 'brute_force':
                    confidence -= 0.4
                    reasoning.append("Brute force becomes inefficient for large datasets")
                else:
                    confidence += 0.2
                    reasoning.append("Approximate algorithms more suitable for large datasets")
            
            # Clustering considerations
            if data_profile.has_clusters:
                data_characteristics.append("clustering")
                if algorithm_name in ['ivf', 'hnsw']:
                    confidence += 0.2
                    reasoning.append("Data has natural clusters, beneficial for hierarchical algorithms")
            
            # Dimensionality considerations
            if data_profile.dimensionality > 100:
                data_characteristics.append("high_dimensionality")
                if algorithm_name in ['hnsw', 'lsh']:
                    confidence += 0.1
                    reasoning.append("High-dimensional data suits advanced indexing methods")
            
            # Sparsity considerations
            if data_profile.sparsity_ratio > 0.5:
                data_characteristics.append("sparse")
                if algorithm_name == 'lsh':
                    confidence += 0.2
                    reasoning.append("Sparse data is well-suited for LSH")
            
            # Normalization considerations
            if data_profile.is_normalized:
                data_characteristics.append("normalized")
                if algorithm_name in ['hnsw', 'lsh']:
                    confidence += 0.1
                    reasoning.append("Normalized vectors work well with angular distance metrics")
        
        # Evaluate based on requirements
        accuracy_requirement = requirements.get('accuracy_threshold', 0.95)
        if accuracy_requirement > 0.99:
            if capabilities.exact_search:
                confidence += 0.3
                reasoning.append("High accuracy requirement favors exact search")
            else:
                confidence -= 0.2
                reasoning.append("Approximate algorithms may not meet accuracy requirements")
        
        latency_requirement = requirements.get('max_latency_ms', 100)
        if latency_requirement < 50:
            if algorithm_name == 'brute_force':
                confidence -= 0.3
                reasoning.append("Strict latency requirements favor indexed search")
        
        memory_constraint = requirements.get('max_memory_mb', 1000)
        if memory_constraint < 500:
            if capabilities.memory_efficient:
                confidence += 0.2
                reasoning.append("Memory constraints favor efficient algorithms")
        
        # Capability requirements
        if requirements.get('requires_updates', False) and not capabilities.supports_updates:
            confidence -= 0.5
            reasoning.append("Algorithm does not support required update capability")
        
        if requirements.get('requires_deletes', False) and not capabilities.supports_deletes:
            confidence -= 0.5
            reasoning.append("Algorithm does not support required deletion capability")
        
        # Ensure confidence is in valid range
        confidence = max(0.0, min(1.0, confidence))
        
        # Only recommend if confidence is reasonable
        if confidence < 0.3:
            return None
        
        # Generate parameter recommendations
        recommended_parameters = self._get_recommended_parameters(
            algorithm_name, data_profile, requirements
        )
        
        # Create recommendation
        recommendation = AlgorithmRecommendation(
            algorithm_name=algorithm_name,
            confidence_score=confidence,
            recommended_parameters=recommended_parameters,
            reasoning=reasoning,
            data_characteristics_considered=data_characteristics,
            # TODO: Add performance predictions based on data profile
            predicted_query_time_ms=None,
            predicted_accuracy=None,
            predicted_memory_usage_mb=None
        )
        
        return recommendation
    
    def _get_recommended_parameters(
        self,
        algorithm_name: str,
        data_profile: Optional[DataProfile],
        requirements: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Get recommended parameters for an algorithm.
        
        Args:
            algorithm_name: Algorithm identifier
            data_profile: Data characteristics
            requirements: Performance requirements
            
        Returns:
            Dict: Recommended parameters
        """
        params = {}
        
        if algorithm_name == 'brute_force':
            # Brute force parameters
            params['distance_metric'] = 'cosine'
            params['normalize_vectors'] = True
            
            if data_profile and not data_profile.is_normalized:
                params['normalize_vectors'] = False
        
        elif algorithm_name == 'hnsw':
            # HNSW parameters based on data characteristics
            if data_profile:
                if data_profile.sample_size < 10000:
                    params['M'] = 16
                    params['efConstruction'] = 200
                elif data_profile.sample_size < 100000:
                    params['M'] = 32
                    params['efConstruction'] = 400
                else:
                    params['M'] = 48
                    params['efConstruction'] = 500
                
                # Search parameter based on accuracy requirements
                accuracy_req = requirements.get('accuracy_threshold', 0.95)
                if accuracy_req > 0.98:
                    params['efSearch'] = 100
                elif accuracy_req > 0.95:
                    params['efSearch'] = 50
                else:
                    params['efSearch'] = 25
        
        elif algorithm_name == 'ivf':
            # IVF parameters
            if data_profile:
                # Number of clusters based on dataset size
                nlist = min(int(np.sqrt(data_profile.sample_size)), 4096)
                params['nlist'] = max(100, nlist)
                
                # Search clusters based on accuracy requirements
                accuracy_req = requirements.get('accuracy_threshold', 0.95)
                if accuracy_req > 0.98:
                    params['nprobe'] = max(params['nlist'] // 4, 10)
                elif accuracy_req > 0.95:
                    params['nprobe'] = max(params['nlist'] // 8, 5)
                else:
                    params['nprobe'] = max(params['nlist'] // 16, 2)
        
        elif algorithm_name == 'lsh':
            # LSH parameters
            if data_profile:
                # Number of permutations based on accuracy requirements
                accuracy_req = requirements.get('accuracy_threshold', 0.95)
                if accuracy_req > 0.98:
                    params['num_perm'] = 256
                elif accuracy_req > 0.95:
                    params['num_perm'] = 128
                else:
                    params['num_perm'] = 64
                
                params['threshold'] = accuracy_req
        
        return params