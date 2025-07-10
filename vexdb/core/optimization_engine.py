"""
Optimization Engine for intelligent algorithm selection and parameter tuning.

This module implements the core machine learning-based optimization system
that automatically selects algorithms and tunes parameters based on data
characteristics, query patterns, and performance feedback.
"""

import asyncio
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import numpy as np
from dataclasses import dataclass, asdict
from collections import defaultdict, deque
import logging

# ML imports
try:
    from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier
    from sklearn.linear_model import LinearRegression, LogisticRegression
    from sklearn.model_selection import train_test_split
    from sklearn.preprocessing import StandardScaler
    from sklearn.metrics import mean_squared_error, accuracy_score
except ImportError:
    # Fallback for environments without sklearn
    RandomForestRegressor = None
    RandomForestClassifier = None

from vexdb.models.profiling import (
    DataProfile, 
    QueryPattern, 
    PerformanceMetrics, 
    AlgorithmRecommendation,
    DataCharacteristic
)
from vexdb.config.settings import get_settings

logger = logging.getLogger(__name__)


@dataclass
class TrainingExample:
    """Single training example for ML models."""
    features: List[float]
    algorithm: str
    parameters: Dict[str, Any]
    performance: PerformanceMetrics
    timestamp: datetime


@dataclass
class OptimizationContext:
    """Context for optimization decisions."""
    data_profile: DataProfile
    query_patterns: List[QueryPattern]
    performance_requirements: Dict[str, float]
    resource_constraints: Dict[str, float]
    current_algorithm: Optional[str] = None


class PerformancePredictor:
    """ML model for predicting algorithm performance."""
    
    def __init__(self):
        self.latency_model = None
        self.accuracy_model = None
        self.memory_model = None
        self.scaler = StandardScaler()
        self.is_trained = False
        self.feature_names = []
        
    def extract_features(self, context: OptimizationContext) -> List[float]:
        """Extract feature vector from optimization context."""
        profile = context.data_profile
        
        features = [
            # Data characteristics
            profile.dimensionality,
            profile.sample_size,
            profile.sparsity_ratio,
            profile.effective_dimensions,
            len(profile.mean_vector) if profile.mean_vector else 0,
            float(profile.has_clusters),
            profile.estimated_clusters or 0,
            profile.silhouette_score or 0,
            float(profile.is_normalized),
            
            # Query patterns
            len(context.query_patterns),
            np.mean([qp.avg_k for qp in context.query_patterns]) if context.query_patterns else 5,
            np.mean([qp.frequency for qp in context.query_patterns]) if context.query_patterns else 1,
            
            # Resource constraints
            context.resource_constraints.get('max_memory_gb', 8),
            context.resource_constraints.get('max_cpu_cores', 4),
            
            # Performance requirements
            context.performance_requirements.get('max_latency_ms', 100),
            context.performance_requirements.get('min_accuracy', 0.95),
        ]
        
        return features
    
    def train(self, examples: List[TrainingExample]) -> bool:
        """Train performance prediction models."""
        if not examples or len(examples) < 10:
            logger.warning("Insufficient training data", example_count=len(examples))
            return False
            
        if RandomForestRegressor is None:
            logger.warning("Scikit-learn not available, cannot train models")
            return False
        
        try:
            # Prepare training data
            X = np.array([ex.features for ex in examples])
            y_latency = np.array([ex.performance.avg_latency_ms for ex in examples])
            y_accuracy = np.array([ex.performance.accuracy for ex in examples])
            y_memory = np.array([ex.performance.memory_usage_mb for ex in examples])
            
            # Scale features
            X_scaled = self.scaler.fit_transform(X)
            
            # Train models
            self.latency_model = RandomForestRegressor(n_estimators=100, random_state=42)
            self.accuracy_model = RandomForestRegressor(n_estimators=100, random_state=42)
            self.memory_model = RandomForestRegressor(n_estimators=100, random_state=42)
            
            self.latency_model.fit(X_scaled, y_latency)
            self.accuracy_model.fit(X_scaled, y_accuracy)
            self.memory_model.fit(X_scaled, y_memory)
            
            self.is_trained = True
            
            # Log training results
            latency_score = self.latency_model.score(X_scaled, y_latency)
            accuracy_score_val = self.accuracy_model.score(X_scaled, y_accuracy)
            memory_score = self.memory_model.score(X_scaled, y_memory)
            
            logger.info("Performance models trained successfully",
                       latency_r2=latency_score,
                       accuracy_r2=accuracy_score_val,
                       memory_r2=memory_score,
                       training_examples=len(examples))
            
            return True
            
        except Exception as e:
            logger.error("Failed to train performance models", error=str(e))
            return False
    
    def predict(self, context: OptimizationContext) -> Optional[Dict[str, float]]:
        """Predict performance metrics for given context."""
        if not self.is_trained:
            return None
            
        try:
            features = self.extract_features(context)
            features_scaled = self.scaler.transform([features])
            
            predictions = {
                'predicted_latency_ms': float(self.latency_model.predict(features_scaled)[0]),
                'predicted_accuracy': float(self.accuracy_model.predict(features_scaled)[0]),
                'predicted_memory_mb': float(self.memory_model.predict(features_scaled)[0])
            }
            
            return predictions
            
        except Exception as e:
            logger.error("Failed to predict performance", error=str(e))
            return None


class AlgorithmSelector:
    """Intelligent algorithm selection based on data characteristics and requirements."""
    
    def __init__(self):
        self.algorithm_profiles = self._load_algorithm_profiles()
        self.selection_model = None
        self.scaler = StandardScaler()
        self.is_trained = False
        
    def _load_algorithm_profiles(self) -> Dict[str, Dict[str, Any]]:
        """Load algorithm characteristic profiles."""
        return {
            'brute_force': {
                'strengths': ['perfect_accuracy', 'simple_implementation'],
                'weaknesses': ['poor_scalability'],
                'optimal_conditions': {
                    'max_dataset_size': 10000,
                    'dimensionality_range': (1, float('inf')),
                    'accuracy_requirement': 1.0
                },
                'resource_multipliers': {
                    'time_complexity': 'O(n)',
                    'space_complexity': 'O(n*d)',
                    'cpu_intensive': True
                }
            },
            'hnsw': {
                'strengths': ['fast_search', 'good_accuracy', 'memory_efficient'],
                'weaknesses': ['build_time', 'parameter_sensitive'],
                'optimal_conditions': {
                    'min_dataset_size': 1000,
                    'dimensionality_range': (50, 2000),
                    'has_clusters': True
                },
                'resource_multipliers': {
                    'time_complexity': 'O(log n)',
                    'space_complexity': 'O(n*M)',
                    'build_cost_high': True
                }
            },
            'ivf': {
                'strengths': ['scalable', 'cluster_aware', 'configurable_accuracy'],
                'weaknesses': ['requires_clusters', 'parameter_tuning'],
                'optimal_conditions': {
                    'min_dataset_size': 50000,
                    'has_clusters': True,
                    'estimated_clusters_min': 50
                },
                'resource_multipliers': {
                    'time_complexity': 'O(sqrt(n))',
                    'space_complexity': 'O(n*d + k*d)',
                    'quantization_friendly': True
                }
            },
            'lsh': {
                'strengths': ['high_dimensional', 'sparse_friendly', 'approximate'],
                'weaknesses': ['parameter_sensitive', 'accuracy_trade_off'],
                'optimal_conditions': {
                    'min_dimensionality': 500,
                    'sparsity_ratio_min': 0.3,
                    'normalized_vectors': True
                },
                'resource_multipliers': {
                    'time_complexity': 'O(d*L)',
                    'space_complexity': 'O(n*L)',
                    'hash_computation': True
                }
            }
        }
    
    def select_algorithm(self, context: OptimizationContext) -> str:
        """Select the best algorithm for given context."""
        if self.is_trained and self.selection_model:
            return self._ml_based_selection(context)
        else:
            return self._rule_based_selection(context)
    
    def _rule_based_selection(self, context: OptimizationContext) -> str:
        """Rule-based algorithm selection fallback."""
        profile = context.data_profile
        
        # Simple decision tree based on data characteristics
        if profile.sample_size < 1000:
            return 'brute_force'
        
        if profile.dimensionality > 1000 and profile.sparsity_ratio > 0.5:
            return 'lsh'
        
        if profile.sample_size > 100000 and profile.has_clusters:
            return 'ivf'
        
        # Default to HNSW for most cases
        return 'hnsw'
    
    def _ml_based_selection(self, context: OptimizationContext) -> str:
        """ML-based algorithm selection."""
        try:
            features = self._extract_selection_features(context)
            features_scaled = self.scaler.transform([features])
            prediction = self.selection_model.predict(features_scaled)[0]
            return prediction
        except Exception as e:
            logger.error("ML selection failed, falling back to rules", error=str(e))
            return self._rule_based_selection(context)
    
    def _extract_selection_features(self, context: OptimizationContext) -> List[float]:
        """Extract features for algorithm selection."""
        profile = context.data_profile
        
        return [
            profile.dimensionality,
            np.log10(profile.sample_size),
            profile.sparsity_ratio,
            float(profile.has_clusters),
            profile.estimated_clusters or 0,
            profile.silhouette_score or 0,
            float(profile.is_normalized),
            len(context.query_patterns),
            context.performance_requirements.get('min_accuracy', 0.95),
            context.resource_constraints.get('max_memory_gb', 8)
        ]
    
    def train_selection_model(self, examples: List[TrainingExample]) -> bool:
        """Train the algorithm selection model."""
        if not examples or len(examples) < 20:
            return False
            
        if LogisticRegression is None:
            return False
        
        try:
            # Prepare data
            contexts = []
            algorithms = []
            
            for ex in examples:
                # Reconstruct context from features (simplified)
                contexts.append(ex.features)
                algorithms.append(ex.algorithm)
            
            X = np.array(contexts)
            y = np.array(algorithms)
            
            # Scale features
            X_scaled = self.scaler.fit_transform(X)
            
            # Train model
            self.selection_model = LogisticRegression(random_state=42)
            self.selection_model.fit(X_scaled, y)
            
            self.is_trained = True
            
            score = self.selection_model.score(X_scaled, y)
            logger.info("Algorithm selection model trained", accuracy=score)
            
            return True
            
        except Exception as e:
            logger.error("Failed to train selection model", error=str(e))
            return False


class ParameterTuner:
    """Intelligent parameter tuning for selected algorithms."""
    
    def __init__(self):
        self.algorithm_defaults = self._load_algorithm_defaults()
        self.tuning_history = defaultdict(list)
        
    def _load_algorithm_defaults(self) -> Dict[str, Dict[str, Any]]:
        """Load default parameters for each algorithm."""
        return {
            'hnsw': {
                'M': 16,
                'efConstruction': 200,
                'ef': 50,
                'maxM': 16,
                'maxM0': 32
            },
            'ivf': {
                'nlist': 100,
                'nprobe': 10,
                'quantizer_type': 'flat',
                'metric': 'L2'
            },
            'lsh': {
                'num_hashtables': 10,
                'hash_size': 20,
                'num_hyperplanes': None,  # Will be set based on dimensionality
                'seed': 42
            },
            'brute_force': {
                'metric': 'cosine',
                'batch_size': 1000
            }
        }
    
    def tune_parameters(
        self, 
        algorithm: str, 
        context: OptimizationContext
    ) -> Dict[str, Any]:
        """Tune parameters for the selected algorithm."""
        if algorithm not in self.algorithm_defaults:
            logger.warning("Unknown algorithm for tuning", algorithm=algorithm)
            return {}
        
        base_params = self.algorithm_defaults[algorithm].copy()
        
        # Apply context-specific tuning
        if algorithm == 'hnsw':
            return self._tune_hnsw_parameters(base_params, context)
        elif algorithm == 'ivf':
            return self._tune_ivf_parameters(base_params, context)
        elif algorithm == 'lsh':
            return self._tune_lsh_parameters(base_params, context)
        else:
            return base_params
    
    def _tune_hnsw_parameters(
        self, 
        base_params: Dict[str, Any], 
        context: OptimizationContext
    ) -> Dict[str, Any]:
        """Tune HNSW-specific parameters."""
        profile = context.data_profile
        params = base_params.copy()
        
        # Adjust M based on dimensionality and dataset size
        if profile.dimensionality < 100:
            params['M'] = 8
        elif profile.dimensionality > 500:
            params['M'] = 32
        
        # Adjust efConstruction based on accuracy requirements and dataset size
        accuracy_req = context.performance_requirements.get('min_accuracy', 0.95)
        if accuracy_req > 0.98:
            params['efConstruction'] = 400
            params['ef'] = 100
        elif accuracy_req < 0.9:
            params['efConstruction'] = 100
            params['ef'] = 25
        
        # Adjust based on dataset size
        if profile.sample_size > 1000000:
            params['efConstruction'] = min(params['efConstruction'] * 2, 800)
        
        return params
    
    def _tune_ivf_parameters(
        self, 
        base_params: Dict[str, Any], 
        context: OptimizationContext
    ) -> Dict[str, Any]:
        """Tune IVF-specific parameters."""
        profile = context.data_profile
        params = base_params.copy()
        
        # Set nlist based on dataset size and estimated clusters
        if profile.estimated_clusters:
            params['nlist'] = min(profile.estimated_clusters * 2, 
                                max(int(np.sqrt(profile.sample_size)), 100))
        else:
            params['nlist'] = max(int(np.sqrt(profile.sample_size)), 100)
        
        # Adjust nprobe based on accuracy requirements
        accuracy_req = context.performance_requirements.get('min_accuracy', 0.95)
        if accuracy_req > 0.98:
            params['nprobe'] = min(params['nlist'] // 4, 50)
        else:
            params['nprobe'] = min(params['nlist'] // 10, 20)
        
        return params
    
    def _tune_lsh_parameters(
        self, 
        base_params: Dict[str, Any], 
        context: OptimizationContext
    ) -> Dict[str, Any]:
        """Tune LSH-specific parameters."""
        profile = context.data_profile
        params = base_params.copy()
        
        # Set number of hyperplanes based on dimensionality
        params['num_hyperplanes'] = min(profile.dimensionality // 10, 100)
        
        # Adjust hash tables based on accuracy requirements
        accuracy_req = context.performance_requirements.get('min_accuracy', 0.95)
        if accuracy_req > 0.95:
            params['num_hashtables'] = 20
        elif accuracy_req < 0.85:
            params['num_hashtables'] = 5
        
        # Adjust hash size based on dataset size
        if profile.sample_size > 1000000:
            params['hash_size'] = 32
        elif profile.sample_size < 10000:
            params['hash_size'] = 12
        
        return params


class OptimizationEngine:
    """
    Core optimization engine for intelligent algorithm selection and parameter tuning.
    
    This class orchestrates the entire optimization process, combining data profiling,
    query pattern analysis, and machine learning to make optimal algorithm decisions.
    """
    
    def __init__(self):
        self.settings = get_settings()
        self.performance_predictor = PerformancePredictor()
        self.algorithm_selector = AlgorithmSelector()
        self.parameter_tuner = ParameterTuner()
        
        # Training data storage
        self.training_examples = deque(maxlen=10000)
        self.optimization_history = deque(maxlen=1000)
        
        # Current state
        self.current_recommendations = {}
        self.last_optimization_time = None
        
        logger.info("Optimization engine initialized")
    
    async def optimize(
        self, 
        data_profile: DataProfile,
        query_patterns: List[QueryPattern] = None,
        performance_requirements: Dict[str, float] = None,
        resource_constraints: Dict[str, float] = None
    ) -> AlgorithmRecommendation:
        """
        Generate algorithm recommendation based on current context.
        
        Args:
            data_profile: Current data characteristics
            query_patterns: Recent query patterns
            performance_requirements: Required performance levels
            resource_constraints: Available resources
            
        Returns:
            AlgorithmRecommendation: Complete recommendation with reasoning
        """
        start_time = time.perf_counter()
        
        # Set defaults
        query_patterns = query_patterns or []
        performance_requirements = performance_requirements or {
            'max_latency_ms': 100,
            'min_accuracy': 0.95
        }
        resource_constraints = resource_constraints or {
            'max_memory_gb': 8,
            'max_cpu_cores': 4
        }
        
        # Create optimization context
        context = OptimizationContext(
            data_profile=data_profile,
            query_patterns=query_patterns,
            performance_requirements=performance_requirements,
            resource_constraints=resource_constraints
        )
        
        # Select algorithm
        selected_algorithm = self.algorithm_selector.select_algorithm(context)
        
        # Tune parameters
        tuned_parameters = self.parameter_tuner.tune_parameters(selected_algorithm, context)
        
        # Predict performance
        performance_prediction = self.performance_predictor.predict(context)
        
        # Generate reasoning
        reasoning = self._generate_reasoning(selected_algorithm, context)
        
        # Create recommendation
        recommendation = AlgorithmRecommendation(
            algorithm_name=selected_algorithm,
            confidence_score=self._calculate_confidence(selected_algorithm, context),
            recommended_parameters=tuned_parameters,
            reasoning=reasoning,
            data_characteristics_considered=self._extract_characteristics(data_profile),
            predicted_query_time_ms=performance_prediction.get('predicted_latency_ms') if performance_prediction else None,
            predicted_accuracy=performance_prediction.get('predicted_accuracy') if performance_prediction else None,
            predicted_memory_usage_mb=performance_prediction.get('predicted_memory_mb') if performance_prediction else None,
            alternative_algorithms=self._generate_alternatives(context, selected_algorithm)
        )
        
        # Store recommendation
        self.current_recommendations[selected_algorithm] = recommendation
        self.last_optimization_time = datetime.utcnow()
        
        optimization_time = (time.perf_counter() - start_time) * 1000
        logger.info("Optimization completed",
                   algorithm=selected_algorithm,
                   confidence=recommendation.confidence_score,
                   optimization_time_ms=optimization_time)
        
        return recommendation
    
    def add_performance_feedback(
        self,
        algorithm: str,
        parameters: Dict[str, Any],
        performance: PerformanceMetrics,
        context_features: List[float]
    ):
        """Add performance feedback for model training."""
        example = TrainingExample(
            features=context_features,
            algorithm=algorithm,
            parameters=parameters,
            performance=performance,
            timestamp=datetime.utcnow()
        )
        
        self.training_examples.append(example)
        
        # Trigger retraining if we have enough new examples
        if len(self.training_examples) % 100 == 0:
            asyncio.create_task(self._retrain_models())
    
    async def _retrain_models(self):
        """Retrain ML models with accumulated data."""
        if len(self.training_examples) < 50:
            return
        
        logger.info("Starting model retraining", examples=len(self.training_examples))
        
        try:
            # Train performance predictor
            predictor_success = self.performance_predictor.train(list(self.training_examples))
            
            # Train algorithm selector
            selector_success = self.algorithm_selector.train_selection_model(list(self.training_examples))
            
            logger.info("Model retraining completed",
                       predictor_trained=predictor_success,
                       selector_trained=selector_success)
                       
        except Exception as e:
            logger.error("Model retraining failed", error=str(e))
    
    def _generate_reasoning(self, algorithm: str, context: OptimizationContext) -> List[str]:
        """Generate human-readable reasoning for algorithm selection."""
        reasoning = []
        profile = context.data_profile
        
        if algorithm == 'brute_force':
            reasoning.append(f"Small dataset size ({profile.sample_size:,} vectors) makes brute force optimal")
            reasoning.append("Perfect accuracy guaranteed with brute force approach")
            
        elif algorithm == 'hnsw':
            reasoning.append("HNSW selected for good balance of speed and accuracy")
            if profile.has_clusters:
                reasoning.append("Data clustering detected, which benefits HNSW performance")
            if profile.dimensionality > 100:
                reasoning.append(f"High dimensionality ({profile.dimensionality}) suits HNSW")
                
        elif algorithm == 'ivf':
            reasoning.append(f"Large dataset ({profile.sample_size:,} vectors) benefits from IVF")
            if profile.has_clusters:
                reasoning.append(f"Strong clustering ({profile.estimated_clusters} clusters) ideal for IVF")
                
        elif algorithm == 'lsh':
            reasoning.append(f"High dimensionality ({profile.dimensionality}) suits LSH")
            if profile.sparsity_ratio > 0.3:
                reasoning.append(f"Sparse data ({profile.sparsity_ratio:.1%} zeros) benefits from LSH")
        
        # Add performance requirements reasoning
        accuracy_req = context.performance_requirements.get('min_accuracy', 0.95)
        if accuracy_req > 0.98:
            reasoning.append("High accuracy requirement considered in parameter tuning")
        
        return reasoning
    
    def _calculate_confidence(self, algorithm: str, context: OptimizationContext) -> float:
        """Calculate confidence score for the recommendation."""
        base_confidence = 0.7
        
        # Increase confidence based on data profile completeness
        if context.data_profile.silhouette_score is not None:
            base_confidence += 0.1
        
        # Increase confidence if we have query patterns
        if context.query_patterns:
            base_confidence += 0.1
        
        # Increase confidence if models are trained
        if self.performance_predictor.is_trained:
            base_confidence += 0.1
        
        return min(base_confidence, 1.0)
    
    def _extract_characteristics(self, profile: DataProfile) -> List[DataCharacteristic]:
        """Extract key data characteristics for reasoning."""
        characteristics = []
        
        characteristics.append(DataCharacteristic(
            name="dimensionality",
            value=profile.dimensionality,
            impact="high",
            description=f"Vector dimensionality: {profile.dimensionality}"
        ))
        
        characteristics.append(DataCharacteristic(
            name="dataset_size",
            value=profile.sample_size,
            impact="high",
            description=f"Dataset size: {profile.sample_size:,} vectors"
        ))
        
        if profile.has_clusters:
            characteristics.append(DataCharacteristic(
                name="clustering",
                value=profile.estimated_clusters or 0,
                impact="medium",
                description=f"Data has {profile.estimated_clusters} clusters"
            ))
        
        if profile.sparsity_ratio > 0.1:
            characteristics.append(DataCharacteristic(
                name="sparsity",
                value=profile.sparsity_ratio,
                impact="medium", 
                description=f"Sparsity ratio: {profile.sparsity_ratio:.1%}"
            ))
        
        return characteristics
    
    def _generate_alternatives(
        self, 
        context: OptimizationContext, 
        selected: str
    ) -> List[Dict[str, Any]]:
        """Generate alternative algorithm recommendations."""
        alternatives = []
        
        # Get all possible algorithms
        algorithms = ['brute_force', 'hnsw', 'ivf', 'lsh']
        
        for algo in algorithms:
            if algo != selected:
                # Simple scoring for alternatives
                score = self._score_algorithm(algo, context)
                params = self.parameter_tuner.tune_parameters(algo, context)
                
                alternatives.append({
                    'algorithm': algo,
                    'score': score,
                    'parameters': params,
                    'reason': f"Alternative option with score {score:.2f}"
                })
        
        # Sort by score and return top 2
        alternatives.sort(key=lambda x: x['score'], reverse=True)
        return alternatives[:2]
    
    def _score_algorithm(self, algorithm: str, context: OptimizationContext) -> float:
        """Simple scoring function for algorithm alternatives."""
        profile = context.data_profile
        
        if algorithm == 'brute_force':
            return 1.0 if profile.sample_size < 10000 else 0.1
        elif algorithm == 'hnsw':
            score = 0.8
            if profile.has_clusters:
                score += 0.1
            return min(score, 1.0)
        elif algorithm == 'ivf':
            score = 0.0
            if profile.sample_size > 50000:
                score += 0.4
            if profile.has_clusters:
                score += 0.4
            return min(score, 1.0)
        elif algorithm == 'lsh':
            score = 0.0
            if profile.dimensionality > 500:
                score += 0.4
            if profile.sparsity_ratio > 0.3:
                score += 0.3
            return min(score, 1.0)
        
        return 0.5
    
    def get_optimization_status(self) -> Dict[str, Any]:
        """Get current optimization engine status."""
        return {
            'is_predictor_trained': self.performance_predictor.is_trained,
            'is_selector_trained': self.algorithm_selector.is_trained,
            'training_examples_count': len(self.training_examples),
            'last_optimization_time': self.last_optimization_time.isoformat() if self.last_optimization_time else None,
            'current_recommendations_count': len(self.current_recommendations),
            'optimization_history_count': len(self.optimization_history)
        }
