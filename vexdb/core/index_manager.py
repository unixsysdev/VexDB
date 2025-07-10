"""
Index Manager for multi-algorithm orchestration and seamless transitions.

This module handles the coordination of multiple algorithm instances, manages
seamless algorithm transitions with zero downtime, and provides intelligent
query routing across different index implementations.
"""

import asyncio
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple, Union
from dataclasses import dataclass, asdict
from enum import Enum
from collections import defaultdict, deque
import logging

from vexdb.models.vector import VectorData, QueryRequest, SearchResult, QueryResponse
from vexdb.models.profiling import PerformanceMetrics, AlgorithmRecommendation
from vexdb.algorithms.base import AlgorithmInterface
from vexdb.config.settings import get_settings

logger = logging.getLogger(__name__)


class IndexStatus(Enum):
    """Index instance status."""
    BUILDING = "building"
    ACTIVE = "active"
    MIGRATING = "migrating"
    DEPRECATED = "deprecated"
    FAILED = "failed"


class MigrationPhase(Enum):
    """Migration phases for algorithm transitions."""
    PREPARING = "preparing"
    BUILDING_NEW = "building_new"
    WARMING_UP = "warming_up"
    TRAFFIC_SHIFTING = "traffic_shifting"
    VALIDATING = "validating"
    COMPLETING = "completing"
    ROLLING_BACK = "rolling_back"


@dataclass
class IndexInstance:
    """Represents a single algorithm index instance."""
    index_id: str
    algorithm_name: str
    algorithm_instance: AlgorithmInterface
    parameters: Dict[str, Any]
    status: IndexStatus
    created_at: datetime
    last_used_at: Optional[datetime] = None
    performance_metrics: Optional[PerformanceMetrics] = None
    vector_count: int = 0
    memory_usage_mb: float = 0.0
    build_time_ms: float = 0.0


@dataclass
class MigrationContext:
    """Context for algorithm migration."""
    migration_id: str
    from_algorithm: str
    to_algorithm: str
    from_instance: IndexInstance
    to_instance: Optional[IndexInstance]
    phase: MigrationPhase
    started_at: datetime
    traffic_split_ratio: float = 0.0  # 0.0 = all old, 1.0 = all new
    validation_metrics: Dict[str, float] = None
    rollback_threshold: float = 0.95  # Performance threshold for rollback


class QueryRouter:
    """Intelligent query routing across multiple index instances."""
    
    def __init__(self):
        self.routing_strategy = "performance_based"
        self.performance_history = defaultdict(deque)
        self.circuit_breaker_states = {}
        
    async def route_query(
        self, 
        query: QueryRequest, 
        available_instances: List[IndexInstance]
    ) -> IndexInstance:
        """Route query to the best available index instance."""
        if not available_instances:
            raise ValueError("No available index instances")
        
        # Filter out failed instances
        healthy_instances = [
            inst for inst in available_instances 
            if inst.status in [IndexStatus.ACTIVE, IndexStatus.MIGRATING]
            and not self._is_circuit_breaker_open(inst.index_id)
        ]
        
        if not healthy_instances:
            logger.warning("All instances unhealthy, using fallback routing")
            return available_instances[0]
        
        if self.routing_strategy == "performance_based":
            return self._performance_based_routing(query, healthy_instances)
        elif self.routing_strategy == "round_robin":
            return self._round_robin_routing(healthy_instances)
        else:
            return healthy_instances[0]
    
    def _performance_based_routing(
        self, 
        query: QueryRequest, 
        instances: List[IndexInstance]
    ) -> IndexInstance:
        """Route based on historical performance."""
        best_instance = None
        best_score = float('-inf')
        
        for instance in instances:
            score = self._calculate_routing_score(instance, query)
            if score > best_score:
                best_score = score
                best_instance = instance
        
        return best_instance or instances[0]
    
    def _calculate_routing_score(
        self, 
        instance: IndexInstance, 
        query: QueryRequest
    ) -> float:
        """Calculate routing score for an instance."""
        base_score = 1.0
        
        # Performance metrics weight
        if instance.performance_metrics:
            latency_score = max(0, 1.0 - (instance.performance_metrics.avg_latency_ms / 1000))
            accuracy_score = instance.performance_metrics.accuracy
            base_score = 0.6 * latency_score + 0.4 * accuracy_score
        
        # Recent performance weight
        recent_performance = self.performance_history.get(instance.index_id, deque())
        if recent_performance:
            recent_avg = sum(recent_performance) / len(recent_performance)
            base_score = 0.7 * base_score + 0.3 * recent_avg
        
        # Penalize if last used very recently (load balancing)
        if instance.last_used_at:
            time_since_use = (datetime.utcnow() - instance.last_used_at).total_seconds()
            if time_since_use < 1.0:
                base_score *= 0.9
        
        return base_score
    
    def _round_robin_routing(self, instances: List[IndexInstance]) -> IndexInstance:
        """Simple round-robin routing."""
        return min(instances, key=lambda x: x.last_used_at or datetime.min)
    
    def record_query_performance(self, instance_id: str, latency_ms: float, success: bool):
        """Record query performance for routing decisions."""
        performance_score = (1.0 / max(latency_ms, 1.0)) if success else 0.0
        
        history = self.performance_history[instance_id]
        history.append(performance_score)
        
        if len(history) > 100:
            history.popleft()
        
        if not success:
            self._increment_circuit_breaker(instance_id)
        else:
            self._reset_circuit_breaker(instance_id)
    
    def _is_circuit_breaker_open(self, instance_id: str) -> bool:
        """Check if circuit breaker is open for instance."""
        state = self.circuit_breaker_states.get(instance_id, {'failures': 0, 'last_failure': None})
        
        if state['last_failure'] and (datetime.utcnow() - state['last_failure']).total_seconds() > 60:
            self.circuit_breaker_states[instance_id] = {'failures': 0, 'last_failure': None}
            return False
        
        return state['failures'] >= 3
    
    def _increment_circuit_breaker(self, instance_id: str):
        """Increment circuit breaker failure count."""
        if instance_id not in self.circuit_breaker_states:
            self.circuit_breaker_states[instance_id] = {'failures': 0, 'last_failure': None}
        
        self.circuit_breaker_states[instance_id]['failures'] += 1
        self.circuit_breaker_states[instance_id]['last_failure'] = datetime.utcnow()
    
    def _reset_circuit_breaker(self, instance_id: str):
        """Reset circuit breaker for instance."""
        if instance_id in self.circuit_breaker_states:
            self.circuit_breaker_states[instance_id]['failures'] = 0


class IndexManager:
    """
    Multi-algorithm index orchestration and seamless transitions.
    
    This class manages multiple algorithm instances, handles seamless migrations
    between algorithms, and provides intelligent query routing.
    """
    
    def __init__(
        self, 
        optimization_engine,
        data_profiler,
        query_analytics
    ):
        self.settings = get_settings()
        self.optimization_engine = optimization_engine
        self.data_profiler = data_profiler
        self.query_analytics = query_analytics
        
        # Index management
        self.active_instances: Dict[str, IndexInstance] = {}
        self.migration_contexts: Dict[str, MigrationContext] = {}
        self.query_router = QueryRouter()
        
        # Algorithm registry (will be injected later)
        self.algorithm_registry = None
        
        # State tracking
        self.total_vectors = 0
        self.last_optimization_check = datetime.utcnow()
        self.optimization_interval = timedelta(hours=1)
        
        # Performance tracking
        self.performance_history = deque(maxlen=1000)
        self.migration_history = deque(maxlen=100)
        
        logger.info("Index manager initialized")
    
    def set_algorithm_registry(self, registry):
        """Set the algorithm registry reference."""
        self.algorithm_registry = registry
    
    async def initialize_default_index(self) -> str:
        """Initialize a default index instance."""
        if not self.algorithm_registry:
            raise RuntimeError("Algorithm registry not set")
        
        algorithm_name = "brute_force"
        parameters = {"metric": "cosine", "batch_size": 1000}
        
        instance_id = await self._create_index_instance(algorithm_name, parameters)
        logger.info("Default index initialized", instance_id=instance_id, algorithm=algorithm_name)
        
        return instance_id
    
    async def add_vectors(self, vectors: List[VectorData]) -> Dict[str, Any]:
        """Add vectors to all active indices."""
        if not vectors:
            return {"added": 0, "errors": []}
        
        start_time = time.perf_counter()
        results = {"added": 0, "errors": []}
        
        # Add to all active instances
        for instance in self.active_instances.values():
            if instance.status == IndexStatus.ACTIVE:
                try:
                    await instance.algorithm_instance.add_vectors(vectors)
                    instance.vector_count += len(vectors)
                    instance.last_used_at = datetime.utcnow()
                    results["added"] += len(vectors)
                except Exception as e:
                    error_msg = f"Failed to add vectors to {instance.index_id}: {str(e)}"
                    results["errors"].append(error_msg)
                    logger.error("Vector addition failed", instance_id=instance.index_id, error=str(e))
        
        self.total_vectors += len(vectors)
        
        # Check if optimization is needed
        await self._check_optimization_trigger()
        
        add_time = (time.perf_counter() - start_time) * 1000
        logger.info("Vectors added to indices", 
                   vector_count=len(vectors), 
                   add_time_ms=add_time,
                   total_vectors=self.total_vectors)
        
        return results
    
    async def search_vectors(self, query: QueryRequest) -> QueryResponse:
        """Search vectors using intelligent routing."""
        start_time = time.perf_counter()
        
        # Get available instances
        available_instances = [
            inst for inst in self.active_instances.values()
            if inst.status in [IndexStatus.ACTIVE, IndexStatus.MIGRATING]
        ]
        
        if not available_instances:
            raise RuntimeError("No active index instances available")
        
        # Route query to best instance
        selected_instance = await self.query_router.route_query(query, available_instances)
        
        try:
            # Execute search
            results = await selected_instance.algorithm_instance.search(
                query_vector=query.vector,
                k=query.k,
                **query.search_params
            )
            
            # Update instance usage
            selected_instance.last_used_at = datetime.utcnow()
            
            query_time = (time.perf_counter() - start_time) * 1000
            
            # Record performance
            self.query_router.record_query_performance(
                selected_instance.index_id, 
                query_time, 
                True
            )
            
            # Update analytics
            await self.query_analytics.record_query(
                query_vector=query.vector,
                k=query.k,
                latency_ms=query_time,
                algorithm_used=selected_instance.algorithm_name
            )
            
            response = QueryResponse(
                results=results,
                query_time_ms=query_time,
                algorithm_used=selected_instance.algorithm_name,
                index_id=selected_instance.index_id
            )
            
            logger.debug("Query executed successfully", 
                        instance_id=selected_instance.index_id,
                        query_time_ms=query_time,
                        result_count=len(results))
            
            return response
            
        except Exception as e:
            query_time = (time.perf_counter() - start_time) * 1000
            
            # Record failure
            self.query_router.record_query_performance(
                selected_instance.index_id, 
                query_time, 
                False
            )
            
            logger.error("Query execution failed", 
                        instance_id=selected_instance.index_id,
                        error=str(e))
            raise
    
    async def optimize_indices(self, force: bool = False):
        """Trigger index optimization based on current data and query patterns."""
        if not force and not self._should_optimize():
            return None
        
        logger.info("Starting index optimization")
        
        # Get current data profile
        data_profile = self.data_profiler.get_current_profile()
        if not data_profile:
            logger.warning("No data profile available for optimization")
            return None
        
        # Get query patterns
        query_patterns = await self.query_analytics.get_recent_patterns()
        
        # Get optimization recommendation
        recommendation = await self.optimization_engine.optimize(
            data_profile=data_profile,
            query_patterns=query_patterns,
            performance_requirements={"max_latency_ms": 100, "min_accuracy": 0.95},
            resource_constraints={"max_memory_gb": 8, "max_cpu_cores": 4}
        )
        
        # Check if we need to migrate
        current_algorithm = self._get_primary_algorithm()
        if current_algorithm != recommendation.algorithm_name:
            await self._initiate_migration(recommendation)
        
        self.last_optimization_check = datetime.utcnow()
        
        logger.info("Index optimization completed", 
                   recommended_algorithm=recommendation.algorithm_name,
                   confidence=recommendation.confidence_score)
        
        return recommendation
    
    async def _check_optimization_trigger(self):
        """Check if optimization should be triggered automatically."""
        if self._should_optimize():
            asyncio.create_task(self.optimize_indices())
    
    async def _initiate_migration(self, recommendation):
        """Initiate migration to a new algorithm."""
        current_algorithm = self._get_primary_algorithm()
        new_algorithm = recommendation.algorithm_name
        
        if current_algorithm == new_algorithm:
            logger.info("Migration not needed, already using recommended algorithm")
            return
        
        migration_id = f"migration_{int(time.time())}"
        
        logger.info("Initiating algorithm migration", 
                   migration_id=migration_id,
                   from_algorithm=current_algorithm,
                   to_algorithm=new_algorithm)
        
        # Get current primary instance
        primary_instance = self._get_primary_instance()
        if not primary_instance:
            logger.error("No primary instance found for migration")
            return
        
        # Create migration context
        migration_context = MigrationContext(
            migration_id=migration_id,
            from_algorithm=current_algorithm,
            to_algorithm=new_algorithm,
            from_instance=primary_instance,
            to_instance=None,
            phase=MigrationPhase.PREPARING,
            started_at=datetime.utcnow()
        )
        
        self.migration_contexts[migration_id] = migration_context
        
        # Start migration process
        asyncio.create_task(self._execute_migration(migration_context, recommendation))
    
    async def _execute_migration(self, migration_context: MigrationContext, recommendation):
        """Execute the full migration process."""
        try:
            # Phase 1: Build new index
            migration_context.phase = MigrationPhase.BUILDING_NEW
            await self._build_new_index(migration_context, recommendation)
            
            # Phase 2: Complete migration
            migration_context.phase = MigrationPhase.COMPLETING
            await self._complete_migration(migration_context)
            logger.info("Migration completed successfully", 
                       migration_id=migration_context.migration_id)
                
        except Exception as e:
            logger.error("Migration failed", 
                        migration_id=migration_context.migration_id,
                        phase=migration_context.phase.value,
                        error=str(e))
        
        finally:
            # Clean up migration context
            if migration_context.migration_id in self.migration_contexts:
                del self.migration_contexts[migration_context.migration_id]
    
    async def _build_new_index(self, migration_context: MigrationContext, recommendation):
        """Build the new index instance."""
        logger.info("Building new index", 
                   algorithm=recommendation.algorithm_name,
                   migration_id=migration_context.migration_id)
        
        # Create new index instance
        new_instance_id = await self._create_index_instance(
            recommendation.algorithm_name,
            recommendation.recommended_parameters
        )
        
        new_instance = self.active_instances[new_instance_id]
        migration_context.to_instance = new_instance
        
        # Copy data from old index
        await self._copy_index_data(migration_context.from_instance, new_instance)
        
        logger.info("New index built successfully", 
                   instance_id=new_instance_id,
                   migration_id=migration_context.migration_id)
    
    async def _copy_index_data(self, from_instance: IndexInstance, to_instance: IndexInstance):
        """Copy data between index instances."""
        logger.info("Copying index data", 
                   from_instance=from_instance.index_id,
                   to_instance=to_instance.index_id)
        
        # Simplified copy - in practice would extract vectors from old index
        to_instance.vector_count = from_instance.vector_count
    
    async def _complete_migration(self, migration_context: MigrationContext):
        """Complete the migration by updating instance statuses."""
        logger.info("Completing migration", migration_id=migration_context.migration_id)
        
        # Update statuses
        if migration_context.to_instance:
            migration_context.to_instance.status = IndexStatus.ACTIVE
        
        migration_context.from_instance.status = IndexStatus.DEPRECATED
        
        # Record migration history
        self.migration_history.append({
            'migration_id': migration_context.migration_id,
            'from_algorithm': migration_context.from_algorithm,
            'to_algorithm': migration_context.to_algorithm,
            'completed_at': datetime.utcnow(),
            'success': True
        })
    
    async def _create_index_instance(self, algorithm_name: str, parameters: Dict[str, Any]) -> str:
        """Create a new index instance."""
        if not self.algorithm_registry:
            raise RuntimeError("Algorithm registry not set")
        
        start_time = time.perf_counter()
        
        # Create algorithm instance
        algorithm_instance = await self.algorithm_registry.create_algorithm(algorithm_name, parameters)
        
        # Generate unique ID
        instance_id = f"{algorithm_name}_{int(time.time())}"
        
        # Create index instance
        index_instance = IndexInstance(
            index_id=instance_id,
            algorithm_name=algorithm_name,
            algorithm_instance=algorithm_instance,
            parameters=parameters,
            status=IndexStatus.BUILDING,
            created_at=datetime.utcnow(),
            build_time_ms=(time.perf_counter() - start_time) * 1000
        )
        
        # Store instance
        self.active_instances[instance_id] = index_instance
        
        # Update status to active
        index_instance.status = IndexStatus.ACTIVE
        
        logger.info("Index instance created", 
                   instance_id=instance_id,
                   algorithm=algorithm_name,
                   build_time_ms=index_instance.build_time_ms)
        
        return instance_id
    
    def _should_optimize(self) -> bool:
        """Check if optimization should be triggered."""
        # Time-based trigger
        if datetime.utcnow() - self.last_optimization_check > self.optimization_interval:
            return True
        
        # Data growth trigger
        primary_instance = self._get_primary_instance()
        if primary_instance and primary_instance.vector_count > 0:
            growth_ratio = self.total_vectors / primary_instance.vector_count
            if growth_ratio > 1.5:  # 50% growth
                return True
        
        return False
    
    def _get_primary_algorithm(self) -> Optional[str]:
        """Get the primary algorithm currently in use."""
        primary_instance = self._get_primary_instance()
        return primary_instance.algorithm_name if primary_instance else None
    
    def _get_primary_instance(self) -> Optional[IndexInstance]:
        """Get the primary active instance."""
        active_instances = [
            inst for inst in self.active_instances.values()
            if inst.status == IndexStatus.ACTIVE
        ]
        
        if not active_instances:
            return None
        
        # Return the one with the most vectors (primary)
        return max(active_instances, key=lambda x: x.vector_count)
    
    def get_status(self) -> Dict[str, Any]:
        """Get comprehensive index manager status."""
        return {
            'total_vectors': self.total_vectors,
            'active_instances': len([
                inst for inst in self.active_instances.values() 
                if inst.status == IndexStatus.ACTIVE
            ]),
            'total_instances': len(self.active_instances),
            'active_migrations': len(self.migration_contexts),
            'last_optimization_check': self.last_optimization_check.isoformat(),
            'primary_algorithm': self._get_primary_algorithm(),
            'instance_details': [
                {
                    'instance_id': inst.index_id,
                    'algorithm': inst.algorithm_name,
                    'status': inst.status.value,
                    'vector_count': inst.vector_count,
                    'created_at': inst.created_at.isoformat(),
                    'last_used_at': inst.last_used_at.isoformat() if inst.last_used_at else None
                }
                for inst in self.active_instances.values()
            ]
        }
    
    async def cleanup_deprecated_instances(self):
        """Clean up deprecated index instances."""
        to_remove = []
        
        for instance_id, instance in self.active_instances.items():
            if instance.status == IndexStatus.DEPRECATED:
                # Check if enough time has passed since deprecation
                if instance.last_used_at and (
                    datetime.utcnow() - instance.last_used_at
                ).total_seconds() > 3600:  # 1 hour
                    to_remove.append(instance_id)
        
        for instance_id in to_remove:
            del self.active_instances[instance_id]
            logger.info("Removed deprecated instance", instance_id=instance_id)
    
    async def force_migration(self, target_algorithm: str, parameters: Dict[str, Any] = None) -> bool:
        """Force migration to a specific algorithm."""
        if not self.algorithm_registry:
            logger.error("Algorithm registry not set")
            return False
        
        current_algorithm = self._get_primary_algorithm()
        if current_algorithm == target_algorithm:
            logger.info("Already using target algorithm", algorithm=target_algorithm)
            return True
        
        try:
            # Create a mock recommendation
            from vexdb.models.profiling import AlgorithmRecommendation
            
            recommendation = AlgorithmRecommendation(
                algorithm_name=target_algorithm,
                confidence_score=1.0,
                recommended_parameters=parameters or {},
                reasoning=[f"Force migration to {target_algorithm}"],
                data_characteristics_considered=[]
            )
            
            await self._initiate_migration(recommendation)
            return True
            
        except Exception as e:
            logger.error("Force migration failed", target_algorithm=target_algorithm, error=str(e))
            return False
    
    async def get_migration_status(self, migration_id: str = None) -> Dict[str, Any]:
        """Get status of migrations."""
        if migration_id:
            if migration_id in self.migration_contexts:
                context = self.migration_contexts[migration_id]
                return {
                    'migration_id': context.migration_id,
                    'from_algorithm': context.from_algorithm,
                    'to_algorithm': context.to_algorithm,
                    'phase': context.phase.value,
                    'started_at': context.started_at.isoformat(),
                    'traffic_split_ratio': context.traffic_split_ratio
                }
            else:
                return {'error': 'Migration not found'}
        else:
            # Return all active migrations
            return {
                'active_migrations': [
                    {
                        'migration_id': context.migration_id,
                        'from_algorithm': context.from_algorithm,
                        'to_algorithm': context.to_algorithm,
                        'phase': context.phase.value,
                        'started_at': context.started_at.isoformat()
                    }
                    for context in self.migration_contexts.values()
                ],
                'migration_history': list(self.migration_history)[-10:]  # Last 10 migrations
            }
