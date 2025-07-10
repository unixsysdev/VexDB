"""
Core components package for VexDB.
"""

from .data_profiler import DataProfiler
from .algorithm_registry import AlgorithmRegistry
from .query_analytics import QueryAnalytics
from .optimization_engine import OptimizationEngine
from .index_manager import IndexManager

__all__ = [
    "DataProfiler",
    "AlgorithmRegistry", 
    "QueryAnalytics",
    "OptimizationEngine",
    "IndexManager",
]