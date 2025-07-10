"""
VexDB - Adaptive Vector Database System

An intelligent vector database that automatically optimizes indexing algorithms
and configurations based on data characteristics and query patterns.
"""

__version__ = "0.1.0"
__author__ = "VexDB Team"
__description__ = "Adaptive Vector Database System"

# Import core components
from .core.data_profiler import DataProfiler
from .core.algorithm_registry import AlgorithmRegistry
from .core.query_analytics import QueryAnalytics
from .algorithms.brute_force import BruteForceAlgorithm

__all__ = [
    "DataProfiler",
    "AlgorithmRegistry",
    "QueryAnalytics",
    "BruteForceAlgorithm",
]