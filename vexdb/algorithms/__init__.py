"""
Algorithms package for VexDB.
"""

from .base import AlgorithmInterface, AlgorithmCapabilities
from .brute_force import BruteForceAlgorithm

from .ivf import IVFAlgorithm
from .lsh import LSHAlgorithm
# TODO: Import HNSW when available
# from .hnsw import HNSWAlgorithm

__all__ = [
    "AlgorithmInterface",
    "AlgorithmCapabilities",
    "BruteForceAlgorithm",
    "IVFAlgorithm",
    "LSHAlgorithm",
    # TODO: Add HNSW when available
    # "HNSWAlgorithm",
]