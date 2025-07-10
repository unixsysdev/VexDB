#!/usr/bin/env python3
"""
Quick test script to verify VexDB setup.
"""

import sys
import os

# Add current directory to Python path
sys.path.insert(0, os.getcwd())

try:
    print("Testing VexDB import...")
    import vexdb
    print(f"VexDB v{vexdb.__version__} imported successfully!")
    
    print("\nTesting core imports...")
    from vexdb.config import get_settings
    print("Config module imported")
    
    from vexdb.models.vector import VectorData
    print("Vector models imported")
    
    from vexdb.algorithms.brute_force import BruteForceAlgorithm
    print("Brute force algorithm imported")
    
    print("\nTesting FastAPI app creation...")
    from vexdb.main import create_app
    app = create_app()
    print("FastAPI app created successfully")
    
    print("\nAll tests passed! VexDB is ready to go!")
    
except ImportError as e:
    print(f"Import error: {e}")
    sys.exit(1)
except Exception as e:
    print(f"Error: {e}")
    sys.exit(1)