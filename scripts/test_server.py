#!/usr/bin/env python3
"""
Quick VexDB Server Test - Updated for venv usage
"""
"""
Quick VexDB Server Test

Simple script to start the VexDB server and verify it's working.
"""

import sys
import os
import logging
import requests
import time

# Note: Run this with the venv python: venv\Scripts\python.exe scripts/test_server.py

# Setup basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s'
)

logger = logging.getLogger("VexDBTest")

def test_server():
    """Test basic server functionality."""
    base_url = "http://localhost:8000"
    
    logger.info("🔍 Testing VexDB server functionality...")
    
    # Test health endpoint
    try:
        response = requests.get(f"{base_url}/health")
        if response.status_code == 200:
            logger.info("✅ Health check passed")
        else:
            logger.error(f"❌ Health check failed: {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"❌ Health check error: {e}")
        return False
    
    # Test system status
    try:
        response = requests.get(f"{base_url}/admin/status")
        if response.status_code == 200:
            status = response.json()
            logger.info("✅ System status accessible")
            logger.info(f"   - Primary algorithm: {status.get('primary_algorithm', 'unknown')}")
            logger.info(f"   - Total vectors: {status.get('total_vectors', 0)}")
        else:
            logger.error(f"❌ Status check failed: {response.status_code}")
    except Exception as e:
        logger.error(f"❌ Status check error: {e}")
    
    # Test adding a simple vector
    try:
        test_vector = {
            "id": "test_vector_1",
            "embedding": [0.1, 0.2, 0.3, 0.4, 0.5],
            "metadata": {"test": True}
        }
        
        response = requests.post(f"{base_url}/vectors", json=test_vector)
        if response.status_code == 200:
            logger.info("✅ Vector addition works")
        else:
            logger.error(f"❌ Vector addition failed: {response.status_code}")
    except Exception as e:
        logger.error(f"❌ Vector addition error: {e}")
    
    # Test search
    try:
        search_query = {
            "vector": [0.1, 0.2, 0.3, 0.4, 0.5],
            "k": 5
        }
        
        response = requests.post(f"{base_url}/search", json=search_query)
        if response.status_code == 200:
            result = response.json()
            logger.info(f"✅ Search works - found {len(result.get('results', []))} results")
            logger.info(f"   - Algorithm used: {result.get('algorithm_used', 'unknown')}")
        else:
            logger.error(f"❌ Search failed: {response.status_code}")
    except Exception as e:
        logger.error(f"❌ Search error: {e}")
    
    logger.info("🎉 Basic functionality test completed!")
    return True

if __name__ == "__main__":
    print("🧪 VexDB Quick Server Test")
    print("=" * 30)
    print("Make sure the server is running with:")
    print("python -m uvicorn vexdb.main:app --reload --host 0.0.0.0 --port 8000")
    print()
    
    input("Press Enter when server is running...")
    
    test_server()
