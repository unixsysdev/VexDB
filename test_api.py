#!/usr/bin/env python3
"""
Test script to start VexDB server and perform basic API tests.
"""

import asyncio
import httpx
import json
from typing import List

async def test_api_endpoints():
    """Test basic API endpoints."""
    base_url = "http://localhost:8000"
    
    async with httpx.AsyncClient() as client:
        print("Testing API endpoints...")
        
        # Test root endpoint
        try:
            response = await client.get(f"{base_url}/")
            print(f"✅ Root endpoint: {response.status_code} - {response.json()}")
        except Exception as e:
            print(f"❌ Root endpoint failed: {e}")
            return False
        
        # Test health check
        try:
            response = await client.get(f"{base_url}/health")
            print(f"✅ Health check: {response.status_code} - {response.json()}")
        except Exception as e:
            print(f"❌ Health check failed: {e}")
            return False
        
        # Test available algorithms
        try:
            response = await client.get(f"{base_url}/api/v1/search/algorithms")
            print(f"✅ Algorithms endpoint: {response.status_code}")
            algorithms = response.json()
            print(f"   Available algorithms: {list(algorithms['algorithms'].keys())}")
        except Exception as e:
            print(f"❌ Algorithms endpoint failed: {e}")
            return False
        
        # Test system status
        try:
            response = await client.get(f"{base_url}/api/v1/admin/status")
            print(f"✅ System status: {response.status_code}")
            status = response.json()
            print(f"   System status: {status['status']}")
        except Exception as e:
            print(f"❌ System status failed: {e}")
            return False
        
        print("\n🎉 All API tests passed!")
        return True

if __name__ == "__main__":
    print("Run this after starting the VexDB server with:")
    print("venv\\Scripts\\python.exe -m uvicorn vexdb.main:app --reload")
    print("\nTo test the API endpoints manually:")
    
    print("\n1. Root endpoint: http://localhost:8000/")
    print("2. Health check: http://localhost:8000/health") 
    print("3. API docs: http://localhost:8000/docs")
    print("4. Available algorithms: http://localhost:8000/api/v1/search/algorithms")
    print("5. System status: http://localhost:8000/api/v1/admin/status")
    
    # Run async test if httpx is available
    try:
        import httpx
        print("\n" + "="*50)
        print("Testing API endpoints...")
        asyncio.run(test_api_endpoints())
    except ImportError:
        print("\nInstall httpx to run automated API tests: pip install httpx")
    except Exception as e:
        print(f"\nAPI tests failed: {e}")
        print("Make sure the server is running first!")