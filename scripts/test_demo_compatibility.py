#!/usr/bin/env python3
"""
Test the comprehensive migration demo script components - Windows compatible
"""

import sys
import os
import subprocess
import time

def test_imports():
    """Test if all required imports work in venv."""
    print("Testing imports in virtual environment...")
    
    test_script = '''
import requests
import numpy as np
import logging
import json
import subprocess
print("All imports successful!")
'''
    
    try:
        result = subprocess.run(
            ['venv\\Scripts\\python.exe', '-c', test_script],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode == 0:
            print("PASS: Import test passed!")
            return True
        else:
            print(f"FAIL: Import test failed: {result.stderr}")
            return False
    except Exception as e:
        print(f"FAIL: Import test error: {e}")
        return False

def test_vexdb_module():
    """Test if VexDB module loads properly."""
    print("Testing VexDB module loading...")
    
    test_script = '''
try:
    import vexdb
    import vexdb.main
    print("VexDB module loads successfully!")
except Exception as e:
    print(f"VexDB module error: {e}")
    import traceback
    traceback.print_exc()
'''
    
    try:
        result = subprocess.run(
            ['venv\\Scripts\\python.exe', '-c', test_script],
            capture_output=True,
            text=True,
            timeout=15
        )
        
        print(f"Return code: {result.returncode}")
        print(f"Output: {result.stdout}")
        if result.stderr:
            print(f"Errors: {result.stderr[:300]}")
        
        return result.returncode == 0
        
    except Exception as e:
        print(f"FAIL: VexDB test error: {e}")
        return False

def test_server_start():
    """Test if we can start the server programmatically."""
    print("Testing server startup...")
    
    try:
        # Start server in background
        server_process = subprocess.Popen(
            ['venv\\Scripts\\python.exe', '-m', 'uvicorn', 'vexdb.main:app', '--host', '0.0.0.0', '--port', '8001'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        # Wait a bit for startup
        time.sleep(5)
        
        # Check if process is still running
        if server_process.poll() is None:
            print("PASS: Server started successfully!")
            
            # Test health endpoint
            try:
                import requests
                response = requests.get("http://localhost:8001/health", timeout=5)
                if response.status_code == 200:
                    print("PASS: Server health check passed!")
                    server_process.terminate()
                    server_process.wait()
                    return True
                else:
                    print(f"FAIL: Health check failed: {response.status_code}")
            except Exception as e:
                print(f"FAIL: Health check error: {e}")
        else:
            print("FAIL: Server failed to start")
            stdout, stderr = server_process.communicate()
            print(f"STDOUT: {stdout.decode()[:200] if stdout else 'None'}")
            print(f"STDERR: {stderr.decode()[:200] if stderr else 'None'}")
        
        # Cleanup
        server_process.terminate()
        server_process.wait()
        return False
        
    except Exception as e:
        print(f"FAIL: Server test error: {e}")
        return False

def main():
    print("VexDB Comprehensive Demo Test Suite")
    print("=" * 50)
    
    # Run tests
    tests = [
        ("Import Dependencies", test_imports),
        ("VexDB Module Loading", test_vexdb_module),
        ("Server Startup", test_server_start),
    ]
    
    results = {}
    for test_name, test_func in tests:
        print(f"\n{test_name}:")
        print("-" * 30)
        results[test_name] = test_func()
    
    # Summary
    print(f"\nTest Results Summary:")
    print("=" * 50)
    for test_name, passed in results.items():
        status = "PASSED" if passed else "FAILED"
        print(f"{test_name:<25} {status}")
    
    all_passed = all(results.values())
    print(f"\n{'All tests passed!' if all_passed else 'Some tests failed'}")
    
    if all_passed:
        print("\nThe comprehensive migration demo should work!")
        print("Run: venv\\Scripts\\python.exe scripts/adaptive_migration_demo.py")
    else:
        print("\nFix the failing tests before running the comprehensive demo")
        print("Use the simple demo instead: venv\\Scripts\\python.exe scripts/simple_migration_demo.py")

if __name__ == "__main__":
    main()
