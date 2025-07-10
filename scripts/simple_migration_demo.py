#!/usr/bin/env python3
"""
VexDB Migration Demo - Simple Version

This script assumes the VexDB server is already running and tests
the adaptive optimization features through the REST API.

Prerequisites:
1. Start the server: venv\\Scripts\\python.exe -m uvicorn vexdb.main:app --reload --port 8000
2. Run this script: venv\\Scripts\\python.exe scripts/simple_migration_demo.py
"""

import requests
import time
import numpy as np
import logging
from datetime import datetime
import json
import os

# Setup logging to both file and console
def setup_logging():
    os.makedirs('logs', exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"logs/simple_demo_{timestamp}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-8s | %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    
    print(f"🔍 Logging to: {log_file}")
    return log_file

class SimpleMigrationDemo:
    def __init__(self, base_url="http://localhost:8000"):
        self.base_url = base_url
        self.logger = logging.getLogger("SimpleMigrationDemo")
        
    def check_server(self):
        """Check if server is running."""
        try:
            response = requests.get(f"{self.base_url}/health", timeout=5)
            if response.status_code == 200:
                self.logger.info("✅ Server is running!")
                return True
            else:
                self.logger.error(f"❌ Server health check failed: {response.status_code}")
                return False
        except requests.RequestException as e:
            self.logger.error(f"❌ Cannot connect to server: {e}")
            self.logger.error("   Make sure the server is running with:")
            self.logger.error("   venv\\Scripts\\python.exe -m uvicorn vexdb.main:app --reload --port 8000")
            return False
    
    def get_system_status(self):
        """Get and log current system status."""
        try:
            response = requests.get(f"{self.base_url}/admin/status")
            if response.status_code == 200:
                status = response.json()
                self.logger.info("📊 System Status:")
                self.logger.info(f"   - Total vectors: {status.get('total_vectors', 0)}")
                self.logger.info(f"   - Active instances: {status.get('active_instances', 0)}")
                self.logger.info(f"   - Primary algorithm: {status.get('primary_algorithm', 'unknown')}")
                return status
            else:
                self.logger.error(f"❌ Failed to get status: {response.status_code}")
        except Exception as e:
            self.logger.error(f"❌ Status request failed: {e}")
        return None
    
    def add_test_vectors(self, count=100, dim=128, batch_size=20):
        """Add test vectors to trigger optimization."""
        self.logger.info(f"📥 Adding {count} test vectors ({dim} dimensions)")
        
        # Generate clustered test data
        cluster_centers = np.random.randn(5, dim)
        vectors = []
        
        for i in range(count):
            cluster_id = i % 5
            # Add noise around cluster center
            vector = cluster_centers[cluster_id] + np.random.normal(0, 0.3, dim)
            
            vectors.append({
                "id": f"test_vector_{i}",
                "embedding": vector.tolist(),
                "metadata": {
                    "cluster": cluster_id,
                    "test_batch": "migration_demo",
                    "index": i
                }
            })
        
        # Add in batches
        total_added = 0
        for i in range(0, len(vectors), batch_size):
            batch = vectors[i:i + batch_size]
            
            try:
                response = requests.post(f"{self.base_url}/vectors/batch", json={"vectors": batch})
                if response.status_code == 200:
                    result = response.json()
                    added = result.get("added", 0)
                    total_added += added
                    self.logger.info(f"✅ Added batch {i//batch_size + 1}: {added} vectors")
                else:
                    self.logger.error(f"❌ Batch {i//batch_size + 1} failed: {response.status_code}")
            except Exception as e:
                self.logger.error(f"❌ Batch error: {e}")
            
            time.sleep(0.1)  # Small delay
        
        self.logger.info(f"📊 Total vectors added: {total_added}/{count}")
        return total_added
    
    def perform_searches(self, num_queries=10):
        """Perform search queries and measure performance."""
        self.logger.info(f"🔍 Performing {num_queries} search queries")
        
        query_times = []
        algorithms_used = []
        
        for i in range(num_queries):
            # Generate random query vector
            query_vector = np.random.randn(128).tolist()
            
            try:
                start_time = time.perf_counter()
                
                response = requests.post(f"{self.base_url}/search", json={
                    "vector": query_vector,
                    "k": 5
                })
                
                end_time = time.perf_counter()
                query_time = (end_time - start_time) * 1000
                
                if response.status_code == 200:
                    result = response.json()
                    algorithm = result.get("algorithm_used", "unknown")
                    results_count = len(result.get("results", []))
                    
                    query_times.append(query_time)
                    algorithms_used.append(algorithm)
                    
                    self.logger.info(f"  Query {i+1}: {query_time:.1f}ms, {results_count} results, {algorithm}")
                else:
                    self.logger.error(f"❌ Query {i+1} failed: {response.status_code}")
                    
            except Exception as e:
                self.logger.error(f"❌ Query {i+1} error: {e}")
        
        if query_times:
            avg_time = np.mean(query_times)
            self.logger.info(f"📈 Average query time: {avg_time:.1f}ms")
            
            # Count algorithm usage
            from collections import Counter
            algo_counts = Counter(algorithms_used)
            self.logger.info(f"🔧 Algorithms used: {dict(algo_counts)}")
        
        return query_times, algorithms_used
    
    def trigger_optimization(self):
        """Trigger manual optimization."""
        self.logger.info("🎯 Triggering optimization...")
        
        try:
            response = requests.post(f"{self.base_url}/admin/optimize")
            if response.status_code == 200:
                result = response.json()
                self.logger.info("✅ Optimization completed!")
                self.logger.info(f"   - Recommended: {result.get('algorithm_name', 'none')}")
                self.logger.info(f"   - Confidence: {result.get('confidence_score', 0):.2f}")
                self.logger.info(f"   - Reasoning: {', '.join(result.get('reasoning', []))}")
                return result
            else:
                self.logger.error(f"❌ Optimization failed: {response.status_code}")
        except Exception as e:
            self.logger.error(f"❌ Optimization error: {e}")
        
        return None
    
    def run_demo(self):
        """Run the complete migration demo."""
        self.logger.info("🚀 Starting VexDB Migration Demo")
        self.logger.info("=" * 50)
        
        # Check server
        if not self.check_server():
            return False
        
        try:
            # Phase 1: Initial state
            self.logger.info("\n🏁 PHASE 1: Initial System State")
            initial_status = self.get_system_status()
            
            # Phase 2: Add small batch of vectors
            self.logger.info("\n🏁 PHASE 2: Adding Initial Vectors")
            added = self.add_test_vectors(count=50, dim=128)
            time.sleep(1)
            self.get_system_status()
            
            # Phase 3: Test initial queries
            self.logger.info("\n🏁 PHASE 3: Initial Performance Test")
            initial_times, initial_algos = self.perform_searches(num_queries=5)
            
            # Phase 4: Add more vectors to trigger optimization
            self.logger.info("\n🏁 PHASE 4: Adding More Vectors")
            added += self.add_test_vectors(count=200, dim=128)
            time.sleep(1)
            
            # Phase 5: Trigger optimization
            self.logger.info("\n🏁 PHASE 5: Optimization Trigger")
            optimization_result = self.trigger_optimization()
            time.sleep(2)  # Wait for potential migration
            self.get_system_status()
            
            # Phase 6: Test performance after optimization
            self.logger.info("\n🏁 PHASE 6: Post-Optimization Performance")
            final_times, final_algos = self.perform_searches(num_queries=10)
            
            # Phase 7: Add different type of data (sparse/high-dim)
            self.logger.info("\n🏁 PHASE 7: Adding Diverse Data")
            self.add_sparse_vectors(count=100, dim=256)
            time.sleep(1)
            
            # Final optimization
            self.logger.info("\n🏁 PHASE 8: Final Optimization")
            final_optimization = self.trigger_optimization()
            time.sleep(2)
            
            # Final state
            self.logger.info("\n🏁 FINAL STATE")
            final_status = self.get_system_status()
            
            # Summary
            self.logger.info("\n🎉 DEMO COMPLETED!")
            self.logger.info("=" * 50)
            if initial_status and final_status:
                self.logger.info(f"📊 Summary:")
                self.logger.info(f"   - Vectors added: {final_status.get('total_vectors', 0)}")
                self.logger.info(f"   - Algorithm evolution: {initial_status.get('primary_algorithm')} → {final_status.get('primary_algorithm')}")
                
                if initial_times and final_times:
                    self.logger.info(f"   - Performance change: {np.mean(initial_times):.1f}ms → {np.mean(final_times):.1f}ms")
            
            return True
            
        except KeyboardInterrupt:
            self.logger.info("\n🛑 Demo interrupted by user")
            return False
        except Exception as e:
            self.logger.error(f"\n❌ Demo failed: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return False
    
    def add_sparse_vectors(self, count=100, dim=256, sparsity=0.7):
        """Add sparse high-dimensional vectors."""
        self.logger.info(f"📥 Adding {count} sparse vectors ({dim}D, {sparsity:.0%} sparse)")
        
        vectors = []
        for i in range(count):
            # Create sparse vector
            vector = np.random.randn(dim)
            mask = np.random.random(dim) < sparsity
            vector[mask] = 0
            
            vectors.append({
                "id": f"sparse_vector_{i}",
                "embedding": vector.tolist(),
                "metadata": {
                    "type": "sparse",
                    "sparsity": sparsity,
                    "dimension": dim
                }
            })
        
        # Add in batch
        try:
            response = requests.post(f"{self.base_url}/vectors/batch", json={"vectors": vectors})
            if response.status_code == 200:
                result = response.json()
                added = result.get("added", 0)
                self.logger.info(f"✅ Added {added} sparse vectors")
                return added
            else:
                self.logger.error(f"❌ Failed to add sparse vectors: {response.status_code}")
        except Exception as e:
            self.logger.error(f"❌ Sparse vector addition error: {e}")
        
        return 0

def main():
    print("🚀 VexDB Simple Migration Demo")
    print("=" * 40)
    print("Prerequisites:")
    print("1. Start server: venv\\Scripts\\python.exe -m uvicorn vexdb.main:app --reload --port 8000")
    print("2. Wait for server to be ready")
    print("3. Run this demo")
    print()
    
    # Setup logging
    log_file = setup_logging()
    
    # Create and run demo
    demo = SimpleMigrationDemo()
    success = demo.run_demo()
    
    print(f"\n{'✅ Demo completed successfully!' if success else '❌ Demo failed'}")
    print(f"📄 Check the log file for details: {log_file}")

if __name__ == "__main__":
    main()
