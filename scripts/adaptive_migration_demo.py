#!/usr/bin/env python3
"""
VexDB Adaptive Migration Demo Script

This script demonstrates the adaptive optimization system by:
1. Starting the VexDB API server
2. Using the REST API to add different types of vector data
3. Triggering automatic optimizations and migrations via API
4. Logging all activities for analysis
5. Testing query performance across different algorithms
"""

import asyncio
import logging
import time
import numpy as np
from datetime import datetime
from typing import List, Dict, Any
import json
import os
import sys
import requests
import threading
import subprocess
from concurrent.futures import ThreadPoolExecutor

# Setup comprehensive logging
def setup_logging():
    """Setup detailed logging to both file and console."""
    # Create logs directory if it doesn't exist
    os.makedirs('logs', exist_ok=True)
    
    # Create timestamp for log file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"logs/vexdb_migration_demo_{timestamp}.log"
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s | %(name)-20s | %(levelname)-8s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # File handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    
    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    # Reduce noise from some libraries
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    
    print(f"🔍 Logging to: {log_file}")
    return log_file

class VexDBAPIDemo:
    """Demo class to test VexDB adaptive optimization via REST API."""
    
    def __init__(self, base_url="http://localhost:8000"):
        self.logger = logging.getLogger("VexDBAPIDemo")
        self.base_url = base_url
        self.server_process = None
        self.performance_metrics = []
        
    def start_server(self):
        """Start the VexDB API server."""
        self.logger.info("🚀 Starting VexDB API server...")
        
        try:
            # Start server in background
            cmd = [sys.executable, "-m", "uvicorn", "vexdb.main:app", 
                   "--host", "0.0.0.0", "--port", "8000", "--log-level", "info"]
            
            self.server_process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=os.getcwd()
            )
            
            # Wait for server to start
            max_retries = 30
            for i in range(max_retries):
                try:
                    response = requests.get(f"{self.base_url}/health", timeout=2)
                    if response.status_code == 200:
                        self.logger.info("✅ VexDB API server is running!")
                        return True
                except requests.RequestException:
                    if i < max_retries - 1:
                        self.logger.info(f"⏳ Waiting for server to start... ({i+1}/{max_retries})")
                        time.sleep(2)
                    continue
            
            self.logger.error("❌ Failed to start VexDB API server")
            return False
            
        except Exception as e:
            self.logger.error(f"❌ Error starting server: {e}")
            return False
    
    def stop_server(self):
        """Stop the VexDB API server."""
        if self.server_process:
            self.logger.info("🛑 Stopping VexDB API server...")
            self.server_process.terminate()
            self.server_process.wait()
            self.logger.info("✅ Server stopped")
    
    def check_server_health(self):
        """Check if the server is healthy."""
        try:
            response = requests.get(f"{self.base_url}/health")
            return response.status_code == 200
        except:
            return False
    
    def generate_test_datasets(self):
        """Generate different types of vector datasets to test adaptive behavior."""
        self.logger.info("📊 Generating test datasets for adaptive optimization")
        
        datasets = {}
        
        # Dataset 1: Small, clustered data (should start with brute_force, maybe migrate to IVF)
        self.logger.info("Generating clustered dataset (1000 vectors, 128 dims)")
        datasets['clustered_small'] = self._generate_clustered_vectors(1000, 128, num_clusters=5)
        
        # Dataset 2: High-dimensional sparse data (should trigger LSH)
        self.logger.info("Generating sparse high-dimensional dataset (2000 vectors, 512 dims)")
        datasets['sparse_high_dim'] = self._generate_sparse_vectors(2000, 512, sparsity=0.7)
        
        # Dataset 3: Large clustered dataset (should trigger IVF)
        self.logger.info("Generating large clustered dataset (5000 vectors, 256 dims)")
        datasets['large_clustered'] = self._generate_clustered_vectors(5000, 256, num_clusters=10)
        
        # Dataset 4: Normalized vectors (good for cosine similarity)
        self.logger.info("Generating normalized dataset (3000 vectors, 384 dims)")
        datasets['normalized'] = self._generate_normalized_vectors(3000, 384)
        
        return datasets
    
    def _generate_clustered_vectors(self, num_vectors: int, dim: int, num_clusters: int) -> List[Dict]:
        """Generate vectors with natural clustering."""
        vectors = []
        vectors_per_cluster = num_vectors // num_clusters
        
        # Generate cluster centers
        cluster_centers = np.random.randn(num_clusters, dim)
        
        for cluster_id in range(num_clusters):
            for i in range(vectors_per_cluster):
                # Generate vector around cluster center with some noise
                vector = cluster_centers[cluster_id] + np.random.normal(0, 0.3, dim)
                
                vectors.append({
                    "id": f"clustered_{cluster_id}_{i}",
                    "embedding": vector.tolist(),
                    "metadata": {
                        "cluster": cluster_id,
                        "dataset": "clustered",
                        "type": "synthetic"
                    }
                })
        
        return vectors
    
    def _generate_sparse_vectors(self, num_vectors: int, dim: int, sparsity: float) -> List[Dict]:
        """Generate sparse high-dimensional vectors."""
        vectors = []
        
        for i in range(num_vectors):
            # Create sparse vector
            vector = np.random.randn(dim)
            # Make it sparse by zeroing out random elements
            mask = np.random.random(dim) < sparsity
            vector[mask] = 0
            
            vectors.append({
                "id": f"sparse_{i}",
                "embedding": vector.tolist(),
                "metadata": {
                    "sparsity": sparsity,
                    "dataset": "sparse",
                    "type": "synthetic"
                }
            })
        
        return vectors
    
    def _generate_normalized_vectors(self, num_vectors: int, dim: int) -> List[Dict]:
        """Generate normalized vectors (unit length)."""
        vectors = []
        
        for i in range(num_vectors):
            # Generate random vector and normalize
            vector = np.random.randn(dim)
            vector = vector / np.linalg.norm(vector)
            
            vectors.append({
                "id": f"normalized_{i}",
                "embedding": vector.tolist(),
                "metadata": {
                    "norm": float(np.linalg.norm(vector)),
                    "dataset": "normalized",
                    "type": "synthetic"
                }
            })
        
        return vectors
    
    def add_vectors_batch(self, vectors: List[Dict], batch_size: int = 100):
        """Add vectors to VexDB via API in batches."""
        self.logger.info(f"📥 Adding {len(vectors)} vectors in batches of {batch_size}")
        
        total_added = 0
        errors = []
        
        for i in range(0, len(vectors), batch_size):
            batch = vectors[i:i + batch_size]
            
            try:
                response = requests.post(
                    f"{self.base_url}/vectors/batch",
                    json={"vectors": batch},
                    timeout=30
                )
                
                if response.status_code == 200:
                    result = response.json()
                    added_count = result.get("added", 0)
                    total_added += added_count
                    
                    self.logger.info(f"✅ Batch {i//batch_size + 1}: Added {added_count} vectors")
                    
                    if result.get("errors"):
                        errors.extend(result["errors"])
                        self.logger.warning(f"⚠️  Batch had {len(result['errors'])} errors")
                else:
                    error_msg = f"HTTP {response.status_code}: {response.text}"
                    errors.append(error_msg)
                    self.logger.error(f"❌ Batch {i//batch_size + 1} failed: {error_msg}")
                
            except Exception as e:
                error_msg = f"Request failed: {str(e)}"
                errors.append(error_msg)
                self.logger.error(f"❌ Batch {i//batch_size + 1} error: {error_msg}")
            
            # Small delay between batches
            time.sleep(0.1)
        
        self.logger.info(f"📊 Total vectors added: {total_added}/{len(vectors)}")
        if errors:
            self.logger.warning(f"⚠️  Total errors: {len(errors)}")
        
        return total_added, errors
    
    def perform_search_queries(self, query_vectors: List[List[float]], k: int = 10, num_queries: int = 20):
        """Perform multiple search queries and measure performance."""
        self.logger.info(f"🔍 Performing {num_queries} search queries (k={k})")
        
        query_times = []
        algorithms_used = []
        
        for i in range(min(num_queries, len(query_vectors))):
            query_vector = query_vectors[i]
            
            try:
                start_time = time.perf_counter()
                
                response = requests.post(
                    f"{self.base_url}/search",
                    json={
                        "vector": query_vector,
                        "k": k,
                        "search_params": {}
                    },
                    timeout=10
                )
                
                end_time = time.perf_counter()
                query_time = (end_time - start_time) * 1000
                
                if response.status_code == 200:
                    result = response.json()
                    algorithm_used = result.get("algorithm_used", "unknown")
                    results_count = len(result.get("results", []))
                    
                    query_times.append(query_time)
                    algorithms_used.append(algorithm_used)
                    
                    self.logger.info(f"🎯 Query {i+1}: {query_time:.1f}ms, "
                                   f"{results_count} results, algorithm: {algorithm_used}")
                else:
                    self.logger.error(f"❌ Query {i+1} failed: HTTP {response.status_code}")
                
            except Exception as e:
                self.logger.error(f"❌ Query {i+1} error: {str(e)}")
            
            # Small delay between queries
            time.sleep(0.05)
        
        if query_times:
            avg_time = np.mean(query_times)
            p95_time = np.percentile(query_times, 95)
            self.logger.info(f"📈 Query performance: avg={avg_time:.1f}ms, p95={p95_time:.1f}ms")
            
            # Log algorithm distribution
            from collections import Counter
            algo_counts = Counter(algorithms_used)
            self.logger.info(f"🔧 Algorithms used: {dict(algo_counts)}")
        
        return query_times, algorithms_used
    
    def get_system_status(self):
        """Get current system status and metrics."""
        try:
            response = requests.get(f"{self.base_url}/admin/status")
            if response.status_code == 200:
                status = response.json()
                self.logger.info("📊 System Status:")
                self.logger.info(f"   - Total vectors: {status.get('total_vectors', 0)}")
                self.logger.info(f"   - Active instances: {status.get('active_instances', 0)}")
                self.logger.info(f"   - Primary algorithm: {status.get('primary_algorithm', 'unknown')}")
                return status
        except Exception as e:
            self.logger.error(f"❌ Failed to get system status: {e}")
        return None
    
    def trigger_optimization(self):
        """Trigger manual optimization."""
        self.logger.info("🎯 Triggering manual optimization...")
        try:
            response = requests.post(f"{self.base_url}/admin/optimize")
            if response.status_code == 200:
                result = response.json()
                self.logger.info("✅ Optimization completed!")
                self.logger.info(f"   - Recommended algorithm: {result.get('algorithm_name', 'none')}")
                self.logger.info(f"   - Confidence: {result.get('confidence_score', 0):.2f}")
                return result
        except Exception as e:
            self.logger.error(f"❌ Optimization failed: {e}")
        return None
    
    def get_analytics(self):
        """Get performance analytics."""
        try:
            response = requests.get(f"{self.base_url}/analytics/performance")
            if response.status_code == 200:
                analytics = response.json()
                self.logger.info("📈 Performance Analytics:")
                self.logger.info(f"   - Query patterns: {len(analytics.get('query_patterns', []))}")
                self.logger.info(f"   - Data profile available: {analytics.get('has_data_profile', False)}")
                return analytics
        except Exception as e:
            self.logger.error(f"❌ Failed to get analytics: {e}")
        return None
    
    async def run_complete_demo(self):
        """Run the complete adaptive optimization demo."""
        self.logger.info("🎯 Starting VexDB Adaptive Migration Demo via REST API")
        
        try:
            # Phase 1: Start server and check health
            if not self.start_server():
                self.logger.error("❌ Failed to start server, aborting demo")
                return
            
            # Wait a bit more for full initialization
            time.sleep(3)
            
            # Phase 2: Generate test data
            datasets = self.generate_test_datasets()
            
            # Phase 3: Load small clustered dataset first
            self.logger.info("🏁 Phase 1: Loading small clustered dataset")
            self.get_system_status()
            
            added, errors = self.add_vectors_batch(datasets['clustered_small'][:500], batch_size=50)
            self.logger.info(f"📥 Added {added} vectors from clustered_small dataset")
            
            # Perform some queries
            query_vectors = [v["embedding"] for v in datasets['clustered_small'][:10]]
            self.perform_search_queries(query_vectors, k=5, num_queries=10)
            
            # Check system status
            self.get_system_status()
            
            # Phase 4: Add more data to trigger optimization
            self.logger.info("🏁 Phase 2: Adding more data to trigger optimization")
            
            added, errors = self.add_vectors_batch(datasets['clustered_small'][500:], batch_size=50)
            self.logger.info(f"📥 Added {added} more vectors")
            
            # Trigger optimization manually
            time.sleep(2)
            optimization_result = self.trigger_optimization()
            
            # Wait for potential migration
            time.sleep(5)
            self.get_system_status()
            
            # Phase 5: Load sparse high-dimensional data
            self.logger.info("🏁 Phase 3: Loading sparse high-dimensional dataset")
            
            added, errors = self.add_vectors_batch(datasets['sparse_high_dim'][:1000], batch_size=100)
            self.logger.info(f"📥 Added {added} sparse vectors")
            
            # Test queries with sparse data
            sparse_queries = [v["embedding"] for v in datasets['sparse_high_dim'][:5]]
            self.perform_search_queries(sparse_queries, k=10, num_queries=5)
            
            # Trigger optimization
            time.sleep(2)
            optimization_result = self.trigger_optimization()
            time.sleep(5)
            
            # Phase 6: Load large clustered dataset
            self.logger.info("🏁 Phase 4: Loading large clustered dataset")
            
            added, errors = self.add_vectors_batch(datasets['large_clustered'][:2000], batch_size=200)
            self.logger.info(f"📥 Added {added} large clustered vectors")
            
            # Test performance with larger dataset
            large_queries = [v["embedding"] for v in datasets['large_clustered'][:10]]
            self.perform_search_queries(large_queries, k=15, num_queries=15)
            
            # Final optimization
            time.sleep(2)
            optimization_result = self.trigger_optimization()
            time.sleep(5)
            
            # Phase 7: Final status and analytics
            self.logger.info("🏁 Phase 5: Final system analysis")
            final_status = self.get_system_status()
            analytics = self.get_analytics()
            
            # Performance comparison
            self.logger.info("🔄 Running final performance test...")
            mixed_queries = query_vectors + sparse_queries + large_queries
            final_times, final_algos = self.perform_search_queries(mixed_queries, k=10, num_queries=20)
            
            # Summary
            self.logger.info("🎉 Demo completed successfully!")
            self.logger.info("📊 Summary:")
            if final_status:
                self.logger.info(f"   - Final algorithm: {final_status.get('primary_algorithm')}")
                self.logger.info(f"   - Total vectors: {final_status.get('total_vectors')}")
                self.logger.info(f"   - Active instances: {final_status.get('active_instances')}")
            
            if final_times:
                self.logger.info(f"   - Final avg query time: {np.mean(final_times):.1f}ms")
            
        except KeyboardInterrupt:
            self.logger.info("🛑 Demo interrupted by user")
        except Exception as e:
            self.logger.error(f"❌ Demo failed with error: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
        finally:
            # Cleanup
            self.stop_server()

def main():
    """Main entry point."""
    print("🚀 VexDB Adaptive Migration Demo")
    print("=" * 50)
    
    # Setup logging
    log_file = setup_logging()
    
    # Create and run demo
    demo = VexDBAPIDemo()
    
    try:
        # Run the demo
        asyncio.run(demo.run_complete_demo())
        
        print(f"\n✅ Demo completed! Check the log file for details:")
        print(f"📄 {log_file}")
        
    except KeyboardInterrupt:
        print("\n🛑 Demo interrupted")
    except Exception as e:
        print(f"\n❌ Demo failed: {e}")
    finally:
        # Ensure server is stopped
        demo.stop_server()

if __name__ == "__main__":
    main()
