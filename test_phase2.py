#!/usr/bin/env python3
"""
Test script for Phase 2 components - Core Intelligence.
"""

import sys
import os
import asyncio
import numpy as np
from typing import List

# Add current directory to Python path
sys.path.insert(0, os.getcwd())

async def test_data_profiler():
    """Test DataProfiler functionality."""
    print("=== Testing Data Profiler ===")
    
    try:
        from vexdb.core.data_profiler import DataProfiler
        from vexdb.models.vector import VectorData
        
        # Create test data
        print("Creating test vectors...")
        test_vectors = []
        for i in range(200):
            # Create some clustered data
            if i < 100:
                # Cluster 1: around [1, 1, 1]
                embedding = np.random.normal([1, 1, 1], 0.2, 3).tolist()
            else:
                # Cluster 2: around [-1, -1, -1]  
                embedding = np.random.normal([-1, -1, -1], 0.2, 3).tolist()
            
            vector = VectorData(
                id=f"test_vector_{i}",
                embedding=embedding,
                metadata={"cluster": 1 if i < 100 else 2}
            )
            test_vectors.append(vector)
        
        # Initialize profiler
        profiler = DataProfiler()
        
        # Analyze dataset
        print("Analyzing dataset...")
        profile = await profiler.analyze_dataset(test_vectors)
        
        print("Dataset analysis completed!")
        print(f"   Sample size: {profile.sample_size}")
        print(f"   Dimensionality: {profile.dimensionality}")
        print(f"   Sparsity ratio: {profile.sparsity_ratio:.3f}")
        print(f"   Has clusters: {profile.has_clusters}")
        print(f"   Estimated clusters: {profile.estimated_clusters}")
        print(f"   Is normalized: {profile.is_normalized}")
        print(f"   Distribution type: {profile.distribution_type}")
        
        # Get algorithm hints
        hints = profiler.get_algorithm_hints(profile)
        print(f"   Algorithm hints: {hints}")
        
        return True
        
    except Exception as e:
        print(f"DataProfiler test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

async def test_algorithm_registry():
    """Test AlgorithmRegistry functionality."""
    print("\n=== Testing Algorithm Registry ===")
    
    try:
        from vexdb.core.algorithm_registry import AlgorithmRegistry
        
        # Initialize registry
        registry = AlgorithmRegistry()
        
        # Check available algorithms
        algorithms = registry.get_available_algorithms()
        print(f" Available algorithms: {algorithms}")
        
        # Get algorithm info
        for algo in algorithms:
            info = registry.get_algorithm_info(algo)
            print(f"   {algo}: available={info['available']}, instance_active={info['instance_active']}")
        
        # Test algorithm instance creation
        if 'brute_force' in algorithms:
            print("Creating brute force algorithm instance...")
            instance = await registry.create_algorithm_instance('brute_force')
            algo_info = instance.get_algorithm_info()
            print(f" Instance created: {algo_info['name']}")
            print(f"   Supports updates: {instance.supports_updates()}")
            print(f"   Supports deletes: {instance.supports_deletes()}")
        
        # Test recommendations (without data profile)
        recommendations = registry.recommend_algorithms()
        print(f" Generated {len(recommendations)} recommendations")
        for rec in recommendations:
            print(f"   {rec.algorithm_name}: confidence={rec.confidence_score:.2f}")
        
        return True
        
    except Exception as e:
        print(f" AlgorithmRegistry test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

async def test_query_analytics():
    """Test QueryAnalytics functionality."""
    print("\n=== Testing Query Analytics ===")
    
    try:
        from vexdb.core.query_analytics import QueryAnalytics
        from vexdb.models.vector import QueryRequest, QueryResponse, SearchResult
        
        # Initialize analytics
        analytics = QueryAnalytics()
        
        # Simulate some queries
        print("Simulating query executions...")
        for i in range(50):
            # Create mock query
            query = QueryRequest(
                query_vector=[0.1, 0.2, 0.3],
                k=10 + (i % 5),  # Vary k values
                similarity_threshold=0.8 if i % 3 == 0 else None
            )
            
            # Create mock response
            results = [
                SearchResult(
                    vector_id=f"result_{j}",
                    similarity_score=0.9 - j * 0.1,
                    metadata={},
                    distance=0.1 + j * 0.1
                ) for j in range(query.k)
            ]
            
            response = QueryResponse(
                results=results,
                total_results=len(results),
                query_time_ms=10.0 + np.random.normal(0, 2),
                algorithm_used='brute_force'
            )
            
            # Record query
            await analytics.record_query(
                query=query,
                response=response,
                algorithm_used='brute_force',
                execution_time_ms=response.query_time_ms
            )
        
        # Get current metrics
        metrics = await analytics.get_current_metrics()
        print(f" Recorded {metrics['total_queries']} queries")
        print(f"   Average latency: {metrics['avg_latency_ms']:.2f}ms")
        print(f"   Error count: {metrics['error_count']}")
        
        # Analyze patterns
        pattern = await analytics.analyze_query_patterns(time_window_hours=1)
        print(f" Pattern analysis completed")
        print(f"   Total queries: {pattern.total_queries}")
        print(f"   Average k value: {pattern.avg_k_value:.1f}")
        print(f"   Common k values: {pattern.common_k_values}")
        print(f"   Threshold usage: {pattern.similarity_threshold_usage:.1%}")
        
        # Get algorithm performance
        performance = await analytics.get_algorithm_performance('brute_force')
        if performance:
            print(f" Algorithm performance metrics available")
            print(f"   Average query time: {performance.avg_query_time_ms:.2f}ms")
            print(f"   P95 query time: {performance.p95_query_time_ms:.2f}ms")
        
        return True
        
    except Exception as e:
        print(f" QueryAnalytics test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

async def test_integration():
    """Test integration between components."""
    print("\n=== Testing Component Integration ===")
    
    try:
        from vexdb.core.data_profiler import DataProfiler
        from vexdb.core.algorithm_registry import AlgorithmRegistry
        from vexdb.models.vector import VectorData
        
        # Create test data
        test_vectors = []
        for i in range(100):
            embedding = np.random.normal(0, 1, 5).tolist()
            vector = VectorData(id=f"vec_{i}", embedding=embedding)
            test_vectors.append(vector)
        
        # Profile the data
        profiler = DataProfiler()
        profile = await profiler.analyze_dataset(test_vectors)
        
        # Get algorithm recommendations based on profile
        registry = AlgorithmRegistry()
        recommendations = registry.recommend_algorithms(
            data_profile=profile,
            requirements={'accuracy_threshold': 0.95}
        )
        
        print(f" Integration test completed")
        print(f"   Data profile created for {profile.sample_size} vectors")
        print(f"   Generated {len(recommendations)} algorithm recommendations")
        
        if recommendations:
            best_rec = recommendations[0]
            print(f"   Best recommendation: {best_rec.algorithm_name} (confidence: {best_rec.confidence_score:.2f})")
            print(f"   Reasoning: {best_rec.reasoning}")
        
        return True
        
    except Exception as e:
        print(f" Integration test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

async def main():
    """Run all tests."""
    print("Testing VexDB Phase 2 Components - Core Intelligence")
    print("=" * 60)
    
    # Run individual component tests
    tests = [
        test_data_profiler,
        test_algorithm_registry,
        test_query_analytics,
        test_integration
    ]
    
    results = []
    for test in tests:
        try:
            result = await test()
            results.append(result)
        except Exception as e:
            print(f" Test failed with exception: {e}")
            results.append(False)
    
    # Summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    
    passed = sum(results)
    total = len(results)
    
    test_names = [
        "Data Profiler",
        "Algorithm Registry", 
        "Query Analytics",
        "Component Integration"
    ]
    
    for i, (name, result) in enumerate(zip(test_names, results)):
        status = " PASSED" if result else " FAILED"
        print(f"{name:20} {status}")
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n All Phase 2 components are working perfectly!")
        print("\nNext steps:")
        print("- Start the VexDB server: start_server.bat")
        print("- Test the enhanced API with core intelligence")
        print("- Ready to begin Phase 3: Algorithm Implementations")
    else:
        print(f"\n  {total - passed} test(s) failed. Check the errors above.")
    
    return passed == total

if __name__ == "__main__":
    asyncio.run(main())