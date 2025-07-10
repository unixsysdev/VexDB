"""
Query Analytics for VexDB.

This module monitors and analyzes query patterns to understand workload
characteristics, detect trends, and inform optimization decisions.
"""

import asyncio
import time
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from collections import defaultdict, deque
import numpy as np
import structlog

from ..models.vector import QueryRequest, QueryResponse
from ..models.profiling import QueryPattern, PerformanceMetrics

logger = structlog.get_logger()


class QueryAnalytics:
    """
    Monitors and analyzes query patterns for optimization insights.
    
    The QueryAnalytics component tracks query frequency, patterns, performance,
    and resource usage to provide insights for algorithm optimization and
    system tuning.
    """
    
    def __init__(self, history_size: int = 10000):
        """
        Initialize query analytics.
        
        Args:
            history_size: Maximum number of query records to keep in memory
        """
        self.history_size = history_size
        
        # Query history storage
        self.query_history: deque = deque(maxlen=history_size)
        
        # Real-time metrics
        self.current_metrics = {
            'total_queries': 0,
            'queries_in_last_minute': 0,
            'queries_in_last_hour': 0,
            'avg_latency_ms': 0.0,
            'error_count': 0,
            'active_connections': 0
        }
        
        # Performance tracking by algorithm
        self.algorithm_performance: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        
        # Query pattern caches
        self.hourly_patterns: Dict[int, int] = defaultdict(int)  # Hour -> query count
        self.daily_patterns: Dict[str, int] = defaultdict(int)   # Day -> query count
        self.k_value_distribution: Dict[int, int] = defaultdict(int)
        
        # Time-based buckets for real-time analytics
        self.minute_buckets: deque = deque(maxlen=60)  # Last 60 minutes
        self.hour_buckets: deque = deque(maxlen=24)    # Last 24 hours
        
        # Initialize time buckets
        now = datetime.utcnow()
        for i in range(60):
            bucket_time = now - timedelta(minutes=i)
            self.minute_buckets.append({'timestamp': bucket_time, 'count': 0, 'total_latency': 0.0})
        
        for i in range(24):
            bucket_time = now - timedelta(hours=i)
            self.hour_buckets.append({'timestamp': bucket_time, 'count': 0, 'total_latency': 0.0})
        
        logger.info("Query analytics initialized", history_size=history_size)
    
    async def record_query(
        self, 
        query: QueryRequest, 
        response: QueryResponse, 
        algorithm_used: str,
        execution_time_ms: float,
        error: Optional[str] = None
    ) -> None:
        """
        Record a query execution for analytics.
        
        Args:
            query: The query request
            response: The query response
            algorithm_used: Algorithm that processed the query
            execution_time_ms: Query execution time
            error: Error message if query failed
        """
        timestamp = datetime.utcnow()
        
        # Create query record
        query_record = {
            'timestamp': timestamp,
            'query_id': response.query_id if response else None,
            'k': query.k,
            'similarity_threshold': query.similarity_threshold,
            'algorithm_used': algorithm_used,
            'execution_time_ms': execution_time_ms,
            'results_count': len(response.results) if response else 0,
            'vector_dimension': len(query.query_vector),
            'has_metadata_filters': bool(query.metadata_filters),
            'algorithm_hint': query.algorithm_hint,
            'error': error,
            'success': error is None
        }
        
        # Add to history
        self.query_history.append(query_record)
        
        # Update real-time metrics
        await self._update_real_time_metrics(query_record)
        
        # Update algorithm performance tracking
        self._update_algorithm_performance(algorithm_used, query_record)
        
        # Update pattern caches
        self._update_pattern_caches(query_record)
        
        logger.debug("Query recorded for analytics", 
                    query_id=query_record['query_id'],
                    algorithm=algorithm_used,
                    execution_time_ms=execution_time_ms)
    
    async def get_current_metrics(self) -> Dict[str, Any]:
        """
        Get current real-time metrics.
        
        Returns:
            Dict: Current performance metrics
        """
        # Update time-based metrics
        await self._refresh_time_based_metrics()
        
        return self.current_metrics.copy()
    
    async def analyze_query_patterns(
        self, 
        time_window_hours: int = 24
    ) -> QueryPattern:
        """
        Analyze query patterns over a time window.
        
        Args:
            time_window_hours: Time window for analysis
            
        Returns:
            QueryPattern: Query pattern analysis
        """
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=time_window_hours)
        
        # Filter queries in time window
        relevant_queries = [
            q for q in self.query_history 
            if start_time <= q['timestamp'] <= end_time and q['success']
        ]
        
        if not relevant_queries:
            # Return empty pattern if no queries
            return QueryPattern(
                time_window_start=start_time,
                time_window_end=end_time,
                total_queries=0,
                queries_per_second=0.0,
                peak_qps=0.0,
                avg_k_value=0.0,
                common_k_values=[],
                similarity_threshold_usage=0.0,
                hourly_distribution={},
                daily_distribution={},
                avg_latency_ms=0.0,
                p95_latency_ms=0.0,
                p99_latency_ms=0.0,
                algorithm_usage={}
            )
        
        # Calculate basic metrics
        total_queries = len(relevant_queries)
        time_span_seconds = time_window_hours * 3600
        queries_per_second = total_queries / time_span_seconds if time_span_seconds > 0 else 0.0
        
        # Calculate k-value statistics
        k_values = [q['k'] for q in relevant_queries]
        avg_k_value = np.mean(k_values) if k_values else 0.0
        
        # Find most common k values
        k_counts = defaultdict(int)
        for k in k_values:
            k_counts[k] += 1
        common_k_values = sorted(k_counts.items(), key=lambda x: x[1], reverse=True)[:5]
        common_k_values = [k for k, _ in common_k_values]
        
        # Calculate similarity threshold usage
        threshold_queries = [q for q in relevant_queries if q['similarity_threshold'] is not None]
        threshold_usage = len(threshold_queries) / total_queries if total_queries > 0 else 0.0
        
        # Calculate temporal patterns
        hourly_dist = defaultdict(int)
        daily_dist = defaultdict(int)
        
        for query in relevant_queries:
            hour = query['timestamp'].hour
            day = query['timestamp'].strftime('%A')
            hourly_dist[hour] += 1
            daily_dist[day] += 1
        
        # Calculate latency statistics
        latencies = [q['execution_time_ms'] for q in relevant_queries]
        avg_latency = np.mean(latencies) if latencies else 0.0
        p95_latency = np.percentile(latencies, 95) if latencies else 0.0
        p99_latency = np.percentile(latencies, 99) if latencies else 0.0
        
        # Calculate peak QPS (using 1-minute windows)
        peak_qps = await self._calculate_peak_qps(relevant_queries)
        
        # Calculate algorithm usage
        algorithm_counts = defaultdict(int)
        for query in relevant_queries:
            algorithm_counts[query['algorithm_used']] += 1
        
        # Create pattern analysis
        pattern = QueryPattern(
            time_window_start=start_time,
            time_window_end=end_time,
            total_queries=total_queries,
            queries_per_second=queries_per_second,
            peak_qps=peak_qps,
            avg_k_value=avg_k_value,
            common_k_values=common_k_values,
            similarity_threshold_usage=threshold_usage,
            hourly_distribution=dict(hourly_dist),
            daily_distribution=dict(daily_dist),
            avg_latency_ms=avg_latency,
            p95_latency_ms=p95_latency,
            p99_latency_ms=p99_latency,
            algorithm_usage=dict(algorithm_counts)
        )
        
        logger.info("Query pattern analysis completed",
                   time_window_hours=time_window_hours,
                   total_queries=total_queries,
                   avg_latency_ms=avg_latency)
        
        return pattern
    
    async def get_algorithm_performance(
        self, 
        algorithm_name: str, 
        time_window_hours: int = 24
    ) -> Optional[PerformanceMetrics]:
        """
        Get performance metrics for a specific algorithm.
        
        Args:
            algorithm_name: Algorithm to analyze
            time_window_hours: Time window for analysis
            
        Returns:
            PerformanceMetrics: Algorithm performance or None if no data
        """
        if algorithm_name not in self.algorithm_performance:
            return None
        
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=time_window_hours)
        
        # Filter performance records
        relevant_records = [
            record for record in self.algorithm_performance[algorithm_name]
            if start_time <= record['timestamp'] <= end_time
        ]
        
        if not relevant_records:
            return None
        
        # Calculate performance statistics
        latencies = [r['execution_time_ms'] for r in relevant_records]
        dimensions = [r['vector_dimension'] for r in relevant_records]
        
        metrics = PerformanceMetrics(
            algorithm_name=algorithm_name,
            avg_query_time_ms=float(np.mean(latencies)),
            median_query_time_ms=float(np.median(latencies)),
            p95_query_time_ms=float(np.percentile(latencies, 95)),
            p99_query_time_ms=float(np.percentile(latencies, 99)),
            recall_at_k={},  # TODO: Calculate recall if ground truth available
            precision_at_k={},  # TODO: Calculate precision
            mean_average_precision=0.0,  # TODO: Calculate MAP
            memory_usage_mb=0.0,  # TODO: Get from algorithm instance
            cpu_usage_percent=0.0,  # TODO: Monitor CPU usage
            index_build_time_ms=None,
            queries_per_second=len(relevant_records) / (time_window_hours * 3600),
            max_concurrent_queries=1,  # TODO: Track concurrency
            dataset_size=0,  # TODO: Get from algorithm instance
            vector_dimensionality=int(np.mean(dimensions)) if dimensions else 0
        )
        
        return metrics
    
    def get_trending_queries(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get trending query patterns.
        
        Args:
            limit: Maximum number of trends to return
            
        Returns:
            List[Dict]: Trending query information
        """
        # Analyze recent query patterns
        recent_queries = list(self.query_history)[-1000:]  # Last 1000 queries
        
        # Group by characteristics
        k_trends = defaultdict(int)
        threshold_trends = defaultdict(int)
        dimension_trends = defaultdict(int)
        
        for query in recent_queries:
            k_trends[query['k']] += 1
            
            if query['similarity_threshold']:
                threshold_bucket = round(query['similarity_threshold'], 1)
                threshold_trends[threshold_bucket] += 1
            
            dim_bucket = (query['vector_dimension'] // 100) * 100  # Bucket by 100s
            dimension_trends[dim_bucket] += 1
        
        trends = []
        
        # Top k values
        top_k_values = sorted(k_trends.items(), key=lambda x: x[1], reverse=True)[:limit//2]
        for k, count in top_k_values:
            trends.append({
                'type': 'k_value',
                'value': k,
                'count': count,
                'percentage': count / len(recent_queries) * 100 if recent_queries else 0
            })
        
        # Top dimensions
        top_dimensions = sorted(dimension_trends.items(), key=lambda x: x[1], reverse=True)[:limit//2]
        for dim, count in top_dimensions:
            trends.append({
                'type': 'dimension_range',
                'value': f"{dim}-{dim+99}",
                'count': count,
                'percentage': count / len(recent_queries) * 100 if recent_queries else 0
            })
        
        return trends[:limit]
    
    def detect_anomalies(self, threshold_std: float = 2.0) -> List[Dict[str, Any]]:
        """
        Detect query pattern anomalies.
        
        Args:
            threshold_std: Standard deviation threshold for anomaly detection
            
        Returns:
            List[Dict]: Detected anomalies
        """
        anomalies = []
        
        if len(self.query_history) < 100:
            return anomalies  # Need sufficient data
        
        # Analyze latency anomalies
        recent_latencies = [q['execution_time_ms'] for q in list(self.query_history)[-100:]]
        mean_latency = np.mean(recent_latencies)
        std_latency = np.std(recent_latencies)
        
        for query in list(self.query_history)[-20:]:  # Check last 20 queries
            if abs(query['execution_time_ms'] - mean_latency) > threshold_std * std_latency:
                anomalies.append({
                    'type': 'latency_anomaly',
                    'query_id': query['query_id'],
                    'timestamp': query['timestamp'],
                    'value': query['execution_time_ms'],
                    'expected': mean_latency,
                    'deviation': abs(query['execution_time_ms'] - mean_latency) / std_latency
                })
        
        # Analyze query frequency anomalies
        hourly_counts = defaultdict(int)
        for query in list(self.query_history)[-1000:]:
            hour_bucket = query['timestamp'].replace(minute=0, second=0, microsecond=0)
            hourly_counts[hour_bucket] += 1
        
        if len(hourly_counts) > 5:
            counts = list(hourly_counts.values())
            mean_count = np.mean(counts)
            std_count = np.std(counts)
            
            for hour, count in list(hourly_counts.items())[-5:]:  # Check last 5 hours
                if abs(count - mean_count) > threshold_std * std_count:
                    anomalies.append({
                        'type': 'frequency_anomaly',
                        'timestamp': hour,
                        'value': count,
                        'expected': mean_count,
                        'deviation': abs(count - mean_count) / std_count if std_count > 0 else 0
                    })
        
        return anomalies
    
    async def _update_real_time_metrics(self, query_record: Dict[str, Any]) -> None:
        """Update real-time metrics with new query."""
        self.current_metrics['total_queries'] += 1
        
        if query_record['error']:
            self.current_metrics['error_count'] += 1
        
        # Update rolling averages
        if self.current_metrics['total_queries'] == 1:
            self.current_metrics['avg_latency_ms'] = query_record['execution_time_ms']
        else:
            # Exponential moving average
            alpha = 0.1
            self.current_metrics['avg_latency_ms'] = (
                alpha * query_record['execution_time_ms'] + 
                (1 - alpha) * self.current_metrics['avg_latency_ms']
            )
        
        # Update time buckets
        current_minute = query_record['timestamp'].replace(second=0, microsecond=0)
        current_hour = query_record['timestamp'].replace(minute=0, second=0, microsecond=0)
        
        # Update minute bucket
        if self.minute_buckets and self.minute_buckets[-1]['timestamp'].replace(second=0, microsecond=0) == current_minute:
            self.minute_buckets[-1]['count'] += 1
            self.minute_buckets[-1]['total_latency'] += query_record['execution_time_ms']
        else:
            self.minute_buckets.append({
                'timestamp': current_minute,
                'count': 1,
                'total_latency': query_record['execution_time_ms']
            })
        
        # Update hour bucket
        if self.hour_buckets and self.hour_buckets[-1]['timestamp'].replace(minute=0, second=0, microsecond=0) == current_hour:
            self.hour_buckets[-1]['count'] += 1
            self.hour_buckets[-1]['total_latency'] += query_record['execution_time_ms']
        else:
            self.hour_buckets.append({
                'timestamp': current_hour,
                'count': 1,
                'total_latency': query_record['execution_time_ms']
            })
    
    def _update_algorithm_performance(self, algorithm_name: str, query_record: Dict[str, Any]) -> None:
        """Update algorithm-specific performance tracking."""
        performance_record = {
            'timestamp': query_record['timestamp'],
            'execution_time_ms': query_record['execution_time_ms'],
            'results_count': query_record['results_count'],
            'vector_dimension': query_record['vector_dimension'],
            'k': query_record['k'],
            'success': query_record['success']
        }
        
        self.algorithm_performance[algorithm_name].append(performance_record)
        
        # Keep only recent records (last 1000 per algorithm)
        self.algorithm_performance[algorithm_name] = \
            self.algorithm_performance[algorithm_name][-1000:]
    
    def _update_pattern_caches(self, query_record: Dict[str, Any]) -> None:
        """Update pattern caches for quick access."""
        timestamp = query_record['timestamp']
        
        # Update hourly patterns
        self.hourly_patterns[timestamp.hour] += 1
        
        # Update daily patterns
        day_name = timestamp.strftime('%A')
        self.daily_patterns[day_name] += 1
        
        # Update k-value distribution
        self.k_value_distribution[query_record['k']] += 1
    
    async def _refresh_time_based_metrics(self) -> None:
        """Refresh time-based metrics from buckets."""
        now = datetime.utcnow()
        
        # Count queries in last minute
        minute_ago = now - timedelta(minutes=1)
        queries_last_minute = sum(
            bucket['count'] for bucket in self.minute_buckets
            if bucket['timestamp'] >= minute_ago
        )
        self.current_metrics['queries_in_last_minute'] = queries_last_minute
        
        # Count queries in last hour
        hour_ago = now - timedelta(hours=1)
        queries_last_hour = sum(
            bucket['count'] for bucket in self.hour_buckets
            if bucket['timestamp'] >= hour_ago
        )
        self.current_metrics['queries_in_last_hour'] = queries_last_hour
    
    async def _calculate_peak_qps(self, queries: List[Dict[str, Any]]) -> float:
        """Calculate peak queries per second in 1-minute windows."""
        if not queries:
            return 0.0
        
        # Group queries by minute
        minute_counts = defaultdict(int)
        for query in queries:
            minute_bucket = query['timestamp'].replace(second=0, microsecond=0)
            minute_counts[minute_bucket] += 1
        
        # Find peak
        if minute_counts:
            peak_per_minute = max(minute_counts.values())
            return peak_per_minute / 60.0  # Convert to per second
        
        return 0.0