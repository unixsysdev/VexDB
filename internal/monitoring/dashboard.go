package monitoring

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

// Dashboard provides monitoring dashboards and endpoints
type Dashboard struct {
	logger     *zap.Logger
	config     *DashboardConfig
	collector  *Collector
	tracer     *Tracer
	httpServer *http.Server
	mu         sync.RWMutex
}

// DashboardConfig represents dashboard configuration
type DashboardConfig struct {
	Enabled        bool          `yaml:"enabled" json:"enabled"`
	Host           string        `yaml:"host" json:"host"`
	Port           int           `yaml:"port" json:"port"`
	BasePath       string        `yaml:"base_path" json:"base_path"`
	ReadTimeout    time.Duration `yaml:"read_timeout" json:"read_timeout"`
	WriteTimeout   time.Duration `yaml:"write_timeout" json:"write_timeout"`
	IdleTimeout    time.Duration `yaml:"idle_timeout" json:"idle_timeout"`
	EnableProfiling bool         `yaml:"enable_profiling" json:"enable_profiling"`
	ProfilingPath  string        `yaml:"profiling_path" json:"profiling_path"`
	EnablePprof    bool          `yaml:"enable_pprof" json:"enable_pprof"`
	PprofPath      string        `yaml:"pprof_path" json:"pprof_path"`
	EnableMetrics  bool          `yaml:"enable_metrics" json:"enable_metrics"`
	MetricsPath    string        `yaml:"metrics_path" json:"metrics_path"`
	EnableHealth   bool          `yaml:"enable_health" json:"enable_health"`
	HealthPath     string        `yaml:"health_path" json:"health_path"`
	EnableDebug    bool          `yaml:"enable_debug" json:"enable_debug"`
	DebugPath      string        `yaml:"debug_path" json:"debug_path"`
}

// DefaultDashboardConfig returns default dashboard configuration
func DefaultDashboardConfig() *DashboardConfig {
	return &DashboardConfig{
		Enabled:        true,
		Host:           "0.0.0.0",
		Port:           9090,
		BasePath:       "/",
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   30 * time.Second,
		IdleTimeout:    60 * time.Second,
		EnableProfiling: false,
		ProfilingPath:  "/debug/pprof",
		EnablePprof:    false,
		PprofPath:      "/debug/pprof",
		EnableMetrics:  true,
		MetricsPath:    "/metrics",
		EnableHealth:   true,
		HealthPath:     "/health",
		EnableDebug:    false,
		DebugPath:      "/debug",
	}
}

// NewDashboard creates a new dashboard instance
func NewDashboard(logger *zap.Logger, config *DashboardConfig, collector *Collector, tracer *Tracer) *Dashboard {
	if config == nil {
		config = DefaultDashboardConfig()
	}

	d := &Dashboard{
		logger:    logger,
		config:    config,
		collector: collector,
		tracer:    tracer,
	}

	if config.Enabled {
		d.initializeHTTPServer()
	}

	return d
}

// initializeHTTPServer initializes the HTTP server
func (d *Dashboard) initializeHTTPServer() {
	mux := http.NewServeMux()

	// Register handlers
	if d.config.EnableMetrics {
		mux.Handle(d.config.MetricsPath, promhttp.Handler())
	}

	if d.config.EnableHealth {
		mux.HandleFunc(d.config.HealthPath, d.handleHealth)
	}

	if d.config.EnableDebug {
		mux.HandleFunc(d.config.DebugPath, d.handleDebug)
		mux.HandleFunc(d.config.DebugPath+"/config", d.handleConfig)
		mux.HandleFunc(d.config.DebugPath+"/metrics", d.handleMetrics)
		mux.HandleFunc(d.config.DebugPath+"/health", d.handleHealthChecks)
		mux.HandleFunc(d.config.DebugPath+"/alerts", d.handleAlerts)
		mux.HandleFunc(d.config.DebugPath+"/status", d.handleStatus)
	}

	// Dashboard endpoints
	mux.HandleFunc("/", d.handleDashboard)
	mux.HandleFunc("/dashboard", d.handleDashboard)
	mux.HandleFunc("/dashboard/overview", d.handleOverview)
	mux.HandleFunc("/dashboard/search", d.handleSearchDashboard)
	mux.HandleFunc("/dashboard/storage", d.handleStorageDashboard)
	mux.HandleFunc("/dashboard/cluster", d.handleClusterDashboard)
	mux.HandleFunc("/dashboard/system", d.handleSystemDashboard)
	mux.HandleFunc("/dashboard/replication", d.handleReplicationDashboard)
	mux.HandleFunc("/dashboard/cache", d.handleCacheDashboard)

	// API endpoints
	mux.HandleFunc("/api/v1/metrics", d.handleAPIMetrics)
	mux.HandleFunc("/api/v1/health", d.handleAPIHealth)
	mux.HandleFunc("/api/v1/alerts", d.handleAPIAlerts)
	mux.HandleFunc("/api/v1/status", d.handleAPIStatus)
	mux.HandleFunc("/api/v1/config", d.handleAPIConfig)

	// Add pprof handlers if enabled
	if d.config.EnablePprof {
		d.registerPprofHandlers(mux)
	}

	d.httpServer = &http.Server{
		Addr:         fmt.Sprintf("%s:%d", d.config.Host, d.config.Port),
		Handler:      mux,
		ReadTimeout:  d.config.ReadTimeout,
		WriteTimeout: d.config.WriteTimeout,
		IdleTimeout:  d.config.IdleTimeout,
	}
}

// registerPprofHandlers registers pprof handlers
func (d *Dashboard) registerPprofHandlers(mux *http.ServeMux) {
	// Import pprof and register handlers
	// Note: This would require importing net/http/pprof
	// For now, we'll leave this as a placeholder
	d.logger.Info("pprof handlers would be registered here")
}

// Start starts the dashboard server
func (d *Dashboard) Start() error {
	if !d.config.Enabled {
		d.logger.Info("Dashboard is disabled")
		return nil
	}

	d.logger.Info("Starting dashboard server",
		zap.String("host", d.config.Host),
		zap.Int("port", d.config.Port),
		zap.String("base_path", d.config.BasePath))

	go func() {
		if err := d.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			d.logger.Error("Dashboard server failed", zap.Error(err))
		}
	}()

	return nil
}

// Stop stops the dashboard server
func (d *Dashboard) Stop() error {
	if !d.config.Enabled || d.httpServer == nil {
		return nil
	}

	d.logger.Info("Stopping dashboard server")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	return d.httpServer.Shutdown(ctx)
}

// handleDashboard handles the main dashboard page
func (d *Dashboard) handleDashboard(w http.ResponseWriter, r *http.Request) {
	d.serveHTML(w, `
<!DOCTYPE html>
<html>
<head>
    <title>VexDB Dashboard</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        .nav { margin-bottom: 30px; }
        .nav a { margin-right: 20px; text-decoration: none; color: #333; }
        .nav a:hover { text-decoration: underline; }
        .metric { margin: 10px 0; padding: 10px; background: #f5f5f5; border-radius: 5px; }
        .status { padding: 5px 10px; border-radius: 3px; color: white; }
        .healthy { background: #4CAF50; }
        .unhealthy { background: #f44336; }
    </style>
</head>
<body>
    <h1>VexDB Dashboard</h1>
    <div class="nav">
        <a href="/dashboard/overview">Overview</a>
        <a href="/dashboard/search">Search</a>
        <a href="/dashboard/storage">Storage</a>
        <a href="/dashboard/cluster">Cluster</a>
        <a href="/dashboard/system">System</a>
        <a href="/dashboard/replication">Replication</a>
        <a href="/dashboard/cache">Cache</a>
        <a href="/metrics">Metrics</a>
        <a href="/health">Health</a>
    </div>
    <div id="content">
        <h2>System Status</h2>
        <div id="status">Loading...</div>
    </div>
    <script>
        fetch('/api/v1/status')
            .then(function(response){ return response.json(); })
            .then(function(data){
                var statusDiv = document.getElementById('status');
                var html = ''+
                  '<div class="metric">'+
                    '<strong>Service:</strong> ' + (data.service_name || '') + ' v' + (data.service_version || '') +
                    ' <span class="status ' + ((data.service_up)? 'healthy' : 'unhealthy') + '">' + ((data.service_up)? 'UP' : 'DOWN') + '</span>'+
                  '</div>'+
                  '<div class="metric">'+
                    '<strong>Uptime:</strong> ' + (data.uptime || '') +
                  '</div>'+
                  '<div class="metric">'+
                    '<strong>Health Checks:</strong> ' + (data.healthy_checks || 0) + '/' + (data.total_checks || 0) + ' passed'+
                  '</div>'+
                  '<div class="metric">'+
                    '<strong>Active Alerts:</strong> ' + (data.active_alerts || 0) +
                  '</div>';
                statusDiv.innerHTML = html;
            })
            .catch(function(){
                document.getElementById('status').innerHTML = '<div class="status unhealthy">Error loading status</div>';
            });
    </script>
</body>
</html>
`)
}

// handleOverview handles the overview dashboard
func (d *Dashboard) handleOverview(w http.ResponseWriter, r *http.Request) {
	d.serveHTML(w, `
<!DOCTYPE html>
<html>
<head>
    <title>VexDB Overview</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        .nav { margin-bottom: 30px; }
        .nav a { margin-right: 20px; text-decoration: none; color: #333; }
        .metric { margin: 10px 0; padding: 10px; background: #f5f5f5; border-radius: 5px; }
        .metric-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }
    </style>
</head>
<body>
    <h1>VexDB Overview</h1>
    <div class="nav">
        <a href="/dashboard">← Back</a>
    </div>
    <div class="metric-grid" id="metrics">
        Loading metrics...
    </div>
    <script>
        fetch('/api/v1/metrics')
            .then(function(response){ return response.json(); })
            .then(function(data){
                var metricsDiv = document.getElementById('metrics');
                var html = ''+
                  '<div class="metric">'+
                    '<h3>Service</h3>'+
                    '<p>Status: <span class="status ' + ((data.service_up)? 'healthy' : 'unhealthy') + '">' + ((data.service_up)? 'UP' : 'DOWN') + '</span></p>'+
                    '<p>Version: ' + (data.service_version || '') + '</p>'+
                    '<p>Instance: ' + (data.service_instance || '') + '</p>'+
                  '</div>'+
                  '<div class="metric">'+
                    '<h3>System</h3>'+
                    '<p>Goroutines: ' + (data.system_goroutines || 0) + '</p>'+
                    '<p>Memory: ' + formatBytes(data.system_memory_allocated || 0) + '</p>'+
                    '<p>GC Count: ' + (data.system_gc_count || 0) + '</p>'+
                  '</div>'+
                  '<div class="metric">'+
                    '<h3>Storage</h3>'+
                    '<p>Vectors: ' + (data.storage_vectors_total || 0) + '</p>'+
                    '<p>Segments: ' + (data.storage_segments_total || 0) + '</p>'+
                    '<p>Size: ' + formatBytes(data.storage_size_bytes || 0) + '</p>'+
                  '</div>'+
                  '<div class="metric">'+
                    '<h3>Cluster</h3>'+
                    '<p>Nodes: ' + (data.cluster_nodes_total || 0) + '</p>'+
                    '<p>Replicas: ' + (data.cluster_replicas_total || 0) + '</p>'+
                    '<p>Health Score: ' + (((data.cluster_health_score || 0) * 100).toFixed(1)) + '%</p>'+
                  '</div>';
                metricsDiv.innerHTML = html;
            })
            .catch(error => {
                document.getElementById('metrics').innerHTML = '<div class="metric">Error loading metrics</div>';
            });

        function formatBytes(bytes) {
            if (bytes === 0) return '0 Bytes';
            const k = 1024;
            const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
            const i = Math.floor(Math.log(bytes) / Math.log(k));
            return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
        }
    </script>
</body>
</html>
`)
}

// handleSearchDashboard handles the search dashboard
func (d *Dashboard) handleSearchDashboard(w http.ResponseWriter, r *http.Request) {
	d.serveHTML(w, `
<!DOCTYPE html>
<html>
<head>
    <title>VexDB Search Dashboard</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        .nav { margin-bottom: 30px; }
        .nav a { margin-right: 20px; text-decoration: none; color: #333; }
        .metric { margin: 10px 0; padding: 10px; background: #f5f5f5; border-radius: 5px; }
        .chart { margin: 20px 0; height: 300px; background: #f9f9f9; border: 1px solid #ddd; }
    </style>
</head>
<body>
    <h1>VexDB Search Dashboard</h1>
    <div class="nav">
        <a href="/dashboard">← Back</a>
    </div>
    <div class="metric">
        <h3>Search Operations</h3>
        <p>Total Searches: <span id="search-count">Loading...</span></p>
        <p>Average Duration: <span id="search-duration">Loading...</span></p>
        <p>Error Rate: <span id="search-error-rate">Loading...</span></p>
    </div>
    <div class="chart" id="search-chart">
        Search performance chart would be rendered here
    </div>
    <script>
        fetch('/api/v1/metrics')
            .then(response => response.json())
            .then(data => {
                document.getElementById('search-count').textContent = data.search_count_total || '0';
                document.getElementById('search-duration').textContent = 
                    data.search_duration_seconds_avg ? data.search_duration_seconds_avg.toFixed(3) + 's' : 'N/A';
                document.getElementById('search-error-rate').textContent = 
                    data.search_error_rate ? (data.search_error_rate * 100).toFixed(2) + '%' : '0%';
            })
            .catch(error => {
                document.getElementById('search-count').textContent = 'Error';
                document.getElementById('search-duration').textContent = 'Error';
                document.getElementById('search-error-rate').textContent = 'Error';
            });
    </script>
</body>
</html>
`)
}

// handleStorageDashboard handles the storage dashboard
func (d *Dashboard) handleStorageDashboard(w http.ResponseWriter, r *http.Request) {
	d.serveHTML(w, `
<!DOCTYPE html>
<html>
<head>
    <title>VexDB Storage Dashboard</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        .nav { margin-bottom: 30px; }
        .nav a { margin-right: 20px; text-decoration: none; color: #333; }
        .metric { margin: 10px 0; padding: 10px; background: #f5f5f5; border-radius: 5px; }
        .metric-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }
    </style>
</head>
<body>
    <h1>VexDB Storage Dashboard</h1>
    <div class="nav">
        <a href="/dashboard">← Back</a>
    </div>
    <div class="metric-grid" id="storage-metrics">
        Loading storage metrics...
    </div>
    <script>
        fetch('/api/v1/metrics')
            .then(response => response.json())
            .then(data => {
                const metricsDiv = document.getElementById('storage-metrics');
                metricsDiv.innerHTML = \`
                    <div class="metric">
                        <h3>Vectors</h3>
                        <p>Total: ${data.storage_vectors_total || '0'}</p>
                        <p>Segments: ${data.storage_segments_total || '0'}</p>
                    </div>
                    <div class="metric">
                        <h3>Storage Size</h3>
                        <p>Total: ${formatBytes(data.storage_size_bytes || 0)}</p>
                        <p>Avg per Vector: ${formatBytes((data.storage_size_bytes || 0) / (data.storage_vectors_total || 1))}</p>
                    </div>
                    <div class="metric">
                        <h3>Operations</h3>
                        <p>Total: ${data.storage_operations_total || '0'}</p>
                        <p>Avg Duration: ${data.storage_operation_duration_seconds_avg ? data.storage_operation_duration_seconds_avg.toFixed(6) + 's' : 'N/A'}</p>
                    </div>
                \`;
            })
            .catch(error => {
                document.getElementById('storage-metrics').innerHTML = '<div class="metric">Error loading storage metrics</div>';
            });

        function formatBytes(bytes) {
            if (bytes === 0) return '0 Bytes';
            const k = 1024;
            const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
            const i = Math.floor(Math.log(bytes) / Math.log(k));
            return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
        }
    </script>
</body>
</html>
`)
}

// handleClusterDashboard handles the cluster dashboard
func (d *Dashboard) handleClusterDashboard(w http.ResponseWriter, r *http.Request) {
	d.serveHTML(w, `
<!DOCTYPE html>
<html>
<head>
    <title>VexDB Cluster Dashboard</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        .nav { margin-bottom: 30px; }
        .nav a { margin-right: 20px; text-decoration: none; color: #333; }
        .metric { margin: 10px 0; padding: 10px; background: #f5f5f5; border-radius: 5px; }
        .node { margin: 10px 0; padding: 15px; background: #e8f5e8; border-radius: 5px; border-left: 4px solid #4CAF50; }
        .node.unhealthy { background: #ffeaea; border-left-color: #f44336; }
    </style>
</head>
<body>
    <h1>VexDB Cluster Dashboard</h1>
    <div class="nav">
        <a href="/dashboard">← Back</a>
    </div>
    <div class="metric">
        <h3>Cluster Overview</h3>
        <p>Total Nodes: <span id="cluster-nodes">Loading...</span></p>
        <p>Total Replicas: <span id="cluster-replicas">Loading...</span></p>
        <p>Health Score: <span id="cluster-health">Loading...</span></p>
    </div>
    <div id="cluster-nodes-list">
        Loading cluster nodes...
    </div>
    <script>
        fetch('/api/v1/metrics')
            .then(response => response.json())
            .then(data => {
                document.getElementById('cluster-nodes').textContent = data.cluster_nodes_total || '0';
                document.getElementById('cluster-replicas').textContent = data.cluster_replicas_total || '0';
                document.getElementById('cluster-health').textContent = 
                    data.cluster_health_score ? (data.cluster_health_score * 100).toFixed(1) + '%' : 'N/A';
                
                // This would be populated with actual node data from the API
                document.getElementById('cluster-nodes-list').innerHTML = \`
                    <div class="node">
                        <h4>Node 1</h4>
                        <p>Status: Healthy</p>
                        <p>Role: Primary</p>
                        <p>Replication Lag: 0ms</p>
                    </div>
                    <div class="node">
                        <h4>Node 2</h4>
                        <p>Status: Healthy</p>
                        <p>Role: Replica</p>
                        <p>Replication Lag: 12ms</p>
                    </div>
                \`;
            })
            .catch(error => {
                document.getElementById('cluster-nodes').textContent = 'Error';
                document.getElementById('cluster-replicas').textContent = 'Error';
                document.getElementById('cluster-health').textContent = 'Error';
                document.getElementById('cluster-nodes-list').innerHTML = '<div class="metric">Error loading cluster data</div>';
            });
    </script>
</body>
</html>
`)
}

// handleSystemDashboard handles the system dashboard
func (d *Dashboard) handleSystemDashboard(w http.ResponseWriter, r *http.Request) {
	d.serveHTML(w, `
<!DOCTYPE html>
<html>
<head>
    <title>VexDB System Dashboard</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        .nav { margin-bottom: 30px; }
        .nav a { margin-right: 20px; text-decoration: none; color: #333; }
        .metric { margin: 10px 0; padding: 10px; background: #f5f5f5; border-radius: 5px; }
        .metric-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }
    </style>
</head>
<body>
    <h1>VexDB System Dashboard</h1>
    <div class="nav">
        <a href="/dashboard">← Back</a>
    </div>
    <div class="metric-grid" id="system-metrics">
        Loading system metrics...
    </div>
    <script>
        fetch('/api/v1/metrics')
            .then(response => response.json())
            .then(data => {
                const metricsDiv = document.getElementById('system-metrics');
                metricsDiv.innerHTML = \`
                    <div class="metric">
                        <h3>Memory</h3>
                        <p>Allocated: ${formatBytes(data.system_memory_allocated || 0)}</p>
                        <p>Total: ${formatBytes(data.system_memory_total || 0)}</p>
                        <p>Heap: ${formatBytes(data.system_memory_heap || 0)}</p>
                    </div>
                    <div class="metric">
                        <h3>Runtime</h3>
                        <p>Goroutines: ${data.system_goroutines || '0'}</p>
                        <p>GC Count: ${data.system_gc_count || '0'}</p>
                        <p>GC Pause Total: ${formatDuration(data.system_gc_pause_total_ns || 0)}</p>
                    </div>
                    <div class="metric">
                        <h3>Connections</h3>
                        <p>HTTP: ${data.protocol_connections_http || '0'}</p>
                        <p>gRPC: ${data.protocol_connections_grpc || '0'}</p>
                        <p>WebSocket: ${data.protocol_connections_websocket || '0'}</p>
                    </div>
                \`;
            })
            .catch(error => {
                document.getElementById('system-metrics').innerHTML = '<div class="metric">Error loading system metrics</div>';
            });

        function formatBytes(bytes) {
            if (bytes === 0) return '0 Bytes';
            const k = 1024;
            const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
            const i = Math.floor(Math.log(bytes) / Math.log(k));
            return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
        }

        function formatDuration(nanoseconds) {
            const seconds = nanoseconds / 1e9;
            if (seconds < 1) return (nanoseconds / 1e6).toFixed(2) + 'ms';
            if (seconds < 60) return seconds.toFixed(2) + 's';
            const minutes = Math.floor(seconds / 60);
            const remainingSeconds = seconds % 60;
            return \`\${minutes}m \${remainingSeconds.toFixed(2)}s\`;
        }
    </script>
</body>
</html>
`)
}

// handleReplicationDashboard handles the replication dashboard
func (d *Dashboard) handleReplicationDashboard(w http.ResponseWriter, r *http.Request) {
	d.serveHTML(w, `
<!DOCTYPE html>
<html>
<head>
    <title>VexDB Replication Dashboard</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        .nav { margin-bottom: 30px; }
        .nav a { margin-right: 20px; text-decoration: none; color: #333; }
        .metric { margin: 10px 0; padding: 10px; background: #f5f5f5; border-radius: 5px; }
        .replication-stream { margin: 10px 0; padding: 15px; background: #e8f5e8; border-radius: 5px; }
    </style>
</head>
<body>
    <h1>VexDB Replication Dashboard</h1>
    <div class="nav">
        <a href="/dashboard">← Back</a>
    </div>
    <div class="metric">
        <h3>Replication Overview</h3>
        <p>Total Throughput: <span id="replication-throughput">Loading...</span></p>
        <p>Average Lag: <span id="replication-lag">Loading...</span></p>
        <p>Error Rate: <span id="replication-error-rate">Loading...</span></p>
    </div>
    <div id="replication-streams">
        Loading replication streams...
    </div>
    <script>
        fetch('/api/v1/metrics')
            .then(response => response.json())
            .then(data => {
                document.getElementById('replication-throughput').textContent = 
                    formatBytes(data.replication_throughput_bytes_total || 0) + '/s';
                document.getElementById('replication-lag').textContent = 
                    data.replication_lag_seconds_avg ? data.replication_lag_seconds_avg.toFixed(3) + 's' : 'N/A';
                document.getElementById('replication-error-rate').textContent = 
                    data.replication_error_rate ? (data.replication_error_rate * 100).toFixed(2) + '%' : '0%';
                
                // This would be populated with actual replication stream data
                document.getElementById('replication-streams').innerHTML = \`
                    <div class="replication-stream">
                        <h4>Stream: node1 → node2</h4>
                        <p>Status: Active</p>
                        <p>Lag: 12ms</p>
                        <p>Throughput: 1.2MB/s</p>
                    </div>
                    <div class="replication-stream">
                        <h4>Stream: node1 → node3</h4>
                        <p>Status: Active</p>
                        <p>Lag: 24ms</p>
                        <p>Throughput: 1.1MB/s</p>
                    </div>
                \`;
            })
            .catch(error => {
                document.getElementById('replication-throughput').textContent = 'Error';
                document.getElementById('replication-lag').textContent = 'Error';
                document.getElementById('replication-error-rate').textContent = 'Error';
                document.getElementById('replication-streams').innerHTML = '<div class="metric">Error loading replication data</div>';
            });

        function formatBytes(bytes) {
            if (bytes === 0) return '0 Bytes';
            const k = 1024;
            const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
            const i = Math.floor(Math.log(bytes) / Math.log(k));
            return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
        }
    </script>
</body>
</html>
`)
}

// handleCacheDashboard handles the cache dashboard
func (d *Dashboard) handleCacheDashboard(w http.ResponseWriter, r *http.Request) {
	d.serveHTML(w, `
<!DOCTYPE html>
<html>
<head>
    <title>VexDB Cache Dashboard</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        .nav { margin-bottom: 30px; }
        .nav a { margin-right: 20px; text-decoration: none; color: #333; }
        .metric { margin: 10px 0; padding: 10px; background: #f5f5f5; border-radius: 5px; }
        .cache { margin: 10px 0; padding: 15px; background: #e8f5e8; border-radius: 5px; }
    </style>
</head>
<body>
    <h1>VexDB Cache Dashboard</h1>
    <div class="nav">
        <a href="/dashboard">← Back</a>
    </div>
    <div class="metric">
        <h3>Cache Overview</h3>
        <p>Total Size: <span id="cache-size">Loading...</span></p>
        <p>Hit Rate: <span id="cache-hit-rate">Loading...</span></p>
        <p>Total Evictions: <span id="cache-evictions">Loading...</span></p>
    </div>
    <div id="cache-list">
        Loading cache information...
    </div>
    <script>
        fetch('/api/v1/metrics')
            .then(response => response.json())
            .then(data => {
                const totalHits = data.cache_hits_total || 0;
                const totalMisses = data.cache_misses_total || 0;
                const totalRequests = totalHits + totalMisses;
                const hitRate = totalRequests > 0 ? (totalHits / totalRequests * 100) : 0;
                
                document.getElementById('cache-size').textContent = formatBytes(data.cache_size_bytes || 0);
                document.getElementById('cache-hit-rate').textContent = hitRate.toFixed(2) + '%';
                document.getElementById('cache-evictions').textContent = data.cache_evictions_total || '0';
                
                // This would be populated with actual cache data
                document.getElementById('cache-list').innerHTML = \`
                    <div class="cache">
                        <h4>Vector Cache</h4>
                        <p>Size: ${formatBytes(data.cache_size_vector_cache || 0)}</p>
                        <p>Hit Rate: ${calculateHitRate(data.cache_hits_vector_cache || 0, data.cache_misses_vector_cache || 0)}%</p>
                        <p>Evictions: ${data.cache_evictions_vector_cache || 0}</p>
                    </div>
                    <div class="cache">
                        <h4>Segment Cache</h4>
                        <p>Size: ${formatBytes(data.cache_size_segment_cache || 0)}</p>
                        <p>Hit Rate: ${calculateHitRate(data.cache_hits_segment_cache || 0, data.cache_misses_segment_cache || 0)}%</p>
                        <p>Evictions: ${data.cache_evictions_segment_cache || 0}</p>
                    </div>
                \`;
            })
            .catch(error => {
                document.getElementById('cache-size').textContent = 'Error';
                document.getElementById('cache-hit-rate').textContent = 'Error';
                document.getElementById('cache-evictions').textContent = 'Error';
                document.getElementById('cache-list').innerHTML = '<div class="metric">Error loading cache data</div>';
            });

        function formatBytes(bytes) {
            if (bytes === 0) return '0 Bytes';
            const k = 1024;
            const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
            const i = Math.floor(Math.log(bytes) / Math.log(k));
            return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
        }

        function calculateHitRate(hits, misses) {
            const total = hits + misses;
            return total > 0 ? (hits / total * 100).toFixed(2) : '0.00';
        }
    </script>
</body>
</html>
`)
}

// handleHealth handles health check requests
func (d *Dashboard) handleHealth(w http.ResponseWriter, r *http.Request) {
	healthChecks := d.collector.GetHealthStatus()
	allHealthy := true

	for _, hc := range healthChecks {
		if !hc.LastStatus {
			allHealthy = false
			break
		}
	}

	status := http.StatusOK
	if !allHealthy {
		status = http.StatusServiceUnavailable
	}

	response := map[string]interface{}{
		"status":      "healthy",
		"timestamp":   time.Now().UTC().Format(time.RFC3339),
		"uptime":      time.Since(startTime).String(),
		"version":     "1.0.0",
		"checks":      healthChecks,
	}

	if !allHealthy {
		response["status"] = "unhealthy"
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(response)
}

// handleDebug handles debug requests
func (d *Dashboard) handleDebug(w http.ResponseWriter, r *http.Request) {
	d.serveHTML(w, `
<!DOCTYPE html>
<html>
<head>
    <title>VexDB Debug</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        .nav { margin-bottom: 30px; }
        .nav a { margin-right: 20px; text-decoration: none; color: #333; }
        .section { margin: 20px 0; padding: 20px; background: #f5f5f5; border-radius: 5px; }
    </style>
</head>
<body>
    <h1>VexDB Debug</h1>
    <div class="nav">
        <a href="/dashboard">← Back</a>
    </div>
    <div class="section">
        <h3>Debug Endpoints</h3>
        <ul>
            <li><a href="/debug/config">Configuration</a></li>
            <li><a href="/debug/metrics">Metrics</a></li>
            <li><a href="/debug/health">Health Checks</a></li>
            <li><a href="/debug/alerts">Alerts</a></li>
            <li><a href="/debug/status">Status</a></li>
        </ul>
    </div>
</body>
</html>
`)
}

// handleConfig handles configuration debug requests
func (d *Dashboard) handleConfig(w http.ResponseWriter, r *http.Request) {
	config := map[string]interface{}{
		"dashboard": d.config,
		"collector": d.collector.config,
		"tracer":    d.tracer.config,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(config)
}

// handleMetrics handles metrics debug requests
func (d *Dashboard) handleMetrics(w http.ResponseWriter, r *http.Request) {
	// This would gather and return detailed metrics information
	metrics := map[string]interface{}{
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"message":   "Detailed metrics would be returned here",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(metrics)
}

// handleHealthChecks handles health check debug requests
func (d *Dashboard) handleHealthChecks(w http.ResponseWriter, r *http.Request) {
	healthChecks := d.collector.GetHealthStatus()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(healthChecks)
}

// handleAlerts handles alerts debug requests
func (d *Dashboard) handleAlerts(w http.ResponseWriter, r *http.Request) {
	alerts := d.collector.GetAlerts()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(alerts)
}

// handleStatus handles status debug requests
func (d *Dashboard) handleStatus(w http.ResponseWriter, r *http.Request) {
	status := d.getSystemStatus()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

// handleAPIMetrics handles API metrics requests
func (d *Dashboard) handleAPIMetrics(w http.ResponseWriter, r *http.Request) {
	metrics := d.getAPIMetrics()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(metrics)
}

// handleAPIHealth handles API health requests
func (d *Dashboard) handleAPIHealth(w http.ResponseWriter, r *http.Request) {
	healthChecks := d.collector.GetHealthStatus()
	allHealthy := true

	for _, hc := range healthChecks {
		if !hc.LastStatus {
			allHealthy = false
			break
		}
	}

	response := map[string]interface{}{
		"status":      "healthy",
		"timestamp":   time.Now().UTC().Format(time.RFC3339),
		"uptime":      time.Since(startTime).String(),
		"version":     "1.0.0",
		"checks":      healthChecks,
		"healthy":     allHealthy,
	}

	if !allHealthy {
		response["status"] = "unhealthy"
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleAPIAlerts handles API alerts requests
func (d *Dashboard) handleAPIAlerts(w http.ResponseWriter, r *http.Request) {
	alerts := d.collector.GetAlerts()

	// Convert to a more API-friendly format
	apiAlerts := make([]map[string]interface{}, 0, len(alerts))
	for _, alert := range alerts {
		apiAlerts = append(apiAlerts, map[string]interface{}{
			"id":          alert.ID,
			"name":        alert.Name,
			"description": alert.Description,
			"severity":    alert.Severity,
			"last_trigger": alert.LastTrigger.Format(time.RFC3339),
			"count":       alert.Count,
			"enabled":     alert.Enabled,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(apiAlerts)
}

// handleAPIStatus handles API status requests
func (d *Dashboard) handleAPIStatus(w http.ResponseWriter, r *http.Request) {
	status := d.getSystemStatus()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

// handleAPIConfig handles API config requests
func (d *Dashboard) handleAPIConfig(w http.ResponseWriter, r *http.Request) {
	config := map[string]interface{}{
		"dashboard": d.config,
		"collector": d.collector.config,
		"tracer":    d.tracer.config,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(config)
}

// getSystemStatus returns system status information
func (d *Dashboard) getSystemStatus() map[string]interface{} {
	healthChecks := d.collector.GetHealthStatus()
	alerts := d.collector.GetAlerts()

	healthyChecks := 0
	for _, hc := range healthChecks {
		if hc.LastStatus {
			healthyChecks++
		}
	}

	activeAlerts := 0
	for _, alert := range alerts {
		if alert.Enabled && alert.Count > 0 {
			activeAlerts++
		}
	}

	return map[string]interface{}{
		"service_name":      d.collector.config.ServiceName,
		"service_version":   d.collector.config.ServiceVersion,
		"service_instance":  d.collector.config.ServiceInstance,
		"service_up":        true, // This would be determined by actual service status
		"uptime":           time.Since(startTime).String(),
		"timestamp":        time.Now().UTC().Format(time.RFC3339),
		"total_checks":     len(healthChecks),
		"healthy_checks":   healthyChecks,
		"active_alerts":    activeAlerts,
		"dashboard_enabled": d.config.Enabled,
		"tracing_enabled":  d.tracer.config.Enabled,
	}
}

// getAPIMetrics returns API-friendly metrics
func (d *Dashboard) getAPIMetrics() map[string]interface{} {
	// This would gather actual metrics from Prometheus
	// For now, we'll return a placeholder structure
	return map[string]interface{}{
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"service": map[string]interface{}{
			"name":     d.collector.config.ServiceName,
			"version":  d.collector.config.ServiceVersion,
			"instance": d.collector.config.ServiceInstance,
			"up":       true,
		},
		"system": map[string]interface{}{
			"goroutines":         runtime.NumGoroutine(),
			"memory_allocated":   getMemoryAlloc(),
			"memory_total":       getMemoryTotal(),
			"memory_heap":        getMemoryHeap(),
			"gc_count":           getGCCount(),
			"gc_pause_total_ns":  getGCPauseTotal(),
		},
		"storage": map[string]interface{}{
			"vectors_total":      getStorageVectors(),
			"segments_total":     getStorageSegments(),
			"size_bytes":         getStorageSize(),
			"operations_total":   getStorageOperations(),
			"operation_duration_seconds_avg": getStorageOperationDuration(),
		},
		"cluster": map[string]interface{}{
			"nodes_total":        getClusterNodes(),
			"replicas_total":     getClusterReplicas(),
			"health_score":       getClusterHealthScore(),
		},
		"search": map[string]interface{}{
			"count_total":        getSearchCount(),
			"duration_seconds_avg": getSearchDuration(),
			"results_count_avg":  getSearchResults(),
			"error_rate":         getSearchErrorRate(),
		},
		"replication": map[string]interface{}{
			"throughput_bytes_total": getReplicationThroughput(),
			"lag_seconds_avg":       getReplicationLag(),
			"error_rate":            getReplicationErrorRate(),
		},
		"cache": map[string]interface{}{
			"size_bytes":         getCacheSize(),
			"hits_total":        getCacheHits(),
			"misses_total":      getCacheMisses(),
			"evictions_total":   getCacheEvictions(),
		},
		"protocol": map[string]interface{}{
			"connections_http":     getProtocolConnections("http"),
			"connections_grpc":     getProtocolConnections("grpc"),
			"connections_websocket": getProtocolConnections("websocket"),
		},
	}
}

// serveHTML serves HTML content
func (d *Dashboard) serveHTML(w http.ResponseWriter, html string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(html))
}

// Placeholder functions for metrics gathering
// These would be implemented to gather actual metrics from Prometheus

var startTime = time.Now()

func getMemoryAlloc() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.Alloc
}

func getMemoryTotal() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.Sys
}

func getMemoryHeap() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

func getGCCount() uint32 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.NumGC
}

func getGCPauseTotal() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.PauseTotalNs
}

func getStorageVectors() int64 {
	// Placeholder - would gather from actual metrics
	return 0
}

func getStorageSegments() int64 {
	// Placeholder - would gather from actual metrics
	return 0
}

func getStorageSize() int64 {
	// Placeholder - would gather from actual metrics
	return 0
}

func getStorageOperations() int64 {
	// Placeholder - would gather from actual metrics
	return 0
}

func getStorageOperationDuration() float64 {
	// Placeholder - would gather from actual metrics
	return 0
}

func getClusterNodes() int64 {
	// Placeholder - would gather from actual metrics
	return 0
}

func getClusterReplicas() int64 {
	// Placeholder - would gather from actual metrics
	return 0
}

func getClusterHealthScore() float64 {
	// Placeholder - would gather from actual metrics
	return 1.0
}

func getSearchCount() int64 {
	// Placeholder - would gather from actual metrics
	return 0
}

func getSearchDuration() float64 {
	// Placeholder - would gather from actual metrics
	return 0
}

func getSearchResults() float64 {
	// Placeholder - would gather from actual metrics
	return 0
}

func getSearchErrorRate() float64 {
	// Placeholder - would gather from actual metrics
	return 0
}

func getReplicationThroughput() int64 {
	// Placeholder - would gather from actual metrics
	return 0
}

func getReplicationLag() float64 {
	// Placeholder - would gather from actual metrics
	return 0
}

func getReplicationErrorRate() float64 {
	// Placeholder - would gather from actual metrics
	return 0
}

func getCacheSize() int64 {
	// Placeholder - would gather from actual metrics
	return 0
}

func getCacheHits() int64 {
	// Placeholder - would gather from actual metrics
	return 0
}

func getCacheMisses() int64 {
	// Placeholder - would gather from actual metrics
	return 0
}

func getCacheEvictions() int64 {
	// Placeholder - would gather from actual metrics
	return 0
}

func getProtocolConnections(protocol string) int64 {
	// Placeholder - would gather from actual metrics
	return 0
}

// Import required packages
import (
	"context"
	"net/http"
	"runtime"
)
