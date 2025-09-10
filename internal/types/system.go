package types

import (
	"time"
)

// SystemHealth represents the overall health of the system
type SystemHealth struct {
	Status     string                 `json:"status" yaml:"status"` // "healthy", "unhealthy", "degraded"
	Message    string                 `json:"message,omitempty" yaml:"message,omitempty"`
	Timestamp  time.Time              `json:"timestamp" yaml:"timestamp"`
	Version    string                 `json:"version" yaml:"version"`
	Uptime     time.Duration          `json:"uptime" yaml:"uptime"`
	Services   []ServiceHealth        `json:"services" yaml:"services"`
	Components []ComponentHealth      `json:"components" yaml:"components"`
	Metrics    map[string]interface{} `json:"metrics,omitempty" yaml:"metrics,omitempty"`
}

// ServiceHealth represents the health of a service
type ServiceHealth struct {
	Name    string                 `json:"name" yaml:"name"`
	Status  string                 `json:"status" yaml:"status"` // "healthy", "unhealthy", "degraded"
	Message string                 `json:"message,omitempty" yaml:"message,omitempty"`
	Host    string                 `json:"host" yaml:"host"`
	Port    int                    `json:"port" yaml:"port"`
	Version string                 `json:"version" yaml:"version"`
	Uptime  time.Duration          `json:"uptime" yaml:"uptime"`
	Checks  []HealthCheck          `json:"checks,omitempty" yaml:"checks,omitempty"`
	Metrics map[string]interface{} `json:"metrics,omitempty" yaml:"metrics,omitempty"`
}

// ComponentHealth represents the health of a component
type ComponentHealth struct {
	Name    string                 `json:"name" yaml:"name"`
	Type    string                 `json:"type" yaml:"type"`     // "storage", "search", "network", etc.
	Status  string                 `json:"status" yaml:"status"` // "healthy", "unhealthy", "degraded"
	Message string                 `json:"message,omitempty" yaml:"message,omitempty"`
	Metrics map[string]interface{} `json:"metrics,omitempty" yaml:"metrics,omitempty"`
	Checks  []HealthCheck          `json:"checks,omitempty" yaml:"checks,omitempty"`
}

// MetricPoint represents a single metric data point
type MetricPoint struct {
	Name      string            `json:"name" yaml:"name"`
	Value     float64           `json:"value" yaml:"value"`
	Timestamp time.Time         `json:"timestamp" yaml:"timestamp"`
	Tags      map[string]string `json:"tags,omitempty" yaml:"tags,omitempty"`
	Type      string            `json:"type" yaml:"type"` // "gauge", "counter", "histogram", "summary"
	Help      string            `json:"help,omitempty" yaml:"help,omitempty"`
}

// ConfigVersion represents configuration version information
type ConfigVersion struct {
	Version   string         `json:"version" yaml:"version"`
	Hash      string         `json:"hash" yaml:"hash"`
	Timestamp time.Time      `json:"timestamp" yaml:"timestamp"`
	Source    string         `json:"source" yaml:"source"` // "file", "env", "default"
	ChangedBy string         `json:"changed_by,omitempty" yaml:"changed_by,omitempty"`
	Changes   []ConfigChange `json:"changes,omitempty" yaml:"changes,omitempty"`
}

// ConfigChange represents a configuration change
type ConfigChange struct {
	Path      string      `json:"path" yaml:"path"`
	OldValue  interface{} `json:"old_value,omitempty" yaml:"old_value,omitempty"`
	NewValue  interface{} `json:"new_value,omitempty" yaml:"new_value,omitempty"`
	Timestamp time.Time   `json:"timestamp" yaml:"timestamp"`
	Reason    string      `json:"reason,omitempty" yaml:"reason,omitempty"`
}

// ServiceStatus represents the status of a service
type ServiceStatus struct {
	ID        string                 `json:"id" yaml:"id"`
	Name      string                 `json:"name" yaml:"name"`
	Type      string                 `json:"type" yaml:"type"`     // "vxstorage", "vxinsert", "vxsearch"
	Status    string                 `json:"status" yaml:"status"` // "running", "stopped", "error", "starting", "stopping"
	Host      string                 `json:"host" yaml:"host"`
	Port      int                    `json:"port" yaml:"port"`
	Version   string                 `json:"version" yaml:"version"`
	Uptime    time.Duration          `json:"uptime" yaml:"uptime"`
	Health    string                 `json:"health" yaml:"health"` // "healthy", "unhealthy", "degraded"
	Message   string                 `json:"message,omitempty" yaml:"message,omitempty"`
	Metrics   map[string]interface{} `json:"metrics,omitempty" yaml:"metrics,omitempty"`
	LastCheck time.Time              `json:"last_check" yaml:"last_check"`
	StartedAt time.Time              `json:"started_at" yaml:"started_at"`
}

// SystemMetrics represents system-wide metrics
type SystemMetrics struct {
	Timestamp   time.Time              `json:"timestamp" yaml:"timestamp"`
	Uptime      time.Duration          `json:"uptime" yaml:"uptime"`
	Services    []ServiceMetrics       `json:"services" yaml:"services"`
	Resources   ResourceMetrics        `json:"resources" yaml:"resources"`
	Performance PerformanceMetrics     `json:"performance" yaml:"performance"`
	Custom      map[string]interface{} `json:"custom,omitempty" yaml:"custom,omitempty"`
}

// ServiceMetrics represents metrics for a single service
type ServiceMetrics struct {
	Name       string                 `json:"name" yaml:"name"`
	Type       string                 `json:"type" yaml:"type"`
	Host       string                 `json:"host" yaml:"host"`
	Port       int                    `json:"port" yaml:"port"`
	Status     string                 `json:"status" yaml:"status"`
	Health     string                 `json:"health" yaml:"health"`
	Uptime     time.Duration          `json:"uptime" yaml:"uptime"`
	Requests   int64                  `json:"requests" yaml:"requests"`
	Errors     int64                  `json:"errors" yaml:"errors"`
	Latency    float64                `json:"latency" yaml:"latency"`       // in milliseconds
	Throughput float64                `json:"throughput" yaml:"throughput"` // requests per second
	Custom     map[string]interface{} `json:"custom,omitempty" yaml:"custom,omitempty"`
}

// ResourceMetrics represents system resource metrics
type ResourceMetrics struct {
	CPU     CPUInfo     `json:"cpu" yaml:"cpu"`
	Memory  MemoryInfo  `json:"memory" yaml:"memory"`
	Disk    DiskInfo    `json:"disk" yaml:"disk"`
	Network NetworkInfo `json:"network" yaml:"network"`
}

// CPUInfo represents CPU information
type CPUInfo struct {
	Usage     float64 `json:"usage" yaml:"usage"` // percentage
	Cores     int     `json:"cores" yaml:"cores"`
	Frequency float64 `json:"frequency" yaml:"frequency"` // in MHz
	Load1     float64 `json:"load1" yaml:"load1"`         // 1-minute load average
	Load5     float64 `json:"load5" yaml:"load5"`         // 5-minute load average
	Load15    float64 `json:"load15" yaml:"load15"`       // 15-minute load average
}

// MemoryInfo represents memory information
type MemoryInfo struct {
	Total     uint64  `json:"total" yaml:"total"`         // in bytes
	Used      uint64  `json:"used" yaml:"used"`           // in bytes
	Free      uint64  `json:"free" yaml:"free"`           // in bytes
	Available uint64  `json:"available" yaml:"available"` // in bytes
	Usage     float64 `json:"usage" yaml:"usage"`         // percentage
}

// DiskInfo represents disk information
type DiskInfo struct {
	Total  uint64  `json:"total" yaml:"total"`   // in bytes
	Used   uint64  `json:"used" yaml:"used"`     // in bytes
	Free   uint64  `json:"free" yaml:"free"`     // in bytes
	Usage  float64 `json:"usage" yaml:"usage"`   // percentage
	Reads  uint64  `json:"reads" yaml:"reads"`   // number of reads
	Writes uint64  `json:"writes" yaml:"writes"` // number of writes
}

// NetworkInfo represents network information
type NetworkInfo struct {
	BytesSent     uint64 `json:"bytes_sent" yaml:"bytes_sent"`
	BytesReceived uint64 `json:"bytes_received" yaml:"bytes_received"`
	PacketsSent   uint64 `json:"packets_sent" yaml:"packets_sent"`
	PacketsRecv   uint64 `json:"packets_recv" yaml:"packets_recv"`
	ErrorsIn      uint64 `json:"errors_in" yaml:"errors_in"`
	ErrorsOut     uint64 `json:"errors_out" yaml:"errors_out"`
}

// PerformanceMetrics represents performance metrics
type PerformanceMetrics struct {
	QueryLatency  float64 `json:"query_latency" yaml:"query_latency"`   // in milliseconds
	IngestLatency float64 `json:"ingest_latency" yaml:"ingest_latency"` // in milliseconds
	Throughput    float64 `json:"throughput" yaml:"throughput"`         // operations per second
	ErrorRate     float64 `json:"error_rate" yaml:"error_rate"`         // percentage
	Availability  float64 `json:"availability" yaml:"availability"`     // percentage
	Saturation    float64 `json:"saturation" yaml:"saturation"`         // percentage
}
