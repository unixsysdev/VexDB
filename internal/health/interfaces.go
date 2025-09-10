package health

import (
	"context"
	"time"
)

// HealthStatus represents the health status of a component
type HealthStatus struct {
	Status    HealthState            `json:"status"`
	Message   string                 `json:"message,omitempty"`
	Details   map[string]interface{} `json:"details,omitempty"`
	Timestamp time.Time              `json:"timestamp"`
	Component string                 `json:"component"`
	Version   string                 `json:"version,omitempty"`
	Uptime    time.Duration          `json:"uptime,omitempty"`
	Checks    []*HealthCheck         `json:"checks,omitempty"`
	Metrics   map[string]interface{} `json:"metrics,omitempty"`
}

// HealthState represents the health state
type HealthState string

const (
	HealthStateHealthy   HealthState = "healthy"
	HealthStateDegraded  HealthState = "degraded"
	HealthStateUnhealthy HealthState = "unhealthy"
	HealthStateUnknown   HealthState = "unknown"
)

// HealthCheck represents a single health check
type HealthCheck struct {
	Name      string                 `json:"name"`
	Status    HealthState            `json:"status"`
	Message   string                 `json:"message,omitempty"`
	Details   map[string]interface{} `json:"details,omitempty"`
	Duration  time.Duration          `json:"duration"`
	Timestamp time.Time              `json:"timestamp"`
	Component string                 `json:"component"`
	Metrics   map[string]interface{} `json:"metrics,omitempty"`
}

// HealthChecker defines the interface for health checking
type HealthChecker interface {
	// CheckHealth performs a health check
	CheckHealth(ctx context.Context, detailed bool) (*HealthStatus, error)

	// GetName returns the name of the health checker
	GetName() string

	// GetComponent returns the component being checked
	GetComponent() string

	// GetVersion returns the version of the component
	GetVersion() string
}

// HealthRegistry manages health checkers
type HealthRegistry interface {
	// Register registers a health checker
	Register(checker HealthChecker) error

	// Unregister unregisters a health checker
	Unregister(name string) error

	// GetChecker returns a health checker by name
	GetChecker(name string) (HealthChecker, error)

	// ListCheckers returns all registered health checkers
	ListCheckers() []HealthChecker

	// CheckAll checks the health of all registered components
	CheckAll(ctx context.Context, detailed bool) (*SystemHealth, error)

	// CheckComponent checks the health of a specific component
	CheckComponent(ctx context.Context, name string, detailed bool) (*HealthStatus, error)
}

// SystemHealth represents the overall system health
type SystemHealth struct {
	Status     HealthState              `json:"status"`
	Message    string                   `json:"message,omitempty"`
	Details    map[string]interface{}   `json:"details,omitempty"`
	Timestamp  time.Time                `json:"timestamp"`
	Version    string                   `json:"version,omitempty"`
	Uptime     time.Duration            `json:"uptime,omitempty"`
	Components map[string]*HealthStatus `json:"components"`
	Summary    *HealthSummary           `json:"summary,omitempty"`
	Metrics    map[string]interface{}   `json:"metrics,omitempty"`
}

// HealthSummary provides a summary of system health
type HealthSummary struct {
	TotalComponents     int         `json:"total_components"`
	HealthyComponents   int         `json:"healthy_components"`
	DegradedComponents  int         `json:"degraded_components"`
	UnhealthyComponents int         `json:"unhealthy_components"`
	UnknownComponents   int         `json:"unknown_components"`
	OverallHealth       HealthState `json:"overall_health"`
}

// HealthMonitor monitors the health of components
type HealthMonitor interface {
	// Start starts the health monitoring
	Start(ctx context.Context) error

	// Stop stops the health monitoring
	Stop() error

	// GetSystemHealth returns the current system health
	GetSystemHealth(ctx context.Context, detailed bool) (*SystemHealth, error)

	// GetComponentHealth returns the health of a specific component
	GetComponentHealth(ctx context.Context, name string, detailed bool) (*HealthStatus, error)

	// GetHealthHistory returns the health history of a component
	GetHealthHistory(ctx context.Context, name string, since time.Time) ([]*HealthStatus, error)

	// Subscribe subscribes to health status changes
	Subscribe(ctx context.Context, callback HealthChangeCallback) error

	// Unsubscribe unsubscribes from health status changes
	Unsubscribe(callback HealthChangeCallback) error
}

// HealthChangeCallback is called when health status changes
type HealthChangeCallback func(ctx context.Context, change *HealthChange)

// HealthChange represents a change in health status
type HealthChange struct {
	Component string                 `json:"component"`
	OldStatus HealthState            `json:"old_status"`
	NewStatus HealthState            `json:"new_status"`
	Timestamp time.Time              `json:"timestamp"`
	Message   string                 `json:"message,omitempty"`
	Details   map[string]interface{} `json:"details,omitempty"`
}

// HealthCheckerFunc is a function that implements HealthChecker
type HealthCheckerFunc func(ctx context.Context, detailed bool) (*HealthStatus, error)

// CheckHealth implements HealthChecker interface
func (f HealthCheckerFunc) CheckHealth(ctx context.Context, detailed bool) (*HealthStatus, error) {
	return f(ctx, detailed)
}

// GetName returns a default name for function-based checkers
func (f HealthCheckerFunc) GetName() string {
	return "function-checker"
}

// GetComponent returns a default component for function-based checkers
func (f HealthCheckerFunc) GetComponent() string {
	return "function-component"
}

// GetVersion returns a default version for function-based checkers
func (f HealthCheckerFunc) GetVersion() string {
	return "1.0.0"
}

// ServiceHealthChecker defines the interface for service-specific health checking
type ServiceHealthChecker interface {
	HealthChecker

	// CheckServiceHealth checks the health of the service
	CheckServiceHealth(ctx context.Context, detailed bool) (*ServiceHealthStatus, error)

	// GetServiceMetrics returns service-specific metrics
	GetServiceMetrics(ctx context.Context) (map[string]interface{}, error)

	// GetServiceDependencies returns the dependencies of the service
	GetServiceDependencies() []string
}

// ServiceHealthStatus represents the health status of a service
type ServiceHealthStatus struct {
	HealthStatus
	Dependencies map[string]*HealthStatus `json:"dependencies,omitempty"`
	Performance  *ServicePerformance      `json:"performance,omitempty"`
	Resources    *ServiceResources        `json:"resources,omitempty"`
}

// ServicePerformance represents service performance metrics
type ServicePerformance struct {
	RequestRate  float64 `json:"request_rate,omitempty"`
	ErrorRate    float64 `json:"error_rate,omitempty"`
	Latency      float64 `json:"latency,omitempty"`
	Throughput   float64 `json:"throughput,omitempty"`
	CPUUsage     float64 `json:"cpu_usage,omitempty"`
	MemoryUsage  float64 `json:"memory_usage,omitempty"`
	DiskUsage    float64 `json:"disk_usage,omitempty"`
	NetworkUsage float64 `json:"network_usage,omitempty"`
}

// ServiceResources represents service resource usage
type ServiceResources struct {
	CPU         ResourceUsage `json:"cpu,omitempty"`
	Memory      ResourceUsage `json:"memory,omitempty"`
	Disk        ResourceUsage `json:"disk,omitempty"`
	Network     ResourceUsage `json:"network,omitempty"`
	Connections ResourceUsage `json:"connections,omitempty"`
}

// ResourceUsage represents resource usage information
type ResourceUsage struct {
	Used       float64 `json:"used"`
	Total      float64 `json:"total"`
	Available  float64 `json:"available"`
	Percentage float64 `json:"percentage"`
	Unit       string  `json:"unit"`
}

// NodeHealthChecker defines the interface for node-specific health checking
type NodeHealthChecker interface {
	HealthChecker

	// CheckNodeHealth checks the health of the node
	CheckNodeHealth(ctx context.Context, detailed bool) (*NodeHealthStatus, error)

	// GetNodeMetrics returns node-specific metrics
	GetNodeMetrics(ctx context.Context) (map[string]interface{}, error)

	// GetNodeInfo returns node information
	GetNodeInfo() *NodeInfo
}

// NodeHealthStatus represents the health status of a node
type NodeHealthStatus struct {
	HealthStatus
	Hardware  *NodeHardware  `json:"hardware,omitempty"`
	Network   *NodeNetwork   `json:"network,omitempty"`
	Storage   *NodeStorage   `json:"storage,omitempty"`
	Processes []*NodeProcess `json:"processes,omitempty"`
}

// NodeInfo represents node information
type NodeInfo struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Address     string            `json:"address"`
	Port        int               `json:"port"`
	Cluster     string            `json:"cluster"`
	Role        string            `json:"role"`
	Version     string            `json:"version"`
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
}

// NodeHardware represents node hardware information
type NodeHardware struct {
	CPU     CPUInfo     `json:"cpu,omitempty"`
	Memory  MemoryInfo  `json:"memory,omitempty"`
	Disk    DiskInfo    `json:"disk,omitempty"`
	Network NetworkInfo `json:"network,omitempty"`
	System  SystemInfo  `json:"system,omitempty"`
}

// CPUInfo represents CPU information
type CPUInfo struct {
	Model       string  `json:"model,omitempty"`
	Cores       int     `json:"cores,omitempty"`
	Threads     int     `json:"threads,omitempty"`
	Frequency   float64 `json:"frequency,omitempty"`
	Usage       float64 `json:"usage,omitempty"`
	Temperature float64 `json:"temperature,omitempty"`
}

// MemoryInfo represents memory information
type MemoryInfo struct {
	Total     uint64  `json:"total,omitempty"`
	Used      uint64  `json:"used,omitempty"`
	Available uint64  `json:"available,omitempty"`
	Usage     float64 `json:"usage,omitempty"`
	SwapTotal uint64  `json:"swap_total,omitempty"`
	SwapUsed  uint64  `json:"swap_used,omitempty"`
}

// DiskInfo represents disk information
type DiskInfo struct {
	Total     uint64  `json:"total,omitempty"`
	Used      uint64  `json:"used,omitempty"`
	Available uint64  `json:"available,omitempty"`
	Usage     float64 `json:"usage,omitempty"`
	Device    string  `json:"device,omitempty"`
	Mount     string  `json:"mount,omitempty"`
	Type      string  `json:"type,omitempty"`
}

// NetworkInfo represents network information
type NetworkInfo struct {
	Interface string  `json:"interface,omitempty"`
	IP        string  `json:"ip,omitempty"`
	MAC       string  `json:"mac,omitempty"`
	Speed     float64 `json:"speed,omitempty"`
	RxBytes   uint64  `json:"rx_bytes,omitempty"`
	TxBytes   uint64  `json:"tx_bytes,omitempty"`
	RxPackets uint64  `json:"rx_packets,omitempty"`
	TxPackets uint64  `json:"tx_packets,omitempty"`
}

// SystemInfo represents system information
type SystemInfo struct {
	Hostname string    `json:"hostname,omitempty"`
	OS       string    `json:"os,omitempty"`
	Kernel   string    `json:"kernel,omitempty"`
	Arch     string    `json:"arch,omitempty"`
	Uptime   uint64    `json:"uptime,omitempty"`
	LoadAvg  []float64 `json:"load_avg,omitempty"`
}

// NodeNetwork represents node network status
type NodeNetwork struct {
	Interfaces []*NetworkInterface `json:"interfaces,omitempty"`
	Routing    []*Route            `json:"routing,omitempty"`
	Firewall   []*FirewallRule     `json:"firewall,omitempty"`
}

// NetworkInterface represents a network interface
type NetworkInterface struct {
	Name    string            `json:"name,omitempty"`
	IP      string            `json:"ip,omitempty"`
	MAC     string            `json:"mac,omitempty"`
	Status  string            `json:"status,omitempty"`
	Speed   float64           `json:"speed,omitempty"`
	Metrics map[string]uint64 `json:"metrics,omitempty"`
}

// Route represents a network route
type Route struct {
	Destination string `json:"destination,omitempty"`
	Gateway     string `json:"gateway,omitempty"`
	Interface   string `json:"interface,omitempty"`
	Metric      int    `json:"metric,omitempty"`
}

// FirewallRule represents a firewall rule
type FirewallRule struct {
	Action      string `json:"action,omitempty"`
	Protocol    string `json:"protocol,omitempty"`
	Source      string `json:"source,omitempty"`
	Destination string `json:"destination,omitempty"`
	Ports       []int  `json:"ports,omitempty"`
	Enabled     bool   `json:"enabled,omitempty"`
}

// NodeStorage represents node storage status
type NodeStorage struct {
	Devices     []*StorageDevice `json:"devices,omitempty"`
	Filesystems []*Filesystem    `json:"filesystems,omitempty"`
	Mounts      []*Mount         `json:"mounts,omitempty"`
}

// StorageDevice represents a storage device
type StorageDevice struct {
	Name    string            `json:"name,omitempty"`
	Type    string            `json:"type,omitempty"`
	Size    uint64            `json:"size,omitempty"`
	Model   string            `json:"model,omitempty"`
	Vendor  string            `json:"vendor,omitempty"`
	Serial  string            `json:"serial,omitempty"`
	Metrics map[string]uint64 `json:"metrics,omitempty"`
}

// Filesystem represents a filesystem
type Filesystem struct {
	Device    string   `json:"device,omitempty"`
	Type      string   `json:"type,omitempty"`
	Size      uint64   `json:"size,omitempty"`
	Used      uint64   `json:"used,omitempty"`
	Available uint64   `json:"available,omitempty"`
	Usage     float64  `json:"usage,omitempty"`
	Mount     string   `json:"mount,omitempty"`
	Options   []string `json:"options,omitempty"`
}

// Mount represents a mount point
type Mount struct {
	Device     string            `json:"device,omitempty"`
	Mount      string            `json:"mount,omitempty"`
	Type       string            `json:"type,omitempty"`
	Options    []string          `json:"options,omitempty"`
	Statistics map[string]uint64 `json:"statistics,omitempty"`
}

// NodeProcess represents a running process
type NodeProcess struct {
	PID       int               `json:"pid,omitempty"`
	Name      string            `json:"name,omitempty"`
	CPU       float64           `json:"cpu,omitempty"`
	Memory    float64           `json:"memory,omitempty"`
	Status    string            `json:"status,omitempty"`
	User      string            `json:"user,omitempty"`
	Command   string            `json:"command,omitempty"`
	StartTime time.Time         `json:"start_time,omitempty"`
	Metrics   map[string]uint64 `json:"metrics,omitempty"`
}

// ClusterHealthChecker defines the interface for cluster-specific health checking
type ClusterHealthChecker interface {
	HealthChecker

	// CheckClusterHealth checks the health of the cluster
	CheckClusterHealth(ctx context.Context, detailed bool) (*ClusterHealthStatus, error)

	// GetClusterMetrics returns cluster-specific metrics
	GetClusterMetrics(ctx context.Context) (map[string]interface{}, error)

	// GetClusterInfo returns cluster information
	GetClusterInfo() *ClusterInfo
}

// ClusterHealthStatus represents the health status of a cluster
type ClusterHealthStatus struct {
	HealthStatus
	Nodes       map[string]*NodeHealthStatus    `json:"nodes,omitempty"`
	Services    map[string]*ServiceHealthStatus `json:"services,omitempty"`
	Replication *ClusterReplication             `json:"replication,omitempty"`
	Partition   *ClusterPartition               `json:"partition,omitempty"`
}

// ClusterInfo represents cluster information
type ClusterInfo struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Version     string            `json:"version"`
	Size        int               `json:"size"`
	Nodes       int               `json:"nodes"`
	Services    int               `json:"services"`
	Status      string            `json:"status"`
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
}

// ClusterReplication represents cluster replication status
type ClusterReplication struct {
	Factor           int     `json:"factor,omitempty"`
	HealthyNodes     int     `json:"healthy_nodes,omitempty"`
	UnhealthyNodes   int     `json:"unhealthy_nodes,omitempty"`
	ReplicationLag   float64 `json:"replication_lag,omitempty"`
	ConsistencyLevel string  `json:"consistency_level,omitempty"`
	Status           string  `json:"status,omitempty"`
}

// ClusterPartition represents cluster partition status
type ClusterPartition struct {
	PartitionedNodes []string `json:"partitioned_nodes,omitempty"`
	HealthyNodes     []string `json:"healthy_nodes,omitempty"`
	PartitionCount   int      `json:"partition_count,omitempty"`
	Status           string   `json:"status,omitempty"`
}

// HealthCheckBuilder helps build health checkers
type HealthCheckBuilder interface {
	// Name sets the name of the health checker
	Name(name string) HealthCheckBuilder

	// Component sets the component being checked
	Component(component string) HealthCheckBuilder

	// Version sets the version of the component
	Version(version string) HealthCheckBuilder

	// Check sets the check function
	Check(check HealthCheckerFunc) HealthCheckBuilder

	// Timeout sets the timeout for the health check
	Timeout(timeout time.Duration) HealthCheckBuilder

	// Interval sets the interval for health checks
	Interval(interval time.Duration) HealthCheckBuilder

	// Dependencies sets the dependencies of the health checker
	Dependencies(dependencies []string) HealthCheckBuilder

	// Build builds the health checker
	Build() HealthChecker
}
