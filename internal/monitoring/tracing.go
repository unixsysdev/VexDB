package monitoring

// Tracing integration hooks. These are intentionally minimal no-ops
// so the package compiles even when tracing is disabled. When wiring
// OpenTelemetry per SPEC_RFC, these can be extended.

// InitTracing initializes tracing if enabled in config. Returns a shutdown
// function that should be called on service shutdown.
func InitTracing(cfg *Config) (shutdown func(), _ error) {
    return func() {}, nil
}
