package monitoring

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// Tracer manages distributed tracing for the VxDB system
type Tracer struct {
	logger       *zap.Logger
	config       *TracingConfig
	tracer       trace.Tracer
	provider     *sdktrace.TracerProvider
	shutdownFunc func() error
}

// TracingConfig represents tracing configuration
type TracingConfig struct {
	Enabled      bool          `yaml:"enabled" json:"enabled"`
	ServiceName  string        `yaml:"service_name" json:"service_name"`
	Endpoint     string        `yaml:"endpoint" json:"endpoint"`
	SamplingRate float64       `yaml:"sampling_rate" json:"sampling_rate"` // 0.0 to 1.0
	BatchSize    int           `yaml:"batch_size" json:"batch_size"`
	Timeout      time.Duration `yaml:"timeout" json:"timeout"`
	MaxPayload   int           `yaml:"max_payload" json:"max_payload"`
}

// DefaultTracingConfig returns default tracing configuration
func DefaultTracingConfig() *TracingConfig {
	return &TracingConfig{
		Enabled:      false,
		ServiceName:  "vxdb",
		Endpoint:     "http://localhost:14268/api/traces",
		SamplingRate: 0.1, // 10% sampling
		BatchSize:    512,
		Timeout:      30 * time.Second,
		MaxPayload:   4096,
	}
}

// NewTracer creates a new tracer instance
func NewTracer(logger *zap.Logger, config *TracingConfig) (*Tracer, error) {
	if config == nil {
		config = DefaultTracingConfig()
	}

	t := &Tracer{
		logger: logger,
		config: config,
	}

	if config.Enabled {
		if err := t.initializeTracing(); err != nil {
			return nil, fmt.Errorf("failed to initialize tracing: %w", err)
		}
	} else {
		// Create a no-op tracer when tracing is disabled
		t.tracer = otel.Tracer(config.ServiceName)
	}

	return t, nil
}

// initializeTracing initializes OpenTelemetry tracing
func (t *Tracer) initializeTracing() error {
	// Create Jaeger exporter
	exp, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(t.config.Endpoint)))
	if err != nil {
		return fmt.Errorf("failed to create Jaeger exporter: %w", err)
	}

	// Create resource
	res, err := resource.New(context.Background(),
		resource.WithAttributes(
			semconv.ServiceNameKey.String(t.config.ServiceName),
			semconv.ServiceVersionKey.String("1.0.0"),
		),
	)
	if err != nil {
		return fmt.Errorf("failed to create resource: %w", err)
	}

	// Create trace provider
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.TraceIDRatioBased(t.config.SamplingRate)),
	)

	// Set global trace provider
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	t.provider = tp
	t.tracer = tp.Tracer(t.config.ServiceName)
	t.shutdownFunc = func() error { return tp.Shutdown(context.Background()) }

	t.logger.Info("Tracing initialized",
		zap.String("service", t.config.ServiceName),
		zap.String("endpoint", t.config.Endpoint),
		zap.Float64("sampling_rate", t.config.SamplingRate))

	return nil
}

// StartSpan starts a new span
func (t *Tracer) StartSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	if !t.config.Enabled {
		return ctx, trace.SpanFromContext(ctx)
	}

	return t.tracer.Start(ctx, name, opts...)
}

// StartSpanFromContext starts a new span from an existing context
func (t *Tracer) StartSpanFromContext(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return t.StartSpan(ctx, name, opts...)
}

// WithSpan executes a function within a span
func (t *Tracer) WithSpan(ctx context.Context, name string, fn func(context.Context) error) error {
	if !t.config.Enabled {
		return fn(ctx)
	}

	ctx, span := t.StartSpan(ctx, name)
	defer span.End()

	err := fn(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	return err
}

// RecordError records an error on the current span
func (t *Tracer) RecordError(ctx context.Context, err error) {
	if !t.config.Enabled {
		return
	}

	span := trace.SpanFromContext(ctx)
	if span != nil {
		span.RecordError(err)
	}
}

// SetAttributes sets attributes on the current span
func (t *Tracer) SetAttributes(ctx context.Context, attrs ...attribute.KeyValue) {
	if !t.config.Enabled {
		return
	}

	span := trace.SpanFromContext(ctx)
	if span != nil {
		span.SetAttributes(attrs...)
	}
}

// AddEvent adds an event to the current span
func (t *Tracer) AddEvent(ctx context.Context, name string, attrs ...attribute.KeyValue) {
	if !t.config.Enabled {
		return
	}

	span := trace.SpanFromContext(ctx)
	if span != nil {
		span.AddEvent(name, trace.WithAttributes(attrs...))
	}
}

// SetStatus sets the status on the current span
func (t *Tracer) SetStatus(ctx context.Context, code codes.Code, description string) {
	if !t.config.Enabled {
		return
	}

	span := trace.SpanFromContext(ctx)
	if span != nil {
		span.SetStatus(code, description)
	}
}

// ContextFromSpan extracts context from a span
func (t *Tracer) ContextFromSpan(span trace.Span) context.Context {
	return trace.ContextWithSpan(context.Background(), span)
}

// SpanFromContext extracts span from context
func (t *Tracer) SpanFromContext(ctx context.Context) trace.Span {
	return trace.SpanFromContext(ctx)
}

// Shutdown gracefully shuts down the tracer
func (t *Tracer) Shutdown() error {
	if t.shutdownFunc != nil {
		t.logger.Info("Shutting down tracer")
		return t.shutdownFunc()
	}
	return nil
}

// TraceContextCarrier carries tracing information across process boundaries
type TraceContextCarrier struct {
	TraceID string
	SpanID  string
	Sampled bool
	Baggage map[string]string
}

// NewTraceContextCarrier creates a new trace context carrier
func NewTraceContextCarrier() *TraceContextCarrier {
	return &TraceContextCarrier{
		Baggage: make(map[string]string),
	}
}

// Inject injects tracing context into a carrier
func (t *Tracer) Inject(ctx context.Context, carrier *TraceContextCarrier) {
	if !t.config.Enabled {
		return
	}

	span := trace.SpanFromContext(ctx)
	if span == nil {
		return
	}

	spanCtx := span.SpanContext()
	carrier.TraceID = spanCtx.TraceID().String()
	carrier.SpanID = spanCtx.SpanID().String()
	carrier.Sampled = spanCtx.IsSampled()

	// Extract baggage
	baggage := baggage.FromContext(ctx)
	for _, entry := range baggage.Members() {
		carrier.Baggage[entry.Key()] = entry.Value()
	}
}

// Extract extracts tracing context from a carrier
func (t *Tracer) Extract(ctx context.Context, carrier *TraceContextCarrier) context.Context {
	if !t.config.Enabled {
		return ctx
	}

	if carrier.TraceID == "" || carrier.SpanID == "" {
		return ctx
	}

	// Parse trace ID and span ID
	traceID, err := trace.TraceIDFromHex(carrier.TraceID)
	if err != nil {
		t.logger.Error("Failed to parse trace ID", zap.Error(err))
		return ctx
	}

	spanID, err := trace.SpanIDFromHex(carrier.SpanID)
	if err != nil {
		t.logger.Error("Failed to parse span ID", zap.Error(err))
		return ctx
	}

	// Create span context
	spanCtx := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})

	// Create context with span context
	ctx = trace.ContextWithSpanContext(ctx, spanCtx)

	// Add baggage
	if len(carrier.Baggage) > 0 {
		bag, _ := baggage.New()
		for key, value := range carrier.Baggage {
			member, err := baggage.NewMember(key, value)
			if err != nil {
				t.logger.Error("Failed to create baggage member", zap.String("key", key), zap.Error(err))
				continue
			}
			bag, _ = bag.SetMember(member)
		}
		ctx = baggage.ContextWithBaggage(ctx, bag)
	}

	return ctx
}

// TraceMiddleware provides middleware for HTTP request tracing
type TraceMiddleware struct {
	tracer *Tracer
}

// NewTraceMiddleware creates a new trace middleware
func NewTraceMiddleware(tracer *Tracer) *TraceMiddleware {
	return &TraceMiddleware{
		tracer: tracer,
	}
}

// Wrap wraps an HTTP handler with tracing
func (tm *TraceMiddleware) Wrap(handlerName string, handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, span := tm.tracer.StartSpan(r.Context(), handlerName,
			trace.WithAttributes(
				semconv.HTTPMethodKey.String(r.Method),
				semconv.HTTPURLKey.String(r.URL.String()),
				semconv.HTTPUserAgentKey.String(r.UserAgent()),
				semconv.HTTPSchemeKey.String(r.URL.Scheme),
				semconv.HTTPTargetKey.String(r.URL.Path),
			),
		)
		defer span.End()

		// Update request context
		r = r.WithContext(ctx)

		// Call handler
		handler.ServeHTTP(w, r)

		// Set span status based on response
		if ww, ok := w.(*responseWriterWrapper); ok {
			if ww.status >= 400 {
				span.SetStatus(codes.Error, fmt.Sprintf("HTTP %d", ww.status))
			} else {
				span.SetStatus(codes.Ok, "OK")
			}

			span.SetAttributes(
				semconv.HTTPStatusCodeKey.Int(ww.status),
			)
		}
	})
}

// responseWriterWrapper wraps http.ResponseWriter to capture status code
type responseWriterWrapper struct {
	http.ResponseWriter
	status int
}

func (w *responseWriterWrapper) WriteHeader(statusCode int) {
	w.status = statusCode
	w.ResponseWriter.WriteHeader(statusCode)
}

// GRPCInterceptor provides gRPC interceptor for tracing
func (t *Tracer) GRPCInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		ctx, span := t.StartSpan(ctx, info.FullMethod,
			trace.WithAttributes(
				semconv.RPCSystemKey.String("grpc"),
				semconv.RPCServiceKey.String(info.FullMethod),
			),
		)
		defer span.End()

		resp, err := handler(ctx, req)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		} else {
			span.SetStatus(codes.Ok, "OK")
		}

		return resp, err
	}
}
