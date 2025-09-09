package auth

import (
	"context"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"
	"time"

	"vexdb/internal/logging"
	"vexdb/internal/search/config"

	"github.com/gorilla/mux"
	"go.uber.org/zap"
)

// Middleware provides authentication middleware for the search service
type Middleware struct {
	config *config.AuthConfig
	logger *zap.Logger
}

// NewMiddleware creates a new authentication middleware
func NewMiddleware(cfg *config.AuthConfig, logger *zap.Logger) *Middleware {
	return &Middleware{
		config: cfg,
		logger: logger,
	}
}

// AuthMiddleware returns the authentication middleware function
func (m *Middleware) AuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip authentication if disabled
		if !m.config.Enabled {
			next.ServeHTTP(w, r)
			return
		}

		// Extract authentication information
		authInfo, err := m.extractAuthInfo(r)
		if err != nil {
			m.sendUnauthorized(w, "Invalid authentication header")
			return
		}

		// Validate authentication
		if err := m.validateAuth(authInfo); err != nil {
			m.logger.Warn("Authentication failed", zap.String("type", authInfo.Type), zap.Error(err))
			m.sendUnauthorized(w, "Authentication failed")
			return
		}

		// Add authentication info to context
		ctx := context.WithValue(r.Context(), AuthContextKey, authInfo)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// AuthInfo represents authentication information
type AuthInfo struct {
	Type    string
	UserID  string
	APIKey  string
	Token   string
	Headers map[string]string
}

// AuthContextKey is the key used to store auth info in context
type contextKey string

const AuthContextKey contextKey = "auth_info"

// GetAuthInfo retrieves authentication information from context
func GetAuthInfo(ctx context.Context) (*AuthInfo, bool) {
	authInfo, ok := ctx.Value(AuthContextKey).(*AuthInfo)
	return authInfo, ok
}

// extractAuthInfo extracts authentication information from the request
func (m *Middleware) extractAuthInfo(r *http.Request) (*AuthInfo, error) {
	authInfo := &AuthInfo{
		Headers: make(map[string]string),
	}

	// Extract authorization header
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		// Check for API key in query parameters
		apiKey := r.URL.Query().Get("api_key")
		if apiKey != "" {
			authInfo.Type = "api_key"
			authInfo.APIKey = apiKey
			return authInfo, nil
		}

		// Check for API key in custom header
		apiKey = r.Header.Get("X-API-Key")
		if apiKey != "" {
			authInfo.Type = "api_key"
			authInfo.APIKey = apiKey
			return authInfo, nil
		}

		return nil, fmt.Errorf("no authentication provided")
	}

	// Parse authorization header
	parts := strings.SplitN(authHeader, " ", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid authorization header format")
	}

	scheme := strings.ToLower(parts[0])
	credentials := parts[1]

	switch scheme {
	case "basic":
		authInfo.Type = "basic"
		username, password, err := parseBasicAuth(credentials)
		if err != nil {
			return nil, err
		}
		authInfo.UserID = username
		authInfo.Headers["username"] = username
		authInfo.Headers["password"] = password

	case "bearer":
		authInfo.Type = "bearer"
		authInfo.Token = credentials

	default:
		return nil, fmt.Errorf("unsupported authentication scheme: %s", scheme)
	}

	return authInfo, nil
}

// validateAuth validates the authentication information
func (m *Middleware) validateAuth(authInfo *AuthInfo) error {
	switch authInfo.Type {
	case "basic":
		return m.validateBasicAuth(authInfo.Headers["username"], authInfo.Headers["password"])
	case "bearer":
		return m.validateBearerAuth(authInfo.Token)
	case "api_key":
		return m.validateAPIKey(authInfo.APIKey)
	default:
		return fmt.Errorf("unsupported authentication type: %s", authInfo.Type)
	}
}

// validateBasicAuth validates basic authentication
func (m *Middleware) validateBasicAuth(username, password string) error {
	if m.config.Type != "basic" {
		return fmt.Errorf("basic authentication not enabled")
	}

	// Use constant-time comparison to prevent timing attacks
	usernameMatch := subtle.ConstantTimeCompare([]byte(username), []byte(m.config.BasicAuth.Username)) == 1
	passwordMatch := subtle.ConstantTimeCompare([]byte(password), []byte(m.config.BasicAuth.Password)) == 1

	if !usernameMatch || !passwordMatch {
		return fmt.Errorf("invalid credentials")
	}

	return nil
}

// validateBearerAuth validates bearer token authentication
func (m *Middleware) validateBearerAuth(token string) error {
	if m.config.Type != "bearer" {
		return fmt.Errorf("bearer authentication not enabled")
	}

	// Simple token validation - in production, use JWT or similar
	if token != m.config.BearerAuth.Secret {
		return fmt.Errorf("invalid token")
	}

	return nil
}

// validateAPIKey validates API key authentication
func (m *Middleware) validateAPIKey(apiKey string) error {
	if m.config.Type != "api_key" {
		return fmt.Errorf("API key authentication not enabled")
	}

	// Check if API key is in the allowed list
	for _, allowedKey := range m.config.APIKeys {
		if subtle.ConstantTimeCompare([]byte(apiKey), []byte(allowedKey)) == 1 {
			return nil
		}
	}

	return fmt.Errorf("invalid API key")
}

// parseBasicAuth parses basic authentication credentials
func parseBasicAuth(credentials string) (username, password string, err error) {
	decoded, err := base64.StdEncoding.DecodeString(credentials)
	if err != nil {
		return "", "", fmt.Errorf("failed to decode basic auth credentials: %w", err)
	}

	parts := strings.SplitN(string(decoded), ":", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid basic auth format")
	}

	return parts[0], parts[1], nil
}

// sendUnauthorized sends an unauthorized response
func (m *Middleware) sendUnauthorized(w http.ResponseWriter, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("WWW-Authenticate", m.getAuthenticateHeader())
	w.WriteHeader(http.StatusUnauthorized)

	response := map[string]interface{}{
		"success":   false,
		"error":     "unauthorized",
		"message":   message,
		"timestamp": time.Now().Format(time.RFC3339),
	}

	// Simple JSON encoding - in production, use proper JSON marshaling
	jsonResponse := fmt.Sprintf(`{"success":false,"error":"unauthorized","message":"%s","timestamp":"%s"}`,
		message, time.Now().Format(time.RFC3339))

	w.Write([]byte(jsonResponse))
}

// getAuthenticateHeader returns the appropriate WWW-Authenticate header
func (m *Middleware) getAuthenticateHeader() string {
	switch m.config.Type {
	case "basic":
		return `Basic realm="VexDB Search Service"`
	case "bearer":
		return `Bearer realm="VexDB Search Service"`
	case "api_key":
		return `ApiKey realm="VexDB Search Service"`
	default:
		return `Basic realm="VexDB Search Service"`
	}
}

// RateLimitMiddleware provides rate limiting middleware
type RateLimitMiddleware struct {
	config    *config.RateLimitConfig
	logger    *zap.Logger
	rateLimit *RateLimiter
}

// NewRateLimitMiddleware creates a new rate limiting middleware
func NewRateLimitMiddleware(cfg *config.RateLimitConfig, logger *zap.Logger) *RateLimitMiddleware {
	rateLimit := NewRateLimiter(cfg.RequestsPerSecond, cfg.BurstSize, cfg.CleanupInterval)
	
	return &RateLimitMiddleware{
		config:    cfg,
		logger:    logger,
		rateLimit: rateLimit,
	}
}

// RateLimitMiddleware returns the rate limiting middleware function
func (m *RateLimitMiddleware) RateLimitMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip rate limiting if disabled
		if !m.config.Enabled {
			next.ServeHTTP(w, r)
			return
		}

		// Get identifier for rate limiting
		identifier := m.getIdentifier(r)
		
		// Check rate limit
		if !m.rateLimit.Allow(identifier) {
			m.logger.Warn("Rate limit exceeded", zap.String("identifier", identifier))
			m.sendRateLimitExceeded(w)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// getIdentifier returns the identifier for rate limiting
func (m *RateLimitMiddleware) getIdentifier(r *http.Request) string {
	var identifiers []string

	// Add IP address if enabled
	if m.config.ByIP {
		ip := getClientIP(r)
		identifiers = append(identifiers, fmt.Sprintf("ip:%s", ip))
	}

	// Add user identifier if available and enabled
	if m.config.ByUser {
		if authInfo, ok := GetAuthInfo(r.Context()); ok && authInfo.UserID != "" {
			identifiers = append(identifiers, fmt.Sprintf("user:%s", authInfo.UserID))
		}
	}

	// Add API key if available and enabled
	if m.config.ByAPIKey {
		if authInfo, ok := GetAuthInfo(r.Context()); ok && authInfo.APIKey != "" {
			identifiers = append(identifiers, fmt.Sprintf("api_key:%s", authInfo.APIKey))
		}
	}

	// If no identifiers were added, use IP as fallback
	if len(identifiers) == 0 {
		ip := getClientIP(r)
		identifiers = append(identifiers, fmt.Sprintf("ip:%s", ip))
	}

	// Combine identifiers
	return strings.Join(identifiers, "|")
}

// sendRateLimitExceeded sends a rate limit exceeded response
func (m *RateLimitMiddleware) sendRateLimitExceeded(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-RateLimit-Limit", fmt.Sprintf("%.0f", m.config.RequestsPerSecond))
	w.Header().Set("X-RateLimit-Remaining", "0")
	w.Header().Set("X-RateLimit-Reset", fmt.Sprintf("%d", time.Now().Add(m.config.CleanupInterval).Unix()))
	w.WriteHeader(http.StatusTooManyRequests)

	response := map[string]interface{}{
		"success":   false,
		"error":     "rate_limit_exceeded",
		"message":   "Rate limit exceeded. Please try again later.",
		"timestamp": time.Now().Format(time.RFC3339),
	}

	// Simple JSON encoding - in production, use proper JSON marshaling
	jsonResponse := fmt.Sprintf(`{"success":false,"error":"rate_limit_exceeded","message":"Rate limit exceeded. Please try again later.","timestamp":"%s"}`,
		time.Now().Format(time.RFC3339))

	w.Write([]byte(jsonResponse))
}

// getClientIP extracts the client IP address from the request
func getClientIP(r *http.Request) string {
	// Check X-Forwarded-For header first
	xForwardedFor := r.Header.Get("X-Forwarded-For")
	if xForwardedFor != "" {
		// Take the first IP in the chain
		ips := strings.Split(xForwardedFor, ",")
		if len(ips) > 0 {
			return strings.TrimSpace(ips[0])
		}
	}

	// Check X-Real-IP header
	xRealIP := r.Header.Get("X-Real-IP")
	if xRealIP != "" {
		return xRealIP
	}

	// Fall back to RemoteAddr
	ip := r.RemoteAddr
	if colonIndex := strings.LastIndex(ip, ":"); colonIndex != -1 {
		ip = ip[:colonIndex]
	}
	return ip
}

// CORSMiddleware provides CORS middleware
type CORSMiddleware struct {
	config *config.CORSConfig
}

// NewCORSMiddleware creates a new CORS middleware
func NewCORSMiddleware(cfg *config.CORSConfig) *CORSMiddleware {
	return &CORSMiddleware{config: cfg}
}

// CORSMiddleware returns the CORS middleware function
func (m *CORSMiddleware) CORSMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip CORS if disabled
		if !m.config.Enabled {
			next.ServeHTTP(w, r)
			return
		}

		// Set CORS headers
		w.Header().Set("Access-Control-Allow-Origin", m.getAllowedOrigin(r.Header.Get("Origin")))
		w.Header().Set("Access-Control-Allow-Methods", strings.Join(m.config.AllowedMethods, ", "))
		w.Header().Set("Access-Control-Allow-Headers", strings.Join(m.config.AllowedHeaders, ", "))
		
		if m.config.AllowCredentials {
			w.Header().Set("Access-Control-Allow-Credentials", "true")
		}

		if len(m.config.ExposedHeaders) > 0 {
			w.Header().Set("Access-Control-Expose-Headers", strings.Join(m.config.ExposedHeaders, ", "))
		}

		if m.config.MaxAge > 0 {
			w.Header().Set("Access-Control-Max-Age", fmt.Sprintf("%d", m.config.MaxAge))
		}

		// Handle preflight requests
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// getAllowedOrigin returns the allowed origin for the request
func (m *CORSMiddleware) getAllowedOrigin(requestOrigin string) string {
	// If all origins are allowed
	for _, origin := range m.config.AllowedOrigins {
		if origin == "*" {
			return "*"
		}
	}

	// Check if the request origin is in the allowed list
	for _, origin := range m.config.AllowedOrigins {
		if origin == requestOrigin {
			return requestOrigin
		}
	}

	// Return the first allowed origin as fallback
	if len(m.config.AllowedOrigins) > 0 {
		return m.config.AllowedOrigins[0]
	}

	return ""
}

// LoggingMiddleware provides request logging middleware
type LoggingMiddleware struct {
	logger *zap.Logger
}

// NewLoggingMiddleware creates a new logging middleware
func NewLoggingMiddleware(logger *zap.Logger) *LoggingMiddleware {
	return &LoggingMiddleware{logger: logger}
}

// LoggingMiddleware returns the logging middleware function
func (m *LoggingMiddleware) LoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Create a response writer wrapper to capture status code
		wrapper := &responseWriterWrapper{
			ResponseWriter: w,
			statusCode:     http.StatusOK,
		}

		// Log request
		m.logger.Info("HTTP request started",
			zap.String("method", r.Method),
			zap.String("path", r.URL.Path),
			zap.String("remote_addr", getClientIP(r)),
			zap.String("user_agent", r.UserAgent()),
		)

		// Process request
		next.ServeHTTP(wrapper, r)

		// Log response
		duration := time.Since(start)
		m.logger.Info("HTTP request completed",
			zap.String("method", r.Method),
			zap.String("path", r.URL.Path),
			zap.Int("status", wrapper.statusCode),
			zap.Duration("duration", duration),
			zap.String("remote_addr", getClientIP(r)),
		)
	})
}

// responseWriterWrapper wraps http.ResponseWriter to capture status code
type responseWriterWrapper struct {
	http.ResponseWriter
	statusCode int
}

func (w *responseWriterWrapper) WriteHeader(statusCode int) {
	w.statusCode = statusCode
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *responseWriterWrapper) Write(data []byte) (int, error) {
	return w.ResponseWriter.Write(data)
}