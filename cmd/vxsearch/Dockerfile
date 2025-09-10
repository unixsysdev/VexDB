# Multi-stage build for VxDB Search Service
# Stage 1: Builder
FROM golang:1.21-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git ca-certificates tzdata

# Set working directory
WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags '-extldflags "-static"' -o vxsearch ./cmd/vxsearch

# Stage 2: Runtime
FROM alpine:3.19

# Install runtime dependencies
RUN apk --no-cache add ca-certificates tzdata curl

# Create non-root user
RUN addgroup -g 1000 -S vxdb && \
    adduser -u 1000 -S vxdb -G vxdb

# Create directories
RUN mkdir -p /var/lib/vxdb/data /var/lib/vxdb/wal /var/lib/vxdb/snapshots /var/lib/vxdb/cache /var/log/vxdb /etc/vxdb/certs && \
    chown -R vxdb:vxdb /var/lib/vxdb /var/log/vxdb /etc/vxdb

# Set working directory
WORKDIR /app

# Copy binary from builder stage
COPY --from=builder /app/vxsearch .

# Copy configuration files
COPY --chown=vxdb:vxdb configs/vxsearch-production.yaml /etc/vxdb/config.yaml

# Copy health check script
COPY --chown=vxdb:vxdb scripts/health-check.sh /usr/local/bin/health-check.sh
RUN chmod +x /usr/local/bin/health-check.sh

# Copy cache management script
COPY --chown=vxdb:vxdb scripts/cache-management.sh /usr/local/bin/cache-management.sh
RUN chmod +x /usr/local/bin/cache-management.sh

# Switch to non-root user
USER vxdb

# Expose ports
EXPOSE 8083 9097 9098 9099 7950 7951

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD /usr/local/bin/health-check.sh

# Set environment variables
ENV GIN_MODE=release
ENV VXDB_CONFIG_FILE=/etc/vxdb/config.yaml

# Run the application
CMD ["./vxsearch", "--config", "/etc/vxdb/config.yaml"]