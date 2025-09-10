# Multi-stage build for VxDB Storage Service
# Stage 1: Builder
FROM golang:1.23-alpine AS builder

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
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags '-extldflags "-static"' -o vxstorage ./cmd/vxstorage

# Stage 2: Runtime
FROM alpine:3.19

# Install runtime dependencies
RUN apk --no-cache add ca-certificates tzdata curl lz4-dev

# Create non-root user
RUN addgroup -g 1000 -S vxdb && \
    adduser -u 1000 -S vxdb -G vxdb

# Create directories
RUN mkdir -p /var/lib/vxdb/data /var/lib/vxdb/wal /var/lib/vxdb/snapshots /var/lib/vxdb/storage /var/lib/vxdb/temp /var/lib/vxdb/manifest /var/lib/vxdb/backups /var/log/vxdb /etc/vxdb/certs && \
    chown -R vxdb:vxdb /var/lib/vxdb /var/log/vxdb /etc/vxdb

# Set working directory
WORKDIR /app

# Copy binary from builder stage
COPY --from=builder /app/vxstorage .

# Copy configuration files
COPY --chown=vxdb:vxdb configs/vxstorage-production.yaml /etc/vxdb/config.yaml

# Copy health check script
COPY --chown=vxdb:vxdb scripts/health-check.sh /usr/local/bin/health-check.sh
RUN chmod +x /usr/local/bin/health-check.sh

# Copy backup script
COPY --chown=vxdb:vxdb scripts/backup.sh /usr/local/bin/backup.sh
RUN chmod +x /usr/local/bin/backup.sh

# Copy maintenance script
COPY --chown=vxdb:vxdb scripts/maintenance.sh /usr/local/bin/maintenance.sh
RUN chmod +x /usr/local/bin/maintenance.sh

# Switch to non-root user
USER vxdb

# Expose ports
EXPOSE 8082 9094 9095 9096 7948 7949

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD /usr/local/bin/health-check.sh

# Set environment variables
ENV GIN_MODE=release
ENV VXDB_CONFIG_FILE=/etc/vxdb/config.yaml

# Run the application
CMD ["./vxstorage", "--config", "/etc/vxdb/config.yaml"]