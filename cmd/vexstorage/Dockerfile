# Multi-stage build for VexDB Storage Service
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
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags '-extldflags "-static"' -o vexstorage ./cmd/vexstorage

# Stage 2: Runtime
FROM alpine:3.19

# Install runtime dependencies
RUN apk --no-cache add ca-certificates tzdata curl lz4-dev

# Create non-root user
RUN addgroup -g 1000 -S vexdb && \
    adduser -u 1000 -S vexdb -G vexdb

# Create directories
RUN mkdir -p /var/lib/vexdb/data /var/lib/vexdb/wal /var/lib/vexdb/snapshots /var/lib/vexdb/storage /var/lib/vexdb/temp /var/lib/vexdb/manifest /var/lib/vexdb/backups /var/log/vexdb /etc/vexdb/certs && \
    chown -R vexdb:vexdb /var/lib/vexdb /var/log/vexdb /etc/vexdb

# Set working directory
WORKDIR /app

# Copy binary from builder stage
COPY --from=builder /app/vexstorage .

# Copy configuration files
COPY --chown=vexdb:vexdb configs/vexstorage-production.yaml /etc/vexdb/config.yaml

# Copy health check script
COPY --chown=vexdb:vexdb scripts/health-check.sh /usr/local/bin/health-check.sh
RUN chmod +x /usr/local/bin/health-check.sh

# Copy backup script
COPY --chown=vexdb:vexdb scripts/backup.sh /usr/local/bin/backup.sh
RUN chmod +x /usr/local/bin/backup.sh

# Copy maintenance script
COPY --chown=vexdb:vexdb scripts/maintenance.sh /usr/local/bin/maintenance.sh
RUN chmod +x /usr/local/bin/maintenance.sh

# Switch to non-root user
USER vexdb

# Expose ports
EXPOSE 8082 9094 9095 9096 7948 7949

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD /usr/local/bin/health-check.sh

# Set environment variables
ENV GIN_MODE=release
ENV VEXDB_CONFIG_FILE=/etc/vexdb/config.yaml

# Run the application
CMD ["./vexstorage", "--config", "/etc/vexdb/config.yaml"]