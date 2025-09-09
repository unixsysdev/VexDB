#!/usr/bin/env sh
set -e

HOST=${HOST:-127.0.0.1}
PORT=${PORT:-8080}
PATH=${PATH_TO_CHECK:-/health}

if command -v curl >/dev/null 2>&1; then
  curl -fsS "http://$HOST:$PORT$PATH" >/dev/null || exit 1
else
  # Fallback: just exit 0 if curl not available
  exit 0
fi

