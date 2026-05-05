#!/usr/bin/env bash
# Calix deploy script. Builds a Linux/amd64 binary, ships everything to the
# Hetzner host (root@157.180.16.39), wires up the systemd unit, and reloads
# Caddy.
#
# Idempotent: re-running just rebuilds + restarts.
set -euo pipefail

HOST="${CALIX_HOST:-root@157.180.16.39}"
REMOTE="/opt/calix"

cd "$(dirname "$0")/.."

echo "==> building calix-api (linux/amd64)"
( cd api && GOOS=linux GOARCH=amd64 CGO_ENABLED=0 \
  go build -ldflags "-s -w -X main.calixCommit=$(git rev-parse --short HEAD 2>/dev/null || echo dev)" \
  -o ../dist/calix-api . )

echo "==> rsync to $HOST:$REMOTE"
ssh "$HOST" "mkdir -p $REMOTE/web && id -u calix >/dev/null 2>&1 || useradd -r -s /usr/sbin/nologin calix"
rsync -av --delete dist/calix-api  "$HOST:$REMOTE/calix-api"
rsync -av --delete web/            "$HOST:$REMOTE/web/"
rsync -av deploy/calix.service     "$HOST:/etc/systemd/system/calix.service"

echo "==> caddy snippet"
# Only add the caddy block if it isn't already present.
ssh "$HOST" "grep -q 'calix.reiers.io' /etc/caddy/Caddyfile || cat >> /etc/caddy/Caddyfile" < deploy/Caddyfile.snippet

echo "==> chown + restart"
ssh "$HOST" "chown -R calix:calix $REMOTE && \
            systemctl daemon-reload && \
            systemctl enable --now calix && \
            systemctl restart calix && \
            sleep 1 && \
            systemctl is-active calix && \
            caddy reload --config /etc/caddy/Caddyfile && \
            echo OK"

echo "==> verify"
sleep 3
curl -fsS https://calix.reiers.io/api/v1/health || echo "warn: health check failed (cert/DNS may still be propagating)"
echo
echo "live: https://calix.reiers.io"
