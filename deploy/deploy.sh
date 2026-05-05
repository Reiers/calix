#!/usr/bin/env bash
# Calix deploy script.
#
# Builds a Linux/amd64 binary, ships everything to the Hetzner host
# (root@157.180.16.39), wires up the systemd unit, and reloads nginx.
# Idempotent.
set -euo pipefail

HOST="${CALIX_HOST:-root@157.180.16.39}"
REMOTE="/opt/calix"

cd "$(dirname "$0")/.."

echo "==> building calix-api (linux/amd64)"
mkdir -p dist
( cd api && GOOS=linux GOARCH=amd64 CGO_ENABLED=0 \
  go build -ldflags "-s -w -X main.calixCommit=$(git rev-parse --short HEAD 2>/dev/null || echo dev)" \
  -o ../dist/calix-api . )

echo "==> ensuring user + dirs"
ssh "$HOST" "id -u calix >/dev/null 2>&1 || useradd -r -s /usr/sbin/nologin calix; \
            mkdir -p $REMOTE/web"

echo "==> rsync"
rsync -av --delete dist/calix-api  "$HOST:$REMOTE/calix-api"
rsync -av --delete web/            "$HOST:$REMOTE/web/"
rsync -av deploy/calix.service     "$HOST:/etc/systemd/system/calix.service"
rsync -av deploy/calix.nginx       "$HOST:/etc/nginx/sites-available/calix.reiers.io"

echo "==> chown + perms + systemd + nginx"
ssh "$HOST" bash -se <<'REMOTE_SCRIPT'
set -euo pipefail
chown -R calix:calix /opt/calix
# nginx runs as www-data and needs traverse + read on the static tree.
chmod 755 /opt/calix /opt/calix/web
find /opt/calix/web -type d -exec chmod 755 {} \;
find /opt/calix/web -type f -exec chmod 644 {} \;
chmod 755 /opt/calix/calix-api
ln -sf /etc/nginx/sites-available/calix.reiers.io /etc/nginx/sites-enabled/calix.reiers.io
systemctl daemon-reload
systemctl enable --now calix
systemctl restart calix
sleep 1
systemctl is-active calix
nginx -t
systemctl reload nginx
echo "OK"
REMOTE_SCRIPT

echo "==> verify"
sleep 2
echo "--- /api/v1/health ---"
curl -fsS https://calix.reiers.io/api/v1/health || echo "warn: health probe failed"
echo
echo "--- /api/v1/status ---"
curl -fsS https://calix.reiers.io/api/v1/status | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(f\"  level     : {d['level']}\")
print(f\"  headline  : {d['headline']}\")
print(f\"  epoch     : {d['epoch']:,}  (nv{d['networkVersion']})\")
print(f\"  head age  : {d['headAgeSeconds']}s\")
print(f\"  next up   : {d['upgradeName']} in {d['upgradeSecsLeft']}s ({d['upgradeEpochsLeft']} epochs)\")
" 2>/dev/null || true
echo
echo "live: https://calix.reiers.io"
