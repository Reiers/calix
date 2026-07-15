#!/usr/bin/env bash
# Refresh calix's static post-upgrade audit JSON.
#
# Calix runs against a public Lotus RPC (Glif) that gates the admin methods
# we need for actor manifest / migration audit / state integrity. Rather
# than deploying a permanent SSH tunnel from Hetzner to the calibration
# datacenter, we refresh the audit data off-band: open a temporary tunnel
# from this laptop, pull the data, write web/data/audit.json, push to
# Hetzner via the deploy script, close the tunnel.
#
# Usage:  ./scripts/refresh-audit.sh [<network-version>]
#
# Defaults to nv28 (Fire Horse). When a new upgrade ships, update both the
# canonical manifest CID inside this script and the table in
# .vault/calix-canonical-manifests.md.

set -euo pipefail

NV="${1:-28}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DATA="$ROOT/web/data"
WORK="/tmp/calix-audit"

# nv → activation epoch.
case "$NV" in
  28) ACTIVATION_EPOCH=3694534 ;;
  *)  echo "ERR: unknown nv$NV activation epoch (update this script)" >&2; exit 1 ;;
esac

# nv → canonical manifest CID. Calibration values pulled from a known-good
# lotus node on the day of activation. Mainnet would differ.
case "$NV" in
  25|26) CANONICAL=bafy2bzacecqtwq6hjhj2zy5gwjp76a4tpcg2lt7dps5ycenvynk2ijqqyo65e ;;
  27)    CANONICAL=bafy2bzacecn64rlb52rjsvgopnidz6w42z3zobmjxqek5s4xqjh3ly47rcurg ;;
  28)    CANONICAL=bafy2bzacebkfatnbe6w4rj7lf6gkjh7mywlrpdh2dj6hu2dl4rmtwksszm2hs ;;
  *)     echo "ERR: unknown nv$NV canonical manifest (update this script)" >&2; exit 1 ;;
esac

# 1. Open the temporary tunnel (lexluthr jump → calib node).
JUMP_PW_FILE=$(mktemp); chmod 600 "$JUMP_PW_FILE"
trap 'rm -f "$JUMP_PW_FILE"; lsof -iTCP:1235 -sTCP:LISTEN -t 2>/dev/null | xargs -r kill 2>/dev/null || true' EXIT INT TERM

JUMP_PW=$(grep -E '^- Jump host' "$HOME/.openclaw/workspace/.vault/calibration-tunnel.md" | grep -oE 'password=\S+' | sed 's/password=//')
DEST_PW=$(grep -E '^- Calibration node' "$HOME/.openclaw/workspace/.vault/calibration-tunnel.md" | grep -oE 'password=\S+' | sed 's/password=//')
[ -z "$JUMP_PW" ] && { echo "ERR: jump password not found in vault" >&2; exit 1; }
[ -z "$DEST_PW" ] && { echo "ERR: dest password not found in vault" >&2; exit 1; }
printf '%s' "$JUMP_PW" > "$JUMP_PW_FILE"

PROXY="sshpass -f $JUMP_PW_FILE ssh -W %h:%p -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=$HOME/.ssh/known_hosts_calix -o PubkeyAuthentication=no -o PreferredAuthentications=password lexluthr@37.202.57.171"

echo "==> opening tunnel"
SSHPASS="$DEST_PW" nohup sshpass -e ssh \
  -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile="$HOME/.ssh/known_hosts_calix" \
  -o PubkeyAuthentication=no -o PreferredAuthentications=password \
  -o ServerAliveInterval=30 -o ServerAliveCountMax=3 -o ExitOnForwardFailure=yes \
  -N -L 1235:192.168.3.127:1234 \
  -o "ProxyCommand=$PROXY" \
  reiers@192.168.3.127 \
  >/tmp/calix-tunnel.log 2>&1 &
disown
sleep 4

# 2. Pull the admin token via SSH (don't store it).
TOKEN=$(SSHPASS="$DEST_PW" sshpass -e ssh \
  -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile="$HOME/.ssh/known_hosts_calix" \
  -o PubkeyAuthentication=no -o PreferredAuthentications=password \
  -o "ProxyCommand=$PROXY" \
  reiers@192.168.3.127 \
  'lotus auth api-info --perm admin 2>/dev/null | sed -E "s/^FULLNODE_API_INFO=//; s/:.*//"')

[ -z "$TOKEN" ] && { echo "ERR: could not pull admin token" >&2; exit 1; }
RPC="http://127.0.0.1:1235/rpc/v1"
H="Authorization: Bearer $TOKEN"

# 3. Pull live data into temp dir.
mkdir -p "$WORK"
echo "==> fetching manifest, migration, integrity"
curl -sS "$RPC" -H "$H" -H 'content-type: application/json' \
  --data "{\"jsonrpc\":\"2.0\",\"method\":\"Filecoin.StateActorManifestCID\",\"params\":[$NV],\"id\":1}" > "$WORK/manifest.json"
curl -sS "$RPC" -H "$H" -H 'content-type: application/json' \
  --data "{\"jsonrpc\":\"2.0\",\"method\":\"Filecoin.StateActorCodeCIDs\",\"params\":[$NV],\"id\":1}" > "$WORK/actors.json"
curl -sS "$RPC" -H "$H" -H 'content-type: application/json' \
  --data "{\"jsonrpc\":\"2.0\",\"method\":\"Filecoin.ChainGetTipSetByHeight\",\"params\":[$ACTIVATION_EPOCH,null],\"id\":1}" > "$WORK/ts-activation.json"
TSK=$(python3 -c "import json; print(json.dumps(json.load(open('$WORK/ts-activation.json'))['result']['Cids']))")
curl -sS "$RPC" -H "$H" -H 'content-type: application/json' \
  --data "{\"jsonrpc\":\"2.0\",\"method\":\"Filecoin.StateCompute\",\"params\":[$ACTIVATION_EPOCH, [], $TSK],\"id\":1}" > "$WORK/migration.json"

curl -sS "$RPC" -H "$H" -H 'content-type: application/json' \
  --data '{"jsonrpc":"2.0","method":"Filecoin.ChainHead","params":[],"id":1}' > "$WORK/head.json"
TARGET=$(python3 -c "import json; print(json.load(open('$WORK/head.json'))['result']['Height']-1)")
curl -sS "$RPC" -H "$H" -H 'content-type: application/json' \
  --data "{\"jsonrpc\":\"2.0\",\"method\":\"Filecoin.ChainGetTipSetByHeight\",\"params\":[$TARGET,null],\"id\":1}" > "$WORK/ts-integrity.json"
TSK2=$(python3 -c "import json; print(json.dumps(json.load(open('$WORK/ts-integrity.json'))['result']['Cids']))")
curl -sS "$RPC" -H "$H" -H 'content-type: application/json' \
  --data "{\"jsonrpc\":\"2.0\",\"method\":\"Filecoin.StateCompute\",\"params\":[$TARGET, [], $TSK2],\"id\":1}" > "$WORK/integrity.json"

# 4. Stitch into the static audit JSON.
mkdir -p "$DATA"
NV=$NV TARGET=$TARGET CANONICAL=$CANONICAL ACTIVATION_EPOCH=$ACTIVATION_EPOCH WORK=$WORK DATA=$DATA python3 <<'PY'
import json, time, os

NV = int(os.environ['NV'])
TARGET = int(os.environ['TARGET'])
CANONICAL = os.environ['CANONICAL']
ACTIVATION = int(os.environ['ACTIVATION_EPOCH'])
WORK = os.environ['WORK']
DATA = os.environ['DATA']

manifest = json.load(open(f"{WORK}/manifest.json"))['result']['/']
actors_raw = json.load(open(f"{WORK}/actors.json"))['result']
migration = json.load(open(f"{WORK}/migration.json"))['result']
integrity = json.load(open(f"{WORK}/integrity.json"))['result']

def count_failures(trace):
    return sum(1 for t in trace if t.get('MsgRct',{}).get('ExitCode',0) != 0 or t.get('Error',''))

now = int(time.time())
mig_failures = count_failures(migration['Trace'])
int_failures = count_failures(integrity['Trace'])

audit = {
    "schemaVersion": 1,
    "generatedAt": now,
    "actors": {
        "networkVersion": NV,
        "manifestCID": manifest,
        "canonicalCID": CANONICAL,
        "match": manifest == CANONICAL,
        "haveCanonical": True,
        "actors": sorted(
            [{"name": k, "cid": v["/"]} for k,v in actors_raw.items()],
            key=lambda x: x["name"]
        ),
        "generatedAt": now,
    },
    "migration": {
        "networkVersion": NV,
        "epoch": ACTIVATION,
        "confirmEpoch": ACTIVATION + 11,
        "postStateRoot": migration["Root"]["/"],
        "messages": len(migration["Trace"]),
        "failures": mig_failures,
        "status": "ok" if mig_failures == 0 else "failed",
        "detail": (
            f"{len(migration['Trace'])} messages applied, all exit code 0"
            if mig_failures == 0
            else f"{mig_failures} of {len(migration['Trace'])} messages failed at activation"
        ),
        "generatedAt": now,
    },
    "integrity": {
        "epoch": TARGET,
        "messages": len(integrity["Trace"]),
        "failures": int_failures,
        "postStateRoot": integrity["Root"]["/"],
        "status": "ok" if int_failures == 0 else ("degraded" if int_failures < len(integrity["Trace"]) else "failed"),
        "detail": f"{len(integrity['Trace'])} messages, {int_failures} errors",
        "generatedAt": now,
    },
}

out = f"{DATA}/audit.json"
with open(out, "w") as f:
    json.dump(audit, f, indent=2)
print(f"wrote {out} ({os.path.getsize(out)} bytes)")
print(f"  manifest match: {audit['actors']['match']}")
print(f"  migration:      {audit['migration']['messages']} msgs, {audit['migration']['failures']} failures")
print(f"  integrity:      epoch {audit['integrity']['epoch']}, {audit['integrity']['messages']} msgs, {audit['integrity']['failures']} failures")
PY

echo "==> done. Run ./deploy/deploy.sh to ship."
