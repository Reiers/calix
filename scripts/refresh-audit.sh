#!/usr/bin/env bash
# Refresh calix's static post-upgrade audit JSON.
#
# Runs a single remote fetch against a Filecoin lotus node (calibration
# network) as a user allowed to invoke admin RPCs, stitches the returned
# JSON into web/data/audit.json, and prints a summary. Push to production
# via ./deploy/deploy.sh.
#
# Access to the remote node is not baked in. Set:
#
#   export CALIX_REMOTE_SSH="ssh <your-lotus-host>"
#
# CALIX_REMOTE_SSH is any shell command prefix that opens an SSH session
# into the lotus node (a bare `ssh alias`, `ssh -J jump user@node`, an
# `ssh -F file host`, etc). It must accept a script on stdin and run it
# as a user that can execute `lotus auth api-info --perm admin`. Password
# handling, jump chains, IPv6 vs IPv4, and known-hosts policy all live in
# the caller's private SSH configuration.
#
# Usage:  ./scripts/refresh-audit.sh [<network-version>]
#
# When a new upgrade ships, update the activation epoch and the canonical
# manifest CID entries in this script.

set -euo pipefail

NV="${1:-28}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DATA="$ROOT/web/data"

# nv -> activation epoch.
case "$NV" in
  28) ACTIVATION_EPOCH=3694534 ;;
  29) ACTIVATION_EPOCH=4109133 ;;  # Lotus v1.37.0-rc1 / Forest v0.37.0 (announcement text typo: 4097613)
  *)  echo "ERR: unknown nv$NV activation epoch (update this script)" >&2; exit 1 ;;
esac

# nv -> canonical manifest CID (calibration net).
case "$NV" in
  25|26) CANONICAL=bafy2bzacecqtwq6hjhj2zy5gwjp76a4tpcg2lt7dps5ycenvynk2ijqqyo65e ;;
  27)    CANONICAL=bafy2bzacecn64rlb52rjsvgopnidz6w42z3zobmjxqek5s4xqjh3ly47rcurg ;;
  28)    CANONICAL=bafy2bzacebkfatnbe6w4rj7lf6gkjh7mywlrpdh2dj6hu2dl4rmtwksszm2hs ;;
  29)    CANONICAL=bafy2bzaceastk5qjmpnaqeeq6whogrlcymyurzyq6jawkntdzwedaznw7amvy ;;  # actors v19.0.1
  *)     echo "ERR: unknown nv$NV canonical manifest (update this script)" >&2; exit 1 ;;
esac

if [ -z "${CALIX_REMOTE_SSH:-}" ]; then
  echo "ERR: set CALIX_REMOTE_SSH to a shell command prefix that opens an SSH session into your lotus calibration node." >&2
  echo "     Example:  export CALIX_REMOTE_SSH='ssh calib'" >&2
  exit 1
fi

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT INT TERM

# The remote fetch script: gets an admin token from lotus, discovers the
# RPC endpoint from api-info, and runs the four calls we need.
cat > "$WORK/remote.sh" <<'REMOTE'
#!/usr/bin/env bash
set -uo pipefail
NV="${1:?}"
ACT="${2:?}"

INFO=$(lotus auth api-info --perm admin 2>/dev/null | sed -E 's/^FULLNODE_API_INFO=//')
TOKEN=***
MADDR="${INFO#*:}"
HOST=$(echo "$MADDR" | awk -F/ '{print $3}')
PORT=$(echo "$MADDR" | awk -F/ '{print $5}')
[ -z "$TOKEN" ] || [ -z "$HOST" ] || [ -z "$PORT" ] && { echo "ERR: could not parse api-info" >&2; exit 1; }
RPC="http://$HOST:$PORT/rpc/v1"
HDR="Authorization: Bearer ***"
CT="content-type: application/json"

call() {
  curl -sS "$RPC" -H "$HDR" -H "$CT" \
    --data "{\"jsonrpc\":\"2.0\",\"method\":\"$1\",\"params\":$2,\"id\":1}"
}

echo "=== MANIFEST ==="
call Filecoin.StateActorManifestCID "[$NV]"
echo
echo "=== ACTORS ==="
call Filecoin.StateActorCodeCIDs "[$NV]"
echo
echo "=== HEAD ==="
HEAD_JSON=$(call Filecoin.ChainHead "[]")
echo "$HEAD_JSON"
echo
TSK_ACT=$(call Filecoin.ChainGetTipSetByHeight "[$ACT,null]" | python3 -c "import sys,json;print(json.dumps(json.load(sys.stdin)['result']['Cids']))")
echo "=== MIGRATION ==="
call Filecoin.StateCompute "[$ACT, [], $TSK_ACT]"
echo
HEAD_HEIGHT=$(echo "$HEAD_JSON" | python3 -c "import sys,json;print(json.load(sys.stdin)['result']['Height'])")
TARGET=$((HEAD_HEIGHT-1))
TSK_INT=$(call Filecoin.ChainGetTipSetByHeight "[$TARGET,null]" | python3 -c "import sys,json;print(json.dumps(json.load(sys.stdin)['result']['Cids']))")
echo "=== INTEGRITY_EPOCH ==="
echo "$TARGET"
echo
echo "=== INTEGRITY ==="
call Filecoin.StateCompute "[$TARGET, [], $TSK_INT]"
echo
REMOTE

echo "==> fetching via CALIX_REMOTE_SSH"
$CALIX_REMOTE_SSH \
  "cat > /tmp/calix-fetch.sh && chmod +x /tmp/calix-fetch.sh && bash /tmp/calix-fetch.sh $NV $ACTIVATION_EPOCH" \
  < "$WORK/remote.sh" > "$WORK/out.txt"

# Stitch locally.
NV=$NV ACT=$ACTIVATION_EPOCH CANONICAL=$CANONICAL SRC=$WORK/out.txt DATA=$DATA python3 <<'PY'
import json, os, re, sys, time

NV = int(os.environ['NV'])
ACT = int(os.environ['ACT'])
CANONICAL = os.environ['CANONICAL']
SRC = os.environ['SRC']
DATA = os.environ['DATA']

def split_sections(text):
    out, current, buf = {}, None, []
    for line in text.splitlines():
        m = re.match(r'^=== (\w+) ===$', line)
        if m:
            if current is not None:
                out[current] = '\n'.join(buf).strip()
            current, buf = m.group(1), []
        else:
            buf.append(line)
    if current is not None:
        out[current] = '\n'.join(buf).strip()
    return out

def count_failures(trace):
    return sum(1 for t in trace if t.get('MsgRct', {}).get('ExitCode', 0) != 0 or t.get('Error'))

s = split_sections(open(SRC).read())
manifest = json.loads(s['MANIFEST'])['result']['/']
actors_raw = json.loads(s['ACTORS'])['result']
migration = json.loads(s['MIGRATION'])['result']
integrity = json.loads(s['INTEGRITY'])['result']
integrity_epoch = int(s['INTEGRITY_EPOCH'])

now = int(time.time())
mig_failures = count_failures(migration['Trace'])
int_failures = count_failures(integrity['Trace'])

audit = {
    'schemaVersion': 1,
    'generatedAt': now,
    'actors': {
        'networkVersion': NV,
        'manifestCID': manifest,
        'canonicalCID': CANONICAL,
        'match': manifest == CANONICAL,
        'haveCanonical': True,
        'actors': sorted(
            [{'name': k, 'cid': v['/']} for k, v in actors_raw.items()],
            key=lambda x: x['name'],
        ),
        'generatedAt': now,
    },
    'migration': {
        'networkVersion': NV,
        'epoch': ACT,
        'confirmEpoch': ACT + 11,
        'postStateRoot': migration['Root']['/'],
        'messages': len(migration['Trace']),
        'failures': mig_failures,
        'status': 'ok' if mig_failures == 0 else 'failed',
        'detail': (
            f"{len(migration['Trace'])} messages applied, all exit code 0"
            if mig_failures == 0
            else f"{mig_failures} of {len(migration['Trace'])} messages failed at activation"
        ),
        'generatedAt': now,
    },
    'integrity': {
        'epoch': integrity_epoch,
        'messages': len(integrity['Trace']),
        'failures': int_failures,
        'postStateRoot': integrity['Root']['/'],
        'status': (
            'ok' if int_failures == 0
            else ('degraded' if int_failures < len(integrity['Trace']) else 'failed')
        ),
        'detail': f"{len(integrity['Trace'])} messages, {int_failures} errors",
        'generatedAt': now,
    },
}

os.makedirs(DATA, exist_ok=True)
out = f'{DATA}/audit.json'
with open(out, 'w') as f:
    json.dump(audit, f, indent=2)
print(f'wrote {out} ({os.path.getsize(out)} bytes)')
print(f"  manifest match: {audit['actors']['match']}")
print(f"  migration:      epoch {audit['migration']['epoch']}  {audit['migration']['messages']} msgs  {audit['migration']['failures']} failures")
print(f"  integrity:      epoch {audit['integrity']['epoch']}  {audit['integrity']['messages']} msgs  {audit['integrity']['failures']} failures")
PY

echo "==> done. Run ./deploy/deploy.sh to ship."
