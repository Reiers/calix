<p align="center">
  <img src="assets/logo.svg" alt="Calix" width="320">
</p>

<p align="center">
  <b>Filecoin calibration stability console</b><br>
  Live signals, tipset stream, upgrade readiness.<br>
  <a href="https://calix.reiers.io">calix.reiers.io</a>
</p>

---

Calix is a real-time stability console for the Filecoin **calibration**
network. The audience is the people running calibration at scale: storage
providers, protocol engineers, and the network upgrade team. The job is
"is the network healthy right now, and what's about to go wrong before
the next upgrade?".

Calix is part of the wider calibration stability project at
[reiers.io](https://reiers.io). Sister projects:
- [Plumbline](https://faucet.reiers.io): calibration faucet for tFIL + USDFC
- The Doctor calibration cluster

## What's on the dashboard

### Status header
A sticky bar across the top.
- Operational pill: `OPERATIONAL` / `WATCH` / `DEGRADED` / `UPGRADE PENDING`, computed live from chain head age and proximity to the next network upgrade.
- Live epoch, head age, network version, and current blocks-per-epoch cadence.

### Status banner
First panel on the page. Spells out the current operational level in plain English with a reason. For example, during the run-up to Fire Horse: *"Fire Horse upgrade approaching. Network version 28 activates in 1d 20h."*

### Vital signs (8 KPIs)
Eight cards in a grid, each with an inline 60-epoch sparkline. Each card flips colour (green / amber / red) as the underlying value moves through its thresholds.
- **Blocks per epoch**: calibration steady state is around 2.0
- **Null round %**: count of empty tipsets in the window
- **Average win count**: drand winning tickets per epoch
- **Base fee**: in atto, with K/M scaling for spikes
- **Network QAP**: quality-adjusted power
- **Active miners**: those above min-power
- **Total pledge**: locked FIL across the network
- **Initial pledge per 32 GiB CC**: computed locally via the actor v17 formula

### Tipset stream
Sixty vertical bars covering the last 30 minutes of chain history.
- Green when 3+ blocks landed (healthy)
- Amber for 1–2 (degraded)
- Red for 0 (null round)
Hover any bar for `epoch · block count · timestamp`.

### Upgrade readiness
Two cards: the current network version (nv28 Fire Horse, activated epoch 3,694,534) and a live countdown to the next upgrade: **nv29 Solstice**, epoch 4,109,133, 2026-09-28T12:59:30Z (window 12:59-13:59 UTC). Driven by one schedule table (`calibUpgrades` in `api/main.go`, `UPGRADES` in `web/index.html`) that flips automatically at activation. `/api/v1/upgrade` returns `current`, `next` and the full `upgrades` list.

### Network power chart
Twenty-four-hour QAP trend, drawn as a native SVG line + area. Redraws on window resize so the geometry stays in real pixels.

### Leaderboards
- **Top miners by power**: raw, QAP, 24-hour delta, blocks mined, and on-chain tags (Curio Storage Inc., etc.)
- **Rich list**: top accounts by balance, with actor type and percent of supply

### Ecosystem
- **FEVM**: daily contract creates, invocations, gas
- **Plumbline**: calibration faucet card
- **Open source**: link to this repository

### Tools
Bidirectional epoch ↔ time converter. Calibration genesis 1667326380, 30-second epochs.

## Architecture

```
calix.reiers.io   (Cloudflare → nginx)
  ├── /          → static dashboard (web/index.html)
  └── /api/v1/*  → calix-api (Go, 127.0.0.1:8765)
                    ├── reads f04 (power) + f02 (reward) actor state
                    ├── samples last 60 tipsets via ChainGetTipSetByHeight
                    ├── computes IP via the actor v17 formula
                    ├── classifies status from head age and upgrade window
                    └── proxies + caches a handful of filfox endpoints
```

A single Go binary (~7 MB, no cgo, no third-party deps) plus one static
HTML file (~50 KB, no framework, no bundler). Every endpoint is cached
server-side with stale-on-error fallback so the dashboard never goes
blank when an upstream rate-limits.

## Endpoints

```
GET /api/v1/health           Health + cache freshness
GET /api/v1/version          Calix build info
GET /api/v1/status           Operational status with reason
GET /api/v1/signals          Vital signs (KPIs + 60-epoch sparkline series)
GET /api/v1/tipsets/recent   Last 60 tipsets with block count + win sum
GET /api/v1/upgrade          Next upgrade source of truth
GET /api/v1/miners/top       Top miners by power
GET /api/v1/rich-list        Rich list
GET /api/v1/power-history    Power history (?duration=24h | 7d | 30d)
GET /api/v1/fevm-stats       FEVM daily statistics
GET /api/v1/faucet           Plumbline faucet metadata
GET /api/v1/finality         Chain finality (EC + F3), see below
GET /api/v1/miners/status    Liveness per miner (?addrs=t0143103,...)
GET /metrics                 Prometheus text format
```

`/api/v1/health` only says the process is up. For chain freshness use
`headAgeSeconds` from `/api/v1/status` (healthy below 300).

## Run locally

```sh
# API
cd api && CALIX_ADDR=:8765 go run .

# Dashboard
cd web && python3 -m http.server 8766
# open http://127.0.0.1:8766
```

## Deploy

Run from a workstation, not on the host (the host needs no Go).

Workstation: Go 1.22 or newer, `git`, `rsync`, `ssh` with root (or
sudo-less admin) access to the host.
Host: Ubuntu 24.04 with `rsync` and `nginx` installed and running, and
the Cloudflare Origin CA cert at `/etc/ssl/cloudflare/reiers.io.{crt,key}`
(the paths `deploy/calix.nginx` expects; edit it for other hostnames).

```sh
export CALIX_HOST=root@<plumbline-host>
# optional, for a host not yet behind the public name:
# export CALIX_VERIFY_URL=https://<staging-name>
./deploy/deploy.sh
```

Builds linux/amd64, rsyncs `calix-api` and `web/` to `/opt/calix`,
installs `deploy/calix.service` and the nginx server block, fixes file
modes for `www-data`, restarts, and reloads nginx. Idempotent. Verified
on a fresh Ubuntu 24.04 host on 2026-11-02: under 10 seconds once nginx
and the cert are in place.

After a restart Calix needs about 10 seconds to fill its tipset ring;
until then `/api/v1/miners/status` reports every miner as `down` with
`epoch: 0`.

### Configuration

Environment variables (set in `deploy/calix.service`, or override on the
host with `systemctl edit calix` so a redeploy does not undo it):

| Variable | Default in code | Notes |
| --- | --- | --- |
| `CALIX_ADDR` | `:8080` | The unit sets `127.0.0.1:8765`, which nginx proxies to. |
| `CALIX_LOTUS_RPC` | Glif Calibration `/rpc/v1` | Any Calibration Lotus JSON-RPC. |
| `CALIX_LOTUS_RPC_V2` | `CALIX_LOTUS_RPC` with `/rpc/v2` | For the finality panel. |
| `CALIX_LOTUS_TOKEN` | empty | Bearer token if the RPC needs one. |
| `CALIX_FILFOX_API` | Filfox Calibration API | Leaderboards, rich list, FEVM. |
| `CALIX_CORS` | `*` | `Access-Control-Allow-Origin`. |
| `CALIX_FAUCET_URL` | `https://faucet.reiers.io` | Faucet card. |

Switch RPC without editing the repo:

```sh
systemctl edit calix
# [Service]
# Environment=CALIX_LOTUS_RPC=https://rpc.ankr.com/filecoin_testnet
systemctl restart calix
```

### nv-upgrade audit refresh

`scripts/refresh-audit.sh <nv>` writes `web/data/audit.json`. It needs
SSH access (`CALIX_REMOTE_SSH`) to a Calibration Lotus node where
`lotus auth api-info --perm admin` works; public RPCs do not expose the
calls it uses. For each new network version, update in one commit:
`calibUpgrades` and `canonicalManifestCIDs` in `api/main.go`,
`UPGRADES` in `web/index.html`, and the two `case` tables in
`scripts/refresh-audit.sh`. Then deploy.

## What's coming

- **Per-miner upgrade readiness**: the chain doesn't expose Lotus version directly, but for Storage Providers running Curio we can correlate via the cluster webrpc. A "X of N miners are on Curio v1.28" tile is the single most useful Fire Horse readiness signal.
- **Chain-weight / fork visualisation**: calibration occasionally forks; weight divergence over time is a high-signal stability metric.
- **Cluster lens** behind auth: sealing tasks, recent failures, machines online. Sourced from a Curio cluster's webrpc through a tunnel.
- **Alerts**: a `/api/v1/alerts` surface so other tooling can poll Calix; later, push to Telegram / matrix / webhook.

## License

MIT, see [LICENSE](LICENSE).

### Chain finality

Samples `Filecoin.ChainGetTipSetFinalityStatus` (Lotus v2 API, `CALIX_LOTUS_RPC_V2`, defaults to the v1 URL with `/rpc/v2`) every 30s. Shows the EC probabilistic finality depth (FRC-0089, 2^-30 reorg bound; -1 = bound not met), the F3 finalized depth, the depth the node actually treats as final and which mechanism sets it, plus a 2h history. When the status call returns no F3 tipset, Calix retries `Filecoin.F3GetLatestCertificate` over v1, because Glif's load-balanced backends intermittently fail to reach F3 and would otherwise report false F3 outages. Served at `/api/v1/finality`; exported as `calix_finality_*` in `/metrics`. Suggested by rvagg.
