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
- [Plumbline](https://faucet.reiers.io) — calibration faucet for tFIL + USDFC
- The Doctor calibration cluster

## What's on the dashboard

### Status header
A sticky bar across the top.
- Operational pill — `OPERATIONAL` / `WATCH` / `DEGRADED` / `UPGRADE PENDING`, computed live from chain head age and proximity to the next network upgrade.
- Live epoch, head age, network version, and current blocks-per-epoch cadence.

### Status banner
First panel on the page. Spells out the current operational level in plain English with a reason. For example, during the run-up to Fire Horse: *"Fire Horse upgrade approaching. Network version 28 activates in 1d 20h."*

### Vital signs (8 KPIs)
Eight cards in a grid, each with an inline 60-epoch sparkline. Each card flips colour (green / amber / red) as the underlying value moves through its thresholds.
- **Blocks per epoch** — calibration steady state is around 2.0
- **Null round %** — count of empty tipsets in the window
- **Average win count** — drand winning tickets per epoch
- **Base fee** — in atto, with K/M scaling for spikes
- **Network QAP** — quality-adjusted power
- **Active miners** — those above min-power
- **Total pledge** — locked FIL across the network
- **Initial pledge per 32 GiB CC** — computed locally via the actor v17 formula

### Tipset stream
Sixty vertical bars covering the last 30 minutes of chain history.
- Green when 3+ blocks landed (healthy)
- Amber for 1–2 (degraded)
- Red for 0 (null round)
Hover any bar for `epoch · block count · timestamp`.

### Upgrade readiness
Live countdown to the next network upgrade — currently **nv28 Fire Horse**, epoch 3,694,534, 2026-05-07T14:00:00Z. Days, hours, minutes, seconds. Direct link to the community announcement.

### Network power chart
Twenty-four-hour QAP trend, drawn as a native SVG line + area. Redraws on window resize so the geometry stays in real pixels.

### Leaderboards
- **Top miners by power** — raw, QAP, 24-hour delta, blocks mined, and on-chain tags (Curio Storage Inc., etc.)
- **Rich list** — top accounts by balance, with actor type and percent of supply

### Ecosystem
- **FEVM** — daily contract creates, invocations, gas
- **Plumbline** — calibration faucet card
- **Open source** — link to this repository

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
```

## Run locally

```sh
# API
cd api && CALIX_ADDR=:8765 go run .

# Dashboard
cd web && python3 -m http.server 8766
# open http://127.0.0.1:8766
```

## Deploy

```sh
./deploy/deploy.sh
```

Builds linux/amd64, rsyncs to the host, sets up the systemd unit,
symlinks the nginx server block, fixes file modes for `www-data`, and
reloads.

Configurable via env: `CALIX_ADDR`, `CALIX_LOTUS_RPC`, `CALIX_FILFOX_API`,
`CALIX_CORS`, `CALIX_FAUCET_URL`.

## What's coming

- **Per-miner upgrade readiness** — the chain doesn't expose Lotus version directly, but for Storage Providers running Curio we can correlate via the cluster webrpc. A "X of N miners are on Curio v1.28" tile is the single most useful Fire Horse readiness signal.
- **Chain-weight / fork visualisation** — calibration occasionally forks; weight divergence over time is a high-signal stability metric.
- **Cluster lens** behind auth — sealing tasks, recent failures, machines online. Sourced from a Curio cluster's webrpc through a tunnel.
- **Alerts** — a `/api/v1/alerts` surface so other tooling can poll Calix; later, push to Telegram / matrix / webhook.

## License

MIT, see [LICENSE](LICENSE).
