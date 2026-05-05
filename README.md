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

**Status header**
- Operational / Watch / Degraded / Upgrade Pending pill, computed from chain head age and proximity to the next upgrade
- Live epoch, head age, network version, blocks-per-epoch cadence

**Vital signs (KPI grid with sparklines)**
- Blocks per epoch (calibration steady state ~2.0)
- Null round percent
- Average win count per epoch
- Base fee
- Network QAP, active miners, total pledge, initial pledge per 32 GiB CC

**Tipset stream**
- Last 60 epochs as colour-coded bars: green for healthy, amber for sparse, red for null
- Hover any bar for epoch + block count + timestamp

**Upgrade readiness**
- Live countdown to the next network upgrade (currently **nv28 Fire Horse**, epoch 3,694,534, 2026-05-07T14:00:00Z)
- Direct link to the community announcement

**Network power**
- 24-hour QAP chart, native SVG

**Leaderboards**
- Top miners by power with raw + QAP + 24h delta + tags
- Rich list with actor type and percent of supply

**Ecosystem**
- FEVM daily statistics
- Plumbline faucet card
- Open source link

**Tools**
- Epoch ↔ time converter, calibration genesis 1667326380, 30s epochs

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

Single Go binary (~7 MB, no cgo, no third-party deps) plus one static
HTML file (~45 KB, no framework, no bundler). Every endpoint is cached
server-side with stale-on-error fallback.

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
GET /api/v1/power-history    Power history (24h | 7d | 30d)
GET /api/v1/fevm-stats       FEVM daily statistics
GET /api/v1/faucet           Plumbline faucet metadata
```

## Run locally

```sh
# API
cd api && CALIX_ADDR=:8765 go run .

# Dashboard
cd web && python3 -m http.server 8766
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

## License

MIT, see [LICENSE](LICENSE).
