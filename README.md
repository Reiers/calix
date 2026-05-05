<p align="center">
  <img src="assets/logo.svg" alt="Calix" width="320">
</p>

<p align="center">
  <b>Filecoin calibration testnet dashboard</b><br>
  Network state, leaderboards, and tooling, served at
  <a href="https://calix.reiers.io">calix.reiers.io</a>.
</p>

---

Calix is part of a wider calibration stability effort run from
[reiers.io](https://reiers.io): a small set of public services that make
it easier to operate, debug, and ship against the Filecoin calibration
network. Sister projects include
[Plumbline](https://faucet.reiers.io) (calibration faucet for tFIL +
USDFC) and the
[Doctor calibration cluster](https://github.com/Reiers).

## Features

### Network upgrade tracker
- Live countdown to the next network upgrade (currently **nv28 Fire Horse**, epoch 3,694,534, 2026-05-07T14:00:00Z)
- Direct link to the community announcement
- Sourced from `/api/v1/upgrade`, recomputed every second client-side

### Initial pledge, computed correctly
- Per-32 GiB CC sector initial pledge, computed via the actor v17 formula
- Storage pledge term + consensus pledge term shown decomposed
- Refreshed every 30s from `f04` (power) and `f02` (reward) actor state
- Useful counterpoint to upstream explorers that mishandle calibration's `circulatingSupply`

### Network state
- Network QAP, raw byte power, baseline, block reward
- Total pledge collateral, average IP per sector, miner counts
- 24-hour QAP chart, native SVG, no chart library

### Leaderboards
- Top miners by power (with their on-chain tags)
- Rich list (top accounts by balance, with % of supply)
- Both cached server-side, refreshed once per minute

### Ecosystem
- FEVM daily statistics (contract creates, invocations, gas)
- Plumbline faucet card
- Recent tipset blocks with miner + base fee + age

### Tools
- Epoch ↔ time converter (calibration genesis: 1667326380 / 30 s epochs)
- Same algorithm as [`epochclock`](https://github.com/Reiers/epochclock), expanded to two-way

## Architecture

```
calix.reiers.io   (Cloudflare → nginx)
  ├── /          → static dashboard (web/index.html)
  └── /api/v1/*  → calix-api (Go, 127.0.0.1:8765)
                    ├── reads f04 + f02 from a Lotus RPC
                    ├── computes initial pledge via actor v17
                    └── proxies + caches a handful of filfox endpoints
```

- **API**: a single Go binary (~7 MB, no cgo, no third-party deps), ships via systemd
- **Dashboard**: a single static HTML file (~45 KB), no framework, no bundler
- **Reverse proxy**: nginx
- **Cert**: Cloudflare Origin CA, `*.reiers.io`

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

Builds a `linux/amd64` binary, rsyncs to the host, sets up the systemd
unit, symlinks the nginx server block, and reloads.

Configurable via env vars (`CALIX_ADDR`, `CALIX_LOTUS_RPC`, `CALIX_FILFOX_API`,
`CALIX_CORS`, `CALIX_FAUCET_URL`).

## License

MIT — see [LICENSE](LICENSE).
