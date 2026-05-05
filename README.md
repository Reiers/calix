# Calix

A no-bullshit dashboard for the Filecoin **calibration** testnet.

## Why

[filfox calibration explorer](https://calibration.filfox.info) currently shows numbers like:

> Current Sector Initial Pledge: **−151.8017 FIL/32GiB**

That is not a real on-chain number. Their indexer reports `circulatingSupply: 0`
for calibration, and their IP estimator doesn't guard against it, so the result
goes negative. The actual on-chain initial pledge for a fresh 32GiB CC sector
on calibration is around **+34 FIL** today.

Calix computes the number locally using the actor v17 formula and the live
state of the power and reward actors. No mystery feeds, no negative FIL.

## Architecture

```
calix.reiers.io   (Caddy + Cloudflare)
  ├── /        → static dashboard (calix/web)
  └── /api/*   → Calix API (Go, calix/api)
                  └── reads f04 (power) and f02 (reward) actor state
                  └── computes IP per actor v17 formula
                  └── caches Lotus reads for 15-30s
```

## Run locally

```sh
# API
cd api && CALIX_ADDR=:8765 go run .

# Dashboard
cd web && python3 -m http.server 8766
# Open http://127.0.0.1:8766
```

## Deploy

```sh
./deploy/deploy.sh   # builds linux/amd64, rsyncs to Hetzner, reloads Caddy
```

## License

MIT, see LICENSE.
