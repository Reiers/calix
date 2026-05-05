// Calix calibration API.
//
// Reads chain state directly via Lotus RPC for the bits that need to be
// trusted (initial-pledge math), and proxies a handful of filfox endpoints
// for things that need indexing we don't run (rich list, miner-power
// history, FEVM daily stats). Everything is cached server-side.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

// ----------------------------------------------------------------------------
// Build info
// ----------------------------------------------------------------------------

const calixVersion = "0.2.0"

var calixCommit = "dev"

// Calibration network constants.
const (
	calibGenesisUnix    int64 = 1667326380 // 2022-11-01T18:13:00Z
	calibBlockDelaySecs int64 = 30
	// Nv28 "Fire Horse" upgrade: epoch 3,694,534 (2026-05-07T14:00:00Z)
	nv28UpgradeEpoch    int64 = 3694534
	nv28UpgradeUnix     int64 = 1778162400 // 2026-05-07T14:00:00Z
	nv28Codename              = "Fire Horse"
)

// ----------------------------------------------------------------------------
// Config
// ----------------------------------------------------------------------------

type config struct {
	addr      string
	lotusRPC  string
	filfoxAPI string
	corsAllow string
	faucetURL string
}

func loadConfig() config {
	c := config{
		addr:      envOr("CALIX_ADDR", ":8080"),
		lotusRPC:  envOr("CALIX_LOTUS_RPC", "https://api.calibration.node.glif.io/rpc/v1"),
		filfoxAPI: envOr("CALIX_FILFOX_API", "https://calibration.filfox.info/api/v1"),
		corsAllow: envOr("CALIX_CORS", "*"),
		faucetURL: envOr("CALIX_FAUCET_URL", "https://faucet.reiers.io"),
	}
	flag.StringVar(&c.addr, "addr", c.addr, "listen address")
	flag.StringVar(&c.lotusRPC, "lotus", c.lotusRPC, "Lotus RPC endpoint")
	flag.StringVar(&c.filfoxAPI, "filfox", c.filfoxAPI, "Filfox API base URL")
	flag.StringVar(&c.corsAllow, "cors", c.corsAllow, "CORS allowed origin")
	flag.Parse()
	return c
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

// ----------------------------------------------------------------------------
// Lotus RPC client
// ----------------------------------------------------------------------------

type lotus struct {
	url    string
	hc     *http.Client
	idLock sync.Mutex
	id     int
}

func newLotus(url string) *lotus {
	return &lotus{url: url, hc: &http.Client{Timeout: 15 * time.Second}}
}

type rpcReq struct {
	JSONRPC string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  any    `json:"params"`
	ID      int    `json:"id"`
}

type rpcResp struct {
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func (l *lotus) call(ctx context.Context, method string, params any, out any) error {
	l.idLock.Lock()
	l.id++
	id := l.id
	l.idLock.Unlock()

	body, _ := json.Marshal(rpcReq{JSONRPC: "2.0", Method: method, Params: params, ID: id})
	req, err := http.NewRequestWithContext(ctx, "POST", l.url, strings.NewReader(string(body)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := l.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var rr rpcResp
	if err := json.NewDecoder(resp.Body).Decode(&rr); err != nil {
		return err
	}
	if rr.Error != nil {
		return fmt.Errorf("lotus rpc %s: %s (code %d)", method, rr.Error.Message, rr.Error.Code)
	}
	if out != nil {
		return json.Unmarshal(rr.Result, out)
	}
	return nil
}

// ----------------------------------------------------------------------------
// Filfox proxy client
// ----------------------------------------------------------------------------

type filfox struct {
	base string
	hc   *http.Client
}

func newFilfox(base string) *filfox {
	return &filfox{base: base, hc: &http.Client{Timeout: 15 * time.Second}}
}

func (f *filfox) get(ctx context.Context, path string, q url.Values, out any) error {
	u := f.base + path
	if q != nil {
		u += "?" + q.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "calix/"+calixVersion)
	resp, err := f.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("filfox %s: %d %s", path, resp.StatusCode, string(body))
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// ----------------------------------------------------------------------------
// Cache wrapper
// ----------------------------------------------------------------------------

type cached[T any] struct {
	ttl   time.Duration
	mu    sync.Mutex
	exp   time.Time
	val   T
	fetch func(context.Context) (T, error)
	set   bool
}

func newCached[T any](ttl time.Duration, fetch func(context.Context) (T, error)) *cached[T] {
	return &cached[T]{ttl: ttl, fetch: fetch}
}

func (c *cached[T]) Get(ctx context.Context) (T, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.set && time.Now().Before(c.exp) {
		return c.val, nil
	}
	v, err := c.fetch(ctx)
	if err != nil {
		if c.set {
			log.Printf("cache miss fetch failed, serving stale (%v)", err)
			return c.val, nil
		}
		var zero T
		return zero, err
	}
	c.val = v
	c.exp = time.Now().Add(c.ttl)
	c.set = true
	return v, nil
}

// keyed cache: same fetch fn taking a string param
type kcached[T any] struct {
	ttl   time.Duration
	mu    sync.Mutex
	items map[string]*kentry[T]
	fetch func(ctx context.Context, key string) (T, error)
}

type kentry[T any] struct {
	exp time.Time
	val T
	set bool
}

func newKeyed[T any](ttl time.Duration, fetch func(ctx context.Context, key string) (T, error)) *kcached[T] {
	return &kcached[T]{ttl: ttl, items: map[string]*kentry[T]{}, fetch: fetch}
}

func (c *kcached[T]) Get(ctx context.Context, key string) (T, error) {
	c.mu.Lock()
	e, ok := c.items[key]
	if !ok {
		e = &kentry[T]{}
		c.items[key] = e
	}
	c.mu.Unlock()

	c.mu.Lock()
	if e.set && time.Now().Before(e.exp) {
		v := e.val
		c.mu.Unlock()
		return v, nil
	}
	c.mu.Unlock()

	v, err := c.fetch(ctx, key)
	if err != nil {
		c.mu.Lock()
		defer c.mu.Unlock()
		if e.set {
			return e.val, nil
		}
		var zero T
		return zero, err
	}
	c.mu.Lock()
	e.val = v
	e.exp = time.Now().Add(c.ttl)
	e.set = true
	c.mu.Unlock()
	return v, nil
}

// ----------------------------------------------------------------------------
// Domain types
// ----------------------------------------------------------------------------

type tipsetHead struct {
	Height int64 `json:"Height"`
	Cids   []struct {
		Slash string `json:"/"`
	} `json:"Cids"`
	Blocks []struct {
		Miner         string `json:"Miner"`
		Timestamp     int64  `json:"Timestamp"`
		ParentBaseFee string `json:"ParentBaseFee"`
	} `json:"Blocks"`
}

type powerActorState struct {
	State struct {
		ThisEpochPledgeCollateral string `json:"ThisEpochPledgeCollateral"`
		ThisEpochQAPowerSmoothed  struct {
			PositionEstimate string `json:"PositionEstimate"`
			VelocityEstimate string `json:"VelocityEstimate"`
		} `json:"ThisEpochQAPowerSmoothed"`
		ThisEpochQualityAdjPower string `json:"ThisEpochQualityAdjPower"`
		ThisEpochRawBytePower    string `json:"ThisEpochRawBytePower"`
		MinerCount               int64  `json:"MinerCount"`
		MinerAboveMinPowerCount  int64  `json:"MinerAboveMinPowerCount"`
		RampStartEpoch           int64  `json:"RampStartEpoch"`
		RampDurationEpochs       int64  `json:"RampDurationEpochs"`
	} `json:"State"`
	Balance string `json:"Balance"`
}

type rewardActorState struct {
	State struct {
		ThisEpochReward         string `json:"ThisEpochReward"`
		ThisEpochBaselinePower  string `json:"ThisEpochBaselinePower"`
		ThisEpochRewardSmoothed struct {
			PositionEstimate string `json:"PositionEstimate"`
			VelocityEstimate string `json:"VelocityEstimate"`
		} `json:"ThisEpochRewardSmoothed"`
		Epoch                   int64  `json:"Epoch"`
		EffectiveBaselinePower  string `json:"EffectiveBaselinePower"`
		TotalStoragePowerReward string `json:"TotalStoragePowerReward"`
		CumsumBaseline          string `json:"CumsumBaseline"`
		CumsumRealized          string `json:"CumsumRealized"`
	} `json:"State"`
}

// ----------------------------------------------------------------------------
// Application
// ----------------------------------------------------------------------------

type app struct {
	cfg            config
	rpc            *lotus
	ff             *filfox
	head           *cached[tipsetHead]
	power          *cached[powerActorState]
	reward         *cached[rewardActorState]
	netver         *cached[int]
	topMiners      *cached[json.RawMessage]
	richList       *cached[json.RawMessage]
	powerHistory   *kcached[json.RawMessage]
	fevmStats      *cached[json.RawMessage]
}

func newApp(cfg config) *app {
	a := &app{cfg: cfg, rpc: newLotus(cfg.lotusRPC), ff: newFilfox(cfg.filfoxAPI)}

	a.head = newCached(15*time.Second, func(ctx context.Context) (tipsetHead, error) {
		var t tipsetHead
		err := a.rpc.call(ctx, "Filecoin.ChainHead", []any{}, &t)
		return t, err
	})
	a.power = newCached(30*time.Second, func(ctx context.Context) (powerActorState, error) {
		var s powerActorState
		err := a.rpc.call(ctx, "Filecoin.StateReadState", []any{"f04", nil}, &s)
		return s, err
	})
	a.reward = newCached(30*time.Second, func(ctx context.Context) (rewardActorState, error) {
		var s rewardActorState
		err := a.rpc.call(ctx, "Filecoin.StateReadState", []any{"f02", nil}, &s)
		return s, err
	})
	a.netver = newCached(60*time.Second, func(ctx context.Context) (int, error) {
		var v int
		err := a.rpc.call(ctx, "Filecoin.StateNetworkVersion", []any{nil}, &v)
		return v, err
	})

	a.topMiners = newCached(60*time.Second, func(ctx context.Context) (json.RawMessage, error) {
		var raw json.RawMessage
		q := url.Values{}
		q.Set("pageSize", "20")
		err := a.ff.get(ctx, "/miner/list/power", q, &raw)
		return raw, err
	})
	a.richList = newCached(120*time.Second, func(ctx context.Context) (json.RawMessage, error) {
		var raw json.RawMessage
		q := url.Values{}
		q.Set("pageSize", "20")
		q.Set("page", "0")
		err := a.ff.get(ctx, "/rich-list", q, &raw)
		return raw, err
	})
	a.powerHistory = newKeyed(5*time.Minute, func(ctx context.Context, dur string) (json.RawMessage, error) {
		var raw json.RawMessage
		q := url.Values{}
		q.Set("duration", dur)
		err := a.ff.get(ctx, "/stats/power", q, &raw)
		return raw, err
	})
	a.fevmStats = newCached(5*time.Minute, func(ctx context.Context) (json.RawMessage, error) {
		var raw json.RawMessage
		err := a.ff.get(ctx, "/stats/fevm/daily-statistics", nil, &raw)
		return raw, err
	})

	return a
}

// ----------------------------------------------------------------------------
// IP calculation - actor v17 formula
// ----------------------------------------------------------------------------

const (
	epochsPerDay       = 2880
	ipProjectionPeriod = 20 * epochsPerDay
	atto               = 1_000_000_000_000_000_000
)

var q128 = new(big.Int).Lsh(big.NewInt(1), 128)

func filterEstimate(positionStr string) (*big.Int, error) {
	pos, ok := new(big.Int).SetString(positionStr, 10)
	if !ok {
		return nil, fmt.Errorf("bad positionEstimate %q", positionStr)
	}
	return new(big.Int).Quo(pos, q128), nil
}

func initialPledgeForCCSector(
	pwr powerActorState,
	rwd rewardActorState,
	circulatingSupply *big.Int,
) (storage, consensus, total *big.Int, err error) {
	const sectorBytes = int64(32 * 1024 * 1024 * 1024)
	sectorQAP := big.NewInt(sectorBytes)

	rwdEst, err := filterEstimate(rwd.State.ThisEpochRewardSmoothed.PositionEstimate)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("reward estimate: %w", err)
	}
	netQAP, err := filterEstimate(pwr.State.ThisEpochQAPowerSmoothed.PositionEstimate)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("network QAP estimate: %w", err)
	}
	if netQAP.Sign() == 0 {
		return nil, nil, nil, errors.New("network QAP estimate is zero")
	}
	baseline, ok := new(big.Int).SetString(rwd.State.ThisEpochBaselinePower, 10)
	if !ok {
		return nil, nil, nil, fmt.Errorf("bad baseline power %q", rwd.State.ThisEpochBaselinePower)
	}

	storage = new(big.Int).Mul(rwdEst, sectorQAP)
	storage.Quo(storage, netQAP)
	storage.Mul(storage, big.NewInt(int64(ipProjectionPeriod)))

	if circulatingSupply == nil {
		circulatingSupply = big.NewInt(0)
	}
	denom := new(big.Int).Set(netQAP)
	if baseline.Cmp(denom) > 0 {
		denom.Set(baseline)
	}
	denom.Mul(denom, big.NewInt(100))
	num := new(big.Int).Mul(circulatingSupply, sectorQAP)
	num.Mul(num, big.NewInt(30))
	consensus = new(big.Int).Quo(num, denom)

	total = new(big.Int).Add(storage, consensus)
	if total.Sign() < 0 {
		total.SetInt64(0)
	}
	return storage, consensus, total, nil
}

// ----------------------------------------------------------------------------
// Handlers
// ----------------------------------------------------------------------------

type overviewResp struct {
	Height                int64   `json:"height"`
	Epoch                 int64   `json:"epoch"`
	NetworkVersion        int     `json:"networkVersion"`
	NetworkRawBytePower   string  `json:"networkRawBytePower"`
	NetworkQualityAdjPwr  string  `json:"networkQualityAdjPower"`
	BaselinePower         string  `json:"baselinePower"`
	BlockReward           string  `json:"blockReward"`
	TotalPledgeCollateral string  `json:"totalPledgeCollateral"`
	MinerCount            int64   `json:"minerCount"`
	MinersAboveMinPower   int64   `json:"minersAboveMinPower"`
	IPPerCCSectorAtto     string  `json:"ipPerCCSectorAtto"`
	IPPerCCSectorFIL      float64 `json:"ipPerCCSectorFIL"`
	IPStorageTermAtto     string  `json:"ipStorageTermAtto"`
	IPConsensusTermAtto   string  `json:"ipConsensusTermAtto"`
	GeneratedAt           int64   `json:"generatedAt"`
}

func (a *app) handleOverview(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	head, err := a.head.Get(ctx)
	if err != nil {
		writeError(w, 502, "fetch head: "+err.Error())
		return
	}
	pwr, err := a.power.Get(ctx)
	if err != nil {
		writeError(w, 502, "fetch power: "+err.Error())
		return
	}
	rwd, err := a.reward.Get(ctx)
	if err != nil {
		writeError(w, 502, "fetch reward: "+err.Error())
		return
	}
	nv, _ := a.netver.Get(ctx)

	storage, consensus, total, err := initialPledgeForCCSector(pwr, rwd, big.NewInt(0))
	if err != nil {
		writeError(w, 500, "compute IP: "+err.Error())
		return
	}
	totalFIL, _ := new(big.Float).Quo(new(big.Float).SetInt(total), big.NewFloat(atto)).Float64()

	writeJSON(w, 200, overviewResp{
		Height:                head.Height,
		Epoch:                 rwd.State.Epoch,
		NetworkVersion:        nv,
		NetworkRawBytePower:   pwr.State.ThisEpochRawBytePower,
		NetworkQualityAdjPwr:  pwr.State.ThisEpochQualityAdjPower,
		BaselinePower:         rwd.State.ThisEpochBaselinePower,
		BlockReward:           rwd.State.ThisEpochReward,
		TotalPledgeCollateral: pwr.State.ThisEpochPledgeCollateral,
		MinerCount:            pwr.State.MinerCount,
		MinersAboveMinPower:   pwr.State.MinerAboveMinPowerCount,
		IPPerCCSectorAtto:     total.String(),
		IPPerCCSectorFIL:      totalFIL,
		IPStorageTermAtto:     storage.String(),
		IPConsensusTermAtto:   consensus.String(),
		GeneratedAt:           time.Now().Unix(),
	})
}

func (a *app) handleVersion(w http.ResponseWriter, r *http.Request) {
	nv, _ := a.netver.Get(r.Context())
	writeJSON(w, 200, map[string]any{
		"calixVersion":   calixVersion,
		"calixCommit":    calixCommit,
		"networkVersion": nv,
	})
}

func (a *app) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]any{"ok": true, "ts": time.Now().Unix()})
}

func (a *app) handleBlocksRecent(w http.ResponseWriter, r *http.Request) {
	head, err := a.head.Get(r.Context())
	if err != nil {
		writeError(w, 502, err.Error())
		return
	}
	blocks := make([]map[string]any, 0, len(head.Blocks))
	for _, b := range head.Blocks {
		blocks = append(blocks, map[string]any{
			"miner":         b.Miner,
			"timestamp":     b.Timestamp,
			"parentBaseFee": b.ParentBaseFee,
		})
	}
	writeJSON(w, 200, map[string]any{"height": head.Height, "blocks": blocks})
}

// /api/v1/upgrade — Nv28 Fire Horse countdown source of truth.
func (a *app) handleUpgrade(w http.ResponseWriter, r *http.Request) {
	head, _ := a.head.Get(r.Context())
	now := time.Now().Unix()
	secsLeft := nv28UpgradeUnix - now
	epochsLeft := nv28UpgradeEpoch - head.Height
	writeJSON(w, 200, map[string]any{
		"name":           "Fire Horse",
		"networkVersion": 28,
		"network":        "calibration",
		"epoch":          nv28UpgradeEpoch,
		"timestamp":      nv28UpgradeUnix,
		"timestampISO":   time.Unix(nv28UpgradeUnix, 0).UTC().Format(time.RFC3339),
		"announcement":   "https://github.com/filecoin-project/community/discussions/74#discussioncomment-16540452",
		"currentEpoch":   head.Height,
		"epochsLeft":     epochsLeft,
		"secondsLeft":    secsLeft,
		"genesisUnix":    calibGenesisUnix,
		"epochSeconds":   calibBlockDelaySecs,
	})
}

func (a *app) handleTopMiners(w http.ResponseWriter, r *http.Request) {
	v, err := a.topMiners.Get(r.Context())
	if err != nil {
		writeError(w, 502, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	_, _ = w.Write(v)
}

func (a *app) handleRichList(w http.ResponseWriter, r *http.Request) {
	v, err := a.richList.Get(r.Context())
	if err != nil {
		writeError(w, 502, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	_, _ = w.Write(v)
}

func (a *app) handlePowerHistory(w http.ResponseWriter, r *http.Request) {
	dur := r.URL.Query().Get("duration")
	if dur == "" {
		dur = "24h"
	}
	if dur != "24h" && dur != "7d" && dur != "30d" {
		writeError(w, 400, "duration must be 24h, 7d, or 30d")
		return
	}
	v, err := a.powerHistory.Get(r.Context(), dur)
	if err != nil {
		writeError(w, 502, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	_, _ = w.Write(v)
}

func (a *app) handleFEVM(w http.ResponseWriter, r *http.Request) {
	v, err := a.fevmStats.Get(r.Context())
	if err != nil {
		writeError(w, 502, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	_, _ = w.Write(v)
}

func (a *app) handleFaucet(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]any{
		"name":        "Plumbline",
		"url":         a.cfg.faucetURL,
		"description": "Calibration tFIL + USDFC faucet, dispensed independently.",
	})
}

// ----------------------------------------------------------------------------
// Plumbing
// ----------------------------------------------------------------------------

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]any{"error": msg})
}

func cors(next http.Handler, allow string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", allow)
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == "OPTIONS" {
			w.WriteHeader(204)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func main() {
	cfg := loadConfig()
	a := newApp(cfg)

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/health", a.handleHealth)
	mux.HandleFunc("/api/v1/version", a.handleVersion)
	mux.HandleFunc("/api/v1/overview", a.handleOverview)
	mux.HandleFunc("/api/v1/blocks/recent", a.handleBlocksRecent)
	mux.HandleFunc("/api/v1/upgrade", a.handleUpgrade)
	mux.HandleFunc("/api/v1/miners/top", a.handleTopMiners)
	mux.HandleFunc("/api/v1/rich-list", a.handleRichList)
	mux.HandleFunc("/api/v1/power-history", a.handlePowerHistory)
	mux.HandleFunc("/api/v1/fevm-stats", a.handleFEVM)
	mux.HandleFunc("/api/v1/faucet", a.handleFaucet)

	srv := &http.Server{
		Addr:              cfg.addr,
		Handler:           cors(mux, cfg.corsAllow),
		ReadHeaderTimeout: 10 * time.Second,
	}
	log.Printf("calix api %s+%s listening on %s -> lotus=%s, filfox=%s", calixVersion, calixCommit, cfg.addr, cfg.lotusRPC, cfg.filfoxAPI)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
