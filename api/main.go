// Calix calibration stability console.
//
// A real-time monitoring API for the Filecoin calibration network. Exposes
// signal endpoints derived from on-chain reads (Lotus RPC) and indexed
// public data (filfox), with aggressive server-side caching.
//
// Endpoints under /api/v1:
//
//	GET  /health           -> ok + cache freshness
//	GET  /version          -> calix build info
//	GET  /status           -> overall network status (operational/degraded/upgrade)
//	GET  /signals          -> KPI grid: blocks/epoch, null-rounds, base fee, QAP, miners, pledge, IP
//	GET  /signals/sparkline -> 60-epoch history for each KPI
//	GET  /upgrade          -> next upgrade source of truth
//	GET  /tipsets/recent   -> last N tipsets with block counts and timestamps
//	GET  /miners/top       -> top miners by power (filfox proxy)
//	GET  /rich-list        -> rich list (filfox proxy)
//	GET  /power-history    -> power chart (filfox proxy)
//	GET  /fevm-stats       -> FEVM daily statistics (filfox proxy)
//	GET  /faucet           -> Plumbline faucet metadata
//
// All numbers are returned as strings for safe BigInt parsing client-side.
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
	"strconv"
	"strings"
	"sync"
	"time"
)

const calixVersion = "0.3.0"

var calixCommit = "dev"

// Calibration constants
const (
	calibGenesisUnix    int64 = 1667326380
	calibBlockDelaySecs int64 = 30
	nv28UpgradeEpoch    int64 = 3694534
	nv28UpgradeUnix     int64 = 1778162400
	nv28Codename              = "Fire Horse"

	targetBlocksPerEpoch = 5
	tipsetWindowSize     = 60 // last N tipsets we keep for KPI sparklines
)

// Config

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

// ============================================================================
// Lotus client
// ============================================================================

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
		return fmt.Errorf("lotus rpc %s: %s", method, rr.Error.Message)
	}
	if out != nil {
		return json.Unmarshal(rr.Result, out)
	}
	return nil
}

// ============================================================================
// Filfox client
// ============================================================================

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

// ============================================================================
// Caching
// ============================================================================

type cached[T any] struct {
	ttl   time.Duration
	mu    sync.Mutex
	exp   time.Time
	val   T
	fetch func(context.Context) (T, error)
	set   bool
	last  time.Time
	err   error
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
	c.last = time.Now()
	if err != nil {
		c.err = err
		if c.set {
			log.Printf("cache stale-on-error (%v)", err)
			return c.val, nil
		}
		var zero T
		return zero, err
	}
	c.val = v
	c.exp = time.Now().Add(c.ttl)
	c.set = true
	c.err = nil
	return v, nil
}

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
	if e.set && time.Now().Before(e.exp) {
		v := e.val
		c.mu.Unlock()
		return v, nil
	}
	c.mu.Unlock()
	v, err := c.fetch(ctx, key)
	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		if e.set {
			return e.val, nil
		}
		var zero T
		return zero, err
	}
	e.val = v
	e.exp = time.Now().Add(c.ttl)
	e.set = true
	return v, nil
}

// ============================================================================
// Domain types
// ============================================================================

type tipsetHead struct {
	Height int64    `json:"Height"`
	Cids   []map[string]string `json:"Cids"`
	Blocks []struct {
		Miner         string `json:"Miner"`
		Timestamp     int64  `json:"Timestamp"`
		ParentBaseFee string `json:"ParentBaseFee"`
		ElectionProof struct {
			WinCount int `json:"WinCount"`
		} `json:"ElectionProof"`
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
	} `json:"State"`
}

type rewardActorState struct {
	State struct {
		ThisEpochReward         string `json:"ThisEpochReward"`
		ThisEpochBaselinePower  string `json:"ThisEpochBaselinePower"`
		ThisEpochRewardSmoothed struct {
			PositionEstimate string `json:"PositionEstimate"`
			VelocityEstimate string `json:"VelocityEstimate"`
		} `json:"ThisEpochRewardSmoothed"`
		Epoch int64 `json:"Epoch"`
	} `json:"State"`
}

// ============================================================================
// Tipset ring buffer (last N tipsets, used for sparklines + null-round detection)
// ============================================================================

type tipsetSample struct {
	Height       int64  `json:"height"`
	Timestamp    int64  `json:"timestamp"`
	BlockCount   int    `json:"blockCount"`
	BaseFeeAtto  string `json:"baseFeeAtto"`
	WinCountSum  int    `json:"winCountSum"`
}

type tipsetRing struct {
	mu      sync.Mutex
	samples []tipsetSample
	maxAge  time.Duration
	rpc     *lotus
	headFn  func(context.Context) (tipsetHead, error)
}

func newTipsetRing(rpc *lotus, headFn func(context.Context) (tipsetHead, error)) *tipsetRing {
	return &tipsetRing{
		samples: make([]tipsetSample, 0, tipsetWindowSize+8),
		rpc:     rpc,
		headFn:  headFn,
	}
}

func (r *tipsetRing) snapshot() []tipsetSample {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]tipsetSample, len(r.samples))
	copy(out, r.samples)
	return out
}

// fetchTipsetByHeight queries Lotus for a specific tipset.
func (r *tipsetRing) fetchTipsetByHeight(ctx context.Context, height int64) (*tipsetSample, error) {
	var t tipsetHead
	err := r.rpc.call(ctx, "Filecoin.ChainGetTipSetByHeight", []any{height, nil}, &t)
	if err != nil {
		return nil, err
	}
	if len(t.Blocks) == 0 {
		// null round
		return &tipsetSample{Height: height, BlockCount: 0}, nil
	}
	winSum := 0
	for _, b := range t.Blocks {
		winSum += b.ElectionProof.WinCount
	}
	bf := ""
	if len(t.Blocks) > 0 {
		bf = t.Blocks[0].ParentBaseFee
	}
	return &tipsetSample{
		Height:      t.Height,
		Timestamp:   t.Blocks[0].Timestamp,
		BlockCount:  len(t.Blocks),
		BaseFeeAtto: bf,
		WinCountSum: winSum,
	}, nil
}

// refresh syncs the ring buffer with chain head, filling gaps.
func (r *tipsetRing) refresh(ctx context.Context) error {
	head, err := r.headFn(ctx)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	// determine the range we need
	var lastHeight int64
	if len(r.samples) > 0 {
		lastHeight = r.samples[len(r.samples)-1].Height
	} else {
		lastHeight = head.Height - tipsetWindowSize
	}
	startHeight := lastHeight + 1
	if startHeight < head.Height-tipsetWindowSize {
		startHeight = head.Height - tipsetWindowSize
	}

	// Fetch missing tipsets (release lock during network calls)
	r.mu.Unlock()
	missing := []tipsetSample{}
	for h := startHeight; h <= head.Height; h++ {
		s, err := r.fetchTipsetByHeight(ctx, h)
		if err != nil {
			// Treat fetch errors at the tip as null rounds rather than failing the whole refresh.
			missing = append(missing, tipsetSample{Height: h, BlockCount: 0})
			continue
		}
		missing = append(missing, *s)
	}
	r.mu.Lock()

	// Append + trim
	r.samples = append(r.samples, missing...)
	if n := len(r.samples) - tipsetWindowSize; n > 0 {
		r.samples = r.samples[n:]
	}
	return nil
}

// ============================================================================
// IP calculation - actor v17
// ============================================================================

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

func initialPledgeForCCSector(pwr powerActorState, rwd rewardActorState, circulatingSupply *big.Int) (storage, consensus, total *big.Int, err error) {
	const sectorBytes = int64(32 * 1024 * 1024 * 1024)
	sectorQAP := big.NewInt(sectorBytes)

	rwdEst, err := filterEstimate(rwd.State.ThisEpochRewardSmoothed.PositionEstimate)
	if err != nil {
		return nil, nil, nil, err
	}
	netQAP, err := filterEstimate(pwr.State.ThisEpochQAPowerSmoothed.PositionEstimate)
	if err != nil {
		return nil, nil, nil, err
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

// ============================================================================
// Application
// ============================================================================

type app struct {
	cfg          config
	rpc          *lotus
	ff           *filfox
	head         *cached[tipsetHead]
	power        *cached[powerActorState]
	reward       *cached[rewardActorState]
	netver       *cached[int]
	topMiners    *cached[json.RawMessage]
	richList     *cached[json.RawMessage]
	powerHistory *kcached[json.RawMessage]
	fevmStats    *cached[json.RawMessage]
	ring         *tipsetRing
	ringRefresh  time.Time
	ringMu       sync.Mutex
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

	a.ring = newTipsetRing(a.rpc, a.head.Get)

	// Background tipset refresh every 30s
	go func() {
		ctx := context.Background()
		first := true
		for {
			if !first {
				time.Sleep(30 * time.Second)
			}
			first = false
			if err := a.ring.refresh(ctx); err != nil {
				log.Printf("tipset ring refresh: %v", err)
			}
		}
	}()

	return a
}

// ============================================================================
// Status engine
// ============================================================================

type statusLevel string

const (
	statusOperational statusLevel = "operational"
	statusWatch       statusLevel = "watch"
	statusDegraded    statusLevel = "degraded"
	statusUpgrade     statusLevel = "upgrade"
)

type statusResp struct {
	Level             statusLevel `json:"level"`
	Headline          string      `json:"headline"`
	Detail            string      `json:"detail"`
	Epoch             int64       `json:"epoch"`
	Height            int64       `json:"height"`
	NetworkVersion    int         `json:"networkVersion"`
	HeadAgeSeconds    int64       `json:"headAgeSeconds"`
	UpgradeName       string      `json:"upgradeName"`
	UpgradeEpoch      int64       `json:"upgradeEpoch"`
	UpgradeUnix       int64       `json:"upgradeUnix"`
	UpgradeSecsLeft   int64       `json:"upgradeSecsLeft"`
	UpgradeEpochsLeft int64       `json:"upgradeEpochsLeft"`
	GeneratedAt       int64       `json:"generatedAt"`
}

func (a *app) handleStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	head, err := a.head.Get(ctx)
	if err != nil {
		writeError(w, 502, err.Error())
		return
	}
	nv, _ := a.netver.Get(ctx)
	now := time.Now().Unix()
	headTs := int64(0)
	if len(head.Blocks) > 0 {
		headTs = head.Blocks[0].Timestamp
	}
	headAge := now - headTs

	level := statusOperational
	headline := "All systems operational"
	detail := fmt.Sprintf("Calibration nv%d producing blocks normally", nv)

	upgradeSecs := nv28UpgradeUnix - now
	upgradeEpochs := nv28UpgradeEpoch - head.Height

	switch {
	case headAge > 90:
		level = statusDegraded
		headline = "Chain head is stale"
		detail = fmt.Sprintf("No new tipsets in %d seconds. Possible sync lag or chain stall.", headAge)
	case headAge > 60:
		level = statusWatch
		headline = "Tipset cadence slowing"
		detail = fmt.Sprintf("Last tipset arrived %d seconds ago (target 30 seconds).", headAge)
	case upgradeSecs > 0 && upgradeSecs < 24*3600:
		level = statusUpgrade
		headline = fmt.Sprintf("%s upgrade in <24h", nv28Codename)
		detail = fmt.Sprintf("Network version %d activates at epoch %d, in %s.", 28, nv28UpgradeEpoch, humanDuration(upgradeSecs))
	case upgradeSecs > 0 && upgradeSecs < 72*3600:
		level = statusUpgrade
		headline = fmt.Sprintf("%s upgrade approaching", nv28Codename)
		detail = fmt.Sprintf("Network version %d activates in %s.", 28, humanDuration(upgradeSecs))
	}

	writeJSON(w, 200, statusResp{
		Level:             level,
		Headline:          headline,
		Detail:            detail,
		Epoch:             head.Height,
		Height:            head.Height,
		NetworkVersion:    nv,
		HeadAgeSeconds:    headAge,
		UpgradeName:       nv28Codename,
		UpgradeEpoch:      nv28UpgradeEpoch,
		UpgradeUnix:       nv28UpgradeUnix,
		UpgradeSecsLeft:   upgradeSecs,
		UpgradeEpochsLeft: upgradeEpochs,
		GeneratedAt:       time.Now().Unix(),
	})
}

func humanDuration(secs int64) string {
	if secs < 60 {
		return fmt.Sprintf("%ds", secs)
	}
	if secs < 3600 {
		return fmt.Sprintf("%dm", secs/60)
	}
	if secs < 86400 {
		return fmt.Sprintf("%dh %dm", secs/3600, (secs%3600)/60)
	}
	d := secs / 86400
	h := (secs % 86400) / 3600
	return fmt.Sprintf("%dd %dh", d, h)
}

// ============================================================================
// Signals (KPI grid + sparkline data)
// ============================================================================

type signalsResp struct {
	GeneratedAt   int64                `json:"generatedAt"`
	Epoch         int64                `json:"epoch"`
	Window        int                  `json:"window"`
	BlocksPerEp   signalNum            `json:"blocksPerEpoch"`
	NullRoundPct  signalNum            `json:"nullRoundPercent"`
	BaseFee       signalNum            `json:"baseFee"`
	WinCountAvg   signalNum            `json:"winCountAvg"`
	NetworkQAP    signalNum            `json:"networkQAP"`
	ActiveMiners  signalNum            `json:"activeMiners"`
	TotalPledge   signalNum            `json:"totalPledge"`
	IPPerSector   signalNum            `json:"ipPerSector32GiB"`
	Series        map[string][]float64 `json:"series"`
}

type signalNum struct {
	Value  float64 `json:"value"`
	Unit   string  `json:"unit"`
	Status string  `json:"status"` // ok | watch | bad
}

func (a *app) handleSignals(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	head, err := a.head.Get(ctx)
	if err != nil {
		writeError(w, 502, err.Error())
		return
	}
	pwr, _ := a.power.Get(ctx)
	rwd, _ := a.reward.Get(ctx)

	samples := a.ring.snapshot()
	n := len(samples)
	resp := signalsResp{
		GeneratedAt: time.Now().Unix(),
		Epoch:       head.Height,
		Window:      n,
		Series:      map[string][]float64{},
	}

	// Compute over the window
	var (
		blocksPerEp = make([]float64, 0, n)
		baseFees    = make([]float64, 0, n)
		winCounts   = make([]float64, 0, n)
		nullRounds  = 0
	)

	for _, s := range samples {
		blocksPerEp = append(blocksPerEp, float64(s.BlockCount))
		winCounts = append(winCounts, float64(s.WinCountSum))
		if s.BlockCount == 0 {
			nullRounds++
		}
		if s.BaseFeeAtto != "" {
			bf, _ := strconv.ParseFloat(s.BaseFeeAtto, 64)
			baseFees = append(baseFees, bf) // raw attoFIL; the dashboard formats it adaptively
		}
	}

	// Calibration cadence is much lower than mainnet because there are ~12 active miners.
	// Healthy steady state has been around 1.8-2.5 blocks/epoch. Use thresholds tuned for that.
	avgBlocksPerEp := mean(blocksPerEp)
	resp.BlocksPerEp = signalNum{
		Value:  avgBlocksPerEp,
		Unit:   "blocks",
		Status: classify(avgBlocksPerEp, 1.5, 1.0, true),
	}
	nullPct := 0.0
	if n > 0 {
		nullPct = float64(nullRounds) * 100 / float64(n)
	}
	resp.NullRoundPct = signalNum{
		Value:  nullPct,
		Unit:   "%",
		Status: classify(nullPct, 5, 15, false),
	}
	resp.BaseFee = signalNum{
		Value:  mean(baseFees),
		Unit:   "atto",
		Status: "ok",
	}
	avgWin := mean(winCounts)
	resp.WinCountAvg = signalNum{
		Value:  avgWin,
		Unit:   "wins",
		Status: classify(avgWin, 3, 1.5, true),
	}

	// Network QAP from latest power state
	qapBytes, _ := strconv.ParseFloat(pwr.State.ThisEpochQualityAdjPower, 64)
	resp.NetworkQAP = signalNum{
		Value:  qapBytes / float64(int64(1)<<40),
		Unit:   "TiB",
		Status: "ok",
	}
	resp.ActiveMiners = signalNum{
		Value:  float64(pwr.State.MinerAboveMinPowerCount),
		Unit:   "miners",
		Status: classify(float64(pwr.State.MinerAboveMinPowerCount), 10, 5, true),
	}
	tpc, _ := new(big.Float).SetString(pwr.State.ThisEpochPledgeCollateral)
	if tpc != nil {
		f, _ := tpc.Quo(tpc, big.NewFloat(atto)).Float64()
		resp.TotalPledge = signalNum{Value: f, Unit: "FIL", Status: "ok"}
	}

	storage, _, total, err := initialPledgeForCCSector(pwr, rwd, big.NewInt(0))
	if err == nil {
		fil, _ := new(big.Float).Quo(new(big.Float).SetInt(total), big.NewFloat(atto)).Float64()
		resp.IPPerSector = signalNum{Value: fil, Unit: "FIL/32GiB", Status: "ok"}
		_ = storage
	}

	// Series for sparklines
	resp.Series["blocksPerEpoch"] = blocksPerEp
	resp.Series["baseFeeNFil"] = baseFees
	resp.Series["winCountSum"] = winCounts

	writeJSON(w, 200, resp)
}

func mean(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	s := 0.0
	for _, x := range xs {
		s += x
	}
	return s / float64(len(xs))
}

// classify returns ok/watch/bad based on thresholds.
// If higherIsBetter, ok when v >= okT; watch when v >= warnT; bad otherwise.
// If !higherIsBetter, ok when v <= okT; watch when v <= warnT; bad otherwise.
func classify(v, okT, warnT float64, higherIsBetter bool) string {
	if higherIsBetter {
		switch {
		case v >= okT:
			return "ok"
		case v >= warnT:
			return "watch"
		default:
			return "bad"
		}
	}
	switch {
	case v <= okT:
		return "ok"
	case v <= warnT:
		return "watch"
	default:
		return "bad"
	}
}

// classifyAround returns ok if |v - target| <= okBand, watch if <= warnBand, bad otherwise.
func classifyAround(v, target, okBand, warnBand float64) string {
	d := v - target
	if d < 0 {
		d = -d
	}
	switch {
	case d <= okBand:
		return "ok"
	case d <= warnBand:
		return "watch"
	default:
		return "bad"
	}
}

// ============================================================================
// Other handlers
// ============================================================================

func (a *app) handleHealth(w http.ResponseWriter, r *http.Request) {
	a.ringMu.Lock()
	last := a.ringRefresh
	a.ringMu.Unlock()
	writeJSON(w, 200, map[string]any{
		"ok":        true,
		"ts":        time.Now().Unix(),
		"ringSize":  len(a.ring.snapshot()),
		"ringFresh": time.Since(last).Seconds(),
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

func (a *app) handleUpgrade(w http.ResponseWriter, r *http.Request) {
	head, _ := a.head.Get(r.Context())
	now := time.Now().Unix()
	writeJSON(w, 200, map[string]any{
		"name":           "Fire Horse",
		"networkVersion": 28,
		"network":        "calibration",
		"epoch":          nv28UpgradeEpoch,
		"timestamp":      nv28UpgradeUnix,
		"timestampISO":   time.Unix(nv28UpgradeUnix, 0).UTC().Format(time.RFC3339),
		"announcement":   "https://github.com/filecoin-project/community/discussions/74#discussioncomment-16540452",
		"currentEpoch":   head.Height,
		"epochsLeft":     nv28UpgradeEpoch - head.Height,
		"secondsLeft":    nv28UpgradeUnix - now,
		"genesisUnix":    calibGenesisUnix,
		"epochSeconds":   calibBlockDelaySecs,
	})
}

func (a *app) handleTipsetsRecent(w http.ResponseWriter, r *http.Request) {
	samples := a.ring.snapshot()
	writeJSON(w, 200, map[string]any{
		"window":    len(samples),
		"tipsets":   samples,
		"updatedAt": time.Now().Unix(),
	})
}

func (a *app) handleTopMiners(w http.ResponseWriter, r *http.Request) {
	v, err := a.topMiners.Get(r.Context())
	writeRaw(w, v, err)
}

func (a *app) handleRichList(w http.ResponseWriter, r *http.Request) {
	v, err := a.richList.Get(r.Context())
	writeRaw(w, v, err)
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
	writeRaw(w, v, err)
}

func (a *app) handleFEVM(w http.ResponseWriter, r *http.Request) {
	v, err := a.fevmStats.Get(r.Context())
	writeRaw(w, v, err)
}

func (a *app) handleFaucet(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]any{
		"name":        "Plumbline",
		"url":         a.cfg.faucetURL,
		"status":      a.cfg.faucetURL + "/status",
		"description": "Calibration tFIL + USDFC faucet, dispensed independently.",
	})
}

// ============================================================================
// Plumbing
// ============================================================================

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]any{"error": msg})
}
func writeRaw(w http.ResponseWriter, v json.RawMessage, err error) {
	if err != nil {
		writeError(w, 502, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	_, _ = w.Write(v)
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
	mux.HandleFunc("/api/v1/status", a.handleStatus)
	mux.HandleFunc("/api/v1/signals", a.handleSignals)
	mux.HandleFunc("/api/v1/upgrade", a.handleUpgrade)
	mux.HandleFunc("/api/v1/tipsets/recent", a.handleTipsetsRecent)
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
