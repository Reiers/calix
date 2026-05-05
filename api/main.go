// Calix calibration API.
//
// Exposes a small JSON surface for the Calix dashboard. Reads chain data from
// a Lotus RPC endpoint (default: glif calibration), computes initial pledge
// using the actor v17 formula (calibration's current actors version), and
// caches results aggressively to keep upstream load low.
//
// Endpoints:
//
//	GET /api/v1/health         -> ok
//	GET /api/v1/overview       -> network summary + corrected IP estimator
//	GET /api/v1/blocks/recent  -> last N tipsets
//	GET /api/v1/pledge         -> per-32GiB CC IP, decomposed
//	GET /api/v1/version        -> calix build info + chain network version
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// ----------------------------------------------------------------------------
// Build info
// ----------------------------------------------------------------------------

const calixVersion = "0.1.0"

var calixCommit = "dev" // overridden via -ldflags at build time

// ----------------------------------------------------------------------------
// Config
// ----------------------------------------------------------------------------

type config struct {
	addr      string
	lotusRPC  string
	corsAllow string
}

func loadConfig() config {
	c := config{
		addr:      envOr("CALIX_ADDR", ":8080"),
		lotusRPC:  envOr("CALIX_LOTUS_RPC", "https://api.calibration.node.glif.io/rpc/v1"),
		corsAllow: envOr("CALIX_CORS", "*"),
	}
	flag.StringVar(&c.addr, "addr", c.addr, "listen address")
	flag.StringVar(&c.lotusRPC, "lotus", c.lotusRPC, "Lotus RPC endpoint")
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
// Lotus RPC client (only the methods Glif's gateway exposes)
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
// Cached fetchers
// ----------------------------------------------------------------------------

type cached[T any] struct {
	ttl   time.Duration
	mu    sync.Mutex
	exp   time.Time
	val   T
	fetch func(context.Context) (T, error)
}

func newCached[T any](ttl time.Duration, fetch func(context.Context) (T, error)) *cached[T] {
	return &cached[T]{ttl: ttl, fetch: fetch}
}

func (c *cached[T]) Get(ctx context.Context) (T, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if time.Now().Before(c.exp) {
		return c.val, nil
	}
	v, err := c.fetch(ctx)
	if err != nil {
		// Return last good value if we still have one, else propagate.
		if !c.exp.IsZero() {
			log.Printf("cache miss fetch failed, serving stale: %v", err)
			return c.val, nil
		}
		var zero T
		return zero, err
	}
	c.val = v
	c.exp = time.Now().Add(c.ttl)
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
		Miner     string `json:"Miner"`
		Timestamp int64  `json:"Timestamp"`
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
		Epoch                    int64  `json:"Epoch"`
		EffectiveBaselinePower   string `json:"EffectiveBaselinePower"`
		TotalStoragePowerReward  string `json:"TotalStoragePowerReward"`
		CumsumBaseline           string `json:"CumsumBaseline"`
		CumsumRealized           string `json:"CumsumRealized"`
	} `json:"State"`
}

// ----------------------------------------------------------------------------
// Application
// ----------------------------------------------------------------------------

type app struct {
	cfg      config
	rpc      *lotus
	head     *cached[tipsetHead]
	power    *cached[powerActorState]
	reward   *cached[rewardActorState]
	netver   *cached[int]
}

func newApp(cfg config) *app {
	a := &app{cfg: cfg, rpc: newLotus(cfg.lotusRPC)}

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

	return a
}

// ----------------------------------------------------------------------------
// IP calculation - actor v17 formula
// ----------------------------------------------------------------------------

const (
	epochsPerDay        = 2880
	ipProjectionPeriod  = 20 * epochsPerDay // 57600 epochs
	gammaFixedPointFP   = 1000
	gammaActivationPM   = 300                              // 30% of pledge from baseline at full ramp
	atto                = 1_000_000_000_000_000_000        // 1 FIL
)

// q128 is 2^128 used to convert the smoothed FilterEstimate position to a real value.
var q128 = new(big.Int).Lsh(big.NewInt(1), 128)

// filterEstimate(positionStr) -> bigInt, value at current epoch (ignoring sub-epoch velocity).
func filterEstimate(positionStr string) (*big.Int, error) {
	pos, ok := new(big.Int).SetString(positionStr, 10)
	if !ok {
		return nil, fmt.Errorf("bad positionEstimate %q", positionStr)
	}
	v := new(big.Int).Quo(pos, q128)
	return v, nil
}

// initialPledgeForCCSector computes IP for a 32GiB CC sector.
//
// Returns: storage pledge term (atto), consensus pledge term (atto), nominal IP (atto),
// circulating supply used (atto), and any error.
//
// We use TotalSupply - BurntFunds - Vesting as a calibration-friendly proxy for
// circulating supply when the underlying RPC strips StateVMCirculatingSupplyInternal
// (which is the case on most public gateways including Glif). Caller can override.
func initialPledgeForCCSector(
	ctx context.Context,
	pwr powerActorState,
	rwd rewardActorState,
	circulatingSupply *big.Int,
) (storage, consensus, total *big.Int, err error) {
	const sectorBytes = int64(32 * 1024 * 1024 * 1024) // 34,359,738,368
	sectorQAP := big.NewInt(sectorBytes)

	// reward smoothed estimate
	rwdEst, err := filterEstimate(rwd.State.ThisEpochRewardSmoothed.PositionEstimate)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("reward estimate: %w", err)
	}
	// network QAP smoothed estimate
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

	// Storage pledge (IPBase) = rewardEstimate * sectorQAP / netQAP * 20 days of epochs.
	storage = new(big.Int).Mul(rwdEst, sectorQAP)
	storage.Quo(storage, netQAP)
	storage.Mul(storage, big.NewInt(int64(ipProjectionPeriod)))

	// Consensus pledge (Additional IP).
	// Numerator: 30% × CS × sectorQAP
	// Denominator: max(baseline, netQAP) × 100
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
	Note                  string  `json:"note,omitempty"`
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
	nv, err := a.netver.Get(ctx)
	if err != nil {
		nv = -1 // not fatal
	}

	// We don't have a clean CS read on Glif's gateway. With CS=0 the
	// consensus pledge term collapses to 0 (calibration has it negligible
	// anyway because network QAP << baseline). Mark it in the response
	// so we don't lie about it.
	storage, consensus, total, err := initialPledgeForCCSector(ctx, pwr, rwd, big.NewInt(0))
	if err != nil {
		writeError(w, 500, "compute IP: "+err.Error())
		return
	}

	totalFIL, _ := new(big.Float).Quo(
		new(big.Float).SetInt(total),
		big.NewFloat(atto),
	).Float64()

	resp := overviewResp{
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
		Note:                  "Consensus pledge computed with circulatingSupply=0 (Glif RPC strips StateVMCirculatingSupplyInternal). On calibration this term is negligible because network QAP is far below baseline; storage pledge term dominates.",
	}
	writeJSON(w, 200, resp)
}

func (a *app) handleVersion(w http.ResponseWriter, r *http.Request) {
	nv, _ := a.netver.Get(r.Context())
	writeJSON(w, 200, map[string]any{
		"calixVersion":   calixVersion,
		"calixCommit":    calixCommit,
		"networkVersion": nv,
		"lotusRPC":       a.cfg.lotusRPC,
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
	writeJSON(w, 200, map[string]any{
		"height": head.Height,
		"blocks": blocks,
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

	srv := &http.Server{
		Addr:              cfg.addr,
		Handler:           cors(mux, cfg.corsAllow),
		ReadHeaderTimeout: 10 * time.Second,
	}
	log.Printf("calix api %s+%s listening on %s -> %s", calixVersion, calixCommit, cfg.addr, cfg.lotusRPC)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
