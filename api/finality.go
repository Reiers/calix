package main

// Chain finality, via Filecoin.ChainGetTipSetFinalityStatus (Lotus v2 API).
//
// Lotus runs two finality mechanisms in parallel and uses whichever is more
// recent: F3 certificates, and the EC probabilistic finality calculator
// (FRC-0089), which derives from recent block production the depth at which
// reorg probability drops below 2^-30. If neither is available the node falls
// back to the static 900-epoch assumption. The EC depth doubles as a chain
// health measure: fewer blocks per tipset push it deeper, and -1 means the
// threshold cannot be met at all. Suggested by rvagg.

import (
	"context"
	"log"
	"net/http"
	"sync"
	"time"
)

const (
	finalitySampleEvery   = 30 * time.Second
	finalityHistorySize   = 240 // 2h at 30s
	staticFinalityEpochs  = 900
	ecHealthyMaxDepth     = 40 // healthy mainnet is ~25-35
	finalityRecentSamples = 120
)

type finTipset struct {
	Height int64 `json:"Height"`
}

type finalityStatusRaw struct {
	ECFinalityThresholdDepth int64      `json:"ecFinalityThresholdDepth"`
	ECFinalizedTipSet        *finTipset `json:"ecFinalizedTipSet"`
	F3FinalizedTipSet        *finTipset `json:"f3FinalizedTipSet"`
	FinalizedTipSet          *finTipset `json:"finalizedTipSet"`
	Head                     *finTipset `json:"head"`
}

type finalitySample struct {
	TS         int64  `json:"ts"`
	Head       int64  `json:"head"`
	ECDepth    int64  `json:"ecDepth"`              // -1 = threshold not met
	F3Depth    int64  `json:"f3Depth"`              // -1 = F3 unavailable
	FinalDepth int64  `json:"finalDepth"`           // depth of the node's finalized tipset
	Source     string `json:"source"`               // "f3" | "ec" | "static"
	F3FromCert bool   `json:"f3FromCert,omitempty"` // F3 depth recovered via F3GetLatestCertificate
}

type finalityTracker struct {
	rpc     *lotus // v2, finality status
	rpcV1   *lotus // v1, F3 certificate fallback
	mu      sync.Mutex
	samples []finalitySample
	lastErr string
	lastTry time.Time
}

func newFinalityTracker(rpc, rpcV1 *lotus) *finalityTracker {
	return &finalityTracker{rpc: rpc, rpcV1: rpcV1}
}

// f3CertEpoch returns the last epoch finalized by the latest F3 certificate.
// Public load-balanced RPCs (Glif) have backends that intermittently fail to
// reach F3 ("client error (Connect)"), which makes ChainGetTipSetFinalityStatus
// return a nil F3 tipset even though F3 is finalizing fine. Retry a few times
// so the panel reports F3 health, not RPC backend health.
func (f *finalityTracker) f3CertEpoch(ctx context.Context) (int64, bool) {
	var cert struct {
		ECChain []struct {
			Epoch int64 `json:"Epoch"`
		} `json:"ECChain"`
	}
	for i := 0; i < 4; i++ {
		cert.ECChain = nil
		if err := f.rpcV1.call(ctx, "Filecoin.F3GetLatestCertificate", []any{}, &cert); err == nil && len(cert.ECChain) > 0 {
			return cert.ECChain[len(cert.ECChain)-1].Epoch, true
		}
	}
	return 0, false
}

func (f *finalityTracker) run(ctx context.Context) {
	for {
		f.sample(ctx)
		select {
		case <-ctx.Done():
			return
		case <-time.After(finalitySampleEvery):
		}
	}
}

func (f *finalityTracker) sample(ctx context.Context) {
	cctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	var raw finalityStatusRaw
	err := f.rpc.call(cctx, "Filecoin.ChainGetTipSetFinalityStatus", []any{}, &raw)
	var certEpoch int64
	var certOK bool
	if err == nil && raw.Head != nil && raw.F3FinalizedTipSet == nil {
		certEpoch, certOK = f.f3CertEpoch(cctx)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastTry = time.Now()
	if err == nil && raw.Head == nil {
		err = errNoHead
	}
	if err != nil {
		f.lastErr = err.Error()
		log.Printf("finality sample: %v", err)
		return
	}
	f.lastErr = ""
	s := toSample(raw, time.Now().Unix())
	if certOK && certEpoch <= s.Head {
		s.F3Depth = s.Head - certEpoch
		s.F3FromCert = true
		// A node with working F3 takes the more recent of F3 and EC.
		if s.F3Depth < s.FinalDepth {
			s.FinalDepth = s.F3Depth
			s.Source = "f3"
		}
	}
	// One sample per head; the calculator only moves when the head does.
	if n := len(f.samples); n > 0 && f.samples[n-1].Head == s.Head {
		f.samples[n-1] = s
		return
	}
	f.samples = append(f.samples, s)
	if len(f.samples) > finalityHistorySize {
		f.samples = f.samples[len(f.samples)-finalityHistorySize:]
	}
}

type finErr string

func (e finErr) Error() string { return string(e) }

const errNoHead = finErr("finality status returned no head")

func toSample(r finalityStatusRaw, now int64) finalitySample {
	head := r.Head.Height
	s := finalitySample{TS: now, Head: head, ECDepth: -1, F3Depth: -1, FinalDepth: staticFinalityEpochs, Source: "static"}
	if r.ECFinalityThresholdDepth >= 0 {
		s.ECDepth = r.ECFinalityThresholdDepth
	}
	if r.F3FinalizedTipSet != nil {
		s.F3Depth = head - r.F3FinalizedTipSet.Height
	}
	if r.FinalizedTipSet != nil {
		s.FinalDepth = head - r.FinalizedTipSet.Height
		switch {
		case s.F3Depth >= 0 && s.FinalDepth == s.F3Depth:
			s.Source = "f3"
		case s.ECDepth >= 0 && s.FinalDepth == s.ECDepth:
			s.Source = "ec"
		}
	}
	return s
}

func (f *finalityTracker) latest() (finalitySample, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.samples) == 0 {
		return finalitySample{}, false
	}
	s := f.samples[len(f.samples)-1]
	// Treat samples older than 5 minutes as unavailable.
	if time.Now().Unix()-s.TS > 300 {
		return finalitySample{}, false
	}
	return s, true
}

// finalityLevel grades chain health from one sample.
//
//	ok       EC threshold met at <= 40 epochs, or F3 is finalizing
//	watch    F3 down and EC deeper than 40, or EC threshold not met while F3 holds
//	degraded neither mechanism available: node falls back to 900 epochs
func finalityLevel(s finalitySample) (level, ecLevel string) {
	switch {
	case s.ECDepth < 0:
		ecLevel = "bad"
	case s.ECDepth > ecHealthyMaxDepth:
		ecLevel = "watch"
	default:
		ecLevel = "ok"
	}
	switch {
	case s.F3Depth < 0 && s.ECDepth < 0:
		level = "degraded"
	case s.F3Depth < 0 && s.ECDepth > ecHealthyMaxDepth, s.ECDepth < 0:
		level = "watch"
	default:
		level = "ok"
	}
	return level, ecLevel
}

func (a *app) handleFinality(w http.ResponseWriter, r *http.Request) {
	f := a.fin
	f.mu.Lock()
	hist := make([]finalitySample, len(f.samples))
	copy(hist, f.samples)
	lastErr, lastTry := f.lastErr, f.lastTry
	f.mu.Unlock()

	cur, ok := a.fin.latest()
	if !ok {
		writeJSON(w, 200, map[string]any{
			"available":   false,
			"error":       lastErr,
			"lastAttempt": lastTry.Unix(),
			"method":      "Filecoin.ChainGetTipSetFinalityStatus",
			"generatedAt": time.Now().Unix(),
		})
		return
	}

	recent := hist
	if len(recent) > finalityRecentSamples {
		recent = recent[len(recent)-finalityRecentSamples:]
	}
	var f3Up, ecSum, ecN int64
	ecMin, ecMax := int64(-1), int64(-1)
	for _, s := range recent {
		if s.F3Depth >= 0 {
			f3Up++
		}
		if s.ECDepth >= 0 {
			ecSum += s.ECDepth
			ecN++
			if ecMin < 0 || s.ECDepth < ecMin {
				ecMin = s.ECDepth
			}
			if s.ECDepth > ecMax {
				ecMax = s.ECDepth
			}
		}
	}
	ecAvg := 0.0
	if ecN > 0 {
		ecAvg = float64(ecSum) / float64(ecN)
	}
	level, ecLevel := finalityLevel(cur)

	writeJSON(w, 200, map[string]any{
		"available":         true,
		"method":            "Filecoin.ChainGetTipSetFinalityStatus",
		"level":             level,
		"ecLevel":           ecLevel,
		"head":              cur.Head,
		"ecDepth":           cur.ECDepth,
		"f3Depth":           cur.F3Depth,
		"finalDepth":        cur.FinalDepth,
		"source":            cur.Source,
		"epochSeconds":      calibBlockDelaySecs,
		"staticDepth":       staticFinalityEpochs,
		"ecHealthyMaxDepth": ecHealthyMaxDepth,
		"window": map[string]any{
			"samples":        len(recent),
			"f3AvailablePct": 100 * float64(f3Up) / float64(len(recent)),
			"ecAvg":          ecAvg,
			"ecMin":          ecMin,
			"ecMax":          ecMax,
		},
		"history":     hist,
		"generatedAt": time.Now().Unix(),
	})
}
