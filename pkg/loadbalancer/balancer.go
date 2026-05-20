// Prequal load balancer implementation.
//
// Design follows §4 of "Load is not what you should balance: Introducing Prequal"
// (NSDI '24).  The key ideas:
//
//  1. Asynchronous probing: each incoming query triggers rprobe background probes
//     whose results accumulate in a bounded probe pool.  The current query is
//     assigned using results gathered by *previous* queries' probes.
//
//  2. HCL (Hot-Cold Lexicographic) replica selection: classify pool entries as
//     hot/cold by comparing their RIF against the QRIF-quantile of the pool-wide
//     RIF distribution.  Pick the cold entry with lowest latency; if all are hot,
//     pick the entry with lowest RIF.
//
//  3. Server-local RIF from probe headers (X-RIF) is the primary signal — an
//     instantaneous, leading indicator of future load that outperforms CPU-based
//     trailing signals like WRR uses.
//
// The file also implements WRR, Round-Robin, Random, LeastLoaded, and LL-Po2C so
// they can be run side-by-side for the paper's Figure 6 / Figure 7 comparisons.
package loadbalancer

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// LoadBalancer is the central object.  One instance per algorithm is created
// (docker-compose runs two: prequal on :8080 and weightedrr on :8081).
type LoadBalancer struct {
	servers  []*Server
	pool     *ProbePool // async probe pool — used only by Prequal
	config   *Config
	metrics  *Metrics
	logger   *slog.Logger

	mu           sync.RWMutex
	rrIdx        uint32 // atomic round-robin counter
	removeToggle bool   // alternates oldest ↔ worst-loaded removal (paper §4)

	stats Stats // atomic counters
}

func NewLoadBalancer(cfg *Config, logger *slog.Logger, metrics *Metrics) *LoadBalancer {
	if cfg == nil {
		cfg = defaultConfig()
	}
	applyDefaults(cfg)

	return &LoadBalancer{
		servers: make([]*Server, 0),
		pool:    NewProbePool(cfg.ProbePoolSize, cfg.ProbeAgeTimeout),
		config:  cfg,
		metrics: metrics,
		logger:  logger,
	}
}

func defaultConfig() *Config {
	return &Config{}
}

func applyDefaults(cfg *Config) {
	if cfg.ProbeInterval == 0 {
		cfg.ProbeInterval = time.Second
	}
	if cfg.ProbeTimeout == 0 {
		cfg.ProbeTimeout = 2 * time.Second
	}
	if cfg.HealthCheckPath == "" {
		cfg.HealthCheckPath = "/health"
	}
	if cfg.ProbePoolSize == 0 {
		cfg.ProbePoolSize = 16
	}
	if cfg.ProbeAgeTimeout == 0 {
		cfg.ProbeAgeTimeout = time.Second
	}
	if cfg.ProbeRatePerQuery == 0 {
		cfg.ProbeRatePerQuery = 2
	}
	if cfg.ProbeRemoveRate == 0 {
		cfg.ProbeRemoveRate = 1
	}
	if cfg.DriftRate == 0 {
		cfg.DriftRate = 1.0
	}
	if cfg.SelectionChoices == 0 {
		cfg.SelectionChoices = 2
	}
	if cfg.QRIF == 0 {
		cfg.QRIF = 0.84
	}
	if cfg.Algorithm == "" {
		cfg.Algorithm = AlgorithmPrequal
	}
	if cfg.WeightUpdateInterval == 0 {
		cfg.WeightUpdateInterval = 3 * time.Second
	}
	if cfg.EWMAAlpha == 0 {
		cfg.EWMAAlpha = 0.15
	}
}

// AddServer registers a backend replica.
func (lb *LoadBalancer) AddServer(s *Server) {
	s.Weight = 1.0
	s.EWMALatency = 20.0 // optimistic initial latency estimate
	s.EWMAAlpha = lb.config.EWMAAlpha
	lb.mu.Lock()
	lb.servers = append(lb.servers, s)
	lb.mu.Unlock()
}

// ─── Background goroutines ────────────────────────────────────────────────────

// StartBackground launches:
//   - periodic prober (fills probe pool + updates health)
//   - periodic worst-probe remover (prevents pool degradation)
//   - periodic WRR weight updater (only when algorithm = weightedrr)
func (lb *LoadBalancer) StartBackground() {
	go lb.runPeriodicProber()
	if lb.config.Algorithm == AlgorithmPrequal {
		go lb.runProbeRemover()
	}
	if lb.config.Algorithm == AlgorithmWeightedRR {
		go lb.runWeightUpdater()
	}
}

func (lb *LoadBalancer) runPeriodicProber() {
	// Stagger the first probe slightly so the pool has entries before the first
	// request arrives (typical startup: services probe ~100ms after launch).
	time.Sleep(200 * time.Millisecond)

	ticker := time.NewTicker(lb.config.ProbeInterval)
	defer ticker.Stop()
	for range ticker.C {
		lb.probeAllServers()
	}
}

func (lb *LoadBalancer) probeAllServers() {
	lb.mu.RLock()
	servers := make([]*Server, len(lb.servers))
	copy(servers, lb.servers)
	lb.mu.RUnlock()

	for _, srv := range servers {
		go func(s *Server) {
			firedAt := time.Now() // capture fire time before round-trip so all probes in the batch share
			res := lb.probeServer(s) // a comparable age — prevents fast servers from being evicted as "oldest"
			if res == nil {
				return
			}
			lb.mu.Lock()
			s.IsHealthy = res.isHealthy
			s.LatencyMs = res.latencyMs
			if res.serverRIF >= 0 {
				s.ServerRIF = res.serverRIF
			}
			if res.cpuLoad >= 0 {
				s.CPULoad = res.cpuLoad
			}
			s.LastProbeAt = time.Now()
			lb.mu.Unlock()

			alg := string(lb.config.Algorithm)
			if res.isHealthy {
				lb.metrics.serverHealth.WithLabelValues(s.ID, alg).Set(1)
			} else {
				lb.metrics.serverHealth.WithLabelValues(s.ID, alg).Set(0)
			}
			lb.metrics.serverLatency.WithLabelValues(s.ID, alg).Set(float64(res.latencyMs))
			lb.metrics.serverCPULoad.WithLabelValues(s.ID, alg).Set(float64(res.cpuLoad))

			// Add to probe pool for Prequal.
			if lb.config.Algorithm == AlgorithmPrequal {
				maxUses := lb.computeBreuse()
				lb.pool.Add(&ProbeEntry{
					Server:    s,
					RIF:       res.serverRIF,
					LatencyMs: res.latencyMs,
					Timestamp: firedAt,
					MaxUses:   maxUses,
				})
			}
		}(srv)
	}
}

// probeResult carries the raw output of a single probe HTTP call.
type probeResult struct {
	latencyMs int64
	serverRIF int32
	cpuLoad   int32
	isHealthy bool
}

func (lb *LoadBalancer) probeServer(s *Server) *probeResult {
	ctx, cancel := context.WithTimeout(context.Background(), lb.config.ProbeTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"http://"+s.Address+lb.config.HealthCheckPath, nil)
	if err != nil {
		lb.logger.Warn("probe: create request", slog.String("server", s.ID), slog.Any("err", err))
		return &probeResult{isHealthy: false, serverRIF: -1, cpuLoad: -1}
	}

	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	latencyMs := time.Since(start).Milliseconds()

	if err != nil {
		lb.logger.Debug("probe: request failed", slog.String("server", s.ID), slog.Any("err", err))
		return &probeResult{isHealthy: false, latencyMs: latencyMs, serverRIF: -1, cpuLoad: -1}
	}
	defer resp.Body.Close()

	res := &probeResult{
		latencyMs: latencyMs,
		isHealthy: resp.StatusCode == http.StatusOK,
		serverRIF: -1,
		cpuLoad:   -1,
	}

	// Read server-local RIF from the header exposed by our backend.
	if v := resp.Header.Get("X-RIF"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 32); err == nil {
			res.serverRIF = int32(n)
		}
	}
	if v := resp.Header.Get("X-CPU-Load"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 32); err == nil {
			res.cpuLoad = int32(n)
		}
	}
	return res
}

// triggerAsyncProbes fires rprobe probes to random servers (without replacement)
// and stores results in the pool.  Called per-query from ServeHTTP so the pool
// stays fresh (paper §4, "Probing rate").
func (lb *LoadBalancer) triggerAsyncProbes() {
	lb.mu.RLock()
	servers := make([]*Server, len(lb.servers))
	copy(servers, lb.servers)
	lb.mu.RUnlock()

	if len(servers) == 0 {
		return
	}
	n := int(lb.config.ProbeRatePerQuery)
	if n > len(servers) {
		n = len(servers)
	}

	// Shuffle and take first n — sampling without replacement (paper §4).
	indices := rand.Perm(len(servers))
	for i := 0; i < n; i++ {
		srv := servers[indices[i]]
		go func(s *Server) {
			firedAt := time.Now()
			res := lb.probeServer(s)
			if res == nil || !res.isHealthy {
				return
			}
			lb.mu.Lock()
			s.IsHealthy = res.isHealthy
			s.LatencyMs = res.latencyMs
			if res.serverRIF >= 0 {
				s.ServerRIF = res.serverRIF
			}
			lb.mu.Unlock()

			lb.pool.Add(&ProbeEntry{
				Server:    s,
				RIF:       res.serverRIF,
				LatencyMs: res.latencyMs,
				Timestamp: firedAt,
				MaxUses:   lb.computeBreuse(),
			})
		}(srv)
	}
}

// runProbeRemover periodically removes the worst probe, preventing pool
// degradation (paper §4 "Prequal periodically removes the worst probe").
// Alternates between oldest and most-loaded removal.
func (lb *LoadBalancer) runProbeRemover() {
	ticker := time.NewTicker(lb.config.ProbeInterval / 2)
	defer ticker.Stop()
	for range ticker.C {
		threshold := lb.currentRIFThreshold()
		lb.removeToggle = !lb.removeToggle
		lb.pool.RemoveWorst(threshold, lb.removeToggle)
	}
}

// runWeightUpdater refreshes WRR weights periodically (not per-request) to
// mimic the smoothed historical statistics WRR uses in the paper (§2).
func (lb *LoadBalancer) runWeightUpdater() {
	ticker := time.NewTicker(lb.config.WeightUpdateInterval)
	defer ticker.Stop()
	for range ticker.C {
		lb.recomputeWRRWeights()
	}
}

// recomputeWRRWeights updates per-server weights from the latest probe-measured
// latency (LatencyMs) — analogous to the paper's periodic CPU-utilization
// refresh.  Weights only change at WeightUpdateInterval; between refreshes WRR
// routes with stale weights, which is the trailing-signal weakness the paper
// exploits.  Per-request updates are intentionally absent so the lag is visible.
func (lb *LoadBalancer) recomputeWRRWeights() {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	for _, s := range lb.servers {
		lat := float64(s.LatencyMs)
		if lat <= 0 {
			lat = s.EWMALatency
		}
		if lat <= 0 {
			continue
		}
		// EWMA smoothing over the periodic samples (not per-request).
		if s.EWMALatency <= 0 {
			s.EWMALatency = lat
		} else {
			s.EWMALatency = s.EWMAAlpha*lat + (1-s.EWMAAlpha)*s.EWMALatency
		}
		s.Weight = 1000.0 / s.EWMALatency
	}
}

// computeBreuse implements formula (1) from the paper:
//
//	breuse = max(1, (1+δ) / ((1 - m/n) * rprobe - rremove))
//
// The paper's formula assumes n >> m (large replica set, small pool).  When
// n ≤ m — as in our 3-server testbed — the denominator is negative and the
// formula is undefined.  We fall back to pool_size/n + 1 so that each pool
// slot is reused ~(m/n) times before eviction, keeping the pool filled.
func (lb *LoadBalancer) computeBreuse() int {
	lb.mu.RLock()
	n := float64(len(lb.servers))
	lb.mu.RUnlock()

	if n == 0 {
		return 1
	}
	m := float64(lb.config.ProbePoolSize)
	rp := lb.config.ProbeRatePerQuery
	rr := lb.config.ProbeRemoveRate
	d := lb.config.DriftRate

	denom := (1.0 - m/n) * rp - rr
	if denom <= 0 {
		// Small testbed fallback: reuse each entry m/n times so the pool
		// doesn't deplete faster than probes can refill it.
		v := int(m/n) + 1
		if v < 1 {
			return 1
		}
		return v
	}
	v := (1.0 + d) / denom
	if v < 1 {
		return 1
	}
	return int(v)
}

// currentRIFThreshold returns the QRIF-quantile of the current pool's RIF
// distribution.  Used by the probe remover to decide which probes are "hot".
func (lb *LoadBalancer) currentRIFThreshold() int32 {
	entries := lb.pool.Fresh()
	if len(entries) == 0 {
		return 0
	}
	return rifQuantile(entries, lb.config.QRIF)
}

// ─── Algorithm dispatch ────────────────────────────────────────────────────────

func (lb *LoadBalancer) SelectServer() *Server {
	switch lb.config.Algorithm {
	case AlgorithmPrequal:
		return lb.selectPrequal()
	case AlgorithmWeightedRR:
		return lb.selectWeightedRR()
	case AlgorithmRoundRobin:
		return lb.selectRoundRobin()
	case AlgorithmRandom:
		return lb.selectRandom()
	case AlgorithmLeastLoaded:
		return lb.selectLeastLoaded()
	case AlgorithmLLPo2C:
		return lb.selectLLPo2C()
	default:
		return lb.selectPrequal()
	}
}

// ─── Prequal HCL ─────────────────────────────────────────────────────────────

// selectPrequal implements the Hot-Cold Lexicographic rule (paper §4,
// "Replica selection").
//
// The QRIF quantile is computed over the ENTIRE probe pool (not just d candidates)
// so the hot/cold threshold reflects the global load distribution.
// Cold replicas (RIF ≤ threshold) are sorted by latency; hot replicas by RIF.
func (lb *LoadBalancer) selectPrequal() *Server {
	entries := lb.pool.Fresh()

	// Paper §4: "fall back to selecting a uniformly random replica" when pool < 2.
	if len(entries) < 2 {
		return lb.selectRandom()
	}

	threshold := rifQuantile(entries, lb.config.QRIF)

	var cold, hot []*ProbeEntry
	for _, e := range entries {
		if !e.Server.IsHealthy {
			continue
		}
		if e.RIF > threshold {
			hot = append(hot, e)
		} else {
			cold = append(cold, e)
		}
	}

	var chosen *ProbeEntry
	if len(cold) > 0 {
		// Pick the cold probe with the lowest estimated latency (paper §4 HCL rule).
		chosen = cold[0]
		for _, e := range cold[1:] {
			if e.LatencyMs < chosen.LatencyMs {
				chosen = e
			}
		}
	} else if len(hot) > 0 {
		// All hot: pick the one with the lowest RIF.
		chosen = hot[0]
		for _, e := range hot[1:] {
			if e.RIF < chosen.RIF {
				chosen = e
			}
		}
	}

	if chosen == nil {
		return lb.selectRandom()
	}

	lb.pool.MarkUsed(chosen)
	return chosen.Server
}

// ─── Weighted Round-Robin ─────────────────────────────────────────────────────

// selectWeightedRR is the WRR baseline displaced by Prequal in the paper (§2,§3).
// Weights are inversely proportional to EWMA latency; they are updated every
// WeightUpdateInterval — not per-request — creating the smoothing/lag that
// makes WRR inferior to Prequal under variable antagonist load.
func (lb *LoadBalancer) selectWeightedRR() *Server {
	lb.mu.RLock()
	healthy := lb.healthyServersLocked()
	lb.mu.RUnlock()

	if len(healthy) == 0 {
		return nil
	}

	total := 0.0
	for _, s := range healthy {
		total += s.Weight
	}
	if total <= 0 {
		return healthy[rand.Intn(len(healthy))]
	}

	r := rand.Float64() * total
	cum := 0.0
	for _, s := range healthy {
		cum += s.Weight
		if r <= cum {
			return s
		}
	}
	return healthy[len(healthy)-1]
}

// updateWRRAfterRequest updates the EWMA latency and weight for a server after a
// completed request.  Called by forwardRequest via defer.
func (lb *LoadBalancer) updateWRRAfterRequest(s *Server, latencyMs int64) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	if s.EWMALatency <= 0 {
		s.EWMALatency = float64(latencyMs)
	} else {
		s.EWMALatency = s.EWMAAlpha*float64(latencyMs) + (1-s.EWMAAlpha)*s.EWMALatency
	}
	if s.EWMALatency < 1 {
		s.EWMALatency = 1
	}
	// Weight updated here so WRR has near-per-request information BUT with EWMA
	// smoothing — exactly the trailing-signal limitation described in paper §2.
	s.Weight = 1000.0 / s.EWMALatency
}

// ─── Simple algorithms (for comparison in Figure 7) ──────────────────────────

func (lb *LoadBalancer) selectRoundRobin() *Server {
	lb.mu.RLock()
	healthy := lb.healthyServersLocked()
	lb.mu.RUnlock()
	if len(healthy) == 0 {
		return nil
	}
	idx := atomic.AddUint32(&lb.rrIdx, 1)
	return healthy[int(idx-1)%len(healthy)]
}

func (lb *LoadBalancer) selectRandom() *Server {
	lb.mu.RLock()
	healthy := lb.healthyServersLocked()
	lb.mu.RUnlock()
	if len(healthy) == 0 {
		return nil
	}
	return healthy[rand.Intn(len(healthy))]
}

// selectLeastLoaded uses CLIENT-LOCAL RIF (paper §5.2 "LL policy").
// Suffers because a server can be heavily loaded by *other* clients while
// showing zero client-local RIF from this LB.
func (lb *LoadBalancer) selectLeastLoaded() *Server {
	lb.mu.RLock()
	healthy := lb.healthyServersLocked()
	lb.mu.RUnlock()
	if len(healthy) == 0 {
		return nil
	}
	best := healthy[0]
	for _, s := range healthy[1:] {
		if atomic.LoadInt32(&s.ClientRIF) < atomic.LoadInt32(&best.ClientRIF) {
			best = s
		}
	}
	return best
}

// selectLLPo2C samples 2 servers and picks the one with smaller client-local
// RIF (paper §5.2, also in NGINX/Envoy).
func (lb *LoadBalancer) selectLLPo2C() *Server {
	lb.mu.RLock()
	healthy := lb.healthyServersLocked()
	lb.mu.RUnlock()
	if len(healthy) == 0 {
		return nil
	}
	if len(healthy) == 1 {
		return healthy[0]
	}
	// Sample 2 without replacement.
	i, j := rand.Intn(len(healthy)), rand.Intn(len(healthy)-1)
	if j >= i {
		j++
	}
	a, b := healthy[i], healthy[j]
	if atomic.LoadInt32(&a.ClientRIF) <= atomic.LoadInt32(&b.ClientRIF) {
		return a
	}
	return b
}

// ─── HTTP handling ─────────────────────────────────────────────────────────

func (lb *LoadBalancer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	atomic.AddUint64(&lb.stats.TotalRequests, 1)
	alg := string(lb.config.Algorithm)

	// Trigger async probes for Prequal before selection so the pool is being
	// refreshed while we handle this request (paper §4, async probing).
	if lb.config.Algorithm == AlgorithmPrequal {
		go lb.triggerAsyncProbes()
	}

	server := lb.SelectServer()
	if server == nil {
		atomic.AddUint64(&lb.stats.FailedRequests, 1)
		lb.metrics.requestErrors.WithLabelValues(alg, "no_server").Inc()
		lb.logger.Error("no available server")
		http.Error(w, "no available servers", http.StatusServiceUnavailable)
		return
	}

	lb.metrics.serverSelectionTotal.WithLabelValues(server.ID, alg).Inc()

	start := time.Now()
	lb.forwardRequest(server, w, r)
	duration := time.Since(start)

	lb.metrics.requestDuration.WithLabelValues(alg).Observe(duration.Seconds())
	atomic.AddUint64(&lb.stats.SuccessfulRequests, 1)
}

func (lb *LoadBalancer) forwardRequest(server *Server, w http.ResponseWriter, r *http.Request) {
	alg := string(lb.config.Algorithm)
	atomic.AddInt32(&server.ClientRIF, 1)
	lb.metrics.activeRequests.WithLabelValues(alg).Inc()
	lb.metrics.serverRIF.WithLabelValues(server.ID, alg).Set(
		float64(atomic.LoadInt32(&server.ClientRIF)))

	defer func() {
		atomic.AddInt32(&server.ClientRIF, -1)
		lb.metrics.activeRequests.WithLabelValues(alg).Dec()
		lb.metrics.serverRIF.WithLabelValues(server.ID, alg).Set(
			float64(atomic.LoadInt32(&server.ClientRIF)))
	}()

	targetURL, err := url.Parse("http://" + server.Address)
	if err != nil {
		lb.logger.Error("invalid server address",
			slog.String("server", server.ID), slog.Any("err", err))
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	proxy := httputil.NewSingleHostReverseProxy(targetURL)
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		lb.logger.Error("proxy error",
			slog.String("server", server.ID), slog.Any("err", err))
		atomic.AddUint64(&lb.stats.FailedRequests, 1)
		lb.metrics.requestErrors.WithLabelValues(alg, "proxy_error").Inc()
		http.Error(w, "upstream error", http.StatusBadGateway)
	}
	proxy.ServeHTTP(w, r)
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

func (lb *LoadBalancer) healthyServersLocked() []*Server {
	out := make([]*Server, 0, len(lb.servers))
	for _, s := range lb.servers {
		if s.IsHealthy {
			out = append(out, s)
		}
	}
	return out
}

// rifQuantile returns the q-quantile of the RIF distribution across pool entries.
// q=0.84 means 84 % of servers will be classified cold — the default from paper §4.
func rifQuantile(entries []*ProbeEntry, q float64) int32 {
	if len(entries) == 0 {
		return 0
	}
	rifs := make([]int32, 0, len(entries))
	for _, e := range entries {
		rifs = append(rifs, e.RIF)
	}
	sort.Slice(rifs, func(i, j int) bool { return rifs[i] < rifs[j] })
	idx := int(float64(len(rifs)-1) * q)
	if idx >= len(rifs) {
		idx = len(rifs) - 1
	}
	return rifs[idx]
}

// Servers returns a snapshot of the registered servers (for health endpoint).
func (lb *LoadBalancer) Servers() []*Server {
	lb.mu.RLock()
	defer lb.mu.RUnlock()
	out := make([]*Server, len(lb.servers))
	copy(out, lb.servers)
	return out
}

// Stats returns a copy of the aggregate counters.
func (lb *LoadBalancer) Stats() Stats {
	return Stats{
		TotalRequests:      atomic.LoadUint64(&lb.stats.TotalRequests),
		SuccessfulRequests: atomic.LoadUint64(&lb.stats.SuccessfulRequests),
		FailedRequests:     atomic.LoadUint64(&lb.stats.FailedRequests),
	}
}

// Healthz is a simple self-check handler (mounted at /healthz by cmd/balancer).
func (lb *LoadBalancer) Healthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	s := lb.Stats()
	fmt.Fprintf(w, `{"status":"ok","total":%d,"ok":%d,"err":%d}`,
		s.TotalRequests, s.SuccessfulRequests, s.FailedRequests)
}
