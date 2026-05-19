// Core types for the Prequal load balancer (paper: NSDI '24).
package loadbalancer

import (
	"sync"
	"time"
)

// Algorithm identifies the replica-selection policy.
type Algorithm string

const (
	// AlgorithmPrequal implements the HCL (Hot-Cold Lexicographic) rule with
	// asynchronous probing and a bounded probe pool (paper §4).
	AlgorithmPrequal Algorithm = "prequal"

	// AlgorithmWeightedRR is the WRR baseline the paper displaces (paper §2, §3).
	// Weights are updated periodically using an EWMA of observed latency as a
	// proxy for per-replica CPU utilization.
	AlgorithmWeightedRR Algorithm = "weightedrr"

	// AlgorithmRoundRobin is simple unweighted round-robin.
	AlgorithmRoundRobin Algorithm = "roundrobin"

	// AlgorithmRandom selects a uniformly random healthy server.
	AlgorithmRandom Algorithm = "random"

	// AlgorithmLeastLoaded picks the server with the smallest client-local RIF
	// (paper §5.2 — performs poorly at scale because it ignores server-local RIF).
	AlgorithmLeastLoaded Algorithm = "leastloaded"

	// AlgorithmLLPo2C is Least-Loaded with Power-of-Two-Choices, using
	// client-local RIF (paper §5.2, also implemented in NGINX and Envoy).
	AlgorithmLLPo2C Algorithm = "ll-po2c"
)

// Server represents one backend replica.
type Server struct {
	ID        string
	Address   string
	IsHealthy bool

	// ClientRIF is this load-balancer's own in-flight count to this server.
	// Updated atomically by forwardRequest. This equals server-local RIF when
	// there is a single LB instance — the standard single-datacenter case.
	ClientRIF int32 // read/write via sync/atomic

	// Fields below are updated by the probe goroutine (protected by ProbePool.mu).
	ServerRIF       int32  // from X-RIF response header
	LatencyMs       int64  // from last probe (milliseconds)
	CPULoad         int32  // from X-CPU-Load header (simulated antagonist %)
	LastProbeAt     time.Time

	// WRR state (protected by LoadBalancer.mu).
	Weight      float64 // current routing weight (higher = more traffic)
	EWMALatency float64 // exponentially weighted moving-average latency (ms)
	EWMAAlpha   float64 // smoothing factor
}

// ProbeEntry is one element in the bounded probe pool (paper §4, "The probe pool").
// It carries the load signals observed at the moment of probing plus the timestamp
// needed for staleness detection.
type ProbeEntry struct {
	Server    *Server
	RIF       int32  // server-local RIF at probe time
	LatencyMs int64  // round-trip probe latency (ms)
	Timestamp time.Time
	UseCount  int // number of times this entry has been used for selection
	MaxUses   int // breuse — reuse budget (formula 1 in paper)
}

// ProbePool is the client-side pool of recent probe responses (paper §4).
// It manages staleness, depletion (via reuse), and degradation (via periodic
// removal of the worst entry).
type ProbePool struct {
	entries  []*ProbeEntry
	maxSize  int           // default 16 (paper §4)
	ageLimit time.Duration // probes older than this are stale (default 1s)
	mu       sync.Mutex
}

func NewProbePool(maxSize int, ageLimit time.Duration) *ProbePool {
	if maxSize <= 0 {
		maxSize = 16
	}
	if ageLimit <= 0 {
		ageLimit = time.Second
	}
	return &ProbePool{
		entries:  make([]*ProbeEntry, 0, maxSize),
		maxSize:  maxSize,
		ageLimit: ageLimit,
	}
}

// Add inserts a new probe result. If the pool is at capacity, the oldest entry
// is evicted first (paper §4 "whenever a new probe arrives that would increase
// the pool beyond its size limit, we drop the oldest probe").
func (pp *ProbePool) Add(e *ProbeEntry) {
	pp.mu.Lock()
	defer pp.mu.Unlock()

	if len(pp.entries) >= pp.maxSize {
		pp.removeOldestLocked()
	}
	pp.entries = append(pp.entries, e)
}

// Fresh returns all non-stale entries. A nil/empty slice means pool is empty.
func (pp *ProbePool) Fresh() []*ProbeEntry {
	pp.mu.Lock()
	defer pp.mu.Unlock()

	now := time.Now()
	out := make([]*ProbeEntry, 0, len(pp.entries))
	for _, e := range pp.entries {
		if now.Sub(e.Timestamp) <= pp.ageLimit {
			out = append(out, e)
		}
	}
	return out
}

// MarkUsed increments an entry's use count and removes it when it has reached
// its reuse budget — preventing pool depletion from consuming only the best probes
// (paper §4, "Probe reuse and removal").
func (pp *ProbePool) MarkUsed(e *ProbeEntry) {
	pp.mu.Lock()
	defer pp.mu.Unlock()

	e.UseCount++
	if e.UseCount >= e.MaxUses {
		pp.removeEntryLocked(e)
	}
}

// RemoveWorst alternates between removing the oldest entry and the most-loaded
// entry, preventing degradation (paper §4: "Prequal periodically removes the
// worst probe from the pool").
func (pp *ProbePool) RemoveWorst(rifThreshold int32, removeOldest bool) {
	pp.mu.Lock()
	defer pp.mu.Unlock()

	if len(pp.entries) == 0 {
		return
	}
	if removeOldest {
		pp.removeOldestLocked()
		return
	}
	// Remove the probe with highest RIF (if any hot exist), else highest latency.
	worst := pp.entries[0]
	for _, e := range pp.entries[1:] {
		if e.RIF > rifThreshold {
			if worst.RIF <= rifThreshold || e.RIF > worst.RIF {
				worst = e
			}
		} else if worst.RIF <= rifThreshold && e.LatencyMs > worst.LatencyMs {
			worst = e
		}
	}
	pp.removeEntryLocked(worst)
}

// Len returns the current number of entries (stale or not).
func (pp *ProbePool) Len() int {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	return len(pp.entries)
}

func (pp *ProbePool) removeOldestLocked() {
	if len(pp.entries) == 0 {
		return
	}
	oldest := 0
	for i, e := range pp.entries {
		if e.Timestamp.Before(pp.entries[oldest].Timestamp) {
			oldest = i
		}
	}
	pp.entries = append(pp.entries[:oldest], pp.entries[oldest+1:]...)
}

func (pp *ProbePool) removeEntryLocked(target *ProbeEntry) {
	for i, e := range pp.entries {
		if e == target {
			pp.entries = append(pp.entries[:i], pp.entries[i+1:]...)
			return
		}
	}
}

// Config holds all load-balancer parameters.
type Config struct {
	// Probing
	ProbeInterval     time.Duration // how often the background prober fires
	ProbeTimeout      time.Duration // per-probe HTTP timeout
	HealthCheckPath   string        // path probed (default "/health")
	ProbePoolSize     int           // max entries in pool (default 16)
	ProbeAgeTimeout   time.Duration // stale threshold (default 1s)
	ProbeRatePerQuery float64       // rprobe: probes triggered per incoming query (default 2)
	ProbeRemoveRate   float64       // rremove: worst-probe removals per query (default 1)
	DriftRate         float64       // δ in breuse formula (default 1.0)

	// Selection
	Algorithm        Algorithm
	SelectionChoices int     // d parameter for non-Prequal PodC algorithms
	QRIF             float64 // quantile for hot/cold threshold (default 0.84)

	// WRR weight update interval
	WeightUpdateInterval time.Duration // default 3s
	EWMAAlpha            float64       // smoothing factor for WRR EWMA (default 0.3)
}

// Stats tracks aggregate counters (updated atomically).
type Stats struct {
	TotalRequests     uint64
	SuccessfulRequests uint64
	FailedRequests    uint64
}
