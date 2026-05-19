// Backend server that simulates multi-tenant antagonist load (§2 of Prequal paper).
// Exposes X-RIF header (server-local requests-in-flight) on every response so the
// load balancer can use it as a real-time leading signal.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

// serverRIF is the global server-local RIF counter (all clients combined).
var serverRIF int32

// concSem limits the number of requests processed simultaneously.
// concQueue is the bounded waiting queue; requests wait up to queueTimeout for
// a concSem slot before receiving 503.  This models real CPU saturation: requests
// queue and experience rising latency under load, then timeout at heavy overload.
var concSem   chan struct{}
var concQueue chan struct{}

const queueTimeout = 400 * time.Millisecond

// latencyBuckets tracks recent latency observations per RIF level, used to
// report median latency estimates in probe responses — matching §4 of the paper.
type latencyBuckets struct {
	buckets [32][]int64 // indexed by RIF (capped at 31)
	mu      chan struct{}
}

func newLatencyBuckets() *latencyBuckets {
	lb := &latencyBuckets{mu: make(chan struct{}, 1)}
	lb.mu <- struct{}{}
	return lb
}

func (lb *latencyBuckets) record(rif int32, latencyMs int64) {
	idx := rif
	if idx >= 32 {
		idx = 31
	}
	if idx < 0 {
		idx = 0
	}
	<-lb.mu
	lb.buckets[idx] = append(lb.buckets[idx], latencyMs)
	// Keep only the last 50 samples per bucket.
	if len(lb.buckets[idx]) > 50 {
		lb.buckets[idx] = lb.buckets[idx][len(lb.buckets[idx])-50:]
	}
	lb.mu <- struct{}{}
}

func (lb *latencyBuckets) medianAtRIF(rif int32) int64 {
	idx := rif
	if idx >= 32 {
		idx = 31
	}
	if idx < 0 {
		idx = 0
	}
	<-lb.mu
	samples := make([]int64, len(lb.buckets[idx]))
	copy(samples, lb.buckets[idx])
	lb.mu <- struct{}{}
	if len(samples) == 0 {
		return 0
	}
	// Simple insertion sort for small slices.
	for i := 1; i < len(samples); i++ {
		for j := i; j > 0 && samples[j] < samples[j-1]; j-- {
			samples[j], samples[j-1] = samples[j-1], samples[j]
		}
	}
	return samples[len(samples)/2]
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	serverID := os.Getenv("SERVER_ID")
	if serverID == "" {
		serverID = "unknown"
	}
	cpuLoad := 0
	if v := os.Getenv("CPU_LOAD"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cpuLoad = n
		}
	}

	// MAX_CONCURRENCY caps how many requests the server handles simultaneously.
	// This simulates finite CPU capacity: at CPU_LOAD=60 the antagonist consumes
	// 60% of the host's CPU, leaving 40% — so we give server1/2 a smaller cap.
	// Default: 2 for contended servers (CPU_LOAD>0), 5 for clean servers.
	maxConc := 5
	if cpuLoad > 0 {
		maxConc = 2
	}
	if v := os.Getenv("MAX_CONCURRENCY"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxConc = n
		}
	}
	concSem   = make(chan struct{}, maxConc)
	concQueue = make(chan struct{}, maxConc*3) // waiting room = 3× active slots

	buckets := newLatencyBuckets()

	// setRIFHeaders writes the current server-local RIF and median latency estimate
	// to response headers so the load balancer can use them as probe signals.
	setRIFHeaders := func(w http.ResponseWriter, arrivalRIF int32) {
		w.Header().Set("X-RIF", strconv.Itoa(int(atomic.LoadInt32(&serverRIF))))
		med := buckets.medianAtRIF(arrivalRIF)
		w.Header().Set("X-Latency-Estimate", strconv.FormatInt(med, 10))
		w.Header().Set("X-CPU-Load", strconv.Itoa(cpuLoad))
		w.Header().Set("X-Server-ID", serverID)
	}

	// antagonistDelay simulates variable CPU contention from antagonist processes.
	// The base is a small fixed jitter; CPU_LOAD adds proportional delay representing
	// the host machine being partially occupied by other VMs (paper §2, Figure 2).
	antagonistDelay := func() time.Duration {
		base := 5*time.Millisecond + time.Duration(rand.Intn(5))*time.Millisecond
		if cpuLoad <= 0 {
			return base
		}
		// At CPU_LOAD=60 → +30ms; at CPU_LOAD=80 → +40ms, etc.
		contention := time.Duration(float64(cpuLoad)/100.0*50) * time.Millisecond
		variance := time.Duration(rand.Intn(10)) * time.Millisecond
		return base + contention + variance
	}

	// cpuWork simulates variable query cost (paper §5 testbed uses hash iterations
	// drawn from a normal distribution whose σ = μ).
	cpuWork := func() {
		iterations := 500 + rand.Intn(500)
		for i := 0; i < iterations; i++ {
			h := sha256.Sum256([]byte(fmt.Sprintf("%d-%d", time.Now().UnixNano(), i)))
			_ = hex.EncodeToString(h[:])
		}
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		arrivalRIF := atomic.AddInt32(&serverRIF, 1)
		arrivalRIF-- // value before increment
		start := time.Now()

		defer func() {
			atomic.AddInt32(&serverRIF, -1)
			latencyMs := time.Since(start).Milliseconds()
			buckets.record(arrivalRIF, latencyMs)
		}()

		// Capacity gate: enter the waiting queue, then wait for an active slot.
		// This models CPU saturation: requests queue under overload and are served
		// in order; if the queue is full or the wait exceeds queueTimeout → 503.
		// RIF is already incremented so X-RIF reflects true in-flight pressure.
		select {
		case concQueue <- struct{}{}:
		default:
			// Queue full → server completely overwhelmed.
			setRIFHeaders(w, arrivalRIF)
			http.Error(w, `{"error":"overloaded"}`, http.StatusServiceUnavailable)
			return
		}
		defer func() { <-concQueue }()

		timer := time.NewTimer(queueTimeout)
		defer timer.Stop()
		select {
		case concSem <- struct{}{}:
			defer func() { <-concSem }()
		case <-timer.C:
			// Waited too long in queue — server overloaded.
			setRIFHeaders(w, arrivalRIF)
			http.Error(w, `{"error":"queue_timeout"}`, http.StatusServiceUnavailable)
			return
		}

		cpuWork()
		time.Sleep(antagonistDelay())

		duration := time.Since(start)
		setRIFHeaders(w, arrivalRIF)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"server_id":    serverID,
			"duration_ms":  duration.Milliseconds(),
			"cpu_load_pct": cpuLoad,
			"rif":          atomic.LoadInt32(&serverRIF),
		})
	})

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		// Probe endpoint: does NOT acquire the capacity semaphore — probes observe
		// server state without consuming work capacity, analogous to the paper's
		// server-reported CPU utilization that the LB reads.  RIF is still
		// incremented so the X-RIF header accurately reflects ALL in-flight work.
		arrivalRIF := atomic.AddInt32(&serverRIF, 1)
		arrivalRIF--
		start := time.Now()

		defer func() {
			atomic.AddInt32(&serverRIF, -1)
			latencyMs := time.Since(start).Milliseconds()
			buckets.record(arrivalRIF, latencyMs)
		}()

		time.Sleep(antagonistDelay())

		setRIFHeaders(w, arrivalRIF)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":       "healthy",
			"server_id":    serverID,
			"rif":          atomic.LoadInt32(&serverRIF),
			"cpu_load_pct": cpuLoad,
		})
	})

	log.Printf("[backend] server=%s port=%s cpu_load=%d%%", serverID, port, cpuLoad)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatal(err)
	}
}
