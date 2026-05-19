package loadbalancer

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics exposes per-algorithm labeled Prometheus metrics so that both Prequal
// and WRR can be compared on the same Grafana dashboard.
type Metrics struct {
	// requestDuration is a histogram of end-to-end request latency.
	// Matches the paper's p50/p90/p99/p99.9 plots (Figures 5, 6, 7).
	requestDuration *prometheus.HistogramVec

	// activeRequests tracks live RIF at the load-balancer level (sum over servers).
	activeRequests *prometheus.GaugeVec

	// serverRIF tracks per-server client-local RIF — maps to Figure 4 / Figure 6c.
	serverRIF *prometheus.GaugeVec

	// serverLatency tracks the last observed probe latency per server.
	serverLatency *prometheus.GaugeVec

	// serverHealth is 1=healthy, 0=unhealthy.
	serverHealth *prometheus.GaugeVec

	// serverCPULoad records the antagonist-simulated CPU load per server,
	// corresponding to Figure 6c (CPU utilization distribution).
	serverCPULoad *prometheus.GaugeVec

	// requestErrors counts failed forwarding attempts.
	requestErrors *prometheus.CounterVec

	// serverSelectionTotal counts how many times each server is selected,
	// exposing whether Prequal steers away from contended servers.
	serverSelectionTotal *prometheus.CounterVec
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	factory := promauto.With(reg)

	return &Metrics{
		requestDuration: factory.NewHistogramVec(
			prometheus.HistogramOpts{
				Name: "lb_request_duration_seconds",
				Help: "End-to-end request latency observed by the load balancer.",
				// Fine-grained buckets to capture the paper's p99.9 tail behaviour.
				Buckets: []float64{
					.005, .010, .025, .050, .075,
					.100, .150, .200, .250, .300, .400, .500,
					.750, 1.0, 1.5, 2.0, 3.0, 5.0,
				},
			},
			[]string{"algorithm"},
		),
		activeRequests: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "lb_active_requests",
				Help: "Total requests currently in-flight across all servers.",
			},
			[]string{"algorithm"},
		),
		serverRIF: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "lb_server_rif",
				Help: "Client-local requests-in-flight per server replica.",
			},
			[]string{"server_id", "algorithm"},
		),
		serverLatency: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "lb_server_latency_ms",
				Help: "Latest probe latency per server replica (milliseconds).",
			},
			[]string{"server_id", "algorithm"},
		),
		serverHealth: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "lb_server_health",
				Help: "Server health status: 1=healthy, 0=unhealthy.",
			},
			[]string{"server_id", "algorithm"},
		),
		serverCPULoad: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "lb_server_cpu_load_pct",
				Help: "Antagonist-simulated CPU load percentage reported by the server.",
			},
			[]string{"server_id", "algorithm"},
		),
		requestErrors: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: "lb_request_errors_total",
				Help: "Total number of failed requests (proxy errors, no server, timeouts).",
			},
			[]string{"algorithm", "reason"},
		),
		serverSelectionTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: "lb_server_selection_total",
				Help: "How many times each server was selected for routing.",
			},
			[]string{"server_id", "algorithm"},
		),
	}
}
