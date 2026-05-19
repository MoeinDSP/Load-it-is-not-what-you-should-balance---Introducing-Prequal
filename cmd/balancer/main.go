// cmd/balancer/main.go — entry point for the Prequal load balancer.
//
// Reads configuration from environment variables so the same binary can be
// run as either the Prequal or WRR instance in docker-compose.
//
// Environment variables:
//
//	LB_PORT        listening port (default "8080")
//	LB_ALGORITHM   "prequal" | "weightedrr" | "roundrobin" | "random" |
//	                "leastloaded" | "ll-po2c"  (default "prequal")
//	LB_SERVERS     comma-separated "id=host:port" pairs
//	               e.g. "server1=server1:80,server2=server2:80,server3=server3:80"
//	LB_QRIF        QRIF quantile threshold (default "0.84")
//	LB_PROBE_RATE  rprobe: probes per query (default "2")
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/prequal/loadbalancer/pkg/loadbalancer"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// ── Configuration from environment ─────────────────────────────────────
	port := envOr("LB_PORT", "8080")
	alg := loadbalancer.Algorithm(envOr("LB_ALGORITHM", "prequal"))
	serversRaw := envOr("LB_SERVERS", "server1=server1:80,server2=server2:80,server3=server3:80")
	qrif := envFloat("LB_QRIF", 0.84)
	probeRate := envFloat("LB_PROBE_RATE", 2.0)
	weightInterval := envDuration("LB_WEIGHT_INTERVAL", 3*time.Second)

	// Use a dedicated Prometheus registry per instance so two LB instances
	// can co-exist in the same process during testing without label conflicts.
	reg := prometheus.NewRegistry()
	metrics := loadbalancer.NewMetrics(reg)

	cfg := &loadbalancer.Config{
		ProbeInterval:        time.Second,
		ProbeTimeout:         2 * time.Second,
		HealthCheckPath:      "/health",
		ProbePoolSize:        16,
		ProbeAgeTimeout:      time.Second,
		ProbeRatePerQuery:    probeRate,
		ProbeRemoveRate:      1.0,
		DriftRate:            1.0,
		SelectionChoices:     2,
		Algorithm:            alg,
		QRIF:                 qrif,
		WeightUpdateInterval: weightInterval,
		EWMAAlpha:            0.15,
	}

	lb := loadbalancer.NewLoadBalancer(cfg, logger, metrics)

	// ── Register backend servers ────────────────────────────────────────────
	for _, pair := range strings.Split(serversRaw, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) != 2 {
			logger.Warn("bad server spec, skipping", slog.String("spec", pair))
			continue
		}
		lb.AddServer(&loadbalancer.Server{
			ID:        strings.TrimSpace(parts[0]),
			Address:   strings.TrimSpace(parts[1]),
			IsHealthy: true,
		})
		logger.Info("registered server",
			slog.String("id", parts[0]),
			slog.String("addr", parts[1]))
	}

	lb.StartBackground()
	logger.Info("load balancer started",
		slog.String("algorithm", string(alg)),
		slog.Float64("qrif", qrif))

	// ── HTTP mux ────────────────────────────────────────────────────────────
	mux := http.NewServeMux()
	mux.Handle("/", lb)
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", lb.Healthz)
	mux.HandleFunc("/info", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		servers := lb.Servers()
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"algorithm":%q,"servers":[`, string(alg))
		for i, s := range servers {
			if i > 0 {
				fmt.Fprint(w, ",")
			}
			fmt.Fprintf(w, `{"id":%q,"addr":%q,"healthy":%v}`,
				s.ID, s.Address, s.IsHealthy)
		}
		fmt.Fprint(w, `]}`)
	})

	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// ── Graceful shutdown ───────────────────────────────────────────────────
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		logger.Info("shutting down")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}()

	logger.Info("listening", slog.String("port", port))
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("server error", slog.Any("err", err))
		os.Exit(1)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
