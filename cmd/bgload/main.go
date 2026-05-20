// cmd/bgload/main.go — antagonist background load generator.
//
// Simulates the "other VMs on the same physical host" from the paper (§2,
// Figure 2).  Sends a steady stream of requests directly to target backends
// (bypassing load balancers) so the contention shows up in server-local RIF
// (visible to Prequal probes via X-RIF) but NOT in the LBs' own observed
// latency (so WRR doesn't know about it until its own requests start failing).
//
// Environment variables:
//
//	BG_TARGETS   comma-separated host:port pairs (default "server1:80,server2:80")
//	BG_RATE      requests per second per target (default "15")
package main

import (
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	targetsRaw := os.Getenv("BG_TARGETS")
	if targetsRaw == "" {
		targetsRaw = "server1:80,server2:80"
	}
	targets := strings.Split(targetsRaw, ",")

	rate := 15.0
	if v := os.Getenv("BG_RATE"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
			rate = f
		}
	}

	interval := time.Duration(float64(time.Second) / rate)
	log.Printf("[bgload] targets=%v rate=%.1f/s interval=%s", targets, rate, interval)

	client := &http.Client{Timeout: 5 * time.Second}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		for _, t := range targets {
			go func(addr string) {
				resp, err := client.Get("http://" + addr + "/")
				if err == nil {
					resp.Body.Close()
				}
			}(strings.TrimSpace(t))
		}
	}
}
