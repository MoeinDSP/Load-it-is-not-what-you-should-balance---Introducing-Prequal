# Prequal — NSDI '24 Replication

Implementation of **"Load is not what you should balance: Introducing Prequal"**
(Wydrowski et al., NSDI 2024).

## What this replicates

| Paper figure | What it shows | How to observe |
|---|---|---|
| Fig. 4 | RIF distribution Prequal vs WRR | Grafana → *RIF per Server* panels |
| Fig. 5 | Tail-latency reduction at peak | Grafana → *Request Latency* panel |
| Fig. 6 | Load-ramp: Prequal zero errors vs WRR spiralling errors | `scripts/compare.sh` + Grafana |
| §4 | HCL probe-pool mechanics | `pkg/loadbalancer/balancer.go` |

## Architecture

```
client traffic
      │
  ┌───▼────────┐   ┌─────────────────┐
  │ lb-prequal │   │ lb-weightedrr   │   ← same backends, different algorithm
  │  :8080     │   │  :8081          │
  └───┬──┬──┬──┘   └──┬──┬──┬───────┘
      │  │  │         │  │  │
  ┌───▼──┴──┴─────────▼──┴──┴──────────────┐
  │  server1 (CPU_LOAD=60, ~35ms latency)   │  ← contended (antagonist load)
  │  server2 (CPU_LOAD=60, ~35ms latency)   │  ← contended
  │  server3 (CPU_LOAD=0,   ~8ms latency)   │  ← clean machine
  └─────────────────────────────────────────┘
```

### Key design decisions vs the reference GitHub repo

| Issue in base repo | Fix in this repo |
|---|---|
| Client-local RIF only | Backend exposes `X-RIF` (server-local) header; probe reads it |
| QRIF threshold from 2 candidates only | Computed from **entire probe pool** as the paper specifies |
| No probe pool | Bounded `ProbePool` (size 16, 1s age-out, degradation removal) |
| WRR = basic round-robin | True EWMA-weighted selection updated periodically (trailing signal) |
| Sampling with replacement | Fixed to sampling **without** replacement (paper §4) |

## Quick start

```bash
# Build and start everything
./scripts/setup.sh

# Run Figure-6 load ramp (needs: go install github.com/rakyll/hey@latest)
./scripts/compare.sh --duration 60
```

## Expected results

Under the load ramp:

- **Below 100% allocation**: both algorithms show similar latency (~8ms p50)
- **At ~103% allocation**: WRR p99.9 spikes; Prequal p99.9 unchanged
- **At ~114%+**: WRR begins returning errors; Prequal returns zero errors
- **Grafana traffic panel**: Prequal routes ~95-100% to server3; WRR splits ~1/3 each

This matches paper Figure 6 and the core thesis: **the real goal of a load balancer
is not to balance load — it is to direct load where capacity is available.**

## Algorithm implementations

| Function | Algorithm | Paper section |
|---|---|---|
| `selectPrequal` | HCL with async probe pool | §4 |
| `selectWeightedRR` | EWMA-latency WRR | §2, §3 |
| `selectRoundRobin` | Unweighted round-robin | §5.2 |
| `selectRandom` | Uniform random | §5.2 |
| `selectLeastLoaded` | Client-local RIF LL | §5.2 |
| `selectLLPo2C` | LL with power-of-2-choices | §5.2 |

## Monitoring

| Service | URL | Credentials |
|---|---|---|
| Grafana | http://localhost:3001 | admin / admin |
| Prometheus | http://localhost:9090 | — |
| Prequal metrics | http://localhost:8080/metrics | — |
| WRR metrics | http://localhost:8081/metrics | — |

## Reference

Bartek Wydrowski, Robert Kleinberg, Stephen M. Rumble, Aaron Archer.
*Load is not what you should balance: Introducing Prequal.*
NSDI 2024. https://www.usenix.org/conference/nsdi24/presentation/wydrowski