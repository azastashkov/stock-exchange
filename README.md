# Stock Exchange

A reference low-latency stock exchange in **Java 21 + Spring Boot + Groovy Gradle**, with hand-rolled FIX 4.4 gateway, mmap-based event-sourced IPC, and a hand-rolled 5-node Raft cluster for high availability.

## Headline result

End-to-end load test against the full 5-node Docker Compose cluster (1 leader + 1 warm follower + 3 passive replicas, all communicating over Raft):

| Metric    | Value      |
|-----------|-----------:|
| p50       |  **2.01 ms** |
| p90       |  **3.00 ms** |
| **p99**   |  **3.98 ms** |
| p99.9     |  **4.92 ms** |
| p99.99    |  6.14 ms |
| max       |  6.39 ms |
| sustained orders processed | 90,105 (across 60 s) |
| trades generated           | 229,881 |

`PASS (p99 3.98 ms < 5.00 ms)`

## Quickstart

Prerequisites: **Docker** + **Docker Compose v2**, JDK 21 (only for unit tests / local boot, not required for compose).

```sh
# Build everything (compiles 6 modules, runs 120+ unit & integration tests)
./gradlew build

# Bring up the cluster: 5 exchange nodes + Prometheus + Grafana
docker compose up -d

# Wait until all 5 nodes are reported healthy
docker compose ps

# Browse Grafana: http://localhost:3000  (admin / admin, anonymous viewer enabled)
# The "Stock Exchange" dashboard is auto-provisioned under the "Exchange" folder.

# Drive a 60-second load test at 10k orders/sec target
docker compose --profile test up load-client

# Tear down (preserves named volumes — add -v to wipe)
docker compose down
```

## Architecture

The full architecture diagram is at [`docs/architecture.drawio`](docs/architecture.drawio) — open it in [app.diagrams.net](https://app.diagrams.net/) (or VS Code with the Draw.io extension) and export to PNG/SVG. The text version is below.

### Per-node trading flow

```
                   FIX/TCP from clients
                          │
                ┌─────────▼─────────┐
                │  Client Gateway   │  ← FIX 4.4 session, rate limit, validation
                │  (gateway loop)   │
                └─────────┬─────────┘
                          │ inbound mmap ring
                ┌─────────▼─────────┐
                │   Order Manager   │  ← lifecycle, ClOrdID idempotency
                └─────────┬─────────┘
                          │ om-out mmap ring
                ┌─────────▼─────────┐
                │ Aggregated Risk   │  ← per-account exposure, fat-finger
                └─────────┬─────────┘
                          │ risk-out mmap ring
                ┌─────────▼─────────┐
                │ Sequencer + MatE  │  ← single writer to canonical event log
                └─────────┬─────────┘
                          │ mmap event log (Raft-replicated)
   ┌──────────────────────┼──────────────────────┐
┌──▼──────┐         ┌─────▼──────┐         ┌─────▼──────┐
│ Reporter│         │  Market    │         │  Position  │
│  loop   │         │  Data Pub  │         │   Keeper   │
└──┬──────┘         └─────┬──────┘         └────────────┘
   │ gateway-out ring     │ MD TCP
   ▼                      ▼
ExecutionReports → FIX  → MD subscribers
```

Each loop is a **single-threaded `ApplicationLoop`**. Loops communicate through fixed-size **mmap ring buffers** (single producer, single consumer, `VarHandle.acquire/release` cursors — no locks on the hot path). The canonical event log is also **mmap-backed** and serves both as the trading event stream *and* the Raft log.

### 5-node Raft cluster

```
       ┌──────────┐
       │  Node 2  │  ← HOT (leader, elected via warm-priority hint)
       └────┬─────┘
            │  AppendEntries (Raft RPC, length-prefixed binary)
   ┌────────┼────────┐──────────┐──────────┐
   ▼        ▼        ▼          ▼          ▼
┌──────┐ ┌──────┐ ┌──────┐  ┌──────┐  ┌──────┐
│ N 1  │ │ N 3  │ │ N 4  │  │ N 5  │  │ ...  │
└──────┘ └──────┘ └──────┘  └──────┘  └──────┘
 FOLLOWER  FOLLOWER  FOLLOWER  FOLLOWER
 (warm-pri = 2 → wins when tied)
```

* All 5 nodes run the identical JVM with the full trading pipeline.
* The matching engine accepts orders **only on the leader**. Followers redirect via FIX `BusinessMessageReject` text `"NOT LEADER, use node N"`; the load client honors this hint.
* The Raft log entries *are* event log slots — the same 256-byte slot holds `(term, sequence, payload)`. Slots are appended with status `WRITING` and committed (status `COMMITTED`) by the `CommitApplier` once a majority replicates them. Downstream consumers read only `COMMITTED` slots.
* Election timeout 150–300 ms, heartbeat 50 ms, warm node uses 100–200 ms so it is preferred when tied. Failover budget < 1 second.

### Networking

| Channel              | Transport                          | Format                       |
|----------------------|------------------------------------|------------------------------|
| Client ↔ Gateway     | TCP                                | FIX 4.4 hand-rolled          |
| Node ↔ Node (Raft)   | TCP, length-prefixed frames        | Compact binary RPC           |
| Market data          | TCP fan-out                        | Binary length-prefixed       |
| In-process IPC       | mmap rings (SPSC), 256-byte slots  | Fixed-size binary            |
| Metrics scrape       | HTTP `/actuator/prometheus`        | Prometheus text              |

## Modules

| Module                   | Contents |
|--------------------------|----------|
| `exchange-common`        | mmap event log, mmap ring, application loop, CPU affinity, time, primitives, domain types, command codecs |
| `exchange-engine`        | matching engine, order book (price-time priority), sequencer |
| `exchange-fix`           | hand-rolled FIX 4.4 codec + session + NIO server |
| `exchange-raft`          | hand-rolled Raft (election, log replication, NIO + in-process transports) |
| `exchange-app`           | Spring Boot harness; OM, risk, position, reporter, MD publisher, gateway integration, Raft wiring, observability |
| `load-client`            | standalone Java FIX load test client (HdrHistogram-based) |

## FIX message support

Subset of FIX 4.4 implemented in-house (no QuickFIX/J):

| Type | Purpose |
|------|---------|
| `A`  | Logon         |
| `0`  | Heartbeat     |
| `1`  | TestRequest   |
| `2`  | ResendRequest |
| `4`  | SequenceReset |
| `5`  | Logout        |
| `D`  | NewOrderSingle |
| `F`  | OrderCancelRequest |
| `G`  | OrderCancelReplaceRequest |
| `8`  | ExecutionReport (server → client) |
| `3`  | Reject |
| `j`  | BusinessMessageReject |

Limit and Market orders, Day TIF, BUY/SELL. Symbol whitelist from configuration (`exchange.symbols`). Per-session token-bucket rate limit. Sequence numbers persisted (throttled) per session under `data/fix/`.

## Observability

* Spring Boot Actuator + Micrometer Prometheus registry exposed at `:8080/actuator/prometheus`.
* Prometheus scrapes all 5 nodes every 5 s.
* Grafana dashboard auto-provisioned at `/d/exchange/stock-exchange`. Panels:

  1. Round-trip latency p50 / p99 / p99.9 (line)
  2. Round-trip latency heatmap
  3. Order throughput per node (stacked rate)
  4. Match throughput (trades/sec)
  5. Engine match latency p50 / p99
  6. Ring queue depths per ring
  7. Raft role per node + term
  8. Raft commit lag per follower
  9. JVM GC pause + heap
  10. Top-of-book per symbol
  11. FIX session count
  12. Total orders processed
  13. Event log sequence
  14. Backpressure / risk / dup rejects

## Performance & latency engineering

Choices that make p99 < 5 ms reachable on a JVM:

* **ZGC Generational** (`-XX:+UseZGC -XX:+ZGenerational`), `-XX:+AlwaysPreTouch`, fixed `-Xms == -Xmx`.
* **No allocations on the hot path** after warm-up: pre-allocated `Order` / `OrderEntry` pools, FastUtil primitive collections, thread-local direct `ByteBuffer` for codecs, mutable view objects for events and commands.
* **Single-threaded loops** with adaptive idle backoff (1024 spins → `parkNanos(1)`). One application thread per logical service, no shared mutable state on the hot path.
* **Memory ordering** via `VarHandle.setRelease` / `getAcquire` on slot statuses and ring cursors — no `synchronized` between producer and consumer of a ring.
* **Throttled FIX seq-num persist** (1 s OR session close) — naïvely per-message persist was the dominant outbound bottleneck in early measurements.
* **256-byte slots** sized for typical events (max 222 B payload) keep cache lines effective.
* **CPU pinning hook** via `net.openhft:affinity` — disabled by default (`exchange.cpu-pinning.enabled`); enable on Linux with appropriate `cpuset` for further tail trimming.

### What we don't do (out of scope)

* Stop / Stop-Limit / Iceberg / Hidden orders
* IOC / FOK time-in-force beyond Day
* UDP multicast for market data (TCP fan-out only)
* Authentication beyond FIX SenderCompID whitelist
* Snapshots in Raft (full log retention)
* Cross-data-center / multi-region
* Drop-copy stream
* Self-trade prevention
* RT-kernel / kernel-bypass NIC. We have the busy-poll, allocation discipline, and single-writer architecture, but not the OS-level guarantees for sub-1 ms tails.

## Failover test

```sh
# 1. Confirm the leader (look for role=2 i.e. LEADER):
for i in 1 2 3 4 5; do
  echo -n "node-$i: "
  curl -s http://localhost:$((8080+i))/actuator/prometheus | grep '^exchange_raft_role '
done

# 2. Kill the leader (here exchange-2 is leader by warm-priority):
docker compose stop exchange-2

# 3. Watch Grafana's "Raft role" panel — within ~1 s the warm node (node-2)
#    is gone and another node becomes LEADER. The load client (if running)
#    receives "use node N" in BusinessMessageReject and reconnects.

# 4. Restore:
docker compose start exchange-2
```

## Running unit & integration tests

```sh
./gradlew test          # all modules, ~120 tests
./gradlew check         # tests + style/compile warnings
```

Per-module focus:

```sh
./gradlew :modules:exchange-common:test
./gradlew :modules:exchange-engine:test
./gradlew :modules:exchange-fix:test
./gradlew :modules:exchange-raft:test
./gradlew :modules:exchange-app:test
./gradlew :modules:load-client:test
```

## Configuration

The compose file already wires every required env var. For local non-Docker runs:

| Env var       | Default                                                  | Notes |
|---------------|----------------------------------------------------------|-------|
| `NODE_ID`     | `1`                                                      | This node's id (1..5) |
| `WARM_NODE_ID`| `2`                                                      | Node id with shorter election timeout (preferred leader) |
| `PEERS`       | `1=localhost:9001,2=localhost:9002,...`                  | id=host:port of every node, including self |
| `RAFT_ENABLED`| `false`                                                  | Set `true` to engage cluster mode |
| `DATA_DIR`    | `./data`                                                 | Persistent state root |
| `FIX_PORT`    | `9100`                                                   | TCP port for FIX clients |
| `RAFT_PORT`   | `9001`                                                   | TCP port for Raft RPC |
| `MD_PORT`     | `9200`                                                   | TCP port for market-data subscribers |
| `CPU_PINNING` | `false`                                                  | Set `true` on Linux to bind hot loops to dedicated cores |

## Repository layout

```
.
├── README.md
├── settings.gradle, build.gradle, gradle.properties
├── gradlew(.bat), gradle/wrapper/
├── docker-compose.yml
├── docker/
│   ├── exchange/Dockerfile
│   └── load-client/Dockerfile
├── docs/
│   ├── architecture.drawio
│   └── architecture.png
├── monitoring/
│   ├── prometheus/prometheus.yml
│   └── grafana/{provisioning,dashboards}/...
└── modules/
    ├── exchange-common/   (mmap, ring, loop, domain, commands, events)
    ├── exchange-engine/   (matching engine, order book)
    ├── exchange-fix/      (FIX 4.4 codec + session + server)
    ├── exchange-raft/     (Raft node, RPC, transports)
    ├── exchange-app/      (Spring Boot, all services, wiring)
    └── load-client/       (Java load test client)
```

## License

This is a reference implementation. Use freely; no warranty.
