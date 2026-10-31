**Setup**

- windows/amd64, 16 CPUs, go1.26.4, raftkv dev
- 3-node in-process cluster, real gRPC over loopback, WAL fsynced on every write
- election timeout 300ms-600ms, heartbeat 100ms
- value size 128 B, 100 keys, 15s per scenario

**Throughput and latency**

| Workload | Clients | Ops/sec | p50 (ms) | p95 (ms) | p99 (ms) | max (ms) | Errors |
|---|---:|---:|---:|---:|---:|---:|---:|
| write, 1 client | 1 | 232 | 4.02 | 5.64 | 9.59 | 64.41 | 0 |
| write, 16 clients | 16 | 392 | 27.47 | 64.00 | 172.71 | 1662.69 | 0 |
| read (linearizable), 16 clients | 16 | 14499 | 1.00 | 2.79 | 4.70 | 73.93 | 0 |
| mixed 50/50, 16 clients | 16 | 608 | 22.25 | 55.13 | 81.54 | 197.03 | 0 |

**Failover** (leader crashed; time counted from the crash)

| Trial | New leader elected (ms) | First successful write (ms) |
|---|---:|---:|
| 1 | 416.1 | 488.0 |
| 2 | 480.5 | 713.6 |
| 3 | 525.3 | 573.5 |
| 4 | 369.7 | 605.5 |
| 5 | 423.1 | 614.0 |
| 6 | 1049.6 | 1232.3 |
| 7 | 533.7 | 568.9 |
| 8 | 414.7 | 469.4 |
| 9 | 352.5 | 465.4 |
| 10 | 487.2 | 736.7 |
| **median** | 423.1 | 573.5 |
| **max** | 1049.6 | 1232.3 |

