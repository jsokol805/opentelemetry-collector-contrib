# E2E Test: Pod & Container Startup Duration Metrics

Quickly validate the following metrics on a real kind cluster:

| Metric | Description |
|--------|-------------|
| `k8s.pod.startup_duration` | Total time from pod creation to Ready |
| `k8s.pod.scheduling_duration` | Time from creation to PodScheduled |
| `k8s.pod.initializing_duration` | Time from PodScheduled to Initialized |
| `k8s.pod.containers_ready_duration` | Time from Initialized to ContainersReady |
| `k8s.container.startup_duration` | Time from Initialized to container Running |

## Prerequisites

- [kind](https://kind.sigs.k8s.io/)
- Docker
- kubectl
- make (for building the collector image)

## Quick Start

```bash
# From the repo root:
cd receiver/k8sclusterreceiver/testdata/e2e/startup-metrics

# Full run: create cluster, build collector, deploy, tail logs
./run.sh

# Or step by step:
./run.sh setup    # create kind cluster + build + load image
./run.sh deploy   # apply K8s manifests
./run.sh logs     # tail collector logs (filtered for startup metrics)
./run.sh logs-raw # tail full collector logs (unfiltered)
./run.sh cleanup  # delete the kind cluster
```

## What It Does

1. Creates a kind cluster named `otelcol-startup-e2e`
2. Builds the `otelcontribcol` docker image from the repo root
3. Loads the image into kind
4. Deploys:
   - An OTel Collector with all startup duration metrics **enabled** and a `debug` exporter (logs metrics to stdout)
   - A 2-replica nginx deployment as a test workload
5. Tails the collector logs, filtering for duration metric names

## Interpreting Output

The `debug` exporter prints all collected metrics to the collector's stdout. The `logs` command filters for lines containing duration metric names. You'll see gauge data points with values in **seconds**:

```
k8s.pod.startup_duration  -> Gauge  Value: 3.5
k8s.pod.scheduling_duration -> Gauge  Value: 0.1
...
```

To see full unfiltered output: `./run.sh logs-raw`
