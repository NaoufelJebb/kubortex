---
name: oom-kill
description: Investigate repeated OOMKilled containers and likely memory pressure causes.
metadata:
  match:
    categories: [resource-saturation]
    severities: [warning, high, critical]
    priority: 100
  suggestedActions:
    - type: restart-pod
      when: "Single pod affected and likely transient"
    - type: rollback-deployment
      when: "Recent deployment strongly correlates with onset"
  resolutionMetric:
    promql: 'sum(rate(container_oom_events_total{namespace="{{ .targetNamespace }}"}[5m]))'
    successThreshold: 0
    checkDelaySeconds: 60
---

# OOM Kill

Investigation runbook for repeated OOMKilled containers in a Kubernetes workload.

## When to use

Use this runbook when the incident suggests:
- repeated `OOMKilled` events
- rising memory pressure in a workload
- crash loops likely caused by memory exhaustion

## Investigation Strategy

1. Confirm which containers are being OOMKilled and on which nodes.
2. Correlate the onset with recent deployment changes.
3. Compare memory growth, restart patterns, and log evidence.
4. Distinguish memory leak, bad limits, and noisy-neighbour scenarios.

## Key Questions

- Is this a single container or multiple?
- When did the OOMKills start? Does it correlate with a deployment?
- Is memory usage trending up (leak) or spiking (burst)?
- Are limits set too low relative to working set?

## Evidence to Gather

- Pod events showing OOMKilled reason
- Memory usage metrics over time (container_memory_working_set_bytes)
- Recent deployment history for the affected workload
- Container restart counts and timing
- Node-level memory pressure indicators
