---
name: promql
description: Execute PromQL queries against Prometheus for metric analysis during investigations.
entrypoint: skills.promql.src.promql.create
metadata:
  rateLimit: 20
  maxOutputChars: 50000
---

# PromQL

Execute PromQL queries for metric analysis.

## When to use

Use this skill when you need to:
- Check CPU, memory, or disk utilisation
- Analyse error rates or request latencies
- Correlate metric changes with incident timing
- Verify post-remediation improvement

## Input

- `query`: The PromQL expression to evaluate
- `parameters.start`: Optional start time (ISO 8601)
- `parameters.end`: Optional end time (ISO 8601)
- `parameters.step`: Optional step duration (e.g. "60s")
