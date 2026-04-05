---
name: log-search
description: Search logs via LogQL queries against Loki for error patterns and anomalies.
entrypoint: skills.log_search.src.log_search.create
metadata:
  rateLimit: 15
  maxOutputChars: 50000
---

# Log Search

Search logs via LogQL for error patterns and anomalies.

## When to use

Use this skill when you need to:
- Search for error messages or stack traces
- Find log patterns around incident onset time
- Correlate log output with metric anomalies
- Check for OOM kill messages or panic traces

## Input

- `query`: LogQL expression
- `namespace`: Target namespace (used to scope the query)
- `parameters.start`: Optional start time (ISO 8601)
- `parameters.end`: Optional end time (ISO 8601)
- `parameters.limit`: Max log lines to return (default 100)
