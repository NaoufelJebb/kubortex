---
name: kube-query
description: Query Kubernetes resources such as pods, events, deployments, nodes, and replicasets in a read-only manner.
entrypoint: skills.kube_query.src.kube_query.create
metadata:
  rateLimit: 20
  maxOutputChars: 50000
---

# Kube Query

Read-only Kubernetes inspection skill for querying common cluster resources during investigations.

## When to use

Use this skill when you need to:
- List or inspect pods in a namespace
- Get recent events for a workload
- Check deployment status and replica counts
- Inspect node conditions
- List replicasets

## Input

- `query`: The resource type to query (pods, events, deployments, nodes, replicasets)
- `namespace`: Target namespace
- `parameters.name`: Optional specific resource name
- `parameters.label_selector`: Optional label selector
