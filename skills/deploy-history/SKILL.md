---
name: deploy-history
description: Inspect deployment rollout history, revisions, and image changes via the Kubernetes API.
entrypoint: skills.deploy_history.src.deploy_history.create
metadata:
  rateLimit: 10
  maxOutputChars: 30000
---

# Deploy History

Inspect deployment rollouts, revisions, and image changes.

## When to use

Use this skill when you need to:
- Check recent deployment rollout history
- Compare image versions between revisions
- Correlate deployment changes with incident onset
- Identify which revision to rollback to

## Input

- `query`: Deployment name
- `namespace`: Target namespace
