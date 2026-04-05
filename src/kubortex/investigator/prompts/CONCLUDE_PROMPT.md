Based on all evidence gathered, produce your final investigation conclusion as JSON.

Return **only** the JSON object — no prose, no markdown fences.

Required schema:

```json
{
  "hypothesis": "string — specific, testable root-cause statement",
  "confidence": 0.0,
  "reasoning": "string — how the evidence supports the hypothesis",
  "evidence": [
    {
      "skill": "skill-name",
      "query": "query executed",
      "valueSummary": "what the skill returned",
      "interpretation": "what this means for the hypothesis"
    }
  ],
  "recommendedActions": [
    {
      "type": "restart-pod | scale-up | rollback-deployment | cordon-node | drain-node",
      "target": {
        "kind": "Pod | Deployment | Node",
        "namespace": "namespace",
        "name": "resource-name"
      },
      "parameters": {},
      "rationale": "why this action addresses the root cause"
    }
  ],
  "escalate": false,
  "escalationReason": null,
  "diagnosticPath": [
    {
      "skill": "skill-name",
      "query": "query executed",
      "wasUseful": true
    }
  ]
}
```

Set `escalate: true` and provide `escalationReason` if confidence < 0.60 or if human judgment
is required. Leave `recommendedActions` empty when escalating.
