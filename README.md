# kubortex

Kubortex is my attempt at building an autonomous SRE assistant that watches a given k8s cluster,
tries to understand what’s going wrong, and (bravely) attempts to fix it — all powered by LLMs,
a k8s Operator, and a slightly overconfident agent with tools.

Under the hood, Kubortex is designed as an autonomous SRE platform that ingests cluster
signals (alerts, metrics, logs, and events), performs incident reasoning through LLM-driven
workflows, and executes remediations within defined policy constraints. A Kubernetes Operator
provides the control-plane backbone for declarative state management, lifecycle orchestration,
and reconciliation. A LangGraph-based ReAct agent enables adaptive investigations, leveraging
pluggable Skills (domain-specific integrations) and advisory Runbooks (codified incident strategies)
to drive decision-making.



> [!NOTE]
 >  Status: 🚧 WIP — Currently cooking. Ingredients may change mid-recipe.
