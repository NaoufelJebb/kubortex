"""Application-wide settings loaded from environment variables."""

from __future__ import annotations

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class SharedSettings(BaseSettings):
    """Settings common to all Kubortex components.

    Loaded from env vars prefixed with ``KUBORTEX_``.
    """

    model_config = SettingsConfigDict(env_prefix="KUBORTEX_", populate_by_name=True)

    # -- Kubernetes ----------------------------------------------------------
    namespace: str = "kubortex-system"
    crd_group: str = "kubortex.io"
    crd_version: str = "v1alpha1"

    # -- Observability -------------------------------------------------------
    log_level: str = "info"


class EdgeSettings(SharedSettings):
    """Settings for the kubortex-edge component.

    Env vars (prefix: ``KUBORTEX_``):
    - ``KUBORTEX_HOST``, ``KUBORTEX_PORT``
    - ``KUBORTEX_SLACK_BOT_TOKEN``, ``KUBORTEX_SLACK_CHANNEL``,
      ``KUBORTEX_SLACK_ESCALATION_CHANNEL``
    """

    # -- HTTP server ---------------------------------------------------------
    host: str = "0.0.0.0"
    port: int = 8080

    # -- Slack ---------------------------------------------------------------
    slack_bot_token: str = ""
    slack_channel: str = "#sre-oncall"
    slack_escalation_channel: str = "#sre-escalations"

    # -- Signal correlation --------------------------------------------------
    correlation_window_seconds: int = 300


class OperatorSettings(SharedSettings):
    """Settings for the kubortex-operator component.

    Env vars (prefix: ``KUBORTEX_``):
    - ``KUBORTEX_LIVENESS_PORT``
    - ``KUBORTEX_INVESTIGATION_TIMEOUT_SECONDS``, ``KUBORTEX_INVESTIGATION_MAX_ITERATIONS``
    - ``KUBORTEX_ESCALATION_DEADLINE_MINUTES``, ``KUBORTEX_ESCALATION_CHECK_INTERVAL``
    - ``KUBORTEX_APPROVAL_TIMEOUT_MINUTES``, ``KUBORTEX_APPROVAL_CHECK_INTERVAL``
    - ``KUBORTEX_BUDGET_RESET_INTERVAL``, ``KUBORTEX_MAX_RETRIES``
    """

    # -- HTTP server (liveness probe) ----------------------------------------
    liveness_host: str = "0.0.0.0"
    liveness_port: int = 8080

    # -- Investigation defaults (written into Investigation CRD spec) ---------
    investigation_timeout_seconds: int = 300
    investigation_max_iterations: int = 10
    escalation_deadline_minutes: int = 15

    # -- Timer intervals (kopf decorator arguments, evaluated at import time) -
    escalation_check_interval: int = 30
    approval_check_interval: int = 30
    budget_reset_interval: int = 60

    # -- Approval -------------------------------------------------------------
    approval_timeout_minutes: int = 30

    # -- Retry ----------------------------------------------------------------
    max_retries: int = 2


class InvestigatorSettings(SharedSettings):
    """Settings for the kubortex-investigator component.

    Env vars (prefix: ``KUBORTEX_``):
    - ``KUBORTEX_POD_NAME`` or ``POD_NAME`` (Kubernetes downward API)
    - ``KUBORTEX_POLL_INTERVAL_SECONDS``
    - ``KUBORTEX_LLM_PROVIDER``, ``KUBORTEX_LLM_MODEL``, ``KUBORTEX_LLM_API_KEY``
    - ``KUBORTEX_LLM_TIMEOUT_SECONDS``, ``KUBORTEX_LLM_MAX_TOKENS``
    - ``KUBORTEX_PROMETHEUS_URL``, ``KUBORTEX_LOKI_URL``
    - ``KUBORTEX_PAYLOAD_STORE_PATH``, ``KUBORTEX_PAYLOAD_MAX_SIZE_BYTES``
    - ``KUBORTEX_LEARNING_STORE_PATH``, ``KUBORTEX_LEARNING_MIN_SAMPLES``,
      ``KUBORTEX_LEARNING_DECAY_ALPHA``
    - ``KUBORTEX_MAX_ITERATIONS``, ``KUBORTEX_TIMEOUT_SECONDS``,
      ``KUBORTEX_CHECKPOINT_PATH``
    - ``KUBORTEX_SKILLS_DIR``, ``KUBORTEX_RUNBOOKS_DIR``
    - ``KUBORTEX_CONTEXT_MAX_CHARS``
    """

    # -- Worker identity (Kubernetes downward API or KUBORTEX_ prefix) -------
    pod_name: str = Field(
        default="investigator-0",
        validation_alias=AliasChoices("KUBORTEX_POD_NAME", "POD_NAME"),
    )
    poll_interval_seconds: int = 5

    # -- LLM -----------------------------------------------------------------
    llm_provider: str = "openai"
    llm_model: str = "gpt-4o"
    llm_api_key: str = ""
    llm_timeout_seconds: int = 120
    llm_max_tokens: int = 4096

    # -- Observability data sources ------------------------------------------
    prometheus_url: str = "http://prometheus.monitoring:9090"
    loki_url: str = "http://loki.monitoring:3100"

    # -- Payload store -------------------------------------------------------
    payload_store_path: str = "/data/payloads"
    payload_max_size_bytes: int = 1_048_576  # 1 MiB

    # -- Diagnostic learning -------------------------------------------------
    learning_store_path: str = "/data/learning"
    learning_min_samples: int = 5
    learning_decay_alpha: float = 0.3

    # -- Investigation graph -------------------------------------------------
    max_iterations: int = 10
    timeout_seconds: int = 300
    checkpoint_path: str = "/data/checkpoints"

    # -- Skills & Runbooks ---------------------------------------------------
    skills_dir: str = "skills"
    runbooks_dir: str = "runbooks"

    # -- Context budget ------------------------------------------------------
    context_max_chars: int = 120_000


class RemediatorSettings(SharedSettings):
    """Settings for the kubortex-remediator component.

    Env vars (prefix: ``KUBORTEX_``):
    - ``KUBORTEX_POD_NAME`` or ``POD_NAME`` (Kubernetes downward API)
    - ``KUBORTEX_POLL_INTERVAL_SECONDS``
    """

    # -- Worker identity (Kubernetes downward API or KUBORTEX_ prefix) -------
    pod_name: str = Field(
        default="remediator-0",
        validation_alias=AliasChoices("KUBORTEX_POD_NAME", "POD_NAME"),
    )
    poll_interval_seconds: int = 5
