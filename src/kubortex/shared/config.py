"""Application-wide settings loaded from environment variables."""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class KubortexSettings(BaseSettings):
    """Central configuration for all Kubortex components.

    Values are read from environment variables prefixed with ``KUBORTEX_``.
    """

    model_config = SettingsConfigDict(env_prefix="KUBORTEX_", env_nested_delimiter="__")

    # -- Kubernetes ----------------------------------------------------------
    namespace: str = "kubortex-system"
    crd_group: str = "kubortex.io"
    crd_version: str = "v1alpha1"

    # -- LLM -----------------------------------------------------------------
    llm_provider: str = "openai"
    llm_model: str = "gpt-4o"
    llm_api_key: str = ""
    llm_timeout_seconds: int = 120
    llm_max_tokens: int = 4096

    # -- Prometheus / Loki ---------------------------------------------------
    prometheus_url: str = "http://prometheus.monitoring:9090"
    loki_url: str = "http://loki.monitoring:3100"

    # -- Slack ---------------------------------------------------------------
    slack_bot_token: str = ""
    slack_channel: str = "#sre-oncall"
    slack_escalation_channel: str = "#sre-escalations"

    # -- Payload store -------------------------------------------------------
    payload_store_path: str = "/data/payloads"
    payload_max_size_bytes: int = 1_048_576  # 1 MiB

    # -- Diagnostic learning -------------------------------------------------
    learning_store_path: str = "/data/learning"
    learning_min_samples: int = 5
    learning_decay_alpha: float = 0.3

    # -- Investigator --------------------------------------------------------
    investigator_max_iterations: int = 10
    investigator_timeout_seconds: int = 300
    investigator_checkpoint_path: str = "/data/checkpoints"

    # -- Skills & Runbooks ---------------------------------------------------
    skills_dir: str = "skills"
    runbooks_dir: str = "runbooks"

    # -- Context budget ------------------------------------------------------
    context_max_chars: int = 120_000
