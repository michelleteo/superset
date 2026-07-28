# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Configuration for the orchestrator process."""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from error_orchestrator.priority import PriorityWeights

ENV_PREFIX = "ERROR_ORCHESTRATOR_"


@dataclass
class OrchestratorConfig:
    repo: str = "michelleteo/superset"
    base_branch: str = "master"

    #: Lane capacities — the only place session concurrency is bounded.
    triage_workers: int = 4
    remediation_workers: int = 3
    #: Risk check is cheaper than remediation, so it runs a smaller pool.
    risk_check_workers: int = 2

    ingest_queue_size: int = 10_000
    #: Existing categories offered to the triage session as merge candidates.
    merge_candidate_limit: int = 20
    #: Auto-merge is a no-op dry run unless this is explicitly enabled.
    auto_merge_enabled: bool = False

    devin_api_key: str | None = None
    devin_api_base: str = "https://api.devin.ai/v1"
    devin_poll_interval: float = 10.0
    devin_session_timeout: float = 60 * 60.0

    host: str = "0.0.0.0"  # noqa: S104
    port: int = 8088
    #: Shared secret expected in the ``X-Webhook-Token`` header, when set.
    webhook_token: str | None = None

    weights: PriorityWeights = field(default_factory=PriorityWeights)

    def __post_init__(self) -> None:
        if self.risk_check_workers > self.remediation_workers:
            raise ValueError(
                "risk_check_workers must not exceed remediation_workers: "
                "the risk lane is the cheaper stage"
            )

    @classmethod
    def from_env(cls, environ: dict[str, str] | None = None) -> OrchestratorConfig:
        env = environ if environ is not None else dict(os.environ)

        def _get(name: str, default: str | None = None) -> str | None:
            return env.get(f"{ENV_PREFIX}{name}", default)

        def _int(name: str, default: int) -> int:
            raw = _get(name)
            try:
                return int(raw) if raw else default
            except ValueError:
                return default

        def _float(name: str, default: float) -> float:
            raw = _get(name)
            try:
                return float(raw) if raw else default
            except ValueError:
                return default

        def _bool(name: str, default: bool) -> bool:
            raw = _get(name)
            if raw is None:
                return default
            return raw.strip().lower() in ("1", "true", "yes", "on")

        return cls(
            repo=_get("REPO", "michelleteo/superset") or "michelleteo/superset",
            base_branch=_get("BASE_BRANCH", "master") or "master",
            triage_workers=_int("TRIAGE_WORKERS", 4),
            remediation_workers=_int("REMEDIATION_WORKERS", 3),
            risk_check_workers=_int("RISK_CHECK_WORKERS", 2),
            ingest_queue_size=_int("INGEST_QUEUE_SIZE", 10_000),
            merge_candidate_limit=_int("MERGE_CANDIDATE_LIMIT", 20),
            auto_merge_enabled=_bool("AUTO_MERGE_ENABLED", False),
            devin_api_key=env.get("DEVIN_API_KEY") or _get("DEVIN_API_KEY"),
            devin_api_base=_get("DEVIN_API_BASE", "https://api.devin.ai/v1")
            or "https://api.devin.ai/v1",
            devin_poll_interval=_float("DEVIN_POLL_INTERVAL", 10.0),
            devin_session_timeout=_float("DEVIN_SESSION_TIMEOUT", 3600.0),
            host=_get("HOST", "0.0.0.0") or "0.0.0.0",  # noqa: S104
            port=_int("PORT", 8088),
            webhook_token=_get("WEBHOOK_TOKEN"),
        )
