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

"""The knobs a demo run is configured with, from the CLI or from the UI.

Every setting here is settable both ways: the command line parses into a
:class:`DemoSettings`, and so does the dashboard's setup panel, so a run
started from a browser is the same run as one started from Docker arguments.

The Devin API key is deliberately *not* one of these settings. It is read from
the environment only, and never accepted, echoed or stored by the HTTP surface.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Any, Mapping, Sequence

from error_orchestrator.review import DEFAULT_REVIEWERS

STAGES = ("triage", "remediate", "risk_check")


class SettingsError(ValueError):
    """A setting was missing, malformed or out of range."""


def _number(payload: Mapping[str, Any], key: str, current: float) -> float:
    if key not in payload or payload[key] is None:
        return current
    try:
        return float(payload[key])
    except (TypeError, ValueError) as error:
        raise SettingsError(f"{key} must be a number") from error


def _whole(payload: Mapping[str, Any], key: str, current: int) -> int:
    if key not in payload or payload[key] is None:
        return current
    try:
        return int(payload[key])
    except (TypeError, ValueError) as error:
        raise SettingsError(f"{key} must be a whole number") from error


def _flag(payload: Mapping[str, Any], key: str, current: bool) -> bool:
    if key not in payload or payload[key] is None:
        return current
    return bool(payload[key])


def _names(
    payload: Mapping[str, Any],
    key: str,
    current: tuple[str, ...],
    allow_empty: bool = False,
) -> tuple[str, ...]:
    if key not in payload or payload[key] is None:
        return current
    raw = payload[key]
    values = raw.split(",") if isinstance(raw, str) else raw
    if not isinstance(values, Sequence) or isinstance(values, (bytes, bytearray)):
        raise SettingsError(f"{key} must be a list of names")
    names = tuple(str(value).strip() for value in values if str(value).strip())
    if not names and not allow_empty:
        raise SettingsError(f"{key} must not be empty")
    return names


@dataclass
class DemoSettings:
    """Everything a demo run is parameterised by."""

    #: Traffic.
    rate: float = 1.5
    duplicate_rate: float = 0.55
    variant_rate: float = 0.25
    seed: int | None = None

    #: Pipeline.
    speed: float = 1.0
    triage_workers: int = 4
    remediation_workers: int = 3
    risk_check_workers: int = 2

    #: Humans.
    reviewers: tuple[str, ...] = field(default_factory=lambda: tuple(DEFAULT_REVIEWERS))
    auto_review: bool = True
    review_interval: float = 12.0

    #: Real Devin sessions. The key itself comes from the environment.
    live_devin: bool = False
    live_devin_budget: int = 3
    live_devin_stages: tuple[str, ...] = ("remediate",)
    #: Emit the repository's own seeded defects instead of synthetic ones, so
    #: a real session has something it can actually reproduce and patch.
    seeded_bugs: bool = False

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        """Reject settings that would produce a broken or invisible run."""
        self._validate_traffic()
        self._validate_workers()
        self._validate_humans()
        self._validate_devin()

    def _validate_traffic(self) -> None:
        if self.rate <= 0:
            raise SettingsError("rate must be positive")
        if self.speed <= 0:
            raise SettingsError("speed must be positive")
        for name in ("duplicate_rate", "variant_rate"):
            value = getattr(self, name)
            if not 0.0 <= value <= 1.0:
                raise SettingsError(f"{name} must be between 0 and 1")
        if self.duplicate_rate + self.variant_rate > 1.0:
            raise SettingsError("duplicate_rate + variant_rate must not exceed 1")

    def _validate_workers(self) -> None:
        for name in ("triage_workers", "remediation_workers", "risk_check_workers"):
            if getattr(self, name) < 1:
                raise SettingsError(f"{name} must be at least 1")
        if self.risk_check_workers > self.remediation_workers:
            raise SettingsError(
                "risk_check_workers must not exceed remediation_workers"
            )

    def _validate_humans(self) -> None:
        if self.review_interval <= 0:
            raise SettingsError("review_interval must be positive")

    def _validate_devin(self) -> None:
        if self.live_devin_budget < 0:
            raise SettingsError("live_devin_budget must not be negative")
        unknown = set(self.live_devin_stages) - set(STAGES)
        if unknown:
            raise SettingsError(f"unknown stages: {', '.join(sorted(unknown))}")
        if self.live_devin and not self.live_devin_stages:
            raise SettingsError("live_devin needs at least one stage")

    def merged(self, payload: Mapping[str, Any]) -> DemoSettings:
        """A validated copy with whatever the caller supplied applied on top."""
        seed_given = "seed" in payload and payload["seed"] not in (None, "")
        updated = replace(
            self,
            rate=_number(payload, "rate", self.rate),
            duplicate_rate=_number(payload, "duplicate_rate", self.duplicate_rate),
            variant_rate=_number(payload, "variant_rate", self.variant_rate),
            seed=_whole(payload, "seed", self.seed or 0) if seed_given else None,
            speed=_number(payload, "speed", self.speed),
            triage_workers=_whole(payload, "triage_workers", self.triage_workers),
            remediation_workers=_whole(
                payload, "remediation_workers", self.remediation_workers
            ),
            risk_check_workers=_whole(
                payload, "risk_check_workers", self.risk_check_workers
            ),
            reviewers=_names(payload, "reviewers", self.reviewers),
            auto_review=_flag(payload, "auto_review", self.auto_review),
            review_interval=_number(payload, "review_interval", self.review_interval),
            live_devin=_flag(payload, "live_devin", self.live_devin),
            live_devin_budget=_whole(
                payload, "live_devin_budget", self.live_devin_budget
            ),
            # An empty stage selection is only a problem for a live run, and
            # ``_validate_devin`` is the one that says so.
            live_devin_stages=_names(
                payload,
                "live_devin_stages",
                self.live_devin_stages,
                allow_empty=True,
            ),
            seeded_bugs=_flag(payload, "seeded_bugs", self.seeded_bugs),
        )
        updated.validate()
        return updated

    def as_dict(self) -> dict[str, Any]:
        return {
            "rate": self.rate,
            "duplicate_rate": self.duplicate_rate,
            "variant_rate": self.variant_rate,
            "seed": self.seed,
            "speed": self.speed,
            "triage_workers": self.triage_workers,
            "remediation_workers": self.remediation_workers,
            "risk_check_workers": self.risk_check_workers,
            "reviewers": list(self.reviewers),
            "auto_review": self.auto_review,
            "review_interval": self.review_interval,
            "live_devin": self.live_devin,
            "live_devin_budget": self.live_devin_budget,
            "live_devin_stages": list(self.live_devin_stages),
            "seeded_bugs": self.seeded_bugs,
        }


__all__ = ["DemoSettings", "SettingsError", "STAGES"]
