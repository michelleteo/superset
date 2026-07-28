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

"""Pure handler functions for the three lanes.

Handlers never touch orchestrator state: they take a request, call Devin, and
return a decision. The orchestrator applies that decision to the store and the
state machine, which keeps capacity limits and transitions in one place.
"""

from error_orchestrator.handlers.remediate import (
    remediate_handler,
    RemediationOutcome,
    RemediationRequest,
)
from error_orchestrator.handlers.risk_check import (
    ReviewOutcome,
    risk_check_handler,
    RiskCheckRequest,
    RiskDecision,
)
from error_orchestrator.handlers.triage import (
    triage_handler,
    TriageAction,
    TriageDecision,
    TriageRequest,
)

__all__ = [
    "RemediationOutcome",
    "RemediationRequest",
    "ReviewOutcome",
    "RiskCheckRequest",
    "RiskDecision",
    "TriageAction",
    "TriageDecision",
    "TriageRequest",
    "remediate_handler",
    "risk_check_handler",
    "triage_handler",
]
