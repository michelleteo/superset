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

"""Auto-remediation orchestrator for Superset errors.

The orchestrator consumes error events (e.g. the payloads emitted by
``superset.mcp_service.webhook_logging``), groups them into categories, and
drives each category through a state machine backed by three Devin-powered
handler lanes: triage, remediation and risk check.
"""

from error_orchestrator.config import OrchestratorConfig
from error_orchestrator.models import ErrorEvent, ErrorPool, PoolState
from error_orchestrator.orchestrator import Orchestrator

__all__ = [
    "ErrorEvent",
    "ErrorPool",
    "Orchestrator",
    "OrchestratorConfig",
    "PoolState",
]
