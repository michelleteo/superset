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

"""Run the orchestrator: ``python -m error_orchestrator``.

``--dry-run`` swaps the Devin client for a scripted double, which is handy for
exercising the state machine without spending sessions.
"""

from __future__ import annotations

import argparse
import logging
import sys

import uvicorn

from error_orchestrator.config import OrchestratorConfig
from error_orchestrator.devin_client import DevinClient
from error_orchestrator.ingest import create_app
from error_orchestrator.orchestrator import Orchestrator
from error_orchestrator.simulation import make_dry_run_client

logger = logging.getLogger("error_orchestrator")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="error_orchestrator")
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="use a scripted Devin client instead of the real API",
    )
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    try:
        config = OrchestratorConfig.from_env()
    except ValueError as error:
        logger.error("invalid configuration: %s", error)
        return 2
    if args.host:
        config.host = args.host
    if args.port:
        config.port = args.port

    devin: DevinClient | None = make_dry_run_client() if args.dry_run else None
    if devin is None and not config.devin_api_key:
        logger.error("DEVIN_API_KEY is not set; start with --dry-run to test locally")
        return 2

    app = create_app(Orchestrator(config=config, devin=devin))
    uvicorn.run(
        app, host=config.host, port=config.port, log_level=args.log_level.lower()
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
