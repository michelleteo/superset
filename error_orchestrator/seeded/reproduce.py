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

"""Reproduce one seeded defect: ``python -m ...seeded.reproduce <key>``.

Exits non-zero while the bug is present and zero once it is fixed, so it works
both as a human repro and as the check a remediation session iterates against.
"""

from __future__ import annotations

import argparse
import sys
import traceback

from error_orchestrator.seeded.bugs import SEEDED_BUGS, SEEDED_BUGS_BY_KEY


def main(argv: list[str] | None = None) -> int:
    """Run the named defect; report whether it still fails."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("key", choices=sorted(SEEDED_BUGS_BY_KEY))
    args = parser.parse_args(argv)
    bug = SEEDED_BUGS_BY_KEY[args.key]

    try:
        bug.trigger()
    except Exception:  # pylint: disable=broad-except
        traceback.print_exc()
        print(f"\nSTILL BROKEN: {bug.key} — {bug.title}", file=sys.stderr)
        return 1
    print(f"FIXED: {bug.key} no longer raises")
    return 0


if __name__ == "__main__":  # pragma: no cover - module entry point
    if len(sys.argv) == 1:
        print("seeded bugs:", ", ".join(bug.key for bug in SEEDED_BUGS))
    raise SystemExit(main())
