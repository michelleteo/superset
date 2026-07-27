<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Error orchestrator

Listens for incoming Superset errors, groups them into categories, and drives
each category through triage → remediation → risk check using Devin sessions.

The orchestrator owns the state machine; the handlers are pure functions that
call Devin and return a decision. Every state transition, priority update and
capacity limit lives in `orchestrator.py`.

```
                    ┌──────────────┐
POST /webhook/errors│ ingest queue │  normalize + hash (O(1) dedup)
────────────────────▶              ├──────────────┬───────────────────────────┐
                    └──────────────┘              │ known fingerprint         │
                                                  ▼                           │
                                        add occurrence to pool                │
                                        recompute priority                    │
                                                                              │
   unknown fingerprint ──▶ triage queue ──▶ [triage lane · N workers]         │
                                                  │                           │
                                    merge into pool │ new category            │
                                                  ▼                           ▼
                                        ╔══════════════════════════════════════╗
                                        ║ max-heap keyed on priority score     ║
                                        ║ (occurrences, users, recency)        ║
                                        ║ lazy deletion on priority change     ║
                                        ╚══════════════════════════════════════╝
                                                  │ pop highest priority
                                                  ▼
                                     [remediation lane · M workers]
                                        │                      │
                          could_not_reproduce            fix_proposed
                                                                │
                                                                ▼
                                     [risk check lane · K workers, K ≤ M]
                                        │                      │
                                 awaiting_review          auto_merged
```

States: `new → triaged → reproducing → could_not_reproduce | fix_proposed →
awaiting_review | auto_merged`. Illegal transitions raise
`state_machine.InvalidTransitionError`.

## Design notes

**Normalization before hashing** (`fingerprint.py`). Memory addresses, UUIDs,
timestamps, emails, IPs, paths, quoted strings and bare numbers are templated
out; line numbers are kept. The hash covers the call sequence of function
names (vendor frames dropped) plus the exception type — the templated message
is only part of the hash when there is no traceback to go on, because
interpolated identifiers (`dataset sales` vs `dataset marketing`) cannot be
templated reliably.

**Triage only runs when the hash misses.** Identical traces dedup in O(1)
through a dict; a session is spent only to answer the question the hash cannot
— "is this a genuinely new bug, or the same bug down a different code path?".
A merge is accepted only for a known `pool_id` at confidence ≥ 0.7, otherwise
the fingerprint becomes its own category.

**Lazy deletion in the heap** (`priority.py`). Priority changes on every new
occurrence, and `heapq` cannot re-key. Each change pushes a fresh entry
stamped with the pool's `priority_version`; `pop` discards entries whose
version no longer matches, so pops stay O(log n) amortized.

**Capacity is centrally enforced** (`lanes.py`). Each lane is a semaphore-
limited async loop with a fixed worker count, so the number of concurrent
Devin sessions per stage is bounded by configuration; the risk lane must be
smaller than the remediation lane (`OrchestratorConfig` rejects the reverse).
Remediation workers pull from the heap rather than a queue: when a session
finishes, the freed worker immediately takes the next highest-priority pool.

**Risk registry first, review session second** (`risk_registry.py`).
Deterministic rules (migrations, security surface, CI/release files,
dependency changes, secret-looking additions, blast radius, missing tests)
produce a tier. `HIGH` is a hard veto that goes straight to `awaiting_review`
without spending a session. Otherwise an independent Devin review must approve
with no concerns for auto-merge; anything else flags for a human. The tier
decision is logged in `Orchestrator.decisions`.

## Running it

```bash
pip install -r error_orchestrator/requirements.txt

# Dry run: scripted Devin client, no sessions spent, no API key needed.
python -m error_orchestrator --dry-run --port 8099

# Real run.
export DEVIN_API_KEY=...
export ERROR_ORCHESTRATOR_REPO=michelleteo/superset
export ERROR_ORCHESTRATOR_REMEDIATION_WORKERS=3
export ERROR_ORCHESTRATOR_RISK_CHECK_WORKERS=2
python -m error_orchestrator
```

Point Superset's MCP error webhook at it (see
`superset/mcp_service/webhook_logging.py`):

```bash
export MCP_ERROR_WEBHOOK_URL=http://orchestrator:8088/webhook/errors
export MCP_ERROR_WEBHOOK_HEADERS='{"X-Webhook-Token": "s3cret"}'
```

| Endpoint | Purpose |
| --- | --- |
| `POST /webhook/errors` | Accepts one payload or a batch; returns 202 immediately |
| `GET /healthz` | Liveness |
| `GET /stats` | Queue depths, lane counters, pool counts per state |
| `GET /pools?state=awaiting_review` | Categories, highest priority first |
| `GET /pools/{pool_id}` | One category with its transition history and diff |

`/stats` and `/pools` are the minimum needed to see the system working; the
full observability layer is intentionally left for a follow-up.

Auto-merge is a logged no-op unless `ERROR_ORCHESTRATOR_AUTO_MERGE_ENABLED=1`;
pass a `merge_callback` to `Orchestrator` to perform the actual merge.

## Tests

```bash
pytest error_orchestrator
```

The suite is self-contained: it never imports the Superset app and never talks
to Devin (`devin_client.ScriptedDevinClient` stands in).
