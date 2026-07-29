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
`state_machine.InvalidTransitionError`. `new` is transient — pool creation and
the triage transition happen in one call, so `/stats` always reports `new: 0`.

Malformed input is contained at the edge: a record that cannot be parsed is
logged and skipped rather than failing the batch around it, and non-finite
numerics (`line: 1e400`, `timestamp: nan`) fall back to defaults so they can
never poison a pool or the JSON responses.

## Design notes

**Normalization before hashing** (`fingerprint.py`). Memory addresses, UUIDs,
timestamps, emails, IPs, paths, quoted strings and bare numbers are templated
out; line numbers are kept. When the event carries a traceback, the hash covers
the call sequence of function names (vendor frames dropped) plus the exception
type, and deliberately excludes the message, because interpolated identifiers
(`dataset sales` vs `dataset marketing`) cannot be templated reliably. Events
with no traceback have no call sequence to rely on — one log site emits many
unrelated errors — so there the templated message joins the synthesized
`module:func:line` location in the hash.

**Triage only runs when the hash misses.** Identical traces dedup in O(1)
through a dict; a session is spent only to answer the question the hash cannot
— "is this a genuinely new bug, or the same bug down a different code path?".
When no categories exist yet there is nothing to merge into, so the first
fingerprint skips the session entirely. A merge is accepted only for a known
`pool_id` at confidence ≥ 0.7, otherwise the fingerprint becomes its own
category.

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

Auto-merge is a logged no-op unless `ERROR_ORCHESTRATOR_AUTO_MERGE_ENABLED=1`;
pass a `merge_callback` to `Orchestrator` to perform the actual merge.

## The demo

One command starts everything — error simulator, orchestrator, human review
queue and live dashboard — on a single port, with no Devin API key required:

```bash
pip install -r error_orchestrator/requirements.txt
python -m error_orchestrator.demo --port 8099 --rate 2 --speed 2
```

Open <http://localhost:8099/>.

### With Docker

```bash
# From the repository root.
docker build -f error_orchestrator/Dockerfile -t error-orchestrator-demo .
docker run --rm -p 8099:8099 error-orchestrator-demo --rate 3 --speed 2

# Or:
docker compose -f error_orchestrator/docker-compose.yml up --build
```

Both `-f` flags matter. Without them Docker picks up the repository root's own
`Dockerfile` and `docker-compose.yml`, which build **Superset** and refuse to
start with *"A Default SECRET_KEY was detected"*. This demo never imports
Superset and needs no `SECRET_KEY`.

Every argument is optional — `docker run --rm -p 8099:8099
error-orchestrator-demo` streams with the defaults below. Add `--idle` to boot
with an empty board and start the run from the browser instead.

For real Devin sessions, pass a key at run time; it is never built into the
image, so each person runs the image with **their own** key and their own
sessions:

```bash
docker run --rm -p 8099:8099 \
  -e DEVIN_API_KEY=... \
  -e ERROR_ORCHESTRATOR_REPO=<owner>/<repo> \
  error-orchestrator-demo --idle --live-devin --seeded-bugs --live-devin-budget 2
```

### Run setup, in the browser

Every flag below is also a field in the **Run setup** panel at the top of the
dashboard, so a demo needs no arguments at all:

- **Start run** applies the panel (traffic mix, session speed, worker counts,
  reviewers, auto-review, real Devin) and starts streaming. On an already
  running board it reads *Restart with these settings* and rebuilds the run.
- **Reset** stops the stream and returns to an empty board with the settings
  the process started with — the demo's "take it from the top".

Worker counts, session speed and the Devin client cannot be changed underneath
a running orchestrator, so Start rebuilds it; anything already on the board
belongs to the previous run and is dropped.

The `DEVIN_API_KEY` is deliberately *not* a field. It is read from the server's
environment only, and the panel shows just whether one is present — the key is
never accepted, echoed or stored by the HTTP surface. Without one, the real
Devin checkbox is disabled.

### What you are looking at

```
 simulator ──POST /webhook/errors──▶ orchestrator ──▶ human review queue
     │                                    │                    │
     └────────────── dashboard polls /api/live ────────────────┘
```

The simulator posts real webhook payloads over HTTP, so nothing about the
pipeline is faked for the demo: the same fingerprinting, deduplication, lanes,
risk registry and state machine run as in production. Only the Devin sessions
are stood in for (`DemoDevinClient`), unless you ask for real ones.

The stream mixes four kinds of traffic, so every path through the state machine
is exercised continuously:

| Traffic | What it proves |
| --- | --- |
| Exact repeats of a known error | O(1) hash dedup — occurrence count climbs, no session spent |
| A known bug down a new code path | Triage merges the variant into its existing category |
| A brand new error | Triage opens a new category and queues remediation |
| Mutated errors (endless supply) | The demo never runs out of new categories to work |

Remediation either proposes a fix or gives up (`could_not_reproduce`); the risk
check then auto-merges low-risk diffs and flags migrations, security and large
diffs for a human. **All three terminal states are assigned to a named human**
and stay on the board until someone clears them — auto-merged included, since a
merge still wants verifying.

The dashboard shows worker saturation per lane (`2/3` with a red bar at
capacity), queue depths, throughput per minute, the human backlog and load per
reviewer, plus a live activity feed of workers picking up and finishing work.
Controls let you inject a named scenario, burst five at once, change the error
rate or pause the stream.

**Click a worker lane** for its live work log: which pool each busy slot is on,
how long it has been there and what the lane has recently done. Clicking a
worker opens the pool it is remediating.

**Click any pool or queue item** for its ticket, which is what makes a terminal
state actionable rather than a counter:

| Ticket shows | Ticket lets a human |
| --- | --- |
| The proposed diff, coloured, plus whether a test came with it | Clear it with a resolution |
| Every occurrence merged into the category, refreshed live | Reassign it to another reviewer |
| Risk tier and the reasons a human was asked | Revert an auto-merge, which reopens it as `awaiting_review` |
| The full state history and the Devin session links | |

### Options

Every flag is optional, and each one is also a field in the Run setup panel.
They apply the same whether they come from `python -m error_orchestrator.demo`,
`docker run` (anything after the image name is passed straight through), or the
browser.

| Flag | Default | Purpose |
| --- | --- | --- |
| `--rate` | `1.5` | Simulated errors per second |
| `--speed` | `1.0` | Session speed multiplier; higher makes states change faster |
| `--duplicate-rate` / `--variant-rate` | `0.55` / `0.25` | Traffic mix |
| `--review-interval` | `12` | Seconds between simulated human clears |
| `--no-auto-review` | off | Leave every terminal item for a real human to clear in the UI |
| `--reviewers` | four names | Who the backlog is assigned to |
| `--seed` | none | Reproducible stream |
| `--live-devin` | off | Use the real Devin API |
| `--live-devin-budget` | `3` | Real sessions to spend before falling back to simulated ones (`0` = no limit) |
| `--live-devin-stages` | `remediate` | Which lanes may spend a real session |
| `--seeded-bugs` | off | Emit this repo's own reproducible defects instead of synthetic errors |
| `--idle` | off | Boot without streaming, so the run starts from the UI |

Worker counts come from flags or the environment, so you can watch the lanes
saturate (or just type `1` into *remediation workers* and press Start):

```bash
python -m error_orchestrator.demo --rate 4 --remediation-workers 1 --risk-check-workers 1
ERROR_ORCHESTRATOR_REMEDIATION_WORKERS=1 python -m error_orchestrator.demo --rate 4
```

### Real Devin sessions and real diffs

`--live-devin` swaps the simulated session client for the real API, so
remediation returns a diff Devin actually wrote against the repo:

```bash
export DEVIN_API_KEY=...
export ERROR_ORCHESTRATOR_REPO=michelleteo/superset_demo
python -m error_orchestrator.demo \
  --live-devin --seeded-bugs --rate 0.2 --live-devin-budget 2
```

Real sessions take minutes, which is at odds with a dashboard that has to keep
moving, so by default only the first `--live-devin-budget` remediations go to
the real API and the rest are simulated. That keeps genuine Devin-authored
diffs on screen without the board going quiet.

**Pair `--live-devin` with `--seeded-bugs`.** The default traffic is synthetic:
a real session clones the repo, fails to find the traceback and correctly
reports `could_not_reproduce`. `--seeded-bugs` emits the defects in
[`error_orchestrator/seeded/app.py`](seeded/app.py) instead — genuine bugs
whose tracebacks are captured by *running* them, each with a one-line repro
that a session can iterate against:

```bash
python -m error_orchestrator.seeded.reproduce seeded_datasource_none  # exit 1 until fixed
```

The repro command travels in the error message, so it reaches the remediation
prompt and the ticket. `seeded bugs` is also a checkbox in the setup panel.

The API key is never a setting: it is read from the environment only. If you
distribute the image, each person should pass **their own** key
(`docker run -e DEVIN_API_KEY=...`) so sessions bill to their account.

### Demo API

| Endpoint | Purpose |
| --- | --- |
| `GET /api/live?since=<seq>` | Everything the UI renders; `since` returns only new activity |
| `POST /api/simulator` | `{"running": false}` or `{"rate": 4}` |
| `POST /api/inject` | `{"scenario": "redis_timeout", "count": 5}` |
| `GET /api/pools/{id}` | One ticket: diff, merged instances, history, allowed actions |
| `POST /api/pools/{id}/clear` | Human clears a terminal item |
| `POST /api/pools/{id}/assign` | Reassign to another human |
| `POST /api/pools/{id}/revert` | Roll back an auto-merge and put it back in review |
| `GET /api/settings` | Current settings, whether the run is streaming, whether a Devin key is present |
| `POST /api/settings` | Apply the setup panel and (re)start the run |
| `POST /api/reset` | Stop, wipe the board, restore the starting settings |

## Tests

```bash
pytest error_orchestrator
```

The suite is self-contained: it never imports the Superset app and never talks
to Devin (`devin_client.ScriptedDevinClient` stands in).
