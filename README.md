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

# Error remediation demo

This fork of Apache Superset carries the **error orchestrator** in
[`error_orchestrator/`](error_orchestrator): a service that takes Superset
errors off a webhook, groups them into categories, and drives each category
through triage → remediation → risk check using Devin sessions, ending in an
auto-merge or a ticket assigned to a human.

This repository is a fork of [Apache Superset](https://github.com/apache/superset)
with the solution living in the same tree: the Superset source is untouched
apart from the errors being remediated, and the orchestrator sits beside it in
`error_orchestrator/`. The errors selected to drive the workflow, and what
remediation produced for each, are in [`ISSUES.md`](ISSUES.md).

This README is about running the demo of that workflow. For the orchestrator's
design (fingerprinting, the priority heap, lane capacity, the risk registry) see
[`error_orchestrator/README.md`](error_orchestrator/README.md). For Superset
itself, see [superset.apache.org](https://superset.apache.org).

Everything in the demo is real except the two things a laptop does not have:
Superset producing the errors, and (by default) the Devin sessions. A simulator
posts genuine webhook payloads over HTTP, so the same fingerprinting, dedup,
lanes, state machine and risk rules run as in production.

```
 simulator ──POST /webhook/errors──▶ orchestrator ──▶ human review queue
     │                                    │                    │
     └────────────── dashboard polls /api/live ────────────────┘
```

## 1. Start the app with Docker

Run from the repository root — the image needs the repo as its build context.
No Devin API key is needed for the simulated workflow.

```bash
docker compose -f error_orchestrator/docker-compose.yml up --build
```

Or without compose:

```bash
docker build -f error_orchestrator/Dockerfile -t error-orchestrator-demo .
docker run --rm -p 8099:8099 error-orchestrator-demo --rate 2 --speed 2
```

Everything (simulator, orchestrator, review queue, dashboard) runs in one
container on one port. Anything after the image name is passed to
`python -m error_orchestrator.demo`, so `--rate 3 --speed 2 --idle` etc. work as
`docker run` arguments; worker counts come from the environment
(`ERROR_ORCHESTRATOR_TRIAGE_WORKERS`, `..._REMEDIATION_WORKERS`,
`..._RISK_CHECK_WORKERS`, which must be ≤ remediation workers) or from flags.

Useful starting points:

```bash
# Boot with an empty board and drive the whole run from the browser.
docker run --rm -p 8099:8099 error-orchestrator-demo --idle

# Saturate the lanes: fast traffic, one worker per lane.
docker run --rm -p 8099:8099 error-orchestrator-demo \
  --rate 4 --remediation-workers 1 --risk-check-workers 1

# Nothing clears itself — every terminal item waits for you in the UI.
docker run --rm -p 8099:8099 error-orchestrator-demo --no-auto-review
```

Wait for `demo ready` in the logs, or check
`curl -fs http://localhost:8099/healthz`. `Ctrl-C` (or `docker compose down`)
stops it; all state is in memory, so a restart is a clean slate.

To run it without Docker:

```bash
pip install -r error_orchestrator/requirements.txt
python -m error_orchestrator.demo --port 8099 --rate 2 --speed 2
```

## 2. Load it as a web app

Open <http://localhost:8099/>. The dashboard polls `/api/live` and needs no
build step or login.

The board has four regions:

- **Lanes** — triage, remediation and risk check, each showing worker
  saturation (`2/3`, red bar at capacity), queue depth and throughput per
  minute.
- **Error pools** — every category, highest priority first, with state,
  occurrences, affected users, risk tier and the human it is assigned to.
- **Human queue** — the terminal backlog (`awaiting_review`,
  `auto_merged`, `could_not_reproduce`) and load per reviewer.
- **Activity** — a live feed of workers picking up and finishing work.

## 3. Operate it

### Drive the stream

The toolbar injects a named scenario, bursts five at once, changes the error
rate, or pauses the stream. The dropdown lists scenario names while board rows
are titled from the traceback, so an injection usually shows up as a climbing
occurrence count on a differently named row (or merged into a category triage
chose) rather than as a new row. Traffic is deliberately mixed so every path
through the state machine keeps firing:

| Traffic | What you should see |
| --- | --- |
| Exact repeat of a known error | Occurrence count climbs, no session spent (O(1) hash dedup) |
| Known bug down a new code path | Triage merges the variant into its existing category |
| Brand new error | Triage opens a category and queues remediation |
| Mutated errors | An endless supply, so the board never goes quiet |

### Inspect the work

- **Click a lane** for its live work log: which pool each busy slot is on and
  how long it has been there. Clicking a worker opens the pool it is on.
- **Click any pool or queue item** for its ticket: the proposed diff (coloured,
  and whether a test came with it), every occurrence merged into the category,
  the risk tier and why a human was asked, the full state history, and the Devin
  session links. Real sessions link out; simulated ones read `(simulated)`.
- From a ticket you can **clear** it with a resolution, **reassign** it, or
  **revert** an auto-merge — which reopens it as `awaiting_review`.

All three terminal states are assigned to a named human and stay on the board
until someone clears them, auto-merged included, since a merge still wants
verifying. With `--no-auto-review` (or by unchecking *auto-clear the backlog*)
that someone is you.

### Reconfigure the run from the browser

**Run setup** at the top of the page holds every flag as a field: errors/second,
session speed, duplicate and variant rate, worker counts per lane, seed,
reviewers, auto-review interval, real Devin sessions, seeded bugs, and the real
session budget and stages.

- **Start run** applies the panel and starts streaming. While the stream is
  running it reads *Restart with these settings*: worker counts, session speed
  and the Devin client cannot change underneath a live orchestrator, so the run is
  rebuilt and anything already on the board is dropped. Pausing flips the label
  back to *Start run* without emptying the board, but pressing it still rebuilds
  the run — use **Reset** if all you want is a clean board.
- **Reset** stops the stream and returns to an empty board with the settings the
  process started with — the demo's "take it from the top".

### Simulated sessions (the default)

`DemoDevinClient` stands in for Devin: sessions finish in seconds, remediation
either proposes a fix or reports `could_not_reproduce`, and the risk check
auto-merges low-risk diffs while flagging migrations, security surface and large
diffs for a human. Turn `--speed` up to make states change faster. This mode
needs no credentials and spends nothing.

### Live Devin sessions

`--live-devin` swaps in the real API, so remediation returns a diff Devin
actually wrote against the repo. Pair it with `--seeded-bugs`: the default
traffic is synthetic, so a real session clones the repo, cannot find the
traceback and correctly reports `could_not_reproduce`. Seeded bugs are the
genuine defects listed in [`ISSUES.md`](ISSUES.md) — some in Superset's own
source (`superset/examples/countries.py`, `superset/utils/class_utils.py`), the
rest in [`error_orchestrator/seeded/app.py`](error_orchestrator/seeded/app.py)
for chart paths that cannot run without booting Superset. Their tracebacks are
captured by *running* them, and each carries a one-line repro command that
reaches the remediation prompt and the ticket:

```bash
python -m error_orchestrator.seeded.reproduce seeded_datasource_none  # exit 1 until fixed
```

```bash
docker run --rm -p 8099:8099 \
  -e DEVIN_API_KEY \
  -e ERROR_ORCHESTRATOR_REPO=michelleteo/superset_demo \
  error-orchestrator-demo \
  --live-devin --seeded-bugs --rate 0.2 --live-devin-budget 2
```

Real sessions take minutes to tens of minutes, which is at odds with a dashboard
that has to keep moving, so only the first `--live-devin-budget` remediations
(default 3, `0` = no limit) go to the real API and the rest are simulated;
`--live-devin-stages` chooses which lanes may spend a real session (default
`remediate`) and applies whether or not the budget is capped. Keep the rate low
— you are watching a handful of real sessions, not a stream.

A real session holds its remediation worker for as long as it runs, and it only
returns an answer the orchestrator can act on once it posts a fenced JSON block.
A session that fixes the bug but reports it in prose is asked for the block and,
if it still says nothing machine-readable, is abandoned within a couple of
minutes so the worker goes back to the lane — the pool then reads
`could_not_reproduce`, with the reason and the session's URL on its ticket so you
can go and read what it actually did (see
[`devin_client.py`](error_orchestrator/devin_client.py)).

The API key is **never** a setting: it is read from the server's environment
only, and the panel shows only whether one is present. Without a key the *real
Devin sessions* checkbox is disabled, and `--live-devin` exits with
`--live-devin needs DEVIN_API_KEY`. If you hand the image to someone else, they
should pass their own key so sessions bill to their account.

## Wiring it to a real Superset

The orchestrator runs on its own (`python -m error_orchestrator --dry-run` for a
scripted Devin client); point Superset's MCP error webhook at it:

```bash
export MCP_ERROR_WEBHOOK_URL=http://orchestrator:8088/webhook/errors
export MCP_ERROR_WEBHOOK_HEADERS='{"X-Webhook-Token": "s3cret"}'
```

Auto-merge is a logged no-op unless `ERROR_ORCHESTRATOR_AUTO_MERGE_ENABLED=1`.

## HTTP surface

| Endpoint | Purpose |
| --- | --- |
| `POST /webhook/errors` | One payload or a batch; returns 202 immediately |
| `GET /healthz` | Liveness |
| `GET /stats` | Queue depths, lane counters, pool counts per state |
| `GET /pools?state=awaiting_review` | Categories, highest priority first |
| `GET /api/live?since=<seq>` | Everything the dashboard renders |
| `POST /api/simulator` | `{"running": false}` or `{"rate": 4}` |
| `POST /api/inject` | `{"scenario": "redis_timeout", "count": 5}` |
| `GET /api/settings` / `POST /api/settings` | Read or apply the setup panel |
| `POST /api/reset` | Stop, wipe the board, restore the starting settings |

Ticket actions live at `GET /api/pools/{id}` plus `POST /api/pools/{id}/clear`,
`/assign` and `/revert`.

## Tests

```bash
pytest error_orchestrator
```

Self-contained: never imports the Superset app, never talks to Devin.
