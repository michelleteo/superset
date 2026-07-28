---
name: testing-error-orchestrator
description: How to run and adversarially test the standalone error_orchestrator service (HTTP + asyncio error triage/remediation orchestrator) without Superset or a Devin API key.
---

# Testing `error_orchestrator`

Standalone asyncio service in `error_orchestrator/`. No Superset app, no database, no
Devin credentials required. There is no UI — test it with HTTP requests and an
in-process harness, and do **not** record a screen video for it.

## Running it

```bash
.venv/bin/python -m pytest error_orchestrator -q          # unit suite (fast, self-contained)
.venv/bin/python -m error_orchestrator --dry-run --port 8099   # scripted Devin client, no API key
```
Use absolute paths to the interpreter when backgrounding with `nohup` — a relative
`.venv/bin/python` breaks when the shell's cwd is not the repo root.

Config is env-driven with the `ERROR_ORCHESTRATOR_` prefix (see `config.py`):
`TRIAGE_WORKERS`, `REMEDIATION_WORKERS`, `RISK_CHECK_WORKERS` (must be ≤ remediation,
otherwise startup raises), `INGEST_QUEUE_SIZE`, `WEBHOOK_TOKEN`, `AUTO_MERGE_ENABLED`,
`PORT`, `HOST`. Run one instance per config on its own port; they are independent and
hold all state in memory, so restart to get a clean slate.

Endpoints: `POST /webhook/errors` (single object or batch, 202), `GET /healthz`,
`GET /stats`, `GET /pools[?state=...]`, `GET /pools/{pool_id}`.

## Payload shape

Exactly what `superset/mcp_service/webhook_logging.py` emits:
`{timestamp, level, logger, message, module, func, line, traceback}` (+ optional
`user_id`). `level: INFO`/`DEBUG` is silently ignored, and records without `message`
are dropped, so an event that "disappears" is usually one of those two.

## How to drive the interesting behaviours

- **Same logical error, different instances**: vary memory addresses, UUIDs, ISO
  timestamps, quoted strings and `user_id`; keep the traceback's file/func/line
  sequence identical. The fingerprint is `exception type + frame call sequence`
  (line numbers included, `site-packages` frames dropped). Message text only enters
  the hash when there are **no** frames at all — and a frame is synthesized from
  `module`/`func`/`line` whenever the traceback is missing, so with real webhook
  payloads the message is effectively never hashed.
- **Distinct pools**: change the exception type, or a line number, or the frame path.
- **Waiting for the system to settle**: poll `/stats` until
  `queues.{ingest,triage,risk_check,heap,pending_triage}` are all 0 **and** every
  `lanes.*.in_flight` is 0, then re-check after ~300 ms (a lane can pick up work
  between samples). In-process, `await orchestrator.drain(timeout)` does this.
- **Risk tiers**: `simulation.py` picks `RISKY_DIFF` (touches
  `superset/migrations/…` → registry HIGH → `awaiting_review`, no review session) vs
  `SAFE_DIFF` (touches `tests/` → LOW → `auto_merged`) pseudo-randomly with
  `seed=7`, `risky_rate=0.3`, `reproduce_rate=0.75`. Post ~30 distinct fingerprints
  and you will get all three terminal states; find a specific one by fetching
  `/pools/{id}` and inspecting `diff`.
- **What `/stats` cannot tell you**: how many Devin **sessions** were spent,
  `decided_by` (`registry` vs `devin_review`), review session URLs, and heap pop
  order. For those, build the real object in-process and count prompts:

  ```python
  from error_orchestrator.orchestrator import Orchestrator
  from error_orchestrator.config import OrchestratorConfig
  from error_orchestrator.simulation import make_dry_run_client, classify_prompt
  client = make_dry_run_client(latency=0.05)      # latency is essential, see below
  orch = Orchestrator(config=OrchestratorConfig(...), devin=client)
  orch.start(); await orch.handle_event(event); await orch.drain(30)
  [classify_prompt(p) for p in client.prompts]    # -> triage / remediate / risk_check
  orch.decisions                                  # audit trail incl. decided_by
  ```
  Run it with `PYTHONPATH=<repo root>`; `python -m` works but a bare script does not.
- **Triage sessions can be skipped**: `triage_handler` returns `new_category` without
  calling Devin when there are no existing categories to merge into. Seed one
  unrelated category first if you want to assert on triage session counts.
- **Capacity limits need latency to be provable**: with the default zero-latency
  scripted client, handlers never overlap, so `lanes.*.max_in_flight` stays at 1 no
  matter how large the burst — a broken limit would look identical. Use the
  in-process harness with `make_dry_run_client(latency=0.05)` and assert
  `max_in_flight == configured workers` (saturated, not exceeded).

## Adversarial inputs worth keeping in any regression pass

Non-finite numerics are the sharp edge: `{"line": 1e400}` / `"nan"` / `"inf"` hit
`int(float(...))` in `models.py` and 500 the whole request (the entire batch is lost),
and `{"timestamp": 1e400}` is accepted but then makes `GET /pools` 500 forever because
Starlette's `JSONResponse` serializes with `allow_nan=False`. If these are fixed,
keep them as regression cases; if a similar endpoint 500s mysteriously, suspect a
non-finite float in the response. Also worth re-running: garbage/non-JSON bodies
(400), empty array, bare string, 500-record batch, `level: INFO`, wrong types
(`level` as list, `message` as int, `user_id` as dict), 200 KB unicode messages,
tiny `INGEST_QUEUE_SIZE` (drops land in `stats.queues.dropped_events`, request stays
fast), and `X-Webhook-Token` (auth is checked before JSON parsing; verify the token
never appears in `/stats`, `/pools` or the server log).

Shutdown: SIGTERM and SIGINT are both clean — grep the captured server log for
`Traceback`, `Task was destroyed`, `CancelledError`, `handler failed`, `source failed`
and expect zero of each.

## Devin secrets needed

None. `--dry-run` requires no credentials; only the real (untested) `HttpDevinClient`
path needs `DEVIN_API_KEY`.
