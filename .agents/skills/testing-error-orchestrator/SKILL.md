---
name: testing-error-orchestrator
description: How to run and adversarially test the standalone error_orchestrator service (HTTP + asyncio error triage/remediation orchestrator) without Superset or a Devin API key.
---

# Testing `error_orchestrator`

Standalone asyncio service in `error_orchestrator/`. No Superset app, no database, no
Devin credentials required. The bare service (`python -m error_orchestrator`) has no UI
and is tested with HTTP requests plus an in-process harness. **There is also a demo
dashboard** (`python -m error_orchestrator.demo`, `error_orchestrator/static/dashboard.html`) —
when a change touches the dashboard or its `/api/settings` setup panel, drive it through the
browser and **do** record a screen video.

## Running it

```bash
.venv/bin/python -m pytest error_orchestrator -q          # unit suite (fast, self-contained)
.venv/bin/python -m error_orchestrator --dry-run --port 8099   # scripted Devin client, no API key
# demo + dashboard; --idle boots without traffic so the run starts from the UI's Start button
.venv/bin/python -m error_orchestrator.demo --idle --port 8099 --rate 3 --speed 3
```
Use absolute paths to the interpreter when backgrounding with `nohup` — a relative
`.venv/bin/python` breaks when the shell's cwd is not the repo root. The repo may have **no
`.venv` at all** and the system `python3` may be missing `httpx`; in that case a sibling
Superset checkout's venv (e.g. `/home/ubuntu/repos/superset/.venv/bin/python3`) usually has
`httpx`, `starlette` and `uvicorn` already. Runtime deps are in
`error_orchestrator/requirements.txt`.

Config is env-driven with the `ERROR_ORCHESTRATOR_` prefix (see `config.py`):
`TRIAGE_WORKERS`, `REMEDIATION_WORKERS`, `RISK_CHECK_WORKERS` (must be ≤ remediation,
otherwise startup raises), `INGEST_QUEUE_SIZE`, `WEBHOOK_TOKEN`, `AUTO_MERGE_ENABLED`,
`PORT`, `HOST`. Run one instance per config on its own port; they are independent and
hold all state in memory, so restart to get a clean slate.

Endpoints: `POST /webhook/errors` (single object or batch, 202), `GET /healthz`,
`GET /stats`, `GET /pools[?state=...]`, `GET /pools/{pool_id}`.

## Testing the demo dashboard / setup panel

`dashboard.py` adds `GET /api/settings` (full runtime status: `running`, `settings`, `defaults`,
`live_devin_available`, `live_sessions`), `POST /api/settings` (= the panel's **Start run**;
rebuilds the run, `400` + `{"error": ...}` on a `SettingsError`), `POST /api/reset` (= **Reset**;
re-applies `defaults` with the stream stopped), plus `/api/live`, `/api/inject`,
`/api/pools/{id}/clear|revert|assign`.

- Best evidence pattern: keep a second browser tab on `/api/settings` and reload it after each
  panel action — it proves the settings actually round-tripped instead of just looking applied.
- **Before/after in one screen**: `git worktree add /tmp/eo-master <base-sha>` and run the base
  build on another port. Two dashboards side by side is the strongest proof a panel bug is fixed.
- Panel errors render as small red text to the right of the *Reset* button (`#setup-error`), and
  are cleared on the next attempt. `#run-state` is the `stopped`/`running` pill in the header.
- **The Start button is not at a fixed x**: its label changes from `Start run` to
  `Restart with these settings` once a run is live, which pushes *Reset* right by ~70 px.
  Re-screenshot before clicking *Reset*, or you will restart the run by accident.
- Adding/removing a hint line in the panel shifts the checkbox rows vertically, so coordinates
  are not portable between the base build and the branch build. Zoom in and re-locate per build.
- To saturate a lane visibly: `errors / second` 8, `remediation workers` 1, `risk-check workers` 1
  (validation requires `risk_check_workers <= remediation_workers`). The lane card then reads
  `1/1` + `AT CAPACITY` with a growing queue.
- `speed 3` and `rate 3+` get all three terminal states (`auto_merged`, `awaiting_review`,
  `could not reproduce`) on screen within ~60 s.

### Testing anything near `live_devin` without spending money

Real sessions cost minutes and money, so never tick **real Devin sessions** on an instance that is
streaming traffic. Two safe substitutes:

- Boot a separate instance `--idle --live-devin --live-devin-budget 0 --live-devin-stages remediate`
  with `ERROR_ORCHESTRATOR_DEVIN_API_KEY=dummy` and
  `ERROR_ORCHESTRATOR_DEVIN_API_BASE=http://127.0.0.1:9/v1` (a closed port). `--idle` means no lane
  ever calls Devin, yet `GET /api/settings` still exposes `live_sessions`
  (`spent`/`budget`/`unlimited`/`stages`/`in_flight`) and the dashboard renders the real-session
  pill — enough to verify pill text and the configured stage filter. Confirm nothing was spent with
  `grep -c "spending real Devin session" <log>` → `0`.
- Note `live_sessions` is `null` unless `runtime.orchestrator.devin` is a `BudgetedDevinClient`, so
  a missing pill is itself a meaningful signal about which client got built.

With `DEVIN_API_KEY` present the panel's `real Devin sessions` checkbox is **enabled** and the pill
reads `real Devin ready · <repo>`; without it the checkbox is disabled and the pill says
`no DEVIN_API_KEY — simulated sessions only`. `runtime.apply` raises a `RuntimeError` → HTTP 409 if
`live_devin` is requested without a key, which is a different error from the `400` settings errors.

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
