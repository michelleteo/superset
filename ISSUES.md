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

# Issues selected and remediated

The workflow this fork demonstrates is: Superset raises an error → the
orchestrator groups, prioritises and triages it → a Devin session reproduces and
fixes it → a risk check either auto-merges the diff or hands it to a human. This
file lists the concrete errors chosen to drive that workflow, why each was
picked, how to reproduce it, and what remediation produced.

Two kinds of issue are on the list:

- **Superset defects** — unguarded code in this fork's own Superset source.
  Remediation edits `superset/`.
- **Chart-path defects** — the same class of bug in
  [`error_orchestrator/seeded/app.py`](error_orchestrator/seeded/app.py), a
  miniature slice of the chart-data path. They exist because the Superset
  functions they mirror (query context, time filters, date parsing, CSV export)
  cannot be executed without booting Flask, SQLAlchemy and a metadata database,
  which the single-container demo deliberately does not do.

Every one of them is a *real* failing code path, not a hand-written traceback:
the simulator obtains each traceback by running the defect
([`seeded/bugs.py`](error_orchestrator/seeded/bugs.py)), so the frames the
orchestrator fingerprints are the frames the repository produces.

## Reproducing any of them

```bash
python -m error_orchestrator.seeded.reproduce <key>   # exit 1 while broken, 0 once fixed
```

That command is also what reaches the remediation prompt and the ticket, so a
Devin session has a check to iterate against.

## The Superset defects

### 1. `superset_country_symbol_none` — country lookup with no symbol

- **Where**: `superset/examples/countries.py`, `get()`
- **Error**: `AttributeError: 'NoneType' object has no attribute 'lower'`
- **Why it was selected**: the country column of a map chart's form data is
  optional, so `symbol` legitimately arrives as `None`; `get` calls
  `symbol.lower()` with no guard. A low-risk, single-line fix — the case that
  should flow all the way to auto-merge.
- **Repro**: `python -m error_orchestrator.seeded.reproduce superset_country_symbol_none`
- **Remediation**: return `None` when `symbol` is falsy, before the lookup.

### 2. `superset_country_unknown_field` — country lookup on an unindexed standard

- **Where**: `superset/examples/countries.py`, `get()`
- **Error**: `KeyError: 'iso3'`
- **Why it was selected**: only `cioc`, `cca2`, `cca3` and `name` are
  pre-computed into `all_lookups`, and any other field name indexes straight
  into a `KeyError` that says nothing about the supported set. Same file as #1,
  so it also exercises the orchestrator's deduplication: the two errors share a
  file and function but not a fingerprint, and triage has to decide whether they
  are one category or two.
- **Repro**: `python -m error_orchestrator.seeded.reproduce superset_country_unknown_field`
- **Remediation**: `all_lookups.get(field)` with an explicit error naming the
  valid standards.

### 3. `superset_class_name_no_module` — unqualified class name in config

- **Where**: `superset/utils/class_utils.py`, `load_class_from_name()`
- **Error**: `ValueError: Empty module name`
- **Why it was selected**: an operator setting
  `CUSTOM_SECURITY_MANAGER = "CustomSecurityManager"` (no module path) gets an
  error from deep inside `importlib` that never mentions their config. The
  existing guard only rejects the empty string. Configuration errors are a
  different lane of the risk registry than data-path errors, so this one shows
  the workflow on something a human is more likely to want to look at.
- **Repro**: `python -m error_orchestrator.seeded.reproduce superset_class_name_no_module`
- **Remediation**: reject names without a module path in the existing guard,
  with a message naming the offending value.

## The chart-path defects

| Key | Error | Selected because |
| --- | --- | --- |
| `seeded_datasource_none` | `AttributeError: 'NoneType' object has no attribute 'data'` | An ad-hoc query context has no datasource; the column list assumes one |
| `seeded_granularity_keyerror` | `KeyError: 'granularity_sqla'` | Form key assumed present for charts with no temporal column |
| `seeded_date_parser` | `ValueError: unparseable human readable date` | Empty date range handed straight to a parser |
| `seeded_csv_encoding` | `UnicodeDecodeError` | Driver bytes assumed to be UTF-8 on CSV export |

## Why the defects are still in the tree

They are the demo's input. Fixing them in `master` would leave the seeded mode
with nothing to reproduce, and
`error_orchestrator/tests/test_seeded.py::test_every_seeded_bug_still_fails_for_real`
fails on purpose the moment one of them stops raising — that test is what keeps
this list honest. Remediation is shown where it actually happens: on the ticket
in the dashboard, and in the Devin session it links to.

## Watching a real remediation

With `DEVIN_API_KEY` set, run the demo against these issues and let real
sessions do the work:

```bash
docker run --rm -p 8099:8099 \
  -e DEVIN_API_KEY \
  -e ERROR_ORCHESTRATOR_REPO=michelleteo/superset_demo \
  error-orchestrator-demo \
  --live-devin --seeded-bugs --rate 0.2 --live-devin-budget 2
```

Open <http://localhost:8099/> and click a pool: the ticket carries the diff the
session wrote, the risk tier it was given, whether it was auto-merged or queued
for a human, and a link to the session itself. See the [README](README.md) for
the rest of the walkthrough.
