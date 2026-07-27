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

from __future__ import annotations

from error_orchestrator.fingerprint import (
    canonicalize,
    fingerprint_event,
    normalize_text,
    parse_traceback,
)
from error_orchestrator.models import ErrorEvent
from error_orchestrator.tests.conftest import make_event


def test_normalize_strips_instance_specific_tokens() -> None:
    normalized = normalize_text(
        "user bob@example.com hit 0xdeadbeef at 2026-07-27T10:11:12+00:00 "
        "for 4f2a1c3e-1b2d-4a5e-9c8f-0a1b2c3d4e5f from 10.1.2.3 (attempt 42)"
    )
    assert "<ADDR>" in normalized
    assert "<UUID>" in normalized
    assert "<TIMESTAMP>" in normalized
    assert "<EMAIL>" in normalized
    assert "<IP>" in normalized
    assert "<NUM>" in normalized
    assert "42" not in normalized


def test_same_bug_different_instance_data_shares_fingerprint() -> None:
    first = make_event(dataset="sales", user_id="u1")
    second = make_event(
        dataset="marketing",
        request_id="99999999-8888-7777-6666-555555555555",
        timestamp="2026-07-28T23:59:59+00:00",
        address="0x00ff00ff00ff",
        user_id="u2",
    )
    assert fingerprint_event(first) == fingerprint_event(second)


def test_different_code_path_gets_a_different_fingerprint() -> None:
    other_path = make_event().traceback or ""
    mutated = other_path.replace("get_payload", "get_df_payload_v2")
    assert fingerprint_event(make_event()) != fingerprint_event(
        make_event(traceback=mutated)
    )


def test_line_numbers_are_part_of_the_fingerprint() -> None:
    shifted = (make_event().traceback or "").replace("line 412", "line 415")
    assert fingerprint_event(make_event()) != fingerprint_event(
        make_event(traceback=shifted)
    )


def test_vendor_frames_are_ignored() -> None:
    with_vendor = (make_event().traceback or "").replace(
        "Traceback (most recent call last):",
        "Traceback (most recent call last):\n  File "
        '"/usr/lib/python3.11/site-packages/flask/app.py", line 2, in wsgi_app\n'
        "    response = self.full_dispatch_request()",
    )
    assert fingerprint_event(make_event(traceback=with_vendor)) == fingerprint_event(
        make_event()
    )


def test_canonicalize_extracts_exception_and_frames() -> None:
    canonical = canonicalize(make_event())
    assert canonical.exception_type == "SupersetException"
    assert [frame.func for frame in canonical.frames] == ["explore", "get_payload"]
    assert canonical.frames[0].module == "superset.views.core"
    assert "<UUID>" in canonical.message_template


def test_events_without_traceback_fall_back_to_log_location() -> None:
    event = ErrorEvent(
        message="ValueError: bad value 17", module="superset.tasks", func="run", line=9
    )
    canonical = canonicalize(event)
    assert canonical.exception_type == "ValueError"
    assert canonical.frames[0].render() == "superset.tasks:run:9"
    assert fingerprint_event(event) == fingerprint_event(
        ErrorEvent(
            message="ValueError: bad value 23",
            module="superset.tasks",
            func="run",
            line=9,
        )
    )


def test_parse_traceback_handles_garbage() -> None:
    parsed = parse_traceback("not really a traceback")
    assert parsed.frames == []
    assert parsed.exception_type == ""
