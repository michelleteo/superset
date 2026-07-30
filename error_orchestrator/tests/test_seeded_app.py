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

"""Regressions for the defects seeded in :mod:`error_orchestrator.seeded.app`."""

from __future__ import annotations

from datetime import datetime

import pytest

from error_orchestrator.seeded.app import parse_human_datetime


@pytest.mark.parametrize("submitted", ["", "   ", None])
def test_an_unset_date_range_parses_to_no_boundary(submitted: str | None) -> None:
    assert parse_human_datetime(submitted) is None


def test_a_date_the_explore_ui_submits_still_parses() -> None:
    assert parse_human_datetime("2015-04-03") == datetime(2015, 4, 3)


def test_a_genuinely_unparseable_date_is_still_an_error() -> None:
    with pytest.raises(ValueError, match="does not match format"):
        parse_human_datetime("xxxxxxx")
