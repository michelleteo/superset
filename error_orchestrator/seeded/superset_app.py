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

"""Defects in this fork's *real* Superset source, not in a stand-in module.

Nothing here is injected: each function below calls Superset code as it is
committed in this repository, and the traceback comes out of the Superset file
itself. The defects are all unguarded lookups on caller-supplied values, the
same family as the miniature ones in :mod:`error_orchestrator.seeded.app`.

Superset's package ``__init__`` pulls in Flask, SQLAlchemy and the rest of the
application, none of which the demo image installs, so the modules are loaded
straight off disk by path. The frames still name the real files
(``superset/examples/countries.py``, ``superset/utils/class_utils.py``), which
is what a remediation session needs in order to find the code.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]


def load_superset_module(relative_path: str) -> ModuleType:
    """Import one Superset source file without importing the ``superset`` package."""
    path = REPO_ROOT / relative_path
    name = "seeded_" + relative_path.removesuffix(".py").replace("/", "_")
    if (cached := sys.modules.get(name)) is not None:
        return cached
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:  # pragma: no cover - defensive
        raise ImportError(f"cannot load {relative_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def country_lookup_without_symbol() -> Any:
    """``superset.examples.countries.get`` with the symbol a caller left unset.

    The country field of a chart's form data is optional, so the value handed
    down can be ``None``; ``get`` calls ``.lower()`` on it unguarded.
    """
    countries = load_superset_module("superset/examples/countries.py")
    return countries.get("cca2", None)


def country_lookup_unknown_field() -> Any:
    """``superset.examples.countries.get`` with a country code it does not index.

    Only ``cioc``/``cca2``/``cca3``/``name`` are pre-computed; any other field
    name indexes ``all_lookups`` straight into a ``KeyError``.
    """
    countries = load_superset_module("superset/examples/countries.py")
    return countries.get("iso3", "fra")


def class_name_without_module() -> Any:
    """``superset.utils.class_utils.load_class_from_name`` on an unqualified name.

    The guard only rejects the empty string, so a name with no dots in it
    splits into an empty module path and fails inside ``import_module``.
    """
    class_utils = load_superset_module("superset/utils/class_utils.py")
    return class_utils.load_class_from_name("CustomSecurityManager")
