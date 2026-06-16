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

"""pytest fixtures for the MCP authorization integration test.

The orchestration script (``dev/run_authz_integration_test.sh``) starts a real
Gravitino server and the MCP server in HTTP mode, then exports the connection
details below. When these env vars are absent the whole integration suite is
skipped so a plain ``pytest`` run stays green without external services.

Required environment variables:
    GRAVITINO_URI   e.g. http://localhost:8090
    MCP_URL         e.g. http://localhost:8000/mcp
    MCP_METALAKE    metalake name the MCP server was launched against
"""

import os

import pytest

from tests.integration.gravitino_setup import GravitinoFixture

# Fixtures receive other fixtures as same-named parameters; this is the standard
# pytest pattern, not an accidental shadowing.
# pylint: disable=redefined-outer-name

_REQUIRED_ENV = ("GRAVITINO_URI", "MCP_URL", "MCP_METALAKE")


def _missing_env() -> list:
    return [name for name in _REQUIRED_ENV if not os.environ.get(name)]


@pytest.fixture(scope="session")
def integration_env() -> dict:
    missing = _missing_env()
    if missing:
        pytest.skip(
            "Integration test requires a running Gravitino + MCP server. "
            f"Missing env: {', '.join(missing)}. "
            "Run via dev/run_authz_integration_test.sh."
        )
    return {
        "gravitino_uri": os.environ["GRAVITINO_URI"],
        "mcp_url": os.environ["MCP_URL"],
        "metalake": os.environ["MCP_METALAKE"],
    }


@pytest.fixture(scope="session")
def gravitino_fixture(integration_env: dict) -> GravitinoFixture:
    """Provision metalake/catalogs/user/role/grant once for the whole suite."""
    fixture = GravitinoFixture(
        gravitino_uri=integration_env["gravitino_uri"],
        metalake=integration_env["metalake"],
    )
    fixture.provision()
    yield fixture
    fixture.close()
