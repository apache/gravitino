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

"""Opt-in wire checks for the glossary REST API. See the adjacent README."""

import os
from contextlib import contextmanager
from uuid import uuid4

import httpx
import pytest

V1_JSON = "application/vnd.gravitino.v1+json"


@pytest.fixture(scope="module")
def api():
    uri = os.environ.get("GRAVITINO_URI")
    if not uri:
        pytest.fail("Set GRAVITINO_URI to a Gravitino test server with simple auth.")
    with httpx.Client(
        base_url=uri.rstrip("/") + "/api/",
        auth=(os.environ.get("GRAVITINO_USER", "admin"), "dummy"),
        timeout=30,
    ) as client:
        yield client


@contextmanager
def temporary_metalake(api):
    name = "glossary_contract_" + uuid4().hex
    api.post("metalakes", json={"name": name, "properties": {}}).raise_for_status()
    path = f"metalakes/{name}"
    try:
        yield path
    finally:
        response = api.delete(path, params={"force": "true"})
        response.raise_for_status()
        assert response.json()["dropped"] is True


@pytest.fixture(scope="module")
def lake(api):
    with temporary_metalake(api) as path:
        yield path


@pytest.mark.parametrize(
    "fields,lookup_name",
    [
        ({"name": 123}, "123"),
        ({"name": "Definition", "definition": False}, "Definition"),
        ({"name": "Synonyms", "synonyms": [42]}, "Synonyms"),
        ({"name": "Properties", "properties": {"cost": 3}}, "Properties"),
        ({"name": "SourceRef", "sourceRef": 987}, "SourceRef"),
    ],
    ids=["name", "definition", "synonyms", "properties", "source-ref"],
)
def test_create_rejects_non_string_fields(api, lake, fields, lookup_name):
    response = api.post(f"{lake}/terms", json=fields)
    lookup = api.get(f"{lake}/terms/{lookup_name}")
    assert (response.status_code, lookup.status_code) == (400, 404)


@pytest.mark.parametrize(
    "edit",
    [
        {"@type": "updateDefinition", "newDefinition": False},
        {"@type": "addSynonym", "synonym": 257},
        {"@type": "setProperty", "property": "cost", "value": True},
    ],
    ids=["definition", "synonym", "property"],
)
def test_alter_rejects_non_string_fields_without_writing(api, lake, edit):
    name = edit["@type"]
    api.post(
        f"{lake}/terms", json={"name": name, "definition": "original"}
    ).raise_for_status()
    path = f"{lake}/terms/{name}"
    before = api.get(path).raise_for_status().json()["term"]
    response = api.put(path, json={"updates": [edit]})
    after = api.get(path).raise_for_status().json()["term"]
    assert (response.status_code, after) == (400, before)


TERM_OPERATIONS = [
    pytest.param("GET", "terms", None, id="list"),
    pytest.param("POST", "terms", {"name": "Created"}, id="create"),
    pytest.param("GET", "terms/Wire", None, id="load"),
    pytest.param(
        "PUT",
        "terms/Wire",
        {"updates": [{"@type": "updateDefinition", "newDefinition": "updated"}]},
        id="alter",
    ),
    pytest.param("GET", "terms/Wire/hierarchy", None, id="hierarchy"),
    pytest.param("DELETE", "terms/Wire", None, id="drop"),
    pytest.param("GET", "terms/Wire/objects", None, id="term-bindings"),
]


@pytest.mark.parametrize("method,suffix,body", TERM_OPERATIONS)
def test_negotiates_v1_success_response(api, method, suffix, body):
    with temporary_metalake(api) as path:
        api.post(f"{path}/terms", json={"name": "Wire"}).raise_for_status()
        response = api.request(
            method, f"{path}/{suffix}", json=body, headers={"Accept": V1_JSON}
        )
        assert response.status_code == 200, response.text
        assert response.headers["content-type"].split(";")[0].strip().lower() == V1_JSON


@pytest.fixture(scope="module")
def removed_metalake(api):
    with temporary_metalake(api) as path:
        api.post(f"{path}/terms", json={"name": "Wire"}).raise_for_status()
        missing_term = api.delete(f"{path}/terms/Absent")
        missing_term.raise_for_status()
        assert missing_term.json()["dropped"] is False
    return path


@pytest.mark.parametrize(
    "method,suffix,body",
    [
        *TERM_OPERATIONS,
        pytest.param("GET", "objects/table/c.s.t/terms", None, id="object-list"),
        pytest.param(
            "POST",
            "objects/table/c.s.t/terms",
            {"termsToAdd": ["Wire"]},
            id="object-bind",
        ),
        pytest.param(
            "GET",
            "objects/semantic_model/c.s.m/metrics/metric/terms",
            None,
            id="metric-list",
        ),
        pytest.param(
            "POST",
            "objects/semantic_model/c.s.m/metrics/metric/terms",
            {"termsToAdd": ["Wire"]},
            id="metric-bind",
        ),
    ],
)
def test_removed_metalake_response(api, removed_metalake, method, suffix, body):
    mode = os.environ.get("GRAVITINO_AUTHORIZATION_ENABLED", "true").lower()
    expected = {
        "true": (403, "ForbiddenException"),
        "false": (404, "NoSuchMetalakeException"),
    }
    assert mode in expected, "GRAVITINO_AUTHORIZATION_ENABLED must be true or false"
    response = api.request(method, f"{removed_metalake}/{suffix}", json=body)
    assert (response.status_code, response.json().get("type")) == expected[mode]
