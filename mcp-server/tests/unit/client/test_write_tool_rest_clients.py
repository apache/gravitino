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

import asyncio
import json
import unittest
from unittest.mock import AsyncMock, MagicMock

from mcp_server.client.plain.exception import GravitinoException
from mcp_server.client.plain.plain_rest_client_job_operation import (
    PlainRESTClientJobOperation,
)
from mcp_server.client.plain.plain_rest_client_model_operation import (
    PlainRESTClientModelOperation,
)
from mcp_server.client.plain.plain_rest_client_policy_operation import (
    PlainRESTClientPolicyOperation,
)
from mcp_server.client.plain.plain_rest_client_statistic_operation import (
    PlainRESTClientStatisticOperation,
)


def _make_response(response_json: dict, status_code: int = 200):
    response = MagicMock()
    response.status_code = status_code
    response.json.return_value = response_json
    response.text = str(response_json)
    return response


def _make_client(response_json: dict, status_code: int = 200):
    response = _make_response(response_json, status_code)

    client = MagicMock()
    client.get = AsyncMock(return_value=response)
    client.post = AsyncMock(return_value=response)
    client.put = AsyncMock(return_value=response)
    client.patch = AsyncMock(return_value=response)
    client.delete = AsyncMock(return_value=response)
    return client


def _called_url(mock_method):
    return mock_method.call_args[0][0]


def _called_json(mock_method):
    return mock_method.call_args[1]["json"]


class TestJobWriteRestClient(unittest.TestCase):
    def test_register_job_template_wraps_template_payload(self):
        client = _make_client({"code": 0})
        operation = PlainRESTClientJobOperation("metalake", client)
        job_template = {
            "jobType": "shell",
            "name": "shell_test",
            "executable": "/tmp/test.sh",
        }

        result = asyncio.run(operation.register_job_template(job_template))

        self.assertIsNone(result)
        self.assertEqual(
            "/api/metalakes/metalake/jobs/templates", _called_url(client.post)
        )
        self.assertEqual(
            {"jobTemplate": job_template}, _called_json(client.post)
        )

    def test_delete_job_template_returns_drop_response(self):
        client = _make_client({"code": 0, "dropped": True})
        operation = PlainRESTClientJobOperation("metalake", client)

        result = asyncio.run(operation.delete_job_template("shell_test"))

        self.assertEqual("true", result)
        self.assertEqual(
            "/api/metalakes/metalake/jobs/templates/shell_test",
            _called_url(client.delete),
        )

    def test_alter_job_template_sends_updates_request(self):
        client = _make_client({"code": 0, "jobTemplate": {"name": "shell2"}})
        operation = PlainRESTClientJobOperation("metalake", client)
        updates = [{"@type": "rename", "newName": "shell2"}]

        result = asyncio.run(
            operation.alter_job_template("shell_test", updates)
        )

        self.assertEqual(json.dumps({"name": "shell2"}), result)
        self.assertEqual(
            "/api/metalakes/metalake/jobs/templates/shell_test",
            _called_url(client.put),
        )
        self.assertEqual({"updates": updates}, _called_json(client.put))


class TestModelWriteRestClient(unittest.TestCase):
    def test_update_model_version_aliases_sends_update_request(self):
        client = _make_client(
            {
                "code": 0,
                "modelVersion": {"version": 0, "aliases": ["alias1"]},
            }
        )
        operation = PlainRESTClientModelOperation("metalake", client)

        result = asyncio.run(
            operation.update_model_version_aliases(
                "catalog",
                "schema",
                "model",
                0,
                ["alias1"],
                ["alias2"],
            )
        )

        self.assertEqual(
            {"version": 0, "aliases": ["alias1"]}, json.loads(result)
        )
        self.assertEqual(
            "/api/metalakes/metalake/catalogs/catalog/schemas/schema"
            "/models/model/versions/0",
            _called_url(client.put),
        )
        self.assertEqual(
            {
                "updates": [
                    {
                        "@type": "updateAliases",
                        "aliasesToAdd": ["alias1"],
                        "aliasesToRemove": ["alias2"],
                    }
                ]
            },
            _called_json(client.put),
        )

    def test_update_model_version_aliases_raises_gravitino_error(self):
        client = _make_client(
            {
                "code": 1003,
                "type": "NoSuchModelException",
                "message": "model not found",
            },
            status_code=404,
        )
        operation = PlainRESTClientModelOperation("metalake", client)

        with self.assertRaisesRegex(GravitinoException, "model not found"):
            asyncio.run(
                operation.update_model_version_aliases(
                    "catalog",
                    "schema",
                    "model",
                    0,
                    ["alias1"],
                    [],
                )
            )


class TestPolicyWriteRestClient(unittest.TestCase):
    def test_create_policy_sends_create_request(self):
        client = _make_client(
            {
                "code": 0,
                "policy": {"name": "policy1", "policyType": "custom"},
            }
        )
        operation = PlainRESTClientPolicyOperation("metalake", client)
        content = {
            "customRules": {"rule1": 1},
            "supportedObjectTypes": ["TABLE"],
        }

        result = asyncio.run(
            operation.create_policy(
                "policy1",
                "custom",
                "comment",
                True,
                content,
            )
        )

        self.assertEqual(
            {"name": "policy1", "policyType": "custom"}, json.loads(result)
        )
        self.assertEqual(
            "/api/metalakes/metalake/policies", _called_url(client.post)
        )
        self.assertEqual(
            {
                "name": "policy1",
                "policyType": "custom",
                "comment": "comment",
                "enabled": True,
                "content": content,
            },
            _called_json(client.post),
        )

    def test_alter_policy_sends_updates_request(self):
        client = _make_client({"code": 0, "policy": {"name": "policy2"}})
        operation = PlainRESTClientPolicyOperation("metalake", client)
        updates = [{"@type": "rename", "newName": "policy2"}]

        result = asyncio.run(operation.alter_policy("policy1", updates))

        self.assertEqual({"name": "policy2"}, json.loads(result))
        self.assertEqual(
            "/api/metalakes/metalake/policies/policy1", _called_url(client.put)
        )
        self.assertEqual({"updates": updates}, _called_json(client.put))

    def test_delete_policy_returns_drop_response(self):
        client = _make_client({"code": 0, "dropped": False})
        operation = PlainRESTClientPolicyOperation("metalake", client)

        result = asyncio.run(operation.delete_policy("missing_policy"))

        self.assertEqual("false", result)

    def test_set_policy_sends_enable_request(self):
        client = _make_client({"code": 0})
        operation = PlainRESTClientPolicyOperation("metalake", client)

        result = asyncio.run(operation.set_policy("my_policy", False))

        self.assertEqual(json.dumps({"code": 0}), result)
        self.assertEqual(
            "/api/metalakes/metalake/policies/my_policy",
            _called_url(client.patch),
        )
        self.assertEqual({"enable": False}, _called_json(client.patch))


class TestStatisticWriteRestClient(unittest.TestCase):
    def test_update_statistics_uses_configured_metalake(self):
        client = _make_client({"code": 0})
        operation = PlainRESTClientStatisticOperation(
            "configured_metalake", client
        )

        asyncio.run(
            operation.update_statistics(
                "table",
                "catalog.schema.table",
                {"custom-key": "value"},
            )
        )

        self.assertIn(
            "/api/metalakes/configured_metalake/", _called_url(client.put)
        )
        self.assertEqual(
            {"updates": {"custom-key": "value"}}, _called_json(client.put)
        )

    def test_drop_statistics_sends_names_request(self):
        client = _make_client({"code": 0, "dropped": True})
        operation = PlainRESTClientStatisticOperation("metalake", client)

        result = asyncio.run(
            operation.drop_statistics(
                "table",
                "catalog.schema.table",
                ["custom-key"],
            )
        )

        self.assertEqual("true", result)
        self.assertEqual(
            "/api/metalakes/metalake/objects/table/catalog.schema.table/statistics",
            _called_url(client.post),
        )
        self.assertEqual({"names": ["custom-key"]}, _called_json(client.post))

    def test_update_partition_statistics_sends_updates_request(self):
        client = _make_client({"code": 0})
        operation = PlainRESTClientStatisticOperation("metalake", client)
        updates = [
            {"partitionName": "p1", "statistics": {"custom-key": "value"}}
        ]

        result = asyncio.run(
            operation.update_partition_statistics(
                "table",
                "catalog.schema.table",
                updates,
            )
        )

        self.assertIsNone(result)
        self.assertEqual(
            "/api/metalakes/metalake/objects/table/catalog.schema.table"
            "/statistics/partitions",
            _called_url(client.put),
        )
        self.assertEqual({"updates": updates}, _called_json(client.put))

    def test_drop_partition_statistics_sends_drops_request(self):
        client = _make_client({"code": 0, "dropped": True})
        operation = PlainRESTClientStatisticOperation("metalake", client)
        drops = [{"partitionName": "p1", "statisticNames": ["custom-key"]}]

        result = asyncio.run(
            operation.drop_partition_statistics(
                "table",
                "catalog.schema.table",
                drops,
            )
        )

        self.assertEqual("true", result)
        self.assertEqual(
            "/api/metalakes/metalake/objects/table/catalog.schema.table"
            "/statistics/partitions",
            _called_url(client.post),
        )
        self.assertEqual({"drops": drops}, _called_json(client.post))
