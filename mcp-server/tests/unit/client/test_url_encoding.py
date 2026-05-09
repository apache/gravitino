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

"""Tests that user-supplied identifiers are URL-encoded before being embedded
in request paths, preventing path traversal and query injection attacks."""

import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock

from mcp_server.client.plain.plain_rest_client_catalog_operation import (
    PlainRESTClientCatalogOperation,
)
from mcp_server.client.plain.plain_rest_client_fileset_operation import (
    PlainRESTClientFilesetOperation,
)
from mcp_server.client.plain.plain_rest_client_job_operation import (
    PlainRESTClientJobOperation,
)
from mcp_server.client.plain.plain_rest_client_model_operation import (
    PlainRESTClientModelOperation,
)
from mcp_server.client.plain.plain_rest_client_policy_operation import (
    PlainRESTClientPolicyOperation,
)
from mcp_server.client.plain.plain_rest_client_schema_operation import (
    PlainRESTClientSchemaOperation,
)
from mcp_server.client.plain.plain_rest_client_statistic_operation import (
    PlainRESTClientStatisticOperation,
)
from mcp_server.client.plain.plain_rest_client_table_operation import (
    PlainRESTClientTableOperation,
)
from mcp_server.client.plain.plain_rest_client_tag_operation import (
    PlainRESTClientTagOperation,
)
from mcp_server.client.plain.plain_rest_client_topic_operation import (
    PlainRESTClientTopicOperation,
)


def _make_mock_client(response_json: dict):
    """Return a mock httpx.AsyncClient whose HTTP methods return a fake response."""
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = response_json

    client = MagicMock()
    client.get = AsyncMock(return_value=response)
    client.post = AsyncMock(return_value=response)
    client.put = AsyncMock(return_value=response)
    client.delete = AsyncMock(return_value=response)
    return client


def _called_url(mock_method):
    """Extract the positional URL argument from the first call of a mock method."""
    return mock_method.call_args[0][0]


def _called_params(mock_method):
    """Extract the 'params' keyword argument from the first call of a mock method."""
    return mock_method.call_args[1].get("params", {})


# Payloads that must be encoded to prevent injection
_PATH_TRAVERSAL = "../../admin/users"
_QUERY_INJECTION = "name?admin=true#"
_SLASH = "cat/schema"
_ENCODED_PATH_TRAVERSAL = "..%2F..%2Fadmin%2Fusers"
_ENCODED_QUERY_INJECTION = "name%3Fadmin%3Dtrue%23"
_ENCODED_SLASH = "cat%2Fschema"

METALAKE = "my_metalake"


class TestCatalogOperationUrlEncoding(unittest.TestCase):
    def test_get_list_of_catalogs_encodes_metalake(self):
        client = _make_mock_client({"catalogs": []})
        op = PlainRESTClientCatalogOperation(_PATH_TRAVERSAL, client)
        asyncio.run(op.get_list_of_catalogs())
        url = _called_url(client.get)
        self.assertIn(_ENCODED_PATH_TRAVERSAL, url)
        self.assertNotIn("../../", url)


class TestSchemaOperationUrlEncoding(unittest.TestCase):
    def test_get_list_of_schemas_encodes_catalog_name(self):
        client = _make_mock_client({"identifiers": []})
        op = PlainRESTClientSchemaOperation(METALAKE, client)
        asyncio.run(op.get_list_of_schemas(_PATH_TRAVERSAL))
        url = _called_url(client.get)
        self.assertIn(_ENCODED_PATH_TRAVERSAL, url)
        self.assertNotIn("../../", url)

    def test_get_list_of_schemas_encodes_query_injection(self):
        client = _make_mock_client({"identifiers": []})
        op = PlainRESTClientSchemaOperation(METALAKE, client)
        asyncio.run(op.get_list_of_schemas(_QUERY_INJECTION))
        url = _called_url(client.get)
        self.assertIn(_ENCODED_QUERY_INJECTION, url)
        self.assertNotIn("?admin=true", url)


class TestTableOperationUrlEncoding(unittest.TestCase):
    def test_get_list_of_tables_encodes_path_traversal(self):
        client = _make_mock_client({"identifiers": []})
        op = PlainRESTClientTableOperation(METALAKE, client)
        asyncio.run(op.get_list_of_tables(_PATH_TRAVERSAL, "schema"))
        url = _called_url(client.get)
        self.assertIn(_ENCODED_PATH_TRAVERSAL, url)
        self.assertNotIn("../../", url)

    def test_load_table_encodes_all_segments(self):
        client = _make_mock_client({"table": {}})
        op = PlainRESTClientTableOperation(METALAKE, client)
        asyncio.run(op.load_table(_SLASH, _SLASH, _QUERY_INJECTION))
        url = _called_url(client.get)
        self.assertEqual(url.count(_ENCODED_SLASH), 2)
        self.assertIn(_ENCODED_QUERY_INJECTION, url)
        self.assertNotIn("?admin=true", url)


class TestModelOperationUrlEncoding(unittest.TestCase):
    def test_list_of_models_encodes_path_traversal(self):
        client = _make_mock_client({"identifiers": []})
        op = PlainRESTClientModelOperation(METALAKE, client)
        asyncio.run(op.list_of_models(_PATH_TRAVERSAL, "schema"))
        url = _called_url(client.get)
        self.assertIn(_ENCODED_PATH_TRAVERSAL, url)
        self.assertNotIn("../../", url)

    def test_load_model_encodes_model_name(self):
        client = _make_mock_client({"model": {}})
        op = PlainRESTClientModelOperation(METALAKE, client)
        asyncio.run(op.load_model("catalog", "schema", _QUERY_INJECTION))
        url = _called_url(client.get)
        self.assertIn(_ENCODED_QUERY_INJECTION, url)
        self.assertNotIn("?admin=true", url)

    def test_load_model_version_by_alias_encodes_alias(self):
        client = _make_mock_client({"modelVersion": {}})
        op = PlainRESTClientModelOperation(METALAKE, client)
        asyncio.run(
            op.load_model_version_by_alias(
                "catalog", "schema", "model", _PATH_TRAVERSAL
            )
        )
        url = _called_url(client.get)
        self.assertIn(_ENCODED_PATH_TRAVERSAL, url)
        self.assertNotIn("../../", url)


class TestTopicOperationUrlEncoding(unittest.TestCase):
    def test_list_of_topics_encodes_path_traversal(self):
        client = _make_mock_client({"identifiers": []})
        op = PlainRESTClientTopicOperation(METALAKE, client)
        asyncio.run(op.list_of_topics(_PATH_TRAVERSAL, "schema"))
        url = _called_url(client.get)
        self.assertIn(_ENCODED_PATH_TRAVERSAL, url)
        self.assertNotIn("../../", url)

    def test_load_topic_encodes_topic_name(self):
        client = _make_mock_client({"topic": {}})
        op = PlainRESTClientTopicOperation(METALAKE, client)
        asyncio.run(op.load_topic("catalog", "schema", _QUERY_INJECTION))
        url = _called_url(client.get)
        self.assertIn(_ENCODED_QUERY_INJECTION, url)
        self.assertNotIn("?admin=true", url)


class TestFilesetOperationUrlEncoding(unittest.TestCase):
    def test_list_of_filesets_encodes_path_traversal(self):
        client = _make_mock_client({"identifiers": []})
        op = PlainRESTClientFilesetOperation(METALAKE, client)
        asyncio.run(op.list_of_filesets(_PATH_TRAVERSAL, "schema"))
        url = _called_url(client.get)
        self.assertIn(_ENCODED_PATH_TRAVERSAL, url)
        self.assertNotIn("../../", url)

    def test_load_fileset_encodes_fileset_name(self):
        client = _make_mock_client({"fileset": {}})
        op = PlainRESTClientFilesetOperation(METALAKE, client)
        asyncio.run(op.load_fileset("catalog", "schema", _QUERY_INJECTION))
        url = _called_url(client.get)
        self.assertIn(_ENCODED_QUERY_INJECTION, url)
        self.assertNotIn("?admin=true", url)

    def test_list_files_in_fileset_uses_params_for_query_args(self):
        """sub_path and location_name must go through params=, not be concatenated."""
        client = _make_mock_client({"files": []})
        op = PlainRESTClientFilesetOperation(METALAKE, client)
        asyncio.run(
            op.list_files_in_fileset(
                "catalog",
                "schema",
                "fileset",
                location_name="loc?inject=1",
                sub_path="/some/path?inject=2",
            )
        )
        url = _called_url(client.get)
        params = _called_params(client.get)
        # Injection payloads must NOT appear raw in the path
        self.assertNotIn("inject=1", url)
        self.assertNotIn("inject=2", url)
        # They must be passed as structured params so httpx encodes them
        self.assertIn("location_name", params)
        self.assertIn("sub_path", params)


class TestTagOperationUrlEncoding(unittest.TestCase):
    def test_get_tag_by_name_encodes_tag_name(self):
        client = _make_mock_client({"tag": {}})
        op = PlainRESTClientTagOperation(METALAKE, client)
        asyncio.run(op.get_tag_by_name(_PATH_TRAVERSAL))
        url = _called_url(client.get)
        self.assertIn(_ENCODED_PATH_TRAVERSAL, url)
        self.assertNotIn("../../", url)

    def test_alter_tag_encodes_tag_name(self):
        client = _make_mock_client({"tag": {}})
        op = PlainRESTClientTagOperation(METALAKE, client)
        asyncio.run(op.alter_tag(_QUERY_INJECTION, []))
        url = _called_url(client.put)
        self.assertIn(_ENCODED_QUERY_INJECTION, url)
        self.assertNotIn("?admin=true", url)

    def test_delete_tag_encodes_tag_name(self):
        client = _make_mock_client({})
        op = PlainRESTClientTagOperation(METALAKE, client)
        asyncio.run(op.delete_tag(_PATH_TRAVERSAL))
        url = _called_url(client.delete)
        self.assertIn(_ENCODED_PATH_TRAVERSAL, url)
        self.assertNotIn("../../", url)

    def test_associate_tag_encodes_metadata_full_name_and_type(self):
        client = _make_mock_client({"names": []})
        op = PlainRESTClientTagOperation(METALAKE, client)
        asyncio.run(
            op.associate_tag_with_metadata(_QUERY_INJECTION, _SLASH, [], [])
        )
        url = _called_url(client.post)
        self.assertIn(_ENCODED_QUERY_INJECTION, url)
        self.assertIn(_ENCODED_SLASH, url)
        self.assertNotIn("?admin=true", url)

    def test_list_tags_for_metadata_encodes_metadata_full_name(self):
        client = _make_mock_client({"names": []})
        op = PlainRESTClientTagOperation(METALAKE, client)
        asyncio.run(op.list_tags_for_metadata(_PATH_TRAVERSAL, "table"))
        url = _called_url(client.get)
        self.assertIn(_ENCODED_PATH_TRAVERSAL, url)
        self.assertNotIn("../../", url)

    def test_list_metadata_by_tag_encodes_tag_name(self):
        client = _make_mock_client({"metadataObjects": []})
        op = PlainRESTClientTagOperation(METALAKE, client)
        asyncio.run(op.list_metadata_by_tag(_QUERY_INJECTION))
        url = _called_url(client.get)
        self.assertIn(_ENCODED_QUERY_INJECTION, url)
        self.assertNotIn("?admin=true", url)


class TestJobOperationUrlEncoding(unittest.TestCase):
    def test_get_job_by_id_encodes_job_id(self):
        client = _make_mock_client({"job": {}})
        op = PlainRESTClientJobOperation(METALAKE, client)
        asyncio.run(op.get_job_by_id(_PATH_TRAVERSAL))
        url = _called_url(client.get)
        self.assertIn(_ENCODED_PATH_TRAVERSAL, url)
        self.assertNotIn("../../", url)

    def test_get_job_template_by_name_encodes_name(self):
        client = _make_mock_client({"jobTemplate": {}})
        op = PlainRESTClientJobOperation(METALAKE, client)
        asyncio.run(op.get_job_template_by_name(_QUERY_INJECTION))
        url = _called_url(client.get)
        self.assertIn(_ENCODED_QUERY_INJECTION, url)
        self.assertNotIn("?admin=true", url)

    def test_list_of_jobs_uses_params_for_template_name(self):
        """job_template_name is a query parameter and must not be concatenated into the path."""
        client = _make_mock_client({"jobs": []})
        op = PlainRESTClientJobOperation(METALAKE, client)
        asyncio.run(op.list_of_jobs(_QUERY_INJECTION))
        url = _called_url(client.get)
        params = _called_params(client.get)
        self.assertNotIn("?admin=true", url)
        self.assertIn("jobTemplateName", params)

    def test_cancel_job_encodes_job_id(self):
        client = _make_mock_client({"job": {}})
        op = PlainRESTClientJobOperation(METALAKE, client)
        asyncio.run(op.cancel_job(_PATH_TRAVERSAL))
        url = _called_url(client.post)
        self.assertIn(_ENCODED_PATH_TRAVERSAL, url)
        self.assertNotIn("../../", url)


class TestPolicyOperationUrlEncoding(unittest.TestCase):
    def test_load_policy_encodes_policy_name(self):
        client = _make_mock_client({"policy": {}})
        op = PlainRESTClientPolicyOperation(METALAKE, client)
        asyncio.run(op.load_policy(_PATH_TRAVERSAL))
        url = _called_url(client.get)
        self.assertIn(_ENCODED_PATH_TRAVERSAL, url)
        self.assertNotIn("../../", url)

    def test_associate_policy_encodes_metadata_full_name_and_type(self):
        client = _make_mock_client({"names": []})
        op = PlainRESTClientPolicyOperation(METALAKE, client)
        asyncio.run(
            op.associate_policy_with_metadata(_QUERY_INJECTION, _SLASH, [], [])
        )
        url = _called_url(client.post)
        self.assertIn(_ENCODED_QUERY_INJECTION, url)
        self.assertIn(_ENCODED_SLASH, url)
        self.assertNotIn("?admin=true", url)

    def test_get_policy_for_metadata_encodes_policy_name(self):
        client = _make_mock_client({"policy": {}})
        op = PlainRESTClientPolicyOperation(METALAKE, client)
        asyncio.run(
            op.get_policy_for_metadata(
                "meta.full.name", "table", _PATH_TRAVERSAL
            )
        )
        url = _called_url(client.get)
        self.assertIn(_ENCODED_PATH_TRAVERSAL, url)
        self.assertNotIn("../../", url)

    def test_list_metadata_by_policy_encodes_policy_name(self):
        client = _make_mock_client({"metadataObjects": []})
        op = PlainRESTClientPolicyOperation(METALAKE, client)
        asyncio.run(op.list_metadata_by_policy(_QUERY_INJECTION))
        url = _called_url(client.get)
        self.assertIn(_ENCODED_QUERY_INJECTION, url)
        self.assertNotIn("?admin=true", url)


class TestStatisticOperationUrlEncoding(unittest.TestCase):
    def test_list_of_statistics_encodes_metadata_fullname(self):
        client = _make_mock_client({"statistics": []})
        op = PlainRESTClientStatisticOperation(METALAKE, client)
        asyncio.run(op.list_of_statistics(METALAKE, "table", _PATH_TRAVERSAL))
        url = _called_url(client.get)
        self.assertIn(_ENCODED_PATH_TRAVERSAL, url)
        self.assertNotIn("../../", url)

    def test_list_statistic_for_partition_uses_params_for_partition_names(self):
        """Partition names are query parameters and must not be concatenated into the path."""
        client = _make_mock_client({"partitionStatistics": []})
        op = PlainRESTClientStatisticOperation(METALAKE, client)
        asyncio.run(
            op.list_statistic_for_partition(
                METALAKE,
                "table",
                "catalog.schema.table",
                from_partition_name=_QUERY_INJECTION,
                to_partition_name=_PATH_TRAVERSAL,
            )
        )
        url = _called_url(client.get)
        params = _called_params(client.get)
        self.assertNotIn("?admin=true", url)
        self.assertNotIn("../../", url)
        self.assertIn("from", params)
        self.assertIn("to", params)
