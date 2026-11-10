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

import importlib
import inspect
import pkgutil
import unittest
from http.client import HTTPResponse
from unittest.mock import Mock, patch

import gravitino.client
from gravitino.api.pagination import Page, page_params
from gravitino.dto.responses.entity_list_response import EntityListResponse
from gravitino.dto.responses.name_list_response import NameListResponse
from gravitino.exceptions.base import IllegalArgumentException
from gravitino.name_identifier import NameIdentifier
from gravitino.utils import Response
from tests.unittests import mock_base


def _http_response(json_str: str) -> Response:
    mock_http_resp = Mock(HTTPResponse)
    mock_http_resp.getcode.return_value = 200
    mock_http_resp.read.return_value = json_str
    mock_http_resp.info.return_value = None
    mock_http_resp.url = None
    return Response(mock_http_resp)


def _names(names, next_page_token=None) -> str:
    return NameListResponse(0, names, _next_page_token=next_page_token).to_json()


class TestPagedListing(unittest.TestCase):
    """Paged listing in the Python client: coverage and request/response handling."""

    def test_every_client_class_implements_inherited_paged_listing(self):
        missing = []
        checked = 0
        for module_info in pkgutil.iter_modules(gravitino.client.__path__):
            module = importlib.import_module(f"gravitino.client.{module_info.name}")
            for _, cls in inspect.getmembers(module, inspect.isclass):
                if cls.__module__ != module.__name__ or inspect.isabstract(cls):
                    continue
                for name in dir(cls):
                    if not name.endswith("_page"):
                        continue
                    defining = next(
                        (base for base in cls.__mro__ if name in base.__dict__), None
                    )
                    checked += 1
                    if defining is not None and defining.__module__.startswith(
                        "gravitino.api."
                    ):
                        missing.append(f"{cls.__name__}.{name}")
        self.assertGreater(checked, 0)
        self.assertEqual(
            [],
            sorted(set(missing)),
            "client classes falling through to the unsupported default",
        )

    def test_page_params(self):
        self.assertEqual({"pageSize": "5"}, page_params(5))
        self.assertEqual(
            {"pageSize": "5", "pageToken": "t"}, page_params(5, page_token="t")
        )
        for bad in (0, -1, True, "5", None):
            with self.assertRaises(IllegalArgumentException):
                page_params(bad)
        with self.assertRaises(IllegalArgumentException):
            page_params(5, page_token="")

    def test_metalake_name_listings_walk_pages(self):
        metalake = mock_base.mock_load_metalake()
        calls = {
            "users": metalake.list_user_names_page,
            "groups": metalake.list_group_names_page,
            "roles": metalake.list_role_names_page,
            "tags": metalake.list_tags_page,
            "policies": metalake.list_policies_page,
        }
        for label, call in calls.items():
            with patch(
                "gravitino.utils.http_client.HTTPClient.get",
                side_effect=[
                    _http_response(_names(["a", "b"], "t1")),
                    _http_response(_names(["c"])),
                ],
            ) as mock_get:
                first = call(page_size=2)
                second = call(page_size=2, page_token=first.next_page_token)
            self.assertEqual(Page(items=["a", "b"], next_page_token="t1"), first, label)
            self.assertEqual(Page(items=["c"], next_page_token=None), second, label)
            self.assertEqual(
                {"pageSize": "2"}, mock_get.call_args_list[0].kwargs["params"], label
            )
            self.assertEqual(
                {"pageSize": "2", "pageToken": "t1"},
                mock_get.call_args_list[1].kwargs["params"],
                label,
            )

    def test_list_catalogs_page(self):
        metalake = mock_base.mock_load_metalake()
        body = EntityListResponse(
            0,
            [NameIdentifier.of("metalake_demo", "a")],
            _next_page_token="t1",
        ).to_json()
        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            return_value=_http_response(body),
        ):
            page = metalake.list_catalogs_page(page_size=1)
        self.assertEqual(["a"], page.items)
        self.assertEqual("t1", page.next_page_token)

    def test_detail_params_survive_paging(self):
        metalake = mock_base.mock_load_metalake()
        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            return_value=_http_response('{"code": 0, "users": []}'),
        ) as mock_get:
            page = metalake.list_users_page(page_size=3)
        self.assertEqual([], page.items)
        self.assertIsNone(page.next_page_token)
        self.assertEqual(
            {"details": "true", "pageSize": "3"},
            mock_get.call_args.kwargs["params"],
        )
