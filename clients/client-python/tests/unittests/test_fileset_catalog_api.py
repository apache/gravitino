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
import json
import unittest
from http.client import HTTPResponse
from unittest.mock import patch, Mock

from gravitino import GravitinoClient, Catalog, NameIdentifier
from gravitino.audit.caller_context import CallerContext, CallerContextHolder
from gravitino.audit.fileset_audit_constants import FilesetAuditConstants
from gravitino.audit.fileset_data_operation import FilesetDataOperation
from gravitino.exceptions.handlers.fileset_error_handler import FILESET_ERROR_HANDLER
from gravitino.namespace import Namespace
from gravitino.utils import Response
from tests.unittests import mock_base


@mock_base.mock_data
class TestFilesetCatalogApi(unittest.TestCase):

    def test_get_file_location(self, *mock_method):
        json_data = {"code": 0, "fileLocation": "file:/test/1"}
        json_str = json.dumps(json_data)

        mock_http_resp = Mock(HTTPResponse)
        mock_http_resp.getcode.return_value = 200
        mock_http_resp.read.return_value = json_str
        mock_http_resp.info.return_value = None
        mock_http_resp.url = None
        mock_resp = Response(mock_http_resp)

        metalake_name: str = "metalake_demo"
        catalog_name: str = "fileset_catalog"
        gravitino_client = GravitinoClient(
            uri="http://localhost:8090", metalake_name=metalake_name
        )
        catalog: Catalog = gravitino_client.load_catalog(catalog_name)

        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            return_value=mock_resp,
        ) as mock_get:
            fileset_ident: NameIdentifier = NameIdentifier.of(
                "test", "test_get_file_location"
            )
            context = {
                FilesetAuditConstants.HTTP_HEADER_FILESET_DATA_OPERATION: FilesetDataOperation.RENAME.name
            }
            CallerContextHolder.set(CallerContext(context))
            file_location: str = catalog.as_fileset_catalog().get_file_location(
                fileset_ident, "/test/1"
            )
            # check the get input params as expected
            mock_get.assert_called_once_with(
                catalog.as_fileset_catalog().format_file_location_request_path(
                    Namespace.of("metalake_demo", "fileset_catalog", "test"),
                    fileset_ident.name(),
                ),
                params={"sub_path": "/test/1"},
                headers=context,
                error_handler=FILESET_ERROR_HANDLER,
            )
            # check the caller context is removed
            self.assertIsNone(CallerContextHolder.get())
            # check the response is as expected
            self.assertEqual(file_location, "file:/test/1")
