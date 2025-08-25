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

from gravitino.dto.responses.credential_response import CredentialResponse
from gravitino.dto.responses.file_location_response import FileLocationResponse
from gravitino.dto.responses.model_response import ModelResponse
from gravitino.dto.responses.model_version_list_response import ModelVersionListResponse
from gravitino.dto.responses.model_version_response import ModelVersionResponse
from gravitino.dto.responses.model_version_uri_response import ModelVersionUriResponse
from gravitino.exceptions.base import IllegalArgumentException


class TestResponses(unittest.TestCase):
    def test_file_location_response(self):
        json_data = {"code": 0, "fileLocation": "file:/test/1"}
        json_str = json.dumps(json_data)
        file_location_resp: FileLocationResponse = FileLocationResponse.from_json(
            json_str
        )
        self.assertEqual(file_location_resp.file_location(), "file:/test/1")
        file_location_resp.validate()

    def test_file_location_response_exception(self):
        json_data = {"code": 0, "fileLocation": ""}
        json_str = json.dumps(json_data)
        file_location_resp: FileLocationResponse = FileLocationResponse.from_json(
            json_str
        )
        with self.assertRaises(IllegalArgumentException):
            file_location_resp.validate()

    def test_credential_response(self):
        json_data = {"code": 0, "credentials": []}
        json_str = json.dumps(json_data)
        credential_resp: CredentialResponse = CredentialResponse.from_json(json_str)
        self.assertEqual(0, len(credential_resp.credentials()))
        credential_resp.validate()

        json_data = {
            "code": 0,
            "credentials": [
                {
                    "credentialType": "s3-token",
                    "expireTimeInMs": 1000,
                    "credentialInfo": {
                        "s3-access-key-id": "access-id",
                        "s3-secret-access-key": "secret-key",
                        "s3-session-token": "token",
                    },
                }
            ],
        }
        json_str = json.dumps(json_data)
        credential_resp: CredentialResponse = CredentialResponse.from_json(json_str)
        credential_resp.validate()
        self.assertEqual(1, len(credential_resp.credentials()))
        credential = credential_resp.credentials()[0]
        self.assertEqual("s3-token", credential.credential_type())
        self.assertEqual(1000, credential.expire_time_in_ms())
        self.assertEqual("access-id", credential.credential_info()["s3-access-key-id"])
        self.assertEqual(
            "secret-key", credential.credential_info()["s3-secret-access-key"]
        )
        self.assertEqual("token", credential.credential_info()["s3-session-token"])

    def test_model_response(self):
        json_data = {
            "code": 0,
            "model": {
                "name": "test_model",
                "comment": "test comment",
                "properties": {"key1": "value1"},
                "latestVersion": 0,
                "audit": {
                    "creator": "anonymous",
                    "createTime": "2024-04-05T10:10:35.218Z",
                },
            },
        }
        json_str = json.dumps(json_data)
        model_resp: ModelResponse = ModelResponse.from_json(
            json_str, infer_missing=True
        )
        model_resp.validate()
        self.assertEqual("test_model", model_resp.model().name())
        self.assertEqual(0, model_resp.model().latest_version())
        self.assertEqual("test comment", model_resp.model().comment())
        self.assertEqual({"key1": "value1"}, model_resp.model().properties())
        self.assertEqual("anonymous", model_resp.model().audit_info().creator())
        self.assertEqual(
            "2024-04-05T10:10:35.218Z", model_resp.model().audit_info().create_time()
        )

        json_data_missing = {
            "code": 0,
            "model": {
                "name": "test_model",
                "latestVersion": 0,
                "audit": {
                    "creator": "anonymous",
                    "createTime": "2024-04-05T10:10:35.218Z",
                },
            },
        }
        json_str_missing = json.dumps(json_data_missing)
        model_resp_missing: ModelResponse = ModelResponse.from_json(
            json_str_missing, infer_missing=True
        )
        model_resp_missing.validate()
        self.assertEqual("test_model", model_resp_missing.model().name())
        self.assertEqual(0, model_resp_missing.model().latest_version())
        self.assertIsNone(model_resp_missing.model().comment())
        self.assertIsNone(model_resp_missing.model().properties())

    def test_model_version_list_response(self):
        json_data = {"code": 0, "versions": [0, 1, 2]}
        json_str = json.dumps(json_data)
        resp: ModelVersionListResponse = ModelVersionListResponse.from_json(
            json_str, infer_missing=True
        )
        resp.validate()
        self.assertEqual(3, len(resp.versions()))
        self.assertEqual([0, 1, 2], resp.versions())

        json_data_missing = {"code": 0, "versions": []}
        json_str_missing = json.dumps(json_data_missing)
        resp_missing: ModelVersionListResponse = ModelVersionListResponse.from_json(
            json_str_missing, infer_missing=True
        )
        resp_missing.validate()
        self.assertEqual(0, len(resp_missing.versions()))
        self.assertEqual([], resp_missing.versions())

        json_data_missing_1 = {
            "code": 0,
        }
        json_str_missing_1 = json.dumps(json_data_missing_1)
        resp_missing_1: ModelVersionListResponse = ModelVersionListResponse.from_json(
            json_str_missing_1, infer_missing=True
        )
        self.assertRaises(IllegalArgumentException, resp_missing_1.validate)

    def test_model_version_response(self):
        json_data = {
            "code": 0,
            "modelVersion": {
                "version": 0,
                "aliases": ["alias1", "alias2"],
                "uris": {"unknown": "http://localhost:8080"},
                "comment": "test comment",
                "properties": {"key1": "value1"},
                "audit": {
                    "creator": "anonymous",
                    "createTime": "2024-04-05T10:10:35.218Z",
                },
            },
        }
        json_str = json.dumps(json_data)
        resp: ModelVersionResponse = ModelVersionResponse.from_json(
            json_str, infer_missing=True
        )
        resp.validate()
        self.assertEqual(0, resp.model_version().version())
        self.assertEqual(["alias1", "alias2"], resp.model_version().aliases())
        self.assertEqual("test comment", resp.model_version().comment())
        self.assertEqual({"key1": "value1"}, resp.model_version().properties())
        self.assertEqual("anonymous", resp.model_version().audit_info().creator())
        self.assertEqual(
            "2024-04-05T10:10:35.218Z", resp.model_version().audit_info().create_time()
        )

        json_data = {
            "code": 0,
            "modelVersion": {
                "version": 0,
                "uris": {"unknown": "http://localhost:8080"},
                "audit": {
                    "creator": "anonymous",
                    "createTime": "2024-04-05T10:10:35.218Z",
                },
            },
        }
        json_str = json.dumps(json_data)
        resp: ModelVersionResponse = ModelVersionResponse.from_json(
            json_str, infer_missing=True
        )
        resp.validate()
        self.assertEqual(0, resp.model_version().version())
        self.assertIsNone(resp.model_version().aliases())
        self.assertIsNone(resp.model_version().comment())
        self.assertIsNone(resp.model_version().properties())

        json_data = {
            "code": 0,
            "modelVersion": {
                "uris": {"unknown": "http://localhost:8080"},
                "audit": {
                    "creator": "anonymous",
                    "createTime": "2024-04-05T10:10:35.218Z",
                },
            },
        }
        json_str = json.dumps(json_data)
        resp: ModelVersionResponse = ModelVersionResponse.from_json(
            json_str, infer_missing=True
        )
        self.assertRaises(IllegalArgumentException, resp.validate)

        json_data = {
            "code": 0,
            "modelVersion": {
                "version": 0,
                "audit": {
                    "creator": "anonymous",
                    "createTime": "2024-04-05T10:10:35.218Z",
                },
            },
        }
        json_str = json.dumps(json_data)
        resp: ModelVersionResponse = ModelVersionResponse.from_json(
            json_str, infer_missing=True
        )
        self.assertRaises(IllegalArgumentException, resp.validate)

        json_data = {
            "code": 0,
            "modelVersion": {
                "version": 0,
                "uris": {"unknown": "http://localhost:8080"},
            },
        }
        json_str = json.dumps(json_data)
        resp: ModelVersionResponse = ModelVersionResponse.from_json(
            json_str, infer_missing=True
        )
        self.assertRaises(IllegalArgumentException, resp.validate)

    def test_model_version_uri_response(self):
        json_data = {"code": 0, "uri": "s3://path/to/model"}
        json_str = json.dumps(json_data)
        resp: ModelVersionUriResponse = ModelVersionUriResponse.from_json(
            json_str, infer_missing=True
        )
        resp.validate()
        self.assertEqual("s3://path/to/model", resp.uri())

        json_data_missing = {"code": 0, "uri": ""}
        json_str_missing = json.dumps(json_data_missing)
        resp_missing: ModelVersionUriResponse = ModelVersionUriResponse.from_json(
            json_str_missing, infer_missing=True
        )
        self.assertRaises(IllegalArgumentException, resp_missing.validate)

        json_data_missing_1 = {
            "code": 0,
        }
        json_str_missing_1 = json.dumps(json_data_missing_1)
        resp_missing_1: ModelVersionUriResponse = ModelVersionUriResponse.from_json(
            json_str_missing_1, infer_missing=True
        )
        self.assertRaises(IllegalArgumentException, resp_missing_1.validate)
