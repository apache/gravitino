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
