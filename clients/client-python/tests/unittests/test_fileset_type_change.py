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

from gravitino.api.file.fileset import Fileset
from gravitino.api.file.fileset_change import FilesetChange
from gravitino.client.fileset_catalog import FilesetCatalog
from gravitino.dto.requests.fileset_update_request import FilesetUpdateRequest
from gravitino.dto.requests.fileset_updates_request import FilesetUpdatesRequest


class TestFilesetTypeChange(unittest.TestCase):
    def test_update_type_request(self):
        for fileset_type in Fileset.Type:
            with self.subTest(fileset_type=fileset_type):
                change = FilesetChange.update_type(fileset_type)
                self.assertEqual(fileset_type, change.new_type())
                self.assertEqual(change, FilesetChange.update_type(fileset_type))
                self.assertEqual(
                    hash(change), hash(FilesetChange.update_type(fileset_type))
                )
                self.assertEqual(f"UPDATEFILESETTYPE {fileset_type.name}", str(change))
                request = FilesetCatalog.to_fileset_update_request(change)
                self.assertEqual(change, request.fileset_change())
                updates = FilesetUpdatesRequest([request])
                updates.validate()
                self.assertEqual(
                    {
                        "updates": [
                            {"@type": "updateType", "newType": fileset_type.value}
                        ]
                    },
                    json.loads(updates.to_json()),
                )
        self.assertNotEqual(
            FilesetChange.update_type(Fileset.Type.MANAGED),
            FilesetChange.update_type(Fileset.Type.EXTERNAL),
        )
        self.assertNotEqual(FilesetChange.update_type(Fileset.Type.MANAGED), None)
        self.assertNotEqual(
            FilesetChange.update_type(Fileset.Type.MANAGED),
            FilesetChange.rename("fileset"),
        )

    def test_reject_invalid_type(self):
        for invalid in (None, "managed", "invalid", 1):
            with self.subTest(invalid=invalid):
                with self.assertRaises(ValueError):
                    FilesetChange.update_type(invalid)
                with self.assertRaises(ValueError):
                    FilesetChange.UpdateFilesetType(invalid)
                request = FilesetUpdateRequest.UpdateFilesetTypeRequest(invalid)
                with self.assertRaises(ValueError):
                    FilesetUpdatesRequest([request]).validate()
                with self.assertRaises(ValueError):
                    request.fileset_change()
