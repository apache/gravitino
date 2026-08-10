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

import unittest
from datetime import datetime, timedelta, timezone

from gravitino.dto.audit_dto import AuditDTO


class TestAuditDTO(unittest.TestCase):
    def test_deserialize_datetime(self):
        audit = AuditDTO.from_json("""{
              "creator": "alice",
              "createTime": "2024-04-05T10:10:35.218Z",
              "lastModifier": "bob",
              "lastModifiedTime": "2024-04-05T18:10:35.218+08:00"
            }""")

        expected = datetime(2024, 4, 5, 10, 10, 35, 218000, tzinfo=timezone.utc)
        self.assertEqual(expected, audit.create_time())
        self.assertEqual(expected, audit.last_modified_time())

    def test_deserialize_nanosecond_datetime(self):
        audit = AuditDTO.from_json(
            '{"creator":"alice","createTime":"2026-08-02T16:15:57.286603461Z"}'
        )

        self.assertEqual(
            datetime(2026, 8, 2, 16, 15, 57, 286603, tzinfo=timezone.utc),
            audit.create_time(),
        )

    def test_serialize_datetime(self):
        create_time = datetime(
            2024,
            4,
            5,
            18,
            10,
            35,
            218000,
            tzinfo=timezone(timedelta(hours=8)),
        )
        last_modified_time = datetime(
            2024,
            4,
            6,
            19,
            20,
            45,
            123000,
            tzinfo=timezone(timedelta(hours=8)),
        )
        audit = AuditDTO(
            _creator="alice",
            _create_time=create_time,
            _last_modifier="bob",
            _last_modified_time=last_modified_time,
        )

        self.assertEqual(
            datetime(2024, 4, 5, 10, 10, 35, 218000, tzinfo=timezone.utc),
            audit.create_time(),
        )
        self.assertEqual(
            datetime(2024, 4, 6, 11, 20, 45, 123000, tzinfo=timezone.utc),
            audit.last_modified_time(),
        )

        serialized = audit.to_dict()
        self.assertEqual("2024-04-05T10:10:35.218000Z", serialized["createTime"])
        self.assertEqual("2024-04-06T11:20:45.123000Z", serialized["lastModifiedTime"])

    def test_construct_with_iso_datetime(self):
        audit = AuditDTO(
            _creator="alice",
            _create_time="2024-04-05T10:10:35.218Z",
        )

        self.assertEqual(
            datetime(2024, 4, 5, 10, 10, 35, 218000, tzinfo=timezone.utc),
            audit.create_time(),
        )

    def test_none_fields(self):
        audit = AuditDTO()

        self.assertIsNone(audit.creator())
        self.assertIsNone(audit.create_time())
        self.assertIsNone(audit.last_modifier())
        self.assertIsNone(audit.last_modified_time())

        serialized = audit.to_dict()
        self.assertIsNone(serialized["creator"])
        self.assertIsNone(serialized["createTime"])
        self.assertIsNone(serialized["lastModifier"])
        self.assertIsNone(serialized["lastModifiedTime"])

    def test_invalid_datetime(self):
        with self.assertRaises(ValueError):
            AuditDTO.from_json('{"creator":"alice","createTime":"not-a-datetime"}')
