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

from __future__ import annotations

import json as _json
import unittest
from unittest.mock import MagicMock

from gravitino.api.metadata_object import MetadataObject
from gravitino.client.generic_tag import GenericTag
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.metadata_object_dto import MetadataObjectDTO
from gravitino.dto.tag_dto import TagDTO
from gravitino.exceptions.base import InternalError, NoSuchMetalakeException
from gravitino.utils import HTTPClient
from gravitino.utils.http_client import Response


class TestGenericTag(unittest.TestCase):
    METALAKE = "metalake1"

    TAG_DTO = (
        TagDTO.Builder()
        .name("tag1")
        .comment("comment1")
        .properties({"key1": "value1"})
        .inherited(True)
        .audit_info(AuditDTO(_creator="test", _create_time="2022-01-01T00:00:00Z"))
        .build()
    )

    @classmethod
    def setUpClass(cls):
        cls._rest_client = HTTPClient("http://localhost:8090")
        cls._metalake_name = "metalake_demo"

    def test_create_generic_tag(self):
        generic_tag = GenericTag(self.METALAKE, self.TAG_DTO, self._rest_client)
        self.assertEqual("tag1", generic_tag.name())
        self.assertEqual("comment1", generic_tag.comment())
        self.assertEqual({"key1": "value1"}, generic_tag.properties())
        self.assertEqual(True, generic_tag.inherited())
        self.assertEqual(
            AuditDTO(_creator="test", _create_time="2022-01-01T00:00:00Z"),
            generic_tag.audit_info(),
        )

    def test_generic_tag_associated_objects(self):
        # Test normal situation
        response_body = {
            "code": 0,
            "metadataObjects": [
                {
                    "fullName": "catalog1",
                    "type": "catalog",
                },
                {
                    "fullName": "catalog1.schema1",
                    "type": "schema",
                },
                {
                    "fullName": "catalog1.schema1.table1",
                    "type": "table",
                },
                {
                    "fullName": "catalog1.schema1.table1.column1",
                    "type": "column",
                },
            ],
        }

        expected_associated_objects = [
            MetadataObjectDTO.builder()
            .full_name("catalog1")
            .type(MetadataObject.Type.CATALOG)
            .build(),
            MetadataObjectDTO.builder()
            .full_name("catalog1.schema1")
            .type(MetadataObject.Type.SCHEMA)
            .build(),
            MetadataObjectDTO.builder()
            .full_name("catalog1.schema1.table1")
            .type(MetadataObject.Type.TABLE)
            .build(),
            MetadataObjectDTO.builder()
            .full_name("catalog1.schema1.table1.column1")
            .type(MetadataObject.Type.COLUMN)
            .build(),
        ]
        generic_tag = TestGenericTagEntity(
            self.METALAKE, self.TAG_DTO, self._rest_client, response_body
        )
        objects = generic_tag.associated_objects().objects()
        self.assertEqual(4, len(objects))
        self.assertEqual(4, generic_tag.count())

        for i, v in enumerate(objects):
            self.assertEqual(v, expected_associated_objects[i])
            self.assertEqual(v.full_name(), expected_associated_objects[i].full_name())
            self.assertEqual(v.type(), expected_associated_objects[i].type())
            self.assertEqual(v.parent(), expected_associated_objects[i].parent())
            self.assertEqual(v.name(), expected_associated_objects[i].name())

        # Test return empty array
        generic_tag = TestGenericTagEntity(
            self.METALAKE,
            self.TAG_DTO,
            self._rest_client,
            {
                "code": 0,
                "metadataObjects": [],
            },
        )
        objects = generic_tag.associated_objects().objects()
        self.assertEqual(0, len(objects))
        self.assertEqual(0, generic_tag.count())

        # Test throw NoSuchMetalakeException
        with self.assertRaises(NoSuchMetalakeException):
            generic_tag = TestGenericTagEntity(
                self.METALAKE,
                self.TAG_DTO,
                self._rest_client,
                {
                    "code": 0,
                    "metadataObjects": [],
                },
                NoSuchMetalakeException,
            )
            generic_tag.associated_objects().objects()

        # Test throw internal error
        with self.assertRaises(InternalError):
            generic_tag = TestGenericTagEntity(
                self.METALAKE,
                self.TAG_DTO,
                self._rest_client,
                {
                    "code": 0,
                    "metadataObjects": [],
                },
                InternalError,
            )
            generic_tag.associated_objects().objects()

    def test_hash_and_equal(self) -> None:
        tag_dto1 = (
            TagDTO.Builder()
            .name("tag1")
            .comment("comment1")
            .properties({"key1": "value1"})
            .inherited(True)
            .audit_info(AuditDTO(_creator="test", _create_time="2022-01-01T00:00:00Z"))
            .build()
        )
        generic_tag1 = GenericTag(self.METALAKE, tag_dto1, self._rest_client)

        tag_dto2 = (
            TagDTO.Builder()
            .name("tag1")
            .comment("comment1")
            .properties({"key1": "value1"})
            .inherited(True)
            .audit_info(AuditDTO(_creator="test", _create_time="2022-01-01T00:00:00Z"))
            .build()
        )
        generic_tag2 = GenericTag(self.METALAKE, tag_dto2, self._rest_client)

        tag_dto3 = (
            TagDTO.Builder()
            .name("tag1")
            .comment("comment1")
            .properties({"key1": "value1"})
            .inherited(True)
            .audit_info(AuditDTO(_creator="test", _create_time="2022-01-02T00:00:00Z"))
            .build()
        )
        generic_tag3 = GenericTag(self.METALAKE, tag_dto3, self._rest_client)

        self.assertEqual(generic_tag1, generic_tag2)
        self.assertEqual(hash(generic_tag1), hash(generic_tag2))

        self.assertNotEqual(generic_tag1, generic_tag3)
        self.assertNotEqual(hash(generic_tag1), hash(generic_tag3))


class TestGenericTagEntity(GenericTag):
    def __init__(
        self,
        metalake,
        tag_dto,
        client,
        dump_object=None,
        throw_error=None,
    ) -> None:
        super().__init__(
            metalake,
            tag_dto,
            client,
        )
        self.dump_object = dump_object
        self.throw_error = throw_error

    def get_response(self, url, _=None):
        if self.throw_error is not None:
            raise self.throw_error(f"Raise {self.throw_error.__name__} Error")

        mock_response = MagicMock()
        mock_response.getcode.return_value = 0
        mock_response.read.return_value = _json.dumps(self.dump_object).encode("utf-8")

        mock_response.url = url
        mock_response.info.return_value = {"Content-Type": "application/json"}

        return Response(mock_response)
