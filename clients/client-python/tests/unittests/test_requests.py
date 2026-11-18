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
from typing import cast

from gravitino.api.function.function_type import FunctionType
from gravitino.api.rel.types.types import Types
from gravitino.dto.function.function_column_dto import FunctionColumnDTO
from gravitino.dto.function.function_definition_dto import FunctionDefinitionDTO
from gravitino.dto.function.function_impl_dto import SQLImplDTO
from gravitino.dto.function.function_param_dto import FunctionParamDTO
from gravitino.dto.requests.add_partitions_request import AddPartitionsRequest
from gravitino.dto.requests.function_register_request import FunctionRegisterRequest
from gravitino.dto.requests.function_update_request import (
    AddDefinitionRequest,
    AddImplRequest,
    RemoveDefinitionRequest,
    RemoveImplRequest,
    UpdateCommentRequest,
    UpdateImplRequest,
)
from gravitino.dto.requests.function_updates_request import FunctionUpdatesRequest
from gravitino.dto.requests.table_create_request import TableCreateRequest
from gravitino.exceptions.base import IllegalArgumentException
from tests.unittests.fixtures.table_fixtures import TABLE_CREATE_REQUEST_JSON_STRING


class TestRequests(unittest.TestCase):
    def test_add_partitions_request(self):
        json_str = """
            {
                "partitions": [
                    {
                        "type": "identity",
                        "name": "partition_1",
                        "fieldNames": [["id"]],
                        "values": [
                            {
                                "type": "literal",
                                "dataType": "integer",
                                "value": "0"
                            }
                        ],
                        "properties": {
                            "key1": "value1",
                            "key2": "value2"
                        }
                    }
                ]
            }
        """
        partitions = json.loads(json_str)
        req = AddPartitionsRequest.from_json(json_str)
        req_dict = cast(dict, req.to_dict())
        self.assertListEqual(req_dict["partitions"], partitions["partitions"])

        multiple_partitions_json = """
            {
                "partitions": [
                    {
                        "type": "identity",
                        "name": "partition_1",
                        "fieldNames": [["id"]],
                        "values": [
                            {
                                "type": "literal",
                                "dataType": "integer",
                                "value": "0"
                            }
                        ]
                    },
                    {
                        "type": "identity",
                        "name": "partition_2",
                        "fieldNames": [["id"]],
                        "values": [
                            {
                                "type": "literal",
                                "dataType": "integer",
                                "value": "1"
                            }
                        ]
                    }
                ]
            }
        """
        exceptions = {
            "partitions must not be null": '{"partitions": null}',
            "Haven't yet implemented multiple partitions": multiple_partitions_json,
        }
        for exception_str, json_str in exceptions.items():
            with self.assertRaisesRegex(IllegalArgumentException, exception_str):
                req = AddPartitionsRequest.from_json(json_str)
                req.validate()

    def test_table_create_request(self):
        req = TableCreateRequest.from_json(TABLE_CREATE_REQUEST_JSON_STRING)
        req.validate()

        multiple_auto_increment_json_str = """
            {
                "name": "example_table",
                "comment": "This is an example table",
                "columns": [
                    {
                        "name": "id",
                        "type": "integer",
                        "comment": "id column comment",
                        "nullable": false,
                        "autoIncrement": true,
                        "defaultValue": {
                            "type": "literal",
                            "dataType": "integer",
                            "value": "-1"
                        }
                    },
                    {
                        "name": "age",
                        "type": "integer",
                        "comment": "age column comment",
                        "nullable": false,
                        "autoIncrement": true,
                        "defaultValue": {
                            "type": "literal",
                            "dataType": "integer",
                            "value": "-1"
                        }
                    }
                ]
            }
        """
        exceptions = {
            '"name" field is required and cannot be empty': '{"name":"","columns":[]}',
            "Only one column can be auto-incremented.": multiple_auto_increment_json_str,
        }
        for exception_str, json_str in exceptions.items():
            with self.assertRaisesRegex(IllegalArgumentException, exception_str):
                req = TableCreateRequest.from_json(json_str)
                req.validate()

    def test_function_register_request(self):
        """Test FunctionRegisterRequest."""
        scalar_definition = FunctionDefinitionDTO(
            _parameters=[],
            _return_type=Types.IntegerType.get(),
            _impls=[],
        )
        table_definition = FunctionDefinitionDTO(
            _parameters=[],
            _return_columns=[
                FunctionColumnDTO(_name="out_col", _data_type=Types.IntegerType.get())
            ],
            _impls=[],
        )

        req = FunctionRegisterRequest(
            name="func1",
            function_type=FunctionType.SCALAR,
            deterministic=True,
            definitions=[scalar_definition],
            comment="comment",
        )
        req.validate()
        json_data = json.loads(req.to_json())
        self.assertEqual("func1", json_data["name"])
        self.assertEqual("scalar", json_data["functionType"])
        self.assertTrue(json_data["deterministic"])
        self.assertEqual("comment", json_data["comment"])

        table_req = FunctionRegisterRequest(
            name="func_table",
            function_type=FunctionType.TABLE,
            deterministic=False,
            definitions=[table_definition],
        )
        table_req.validate()

        # Test request without required fields and type-specific validation
        with self.assertRaises(IllegalArgumentException):
            FunctionRegisterRequest(
                name="func1",
                function_type=None,
                deterministic=True,
                definitions=[],
            ).validate()
        with self.assertRaises(IllegalArgumentException):
            FunctionRegisterRequest(
                name="func1",
                function_type=FunctionType.SCALAR,
                deterministic=True,
                definitions=[],
            ).validate()
        with self.assertRaises(IllegalArgumentException):
            FunctionRegisterRequest(
                name="func1",
                function_type=FunctionType.TABLE,
                deterministic=True,
                definitions=[scalar_definition],
            ).validate()
        with self.assertRaises(IllegalArgumentException):
            FunctionRegisterRequest(
                name="func1",
                function_type=FunctionType.SCALAR,
                deterministic=True,
                definitions=[table_definition],
            ).validate()
        with self.assertRaises(IllegalArgumentException):
            FunctionRegisterRequest(
                name="func1",
                function_type=FunctionType.AGGREGATE,
                deterministic=True,
                definitions=[table_definition],
            ).validate()

    def test_function_update_request(self):
        """Test FunctionUpdateRequest."""
        parameters = [
            FunctionParamDTO(_name="p1", _data_type=Types.IntegerType.get()),
        ]
        definition = FunctionDefinitionDTO(
            _parameters=parameters,
            _return_type=Types.IntegerType.get(),
            _impls=[],
        )
        impl = SQLImplDTO(
            _runtime="SPARK",
            _sql="SELECT 1",
            _resources=None,
            _properties={},
        )

        comment_req = UpdateCommentRequest("new comment")
        json_data = json.loads(comment_req.to_json())
        self.assertEqual("updateComment", json_data["@type"])
        self.assertEqual("new comment", json_data["newComment"])
        comment_req.validate()
        UpdateCommentRequest(None).validate()
        UpdateCommentRequest("   ").validate()

        AddDefinitionRequest(definition).validate()
        with self.assertRaises(IllegalArgumentException):
            AddDefinitionRequest(None).validate()

        RemoveDefinitionRequest(parameters).validate()
        with self.assertRaises(IllegalArgumentException):
            RemoveDefinitionRequest(None).validate()

        AddImplRequest(parameters, impl).validate()
        with self.assertRaises(IllegalArgumentException):
            AddImplRequest(parameters, None).validate()

        UpdateImplRequest(parameters, "SPARK", impl).validate()
        with self.assertRaises(IllegalArgumentException):
            UpdateImplRequest(parameters, None, impl).validate()

        RemoveImplRequest(parameters, "SPARK").validate()
        with self.assertRaises(IllegalArgumentException):
            RemoveImplRequest(parameters, None).validate()

    def test_function_updates_request(self):
        """Test FunctionUpdatesRequest."""
        updates = [UpdateCommentRequest("new comment")]
        req = FunctionUpdatesRequest(updates)
        req.validate()
        self.assertEqual(1, len(req.updates()))
        self.assertIsInstance(req.updates()[0], UpdateCommentRequest)
        with self.assertRaises(ValueError):
            FunctionUpdatesRequest(None).validate()
        FunctionUpdatesRequest([]).validate()
