"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

import unittest

from gravitino.dto.responses.metalake_response import MetalakeResponse


class TestMetalake(unittest.TestCase):
    def test_from_json_metalake_response(self):
        str_json = (
            b'{"code":0,"metalake":{"name":"example_name18","comment":"This is a sample comment",'
            b'"properties":{"key1":"value1","key2":"value2"},'
            b'"audit":{"creator":"anonymous","createTime":"2024-04-05T10:10:35.218Z"}}}'
        )
        metalake_response = MetalakeResponse.from_json(str_json, infer_missing=True)
        self.assertEqual(metalake_response.code(), 0)
        self.assertIsNotNone(metalake_response._metalake)
        self.assertEqual(metalake_response._metalake.name(), "example_name18")
        self.assertEqual(
            metalake_response._metalake.audit_info().creator(), "anonymous"
        )

    def test_from_error_json_metalake_response(self):
        str_json = (
            b'{"code":0, "undefine-key1":"undefine-value1", '
            b'"metalake":{"undefine-key2":1, "name":"example_name18","comment":"This is a sample comment",'
            b'"properties":{"key1":"value1","key2":"value2"},'
            b'"audit":{"creator":"anonymous","createTime":"2024-04-05T10:10:35.218Z"}}}'
        )
        metalake_response = MetalakeResponse.from_json(str_json, infer_missing=True)
        self.assertEqual(metalake_response.code(), 0)
        self.assertIsNotNone(metalake_response._metalake)
        self.assertEqual(metalake_response._metalake.name(), "example_name18")
        self.assertEqual(
            metalake_response._metalake.audit_info().creator(), "anonymous"
        )
