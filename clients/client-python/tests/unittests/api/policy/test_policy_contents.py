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

from gravitino.api.metadata_object import MetadataObject
from gravitino.api.policy import IcebergDataCompactionContent, PolicyContents
from gravitino.exceptions.base import IllegalArgumentException


class TestPolicyChange(unittest.TestCase):
    def test_iceberg_compaction_content_uses_defaults(self) -> None:
        content = PolicyContents.iceberg_data_compaction()

        self.assertEqual(
            IcebergDataCompactionContent.DEFAULT_MIN_DATA_FILE_MSE,
            content.rules().get(IcebergDataCompactionContent.MIN_DATA_FILE_MSE_KEY),
        )
        self.assertEqual(
            IcebergDataCompactionContent.DEFAULT_MIN_DELETE_FILE_NUMBER,
            content.rules().get(
                IcebergDataCompactionContent.MIN_DELETE_FILE_NUMBER_KEY
            ),
        )
        self.assertEqual(
            1,
            content.rules().get(IcebergDataCompactionContent.DATA_FILE_MSE_WEIGHT_KEY),
        )
        self.assertEqual(
            100,
            content.rules().get(
                IcebergDataCompactionContent.DELETE_FILE_NUMBER_WEIGHT_KEY
            ),
        )
        self.assertEqual(
            50,
            content.rules().get(IcebergDataCompactionContent.MAX_PARTITION_NUM_KEY),
        )
        self.assertIsNone(content.rules().get("job.options.target-file-size-bytes"))
        self.assertIsNone(content.rules().get("job.options.min-input-files"))
        self.assertIsNone(content.rules().get("job.options.delete-file-threshold"))

        hash(content)

    def test_iceberg_compaction_content_generates_optimizer_fields(self) -> None:
        content = PolicyContents.iceberg_data_compaction(
            1000, 1, {"target-file-size-bytes": "1048576", "min-input-files": "1"}
        )

        self.assertEquals(
            "iceberg-data-compaction", content.properties().get("strategy.type")
        )
        self.assertEquals(
            "builtin-iceberg-rewrite-data-files",
            content.properties().get("job.template-name"),
        )
        self.assertEquals(1000, content.rules().get("minDataFileMse"))
        self.assertEquals(1, content.rules().get("minDeleteFileNumber"))
        self.assertEquals(1, content.rules().get("dataFileMseWeight"))
        self.assertEquals(100, content.rules().get("deleteFileNumberWeight"))
        self.assertEquals(50, content.rules().get("max-partition-num"))
        self.assertEquals(
            "custom-data-file-mse >= minDataFileMse || custom-delete-file-number >= minDeleteFileNumber",
            content.rules().get("trigger-expr"),
        )

        self.assertEquals(
            "custom-data-file-mse * dataFileMseWeight"
            + " + custom-delete-file-number * deleteFileNumberWeight",
            content.rules().get("score-expr"),
        )
        self.assertEquals(
            "1048576", content.rules().get("job.options.target-file-size-bytes")
        )
        self.assertEquals("1", content.rules().get("job.options.min-input-files"))
        self.assertEquals(
            {
                MetadataObject.Type.CATALOG,
                MetadataObject.Type.SCHEMA,
                MetadataObject.Type.TABLE,
            },
            content.supportedObjectTypes(),
        )

    def test_iceberg_compaction_content_supports_custom_weights(self) -> None:
        content = PolicyContents.iceberg_data_compaction(
            1000,
            1,
            3,
            200,
            88,
            {
                "target-file-size-bytes": "1048576",
                "min-input-files": "1",
            },
        )

        self.assertEquals(3, content.rules().get("dataFileMseWeight"))
        self.assertEquals(200, content.rules().get("deleteFileNumberWeight"))
        self.assertEquals(88, content.rules().get("max-partition-num"))

        with self.assertRaises(IllegalArgumentException):
            content.validate()

    def test_iceberg_compaction_content_rejects_invalid_rewrite_option_key(
        self,
    ) -> None:
        content = PolicyContents.iceberg_data_compaction(
            1000,
            1,
            {"job.options.target-file-size-bytes": "1048576"},
        )

        with self.assertRaises(IllegalArgumentException):
            content.validate()

    def test_iceberg_compaction_content_rejects_invalid_max_partition_num(self):
        content = PolicyContents.iceberg_data_compaction(
            1000, 1, 2, 10, 0, {"target-file-size-bytes": "1048576"}
        )

        with self.assertRaises(IllegalArgumentException):
            content.validate()
