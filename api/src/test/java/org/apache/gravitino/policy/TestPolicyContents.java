/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.policy;

import com.google.common.collect.ImmutableSet;
import java.util.Map;
import org.apache.gravitino.MetadataObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPolicyContents {

  @Test
  void testIcebergCompactionContentUsesDefaults() {
    IcebergDataCompactionContent content =
        (IcebergDataCompactionContent) PolicyContents.icebergDataCompaction();

    Assertions.assertEquals(
        IcebergDataCompactionContent.DEFAULT_MIN_DATA_FILE_MSE,
        content.rules().get("minDataFileMse"));
    Assertions.assertEquals(
        IcebergDataCompactionContent.DEFAULT_MIN_DELETE_FILE_NUMBER,
        content.rules().get("minDeleteFileNumber"));
    Assertions.assertEquals(1L, content.rules().get("dataFileMseWeight"));
    Assertions.assertEquals(100L, content.rules().get("deleteFileNumberWeight"));
    Assertions.assertEquals(50L, content.rules().get("max-partition-num"));
    Assertions.assertNull(content.rules().get("job.options.target-file-size-bytes"));
    Assertions.assertNull(content.rules().get("job.options.min-input-files"));
    Assertions.assertNull(content.rules().get("job.options.delete-file-threshold"));
  }

  @Test
  void testIcebergCompactionContentGeneratesOptimizerFields() {
    IcebergDataCompactionContent content =
        (IcebergDataCompactionContent)
            PolicyContents.icebergDataCompaction(
                1000L, 1L, Map.of("target-file-size-bytes", "1048576", "min-input-files", "1"));

    Assertions.assertEquals("iceberg-data-compaction", content.properties().get("strategy.type"));
    Assertions.assertEquals(
        "builtin-iceberg-rewrite-data-files", content.properties().get("job.template-name"));
    Assertions.assertEquals(1000L, content.rules().get("minDataFileMse"));
    Assertions.assertEquals(1L, content.rules().get("minDeleteFileNumber"));
    Assertions.assertEquals(1L, content.rules().get("dataFileMseWeight"));
    Assertions.assertEquals(100L, content.rules().get("deleteFileNumberWeight"));
    Assertions.assertEquals(50L, content.rules().get("max-partition-num"));
    Assertions.assertEquals(
        "custom-data-file-mse >= minDataFileMse || custom-delete-file-number >= minDeleteFileNumber",
        content.rules().get("trigger-expr"));
    Assertions.assertEquals(
        "custom-data-file-mse * dataFileMseWeight"
            + " + custom-delete-file-number * deleteFileNumberWeight",
        content.rules().get("score-expr"));
    Assertions.assertEquals("1048576", content.rules().get("job.options.target-file-size-bytes"));
    Assertions.assertEquals("1", content.rules().get("job.options.min-input-files"));
    Assertions.assertEquals(
        ImmutableSet.of(
            MetadataObject.Type.CATALOG, MetadataObject.Type.SCHEMA, MetadataObject.Type.TABLE),
        content.supportedObjectTypes());
  }

  @Test
  void testIcebergCompactionContentSupportsCustomWeights() {
    IcebergDataCompactionContent content =
        (IcebergDataCompactionContent)
            PolicyContents.icebergDataCompaction(
                1000L,
                1L,
                3L,
                200L,
                88L,
                Map.of("target-file-size-bytes", "1048576", "min-input-files", "1"));

    Assertions.assertEquals(3L, content.rules().get("dataFileMseWeight"));
    Assertions.assertEquals(200L, content.rules().get("deleteFileNumberWeight"));
    Assertions.assertEquals(88L, content.rules().get("max-partition-num"));
    Assertions.assertDoesNotThrow(content::validate);
  }

  @Test
  void testIcebergCompactionContentRejectsInvalidRewriteOptionKey() {
    IcebergDataCompactionContent content =
        (IcebergDataCompactionContent)
            PolicyContents.icebergDataCompaction(
                1000L, 1L, Map.of("job.options.target-file-size-bytes", "1048576"));

    IllegalArgumentException exception =
        Assertions.assertThrows(IllegalArgumentException.class, content::validate);
    Assertions.assertTrue(exception.getMessage().contains("must not start with"));
  }

  @Test
  void testIcebergCompactionContentRejectsInvalidMaxPartitionNum() {
    IcebergDataCompactionContent content =
        (IcebergDataCompactionContent)
            PolicyContents.icebergDataCompaction(
                1000L, 1L, 2L, 10L, 0L, Map.of("target-file-size-bytes", "1048576"));

    IllegalArgumentException exception =
        Assertions.assertThrows(IllegalArgumentException.class, content::validate);
    Assertions.assertTrue(exception.getMessage().contains("maxPartitionNum"));
  }
}
