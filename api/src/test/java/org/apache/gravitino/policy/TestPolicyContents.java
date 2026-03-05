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
  void testIcebergCompactionContentGeneratesOptimizerFields() {
    PolicyContents.IcebergCompactionContent content =
        (PolicyContents.IcebergCompactionContent)
            PolicyContents.icebergCompaction(
                1000L, 1L, Map.of("target-file-size-bytes", "1048576", "min-input-files", "1"));

    Assertions.assertEquals("compaction", content.properties().get("strategy.type"));
    Assertions.assertEquals(
        "builtin-iceberg-rewrite-data-files", content.properties().get("job.template-name"));
    Assertions.assertEquals(1000L, content.rules().get("minDatafileMse"));
    Assertions.assertEquals(1L, content.rules().get("minDeleteFileNumber"));
    Assertions.assertEquals(1L, content.rules().get("datafileMseWeight"));
    Assertions.assertEquals(100L, content.rules().get("deleteFileNumberWeight"));
    Assertions.assertEquals(
        "custom-datafile_mse > minDatafileMse || custom-delete_file_number > minDeleteFileNumber",
        content.rules().get("trigger-expr"));
    Assertions.assertEquals(
        "custom-datafile_mse * datafileMseWeight / 100"
            + " + custom-delete_file_number * deleteFileNumberWeight",
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
    PolicyContents.IcebergCompactionContent content =
        (PolicyContents.IcebergCompactionContent)
            PolicyContents.icebergCompaction(
                1000L,
                1L,
                3L,
                200L,
                Map.of("target-file-size-bytes", "1048576", "min-input-files", "1"));

    Assertions.assertEquals(3L, content.rules().get("datafileMseWeight"));
    Assertions.assertEquals(200L, content.rules().get("deleteFileNumberWeight"));
    Assertions.assertDoesNotThrow(content::validate);
  }

  @Test
  void testIcebergCompactionContentRejectsInvalidRewriteOptionKey() {
    PolicyContents.IcebergCompactionContent content =
        (PolicyContents.IcebergCompactionContent)
            PolicyContents.icebergCompaction(
                1000L, 1L, Map.of("job.options.target-file-size-bytes", "1048576"));

    IllegalArgumentException exception =
        Assertions.assertThrows(IllegalArgumentException.class, content::validate);
    Assertions.assertTrue(exception.getMessage().contains("must not start with"));
  }
}
