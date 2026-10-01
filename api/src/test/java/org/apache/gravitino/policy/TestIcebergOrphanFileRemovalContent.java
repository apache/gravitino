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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestIcebergOrphanFileRemovalContent {
  @Test
  void defaultsAndIdentity() {
    IcebergOrphanFileRemovalContent content =
        (IcebergOrphanFileRemovalContent) PolicyContents.icebergOrphanFileRemoval();
    content.validate();
    Assertions.assertEquals(3, content.olderThanDays());
    Assertions.assertFalse(content.dryRun());
    Assertions.assertNull(content.location());
    Assertions.assertEquals(content, PolicyContents.icebergOrphanFileRemoval(3, null, false));
    Assertions.assertEquals(
        content.hashCode(), PolicyContents.icebergOrphanFileRemoval().hashCode());
    Assertions.assertNotEquals(content, PolicyContents.icebergOrphanFileRemoval(4, null, false));
    Assertions.assertEquals(
        Policy.BuiltInType.ICEBERG_ORPHAN_FILE_REMOVAL,
        Policy.BuiltInType.fromPolicyType("system_iceberg_orphan_file_removal"));
    Assertions.assertEquals(
        IcebergOrphanFileRemovalContent.class,
        Policy.BuiltInType.ICEBERG_ORPHAN_FILE_REMOVAL.contentClass());
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> content.rules().put("job.options.dryRun", true));
  }

  @Test
  void validatesRetentionAndLocation() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> PolicyContents.icebergOrphanFileRemoval(0, null, true).validate());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> PolicyContents.icebergOrphanFileRemoval(-1, null, false).validate());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> PolicyContents.icebergOrphanFileRemoval(1, " ", true).validate());
    PolicyContent content =
        PolicyContents.icebergOrphanFileRemoval(1, "s3://bucket/table/data", true);
    content.validate();
    Assertions.assertEquals("s3://bucket/table/data", content.rules().get("job.options.location"));
  }

  @Test
  void rejectsLocationWhitespace() {
    for (String location :
        new String[] {
          "  s3://bucket/table/data",
          "s3://bucket/table/data ",
          "\ts3://bucket/table/data",
          "s3://bucket/table/data\n",
          "\u2003s3://bucket/table/data",
          "s3://bucket/table/data\u2003",
          "\u2003"
        }) {
      IllegalArgumentException error =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () -> PolicyContents.icebergOrphanFileRemoval(3, location, true).validate());
      Assertions.assertTrue(error.getMessage().contains("location"));
    }
  }

  @Test
  void validatesRetentionUpperBoundary() {
    long maximum = IcebergOrphanFileRemovalContent.MAX_OLDER_THAN_DAYS;
    Assertions.assertDoesNotThrow(
        () -> PolicyContents.icebergOrphanFileRemoval(maximum, null, true).validate());
    for (long days : new long[] {maximum + 1, Long.MAX_VALUE}) {
      IllegalArgumentException error =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () -> PolicyContents.icebergOrphanFileRemoval(days, null, true).validate());
      Assertions.assertTrue(error.getMessage().contains("olderThanDays"));
    }
  }
}
