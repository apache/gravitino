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

package org.apache.gravitino.maintenance.optimizer.recommender.util;

import java.util.List;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionEntry;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.common.PartitionEntryImpl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestPartitionUtils {

  @Test
  void testEncodePartitionPathEmitsJsonArray() {
    List<PartitionEntry> entries =
        List.of(new PartitionEntryImpl("p1", "v1"), new PartitionEntryImpl("p2", "v2"));
    PartitionPath path = PartitionPath.of(entries);

    String encoded = PartitionUtils.encodePartitionPath(path);

    Assertions.assertEquals("[{\"p1\":\"v1\"},{\"p2\":\"v2\"}]", encoded);
  }

  @Test
  void testDecodePartitionPathParsesEntries() {
    String encoded = "[{\"p1\":\"v1\"},{\"p2\":\"v2\"}]";

    PartitionPath path = PartitionUtils.decodePartitionPath(encoded);

    Assertions.assertEquals(2, path.entries().size());
    Assertions.assertEquals("p1", path.entries().get(0).partitionName());
    Assertions.assertEquals("v1", path.entries().get(0).partitionValue());
    Assertions.assertEquals("p2", path.entries().get(1).partitionName());
    Assertions.assertEquals("v2", path.entries().get(1).partitionValue());
  }

  @Test
  void testDecodePartitionPathRejectsBlankInput() {
    IllegalArgumentException ex =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> PartitionUtils.decodePartitionPath(" "));
    Assertions.assertEquals("encodedPartitionPath must not be blank", ex.getMessage());
  }

  @Test
  void testEncodeThenDecodePreservesEntries() {
    List<PartitionEntry> entries =
        List.of(new PartitionEntryImpl("p1", "v1"), new PartitionEntryImpl("p2", "v2"));
    PartitionPath original = PartitionPath.of(entries);

    String encoded = PartitionUtils.encodePartitionPath(original);
    PartitionPath decoded = PartitionUtils.decodePartitionPath(encoded);

    Assertions.assertEquals(original, decoded);
  }
}
