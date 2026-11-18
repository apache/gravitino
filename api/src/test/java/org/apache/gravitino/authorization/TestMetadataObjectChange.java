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
package org.apache.gravitino.authorization;

import com.google.common.collect.Lists;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestMetadataObjectChange {

  private MetadataObject tableObject() {
    return MetadataObjects.of(
        Lists.newArrayList("catalog", "schema", "table"), MetadataObject.Type.TABLE);
  }

  @Test
  void testRemoveEqualsWithDifferentLocations() {
    MetadataObjectChange remove1 =
        MetadataObjectChange.remove(tableObject(), Lists.newArrayList("loc1"));
    MetadataObjectChange remove2 =
        MetadataObjectChange.remove(tableObject(), Lists.newArrayList("loc2"));

    // Before the fix, RemoveMetadataObject.equals cast the argument to RenameMetadataObject and
    // threw ClassCastException here.
    Assertions.assertNotEquals(remove1, remove2);
  }

  @Test
  void testRemoveEqualsSameLocations() {
    MetadataObjectChange remove1 =
        MetadataObjectChange.remove(tableObject(), Lists.newArrayList("loc1"));
    MetadataObjectChange remove2 =
        MetadataObjectChange.remove(tableObject(), Lists.newArrayList("loc1"));

    Assertions.assertEquals(remove1, remove2);
    Assertions.assertEquals(remove1.hashCode(), remove2.hashCode());
  }

  @Test
  void testRemoveInHashSet() {
    Set<MetadataObjectChange> changes =
        new HashSet<>(
            Lists.newArrayList(
                MetadataObjectChange.remove(tableObject(), Lists.newArrayList("loc1"))));

    // HashSet.contains delegates to equals; before the fix this threw ClassCastException.
    Assertions.assertDoesNotThrow(
        () ->
            changes.contains(
                MetadataObjectChange.remove(tableObject(), Lists.newArrayList("loc2"))));
    Assertions.assertFalse(
        changes.contains(MetadataObjectChange.remove(tableObject(), Lists.newArrayList("loc2"))));
    Assertions.assertTrue(
        changes.contains(MetadataObjectChange.remove(tableObject(), Lists.newArrayList("loc1"))));
  }

  @Test
  void testRemoveEqualsWithNullLocations() {
    // remove(mo, null) is constructible; comparing two such instances must not throw NPE.
    MetadataObjectChange remove1 = MetadataObjectChange.remove(tableObject(), null);
    MetadataObjectChange remove2 = MetadataObjectChange.remove(tableObject(), null);

    Assertions.assertEquals(remove1, remove2);
    Assertions.assertEquals(remove1.hashCode(), remove2.hashCode());

    MetadataObjectChange removeWithLocations =
        MetadataObjectChange.remove(tableObject(), List.of("a"));
    Assertions.assertNotEquals(remove1, removeWithLocations);
    Assertions.assertNotEquals(removeWithLocations, remove1);
  }

  @Test
  void testRemoveDefensiveCopyStableHashKey() {
    List<String> mutableList = Lists.newArrayList("loc1");
    MetadataObjectChange.RemoveMetadataObject remove =
        (MetadataObjectChange.RemoveMetadataObject)
            MetadataObjectChange.remove(tableObject(), mutableList);

    Set<MetadataObjectChange> changes = new HashSet<>();
    changes.add(remove);

    // Mutating the caller's list after remove(...) must not change the object's hash key nor be
    // reflected in getLocations().
    mutableList.add("x");

    Assertions.assertTrue(changes.contains(remove));
    Assertions.assertEquals(Lists.newArrayList("loc1"), remove.getLocations());
  }
}
