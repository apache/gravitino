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
package org.apache.gravitino.rel.indexes;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIndexes {

  @Test
  public void testIndexDefensiveCopies() {
    String[][] fieldNames = {{"col1"}};
    Map<String, String> properties = new HashMap<>();
    properties.put("key", "value");

    Index index = Indexes.of(Index.IndexType.PRIMARY_KEY, "idx", fieldNames, properties);

    // Before the fix, IndexImpl stored the caller's arrays and map by reference and returned them
    // directly, so external mutation leaked into the built index.
    fieldNames[0][0] = "mutated";
    properties.put("other", "injected");

    Assertions.assertArrayEquals(new String[] {"col1"}, index.fieldNames()[0]);
    Assertions.assertEquals(ImmutableMap.of("key", "value"), index.properties());
  }

  @Test
  public void testIndexReturnedCollectionsAreIsolated() {
    Index index =
        Indexes.of(
            Index.IndexType.UNIQUE_KEY,
            "idx",
            new String[][] {{"a", "b"}},
            ImmutableMap.of("k", "v"));

    // Mutating what the accessor returned must not change the index state either.
    index.fieldNames()[0][0] = "mutated";
    Assertions.assertEquals("a", index.fieldNames()[0][0]);
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> index.properties().put("k2", "v2"));
  }

  @Test
  public void testNullFieldNameRowIsTolerated() {
    // A null row must not blow up the defensive copy in either the constructor or fieldNames().
    String[][] fieldNames = {{"col1"}, null};

    Index index = Indexes.of(Index.IndexType.PRIMARY_KEY, "idx", fieldNames, ImmutableMap.of());

    String[][] returned = index.fieldNames();
    Assertions.assertArrayEquals(new String[] {"col1"}, returned[0]);
    Assertions.assertNull(returned[1]);

    // equals/hashCode must stay null-safe across the null row.
    Index same =
        Indexes.of(
            Index.IndexType.PRIMARY_KEY, "idx", new String[][] {{"col1"}, null}, ImmutableMap.of());
    Assertions.assertEquals(index, same);
    Assertions.assertEquals(index.hashCode(), same.hashCode());
  }

  @Test
  public void testNullPropertyValueIsTolerated() {
    // Storing the map by reference used to tolerate null values; the defensive copy must too, to
    // match the sibling TableChange.AddIndex contract.
    Map<String, String> properties = new HashMap<>();
    properties.put("key", null);

    Index index =
        Indexes.of(Index.IndexType.PRIMARY_KEY, "idx", new String[][] {{"col1"}}, properties);

    Assertions.assertTrue(index.properties().containsKey("key"));
    Assertions.assertNull(index.properties().get("key"));

    // Still an immutable snapshot: neither the returned map nor later caller mutations leak.
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> index.properties().put("k2", "v2"));
    properties.put("added", "later");
    Assertions.assertFalse(index.properties().containsKey("added"));
  }
}
