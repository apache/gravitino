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

package org.apache.gravitino.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Test;

/** Tests for {@link CollectionUtils}. */
public class TestCollectionUtils {

  @Test
  void unorderedHashCodeIsOrderIndependent() {
    assertEquals(
        CollectionUtils.unorderedHashCode(Arrays.asList("a", "b", "c")),
        CollectionUtils.unorderedHashCode(Arrays.asList("c", "b", "a")),
        "Reordering the elements must not change the hash");
  }

  @Test
  void unorderedHashCodeAgreesWithBagEquality() {
    // isEqualCollection uses bag semantics: cardinality matters, order does not.
    assertEquals(
        CollectionUtils.unorderedHashCode(Arrays.asList(1, 2, 2)),
        CollectionUtils.unorderedHashCode(Arrays.asList(2, 1, 2)),
        "Bag-equal collections must hash the same");
  }

  @Test
  void nullAndEmptyCollectionsHashToZero() {
    assertEquals(0, CollectionUtils.unorderedHashCode(null));
    assertEquals(0, CollectionUtils.unorderedHashCode(Collections.emptyList()));
  }

  @Test
  void unorderedHashCodeToleratesNullElements() {
    // A null element must be treated as zero rather than throwing, matching List.hashCode.
    assertEquals(
        CollectionUtils.unorderedHashCode(Arrays.asList("a", null)),
        CollectionUtils.unorderedHashCode(Arrays.asList(null, "a")),
        "Null elements must be hashed safely and order-independently");
  }
}
