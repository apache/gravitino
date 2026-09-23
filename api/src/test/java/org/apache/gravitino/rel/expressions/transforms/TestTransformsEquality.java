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
package org.apache.gravitino.rel.expressions.transforms;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTransformsEquality {

  @Test
  void testDifferentSingleFieldTransformTypesAreNotEqual() {
    // Before the fix, SingleFieldTransform.equals used an instanceof check, so transforms of
    // different types over the same field compared equal.
    Assertions.assertNotEquals(Transforms.identity("ts"), Transforms.year("ts"));
    Assertions.assertNotEquals(Transforms.year("ts"), Transforms.month("ts"));
    Assertions.assertNotEquals(Transforms.month("ts"), Transforms.day("ts"));
    Assertions.assertNotEquals(Transforms.day("ts"), Transforms.hour("ts"));
    Assertions.assertNotEquals(Transforms.year("ts"), Transforms.hour("ts"));
  }

  @Test
  void testSameSingleFieldTransformTypesAreEqual() {
    Assertions.assertEquals(Transforms.year("ts"), Transforms.year("ts"));
    Assertions.assertEquals(Transforms.identity("ts"), Transforms.identity("ts"));
    Assertions.assertNotEquals(Transforms.year("ts"), Transforms.year("other"));
  }

  @Test
  void testSameTypeSetSemantics() {
    Set<Transform> transforms = new HashSet<>(Arrays.asList(Transforms.year("ts")));

    // Before the fix, a Set silently deduplicated year(ts) against identity(ts).
    Assertions.assertFalse(transforms.contains(Transforms.identity("ts")));
    Assertions.assertTrue(transforms.contains(Transforms.year("ts")));
  }
}
