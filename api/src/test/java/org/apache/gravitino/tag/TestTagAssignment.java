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

package org.apache.gravitino.tag;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTagAssignment {

  @Test
  void testWithoutValuesAssignment() {
    TagAssignment assignment = TagAssignment.withoutValues();

    Assertions.assertFalse(assignment.hasValues());
    Assertions.assertArrayEquals(new String[0], assignment.values());
  }

  @Test
  void testAssignmentWithValues() {
    String[] values = new String[] {"finance", "risk"};
    TagAssignment assignment = TagAssignment.ofValues(values);
    values[0] = "changed";

    Assertions.assertTrue(assignment.hasValues());
    Assertions.assertArrayEquals(new String[] {"finance", "risk"}, assignment.values());

    String[] returnedValues = assignment.values();
    returnedValues[0] = "changed";
    Assertions.assertArrayEquals(new String[] {"finance", "risk"}, assignment.values());
  }

  @Test
  void testRejectInvalidValues() {
    Assertions.assertThrows(IllegalArgumentException.class, TagAssignment::ofValues);
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> TagAssignment.ofValues("finance", null));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> TagAssignment.ofValues("finance", " "));
  }

  @Test
  void testEqualsAndHashCode() {
    TagAssignment assignment1 = TagAssignment.ofValues("finance", "risk");
    TagAssignment assignment2 = TagAssignment.ofValues("finance", "risk");
    TagAssignment assignment3 = TagAssignment.ofValues("risk", "finance");

    Assertions.assertEquals(TagAssignment.withoutValues(), TagAssignment.withoutValues());
    Assertions.assertEquals(assignment1, assignment2);
    Assertions.assertEquals(assignment1.hashCode(), assignment2.hashCode());
    Assertions.assertNotEquals(assignment1, assignment3);
  }
}
