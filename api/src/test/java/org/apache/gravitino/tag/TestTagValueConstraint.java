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

public class TestTagValueConstraint {

  @Test
  void testAnyValueConstraint() {
    TagValueConstraint constraint = TagValueConstraint.anyValue();

    Assertions.assertEquals(TagValueConstraint.Type.ANY_VALUE, constraint.type());
    Assertions.assertArrayEquals(new String[0], constraint.allowedValues());
  }

  @Test
  void testWithoutValuesConstraint() {
    TagValueConstraint constraint = TagValueConstraint.withoutValues();

    Assertions.assertEquals(TagValueConstraint.Type.WITHOUT_VALUES, constraint.type());
    Assertions.assertArrayEquals(new String[0], constraint.allowedValues());
  }

  @Test
  void testAllowedValuesConstraint() {
    String[] allowedValues = new String[] {"finance", "risk"};
    TagValueConstraint constraint = TagValueConstraint.ofAllowedValues(allowedValues);
    allowedValues[0] = "changed";

    Assertions.assertEquals(TagValueConstraint.Type.ALLOWED_VALUES, constraint.type());
    Assertions.assertArrayEquals(new String[] {"finance", "risk"}, constraint.allowedValues());

    String[] returnedValues = constraint.allowedValues();
    returnedValues[0] = "changed";
    Assertions.assertArrayEquals(new String[] {"finance", "risk"}, constraint.allowedValues());
  }

  @Test
  void testRejectInvalidAllowedValues() {
    Assertions.assertThrows(IllegalArgumentException.class, TagValueConstraint::ofAllowedValues);
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> TagValueConstraint.ofAllowedValues("finance", null));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> TagValueConstraint.ofAllowedValues("finance", " "));
  }

  @Test
  void testEqualsAndHashCode() {
    TagValueConstraint constraint1 = TagValueConstraint.ofAllowedValues("finance", "risk");
    TagValueConstraint constraint2 = TagValueConstraint.ofAllowedValues("finance", "risk");
    TagValueConstraint constraint3 = TagValueConstraint.ofAllowedValues("risk", "finance");

    Assertions.assertEquals(TagValueConstraint.anyValue(), TagValueConstraint.anyValue());
    Assertions.assertEquals(TagValueConstraint.withoutValues(), TagValueConstraint.withoutValues());
    Assertions.assertEquals(constraint1, constraint2);
    Assertions.assertEquals(constraint1.hashCode(), constraint2.hashCode());
    Assertions.assertNotEquals(constraint1, constraint3);
  }
}
