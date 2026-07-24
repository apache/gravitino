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

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTagValue {

  @Test
  void testValuedTagValue() {
    TagValue tagValue = TagValue.of("data_domain", "finance");

    Assertions.assertEquals("data_domain", tagValue.name());
    Assertions.assertEquals("finance", tagValue.value().get());
  }

  @Test
  void testValuelessTagValue() {
    TagValue tagValue = TagValue.valueless("pii");

    Assertions.assertEquals("pii", tagValue.name());
    Assertions.assertFalse(tagValue.value().isPresent());
  }

  @Test
  void testConstructorIsPrivate() {
    Constructor<?>[] constructors = TagValue.class.getDeclaredConstructors();

    Assertions.assertEquals(1, constructors.length);
    Assertions.assertTrue(Modifier.isPrivate(constructors[0].getModifiers()));
    Assertions.assertEquals(0, TagValue.class.getConstructors().length);
  }

  @Test
  void testNoJavaBeanAccessors() {
    Assertions.assertThrows(NoSuchMethodException.class, () -> TagValue.class.getMethod("getName"));
    Assertions.assertThrows(
        NoSuchMethodException.class, () -> TagValue.class.getMethod("setName", String.class));
    Assertions.assertThrows(
        NoSuchMethodException.class, () -> TagValue.class.getMethod("getValue"));
    Assertions.assertThrows(
        NoSuchMethodException.class, () -> TagValue.class.getMethod("setValue", String.class));
  }

  @Test
  void testRejectInvalidInput() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> TagValue.of(null, "finance"));
    Assertions.assertThrows(IllegalArgumentException.class, () -> TagValue.of(" ", "finance"));
    Assertions.assertThrows(IllegalArgumentException.class, () -> TagValue.of("tag", null));
    Assertions.assertThrows(IllegalArgumentException.class, () -> TagValue.of("tag", " "));
    Assertions.assertThrows(IllegalArgumentException.class, () -> TagValue.valueless(" "));
  }

  @Test
  void testEqualsAndHashCode() {
    TagValue tagValue1 = TagValue.of("data_domain", "finance");
    TagValue tagValue2 = TagValue.of("data_domain", "finance");
    TagValue tagValue3 = TagValue.of("data_domain", "risk");
    TagValue tagValue4 = TagValue.valueless("data_domain");

    Assertions.assertEquals(tagValue1, tagValue2);
    Assertions.assertEquals(tagValue1.hashCode(), tagValue2.hashCode());
    Assertions.assertNotEquals(tagValue1, tagValue3);
    Assertions.assertNotEquals(tagValue1, tagValue4);
  }
}
