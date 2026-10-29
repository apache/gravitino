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
package org.apache.gravitino.integration.test.container;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Checks that every image in {@link TestImages} is set and pinned to a version, and that an
 * environment override wins over the fallback.
 */
public class TestTestImages {

  @Test
  void testEveryImageIsPinned() throws IllegalAccessException {
    List<String> images = new ArrayList<>();
    for (Field field : TestImages.class.getDeclaredFields()) {
      int modifiers = field.getModifiers();
      if (Modifier.isPublic(modifiers)
          && Modifier.isStatic(modifiers)
          && field.getType() == String.class) {
        String image = (String) field.get(null);
        Assertions.assertNotNull(image, field.getName());
        Assertions.assertFalse(image.isBlank(), field.getName() + " is blank");
        // A colon before the last slash is a registry port, not a tag.
        Assertions.assertTrue(
            image.lastIndexOf(':') > image.lastIndexOf('/'),
            field.getName() + " has no tag: " + image);
        Assertions.assertFalse(
            image.endsWith(":latest") || image.contains(":latest@"),
            field.getName() + " uses the latest tag: " + image);
        images.add(image);
      }
    }
    Assertions.assertFalse(images.isEmpty(), "TestImages declares no images");
  }

  @Test
  void testSetVariableOverridesTheFallback() {
    Assertions.assertEquals(
        "custom:1.0",
        TestImages.resolve(Map.of("IMAGE", "custom:1.0")::get, "IMAGE", "fallback:1.0"));
  }

  @Test
  void testUnsetVariableUsesTheFallback() {
    Assertions.assertEquals(
        "fallback:1.0", TestImages.resolve(Map.<String, String>of()::get, "IMAGE", "fallback:1.0"));
  }

  @Test
  void testBlankVariableUsesTheFallback() {
    Assertions.assertEquals(
        "fallback:1.0", TestImages.resolve(Map.of("IMAGE", "  ")::get, "IMAGE", "fallback:1.0"));
  }
}
