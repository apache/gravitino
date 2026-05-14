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

package org.apache.gravitino.storage.relational;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.gravitino.storage.relational.mapper.it.BackendTestExtension;
import org.apache.gravitino.storage.relational.mapper.it.BackendTypes;
import org.junit.jupiter.api.Test;

public class TestBackendTestExtension {

  @Test
  public void testDockerTestSystemPropertyOverridesEnvironment() {
    String originalValue = System.getProperty("dockerTest");
    try {
      System.setProperty("dockerTest", "false");
      assertFalse(BackendTestExtension.isDockerTestEnabled());
    } finally {
      restoreDockerTestProperty(originalValue);
    }
  }

  @Test
  public void testDockerTestCanBeEnabledBySystemProperty() {
    String originalValue = System.getProperty("dockerTest");
    try {
      System.setProperty("dockerTest", "true");
      assertTrue(BackendTestExtension.isDockerTestEnabled());
    } finally {
      restoreDockerTestProperty(originalValue);
    }
  }

  private void restoreDockerTestProperty(String originalValue) {
    if (originalValue == null) {
      System.clearProperty("dockerTest");
    } else {
      System.setProperty("dockerTest", originalValue);
    }
  }

  @Test
  public void testResolveBackendsUsesInheritedBackendTypes() {
    assertIterableEquals(
        List.of("postgresql"), BackendTestExtension.resolveBackends(ChildTest.class));
  }

  @BackendTypes({"postgresql"})
  private static class ParentTest {}

  private static class ChildTest extends ParentTest {}
}
