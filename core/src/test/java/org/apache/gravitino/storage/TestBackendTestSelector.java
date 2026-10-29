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
package org.apache.gravitino.storage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.apache.gravitino.storage.relational.BackendTestSelector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

/** Tests backend selection for the core database test suites. */
@ResourceLock(value = Resources.SYSTEM_PROPERTIES, mode = ResourceAccessMode.READ_WRITE)
public class TestBackendTestSelector {

  private static final String BACKEND_PROPERTY = "gravitino.core.test.backend";

  private Optional<String> originalBackend = Optional.empty();

  @BeforeEach
  void saveAndClearBackendProperty() {
    originalBackend = Optional.ofNullable(System.getProperty(BACKEND_PROPERTY));
    System.clearProperty(BACKEND_PROPERTY);
  }

  @AfterEach
  void restoreBackendProperty() {
    System.clearProperty(BACKEND_PROPERTY);
    originalBackend.ifPresent(value -> System.setProperty(BACKEND_PROPERTY, value));
  }

  @Test
  void testAbsentSelectionPreservesLegacyBehavior() {
    assertEquals(Optional.empty(), BackendTestSelector.selectedBackend());
    assertTrue(BackendTestSelector.isSelected("h2"));
    assertTrue(BackendTestSelector.isSelected("mysql"));
    assertTrue(BackendTestSelector.isSelected("postgresql"));
  }

  @Test
  void testSelectionIsNormalizedAndValidated() {
    System.setProperty(BACKEND_PROPERTY, " MySQL ");

    assertEquals(Optional.of("mysql"), BackendTestSelector.selectedBackend());
    assertTrue(BackendTestSelector.isSelected("MYSQL"));
    assertFalse(BackendTestSelector.isSelected("h2"));

    System.setProperty(BACKEND_PROPERTY, "unsupported");
    assertThrows(IllegalArgumentException.class, BackendTestSelector::selectedBackend);
  }

  @Test
  void testStorageProviderUsesSelectedBackend() {
    System.setProperty(BACKEND_PROPERTY, "postgresql");

    assertArrayEquals(
        new Object[][] {{"postgresql", true}, {"postgresql", false}},
        AbstractEntityStorageTest.storageProvider());
  }
}
