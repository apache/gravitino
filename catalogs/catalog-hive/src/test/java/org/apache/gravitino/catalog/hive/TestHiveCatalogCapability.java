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
package org.apache.gravitino.catalog.hive;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.EnumSet;
import java.util.Set;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestHiveCatalogCapability {

  private HiveCatalogCapability capability;

  @BeforeEach
  void setUp() {
    capability = new HiveCatalogCapability();
  }

  @Test
  void testCaseSensitiveOnHiveNamesIsUnsupported() {
    Set<Capability.Scope> unsupportedScopes =
        EnumSet.of(Capability.Scope.SCHEMA, Capability.Scope.TABLE, Capability.Scope.COLUMN);

    for (Capability.Scope scope : unsupportedScopes) {
      CapabilityResult result = capability.caseSensitiveOnName(scope);
      assertFalse(result.supported(), "Hive is case-insensitive for " + scope + " names");
      assertNotNull(result.unsupportedMessage());
    }
  }

  @Test
  void testCaseSensitiveOnOtherScopesIsSupported() {
    Set<Capability.Scope> unsupportedScopes =
        EnumSet.of(Capability.Scope.SCHEMA, Capability.Scope.TABLE, Capability.Scope.COLUMN);

    for (Capability.Scope scope : Capability.Scope.values()) {
      if (!unsupportedScopes.contains(scope)) {
        assertTrue(capability.caseSensitiveOnName(scope).supported());
      }
    }
  }

  @Test
  void testCaseSensitiveOnNameRejectsNullScope() {
    assertThrows(IllegalArgumentException.class, () -> capability.caseSensitiveOnName(null));
  }

  @Test
  void testNoSwitchOnEnumSyntheticClass() {
    // Verifies that the if/else rewrite eliminated the compiler-generated $1 switch-map class.
    // If switch-on-enum were still present, javac would produce HiveCatalogCapability$1.class,
    // which is loaded lazily on the first switch execution by the defining IsolatedClassLoader.
    // Closing that classloader before the first switch call would then cause a permanent
    // NoClassDefFoundError. The absence of $1 confirms this trigger is eliminated.
    String syntheticClassResource =
        HiveCatalogCapability.class.getName().replace('.', '/') + "$1.class";
    assertNull(
        HiveCatalogCapability.class.getClassLoader().getResource(syntheticClassResource),
        "HiveCatalogCapability$1 switch-map class must not exist after if/else rewrite");
  }
}
