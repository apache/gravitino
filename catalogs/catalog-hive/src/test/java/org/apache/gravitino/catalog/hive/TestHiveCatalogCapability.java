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

import org.apache.gravitino.connector.capability.Capability;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestHiveCatalogCapability {

  private HiveCatalogCapability capability;

  @BeforeEach
  public void setUp() {
    capability = new HiveCatalogCapability();
  }

  @Test
  public void testCaseSensitiveOnNameForCaseInsensitiveScopes() {
    // Hive is case insensitive for SCHEMA, TABLE and COLUMN names.
    Assertions.assertFalse(capability.caseSensitiveOnName(Capability.Scope.SCHEMA).supported());
    Assertions.assertFalse(capability.caseSensitiveOnName(Capability.Scope.TABLE).supported());
    Assertions.assertFalse(capability.caseSensitiveOnName(Capability.Scope.COLUMN).supported());
  }

  @Test
  public void testCaseSensitiveOnNameForCaseSensitiveScopes() {
    // All other scopes are case sensitive (default behaviour).
    Assertions.assertTrue(capability.caseSensitiveOnName(Capability.Scope.FILESET).supported());
    Assertions.assertTrue(capability.caseSensitiveOnName(Capability.Scope.TOPIC).supported());
    Assertions.assertTrue(capability.caseSensitiveOnName(Capability.Scope.PARTITION).supported());
    Assertions.assertTrue(capability.caseSensitiveOnName(Capability.Scope.MODEL).supported());
  }

  @Test
  public void testCaseSensitiveOnNameSyntheticClassAbsent() throws ClassNotFoundException {
    // The implementation must use if-else rather than switch-on-enum so that the Java compiler
    // does not generate the synthetic HiveCatalogCapability$1 helper class.
    // SchemaNormalizeDispatcher
    // calls caseSensitiveOnName() outside IsolatedClassLoader.withClassLoader(); if $1 exists and
    // is requested from the server classloader, the JVM permanently caches the load failure,
    // breaking every subsequent call until process restart.
    ClassLoader cl = HiveCatalogCapability.class.getClassLoader();
    Assertions.assertThrows(
        ClassNotFoundException.class,
        () -> Class.forName("org.apache.gravitino.catalog.hive.HiveCatalogCapability$1", false, cl),
        "HiveCatalogCapability$1 must not exist; use if-else instead of switch-on-enum");
  }
}
