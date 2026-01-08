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

package org.apache.gravitino.maintenance.optimizer.common.util;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestIdentifierUtils {

  @Test
  void testRemoveCatalogFromIdentifierThrowsWhenCatalogMissing() {
    Namespace namespace = Namespace.of("singleLevel");
    NameIdentifier identifier = NameIdentifier.of(namespace, "tableName");

    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () -> IdentifierUtils.removeCatalogFromIdentifier(identifier));
  }

  @Test
  void testRemoveCatalogFromIdentifierRemovesFirstLevelForTwoLevelNamespace() {
    Namespace namespace = Namespace.of("catalog", "schema");
    NameIdentifier identifier = NameIdentifier.of(namespace, "tableName");

    NameIdentifier result = IdentifierUtils.removeCatalogFromIdentifier(identifier);

    Assertions.assertEquals("schema", result.namespace().levels()[0]);
    Assertions.assertEquals("tableName", result.name());
  }

  @Test
  void testRemoveCatalogFromIdentifierThrowsExceptionForInvalidNamespace() {
    Namespace namespace = Namespace.of();
    NameIdentifier identifier = NameIdentifier.of(namespace, "tableName");

    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () -> IdentifierUtils.removeCatalogFromIdentifier(identifier));
  }

  @Test
  void testGetCatalogNameFromTableIdentifierThrowsWhenCatalogMissing() {
    Namespace namespace = Namespace.of("schema");
    NameIdentifier identifier = NameIdentifier.of(namespace, "tableName");

    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () -> IdentifierUtils.getCatalogNameFromTableIdentifier(identifier));
  }

  @Test
  void testGetCatalogNameFromTableIdentifierReturnsFirstLevelForMultiLevelNamespace() {
    Namespace namespace = Namespace.of("catalog", "schema");
    NameIdentifier identifier = NameIdentifier.of(namespace, "tableName");

    String result = IdentifierUtils.getCatalogNameFromTableIdentifier(identifier);

    Assertions.assertEquals("catalog", result);
  }

  @Test
  void testGetCatalogNameFromTableIdentifierThrowsExceptionForInvalidNamespace() {
    Namespace namespace = Namespace.of();
    NameIdentifier identifier = NameIdentifier.of(namespace, "tableName");

    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () -> IdentifierUtils.getCatalogNameFromTableIdentifier(identifier));
  }

  @Test
  void testRequireTableIdentifierNormalizedSucceedsWhenCatalogPresent() {
    NameIdentifier identifier = NameIdentifier.of("catalog", "schema", "tableName");

    Assertions.assertDoesNotThrow(
        () -> IdentifierUtils.requireTableIdentifierNormalized(identifier));
  }

  @Test
  void testRequireTableIdentifierNormalizedThrowsWhenCatalogMissing() {
    NameIdentifier identifier = NameIdentifier.of("schema", "tableName");

    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () -> IdentifierUtils.requireTableIdentifierNormalized(identifier));
  }
}
