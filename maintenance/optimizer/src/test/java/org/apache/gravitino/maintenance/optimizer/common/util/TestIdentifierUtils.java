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

  @Test
  void testParseTableIdentifierWithDefaultCatalog() {
    NameIdentifier result =
        IdentifierUtils.parseTableIdentifier("db.table", "catalog").orElseThrow();

    Assertions.assertEquals(NameIdentifier.of("catalog", "db", "table"), result);
  }

  @Test
  void testParseTableIdentifierWithoutDefaultCatalogReturnsEmpty() {
    Assertions.assertTrue(IdentifierUtils.parseTableIdentifier("db.table", null).isEmpty());
  }

  @Test
  void testParseTableIdentifierWithThreeLevelIdentifier() {
    NameIdentifier result =
        IdentifierUtils.parseTableIdentifier("catalog.db.table", null).orElseThrow();

    Assertions.assertEquals(NameIdentifier.of("catalog", "db", "table"), result);
  }

  @Test
  void testParseTableIdentifierWithSingleLevelReturnsEmpty() {
    Assertions.assertTrue(IdentifierUtils.parseTableIdentifier("table", "catalog").isEmpty());
  }

  @Test
  void testParseTableIdentifierWithInvalidIdentifierReturnsEmpty() {
    Assertions.assertTrue(
        IdentifierUtils.parseTableIdentifier("catalog.db.schema.extra.table", "c").isEmpty());
  }

  @Test
  void testParseJobIdentifierWithValidIdentifier() {
    NameIdentifier result =
        IdentifierUtils.parseJobIdentifier("org.team.pipeline.job1").orElseThrow();
    Assertions.assertEquals("org.team.pipeline.job1", result.toString());
  }

  @Test
  void testParseJobIdentifierWithSingleLevelName() {
    NameIdentifier result = IdentifierUtils.parseJobIdentifier("job").orElseThrow();
    Assertions.assertEquals("job", result.toString());
  }

  @Test
  void testParseJobIdentifierWithTwoLevelName() {
    NameIdentifier result = IdentifierUtils.parseJobIdentifier("team.job").orElseThrow();
    Assertions.assertEquals("team.job", result.toString());
  }

  @Test
  void testParseJobIdentifierWithBlankOrInvalidIdentifierReturnsEmpty() {
    Assertions.assertTrue(IdentifierUtils.parseJobIdentifier(" ").isEmpty());
    Assertions.assertTrue(IdentifierUtils.parseJobIdentifier("invalid..job").isEmpty());
  }

  @Test
  void testParseJobIdentifierWithTooManyLevels() {
    NameIdentifier result = IdentifierUtils.parseJobIdentifier("a.b.c.d.e").orElseThrow();
    Assertions.assertEquals("a.b.c.d.e", result.toString());
  }
}
