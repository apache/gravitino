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
package org.apache.gravitino.spark.connector.jdbc.doris;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.spark.connector.catalog.GravitinoCatalogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.catalog.TableWritePrivilege;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Tests the Spark write-privilege boundary of the governed Doris catalog. */
public class TestGravitinoDorisCatalogSpark35 {

  @BeforeAll
  static void initCatalogManager() {
    GravitinoCatalogManager.create(
        new SparkConf(false), "user", identity -> mock(GravitinoClient.class));
  }

  @AfterAll
  static void cleanupCatalogManager() {
    GravitinoCatalogManager.get().close();
  }

  @Test
  void testAppendAcceptsOnlyInsert() {
    DorisWritePolicy35 append =
        DorisWritePolicy35.from(
            ImmutableMap.of(
                DorisConnectorConstants35.GRAVITINO_WRITE_MODE,
                DorisConnectorConstants35.WRITE_BATCH));

    assertDoesNotThrow(
        () ->
            GravitinoDorisCatalogSpark35.validateWritePrivileges(
                append, ImmutableSet.of(TableWritePrivilege.INSERT)));
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            GravitinoDorisCatalogSpark35.validateWritePrivileges(
                append, ImmutableSet.of(TableWritePrivilege.INSERT, TableWritePrivilege.DELETE)));
    assertThrows(
        UnsupportedOperationException.class,
        () -> GravitinoDorisCatalogSpark35.validateWritePrivileges(append, ImmutableSet.of()));
    assertThrows(
        UnsupportedOperationException.class,
        () -> GravitinoDorisCatalogSpark35.validateWritePrivileges(append, null));
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            GravitinoDorisCatalogSpark35.validateWritePrivileges(
                append, ImmutableSet.of(TableWritePrivilege.UPDATE)));
  }

  @Test
  void testTruncateAcceptsInsertAndDelete() {
    DorisWritePolicy35 truncate =
        DorisWritePolicy35.from(
            ImmutableMap.of(
                DorisConnectorConstants35.GRAVITINO_WRITE_MODE,
                DorisConnectorConstants35.WRITE_BATCH,
                DorisConnectorConstants35.GRAVITINO_WRITE_OVERWRITE_MODE,
                DorisConnectorConstants35.WRITE_OVERWRITE_TRUNCATE));

    assertDoesNotThrow(
        () ->
            GravitinoDorisCatalogSpark35.validateWritePrivileges(
                truncate, ImmutableSet.of(TableWritePrivilege.INSERT, TableWritePrivilege.DELETE)));
  }

  @Test
  void testRejectsAllSparkCatalogDdl() {
    GravitinoDorisCatalogSpark35 catalog = new GravitinoDorisCatalogSpark35();
    Identifier table = Identifier.of(new String[] {"db"}, "table");
    Identifier renamed = Identifier.of(new String[] {"db"}, "renamed");
    String[] namespace = new String[] {"db"};

    assertThrows(
        UnsupportedOperationException.class,
        () -> catalog.createTable(table, new StructType(), new Transform[0], ImmutableMap.of()));
    assertThrows(
        UnsupportedOperationException.class, () -> catalog.alterTable(table, new TableChange[0]));
    assertThrows(UnsupportedOperationException.class, () -> catalog.dropTable(table));
    assertThrows(UnsupportedOperationException.class, () -> catalog.purgeTable(table));
    assertThrows(UnsupportedOperationException.class, () -> catalog.renameTable(table, renamed));
    assertThrows(
        UnsupportedOperationException.class,
        () -> catalog.createNamespace(namespace, ImmutableMap.of()));
    assertThrows(
        UnsupportedOperationException.class,
        () -> catalog.alterNamespace(namespace, new NamespaceChange[0]));
    assertThrows(UnsupportedOperationException.class, () -> catalog.dropNamespace(namespace, true));
  }
}
