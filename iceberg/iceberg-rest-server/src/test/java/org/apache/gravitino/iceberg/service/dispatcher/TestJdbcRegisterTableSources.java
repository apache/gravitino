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
package org.apache.gravitino.iceberg.service.dispatcher;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapper;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

class TestJdbcRegisterTableSources {
  private static final Namespace NAMESPACE = Namespace.of("ns");
  private static final TableIdentifier SOURCE = TableIdentifier.of(NAMESPACE, "secret");
  private static final TableIdentifier TARGET = TableIdentifier.of(NAMESPACE, "copy");
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.optional(1, "id", Types.LongType.get()));

  @ParameterizedTest
  @ValueSource(strings = {"./secret", "public/../secret", "/secret", "secret/."})
  void testFilesystemAliasesMatchLiveTableWithoutMetadataReads(String suffix, @TempDir Path dir)
      throws Exception {
    Path warehouse = dir.resolve("warehouse");
    Files.createDirectories(warehouse.resolve("public"));
    String root = warehouse.toUri().toString();
    try (IcebergCatalogWrapper wrapper = newCatalog(dir)) {
      wrapper.createNamespace(CreateNamespaceRequest.builder().withNamespace(NAMESPACE).build());
      var created =
          wrapper.createTable(
              NAMESPACE,
              CreateTableRequest.builder()
                  .withName(SOURCE.name())
                  .withSchema(SCHEMA)
                  .withLocation(root + "secret")
                  .build());
      String alias = root + suffix;
      Assertions.assertTrue(
          Files.isSameFile(
              Path.of(URI.create(alias)), Path.of(URI.create(created.tableMetadata().location()))));
      // Model a live pointer written with a filesystem alias by another engine.
      String aliasedPointer = created.metadataLocation().replace("/metadata/", "/./metadata/");
      try (Connection connection =
              DriverManager.getConnection("jdbc:sqlite:" + dir.resolve("catalog.db"));
          PreparedStatement statement =
              connection.prepareStatement(
                  "UPDATE iceberg_tables SET metadata_location = ? WHERE table_name = ?")) {
        statement.setString(1, aliasedPointer);
        statement.setString(2, SOURCE.name());
        Assertions.assertEquals(1, statement.executeUpdate());
      }
      IcebergCatalogWrapper lookup = Mockito.spy(wrapper);
      Assertions.assertEquals(
          List.of(SOURCE),
          RegisterTableSourceResolver.findOverlappingTables(lookup, TARGET, alias));
      Mockito.verify(lookup, Mockito.never()).loadTableMetadata(Mockito.any());
      Mockito.verify(lookup, Mockito.never()).loadTableMetadataFromLocation(Mockito.any());
      Mockito.verify(lookup, Mockito.never()).listNamespace(Mockito.any());
    }
  }

  @Test
  void testExactCurrentPointerWorksOutsideStandardLayout(@TempDir Path dir) throws Exception {
    try (IcebergCatalogWrapper wrapper = newCatalog(dir)) {
      wrapper.createNamespace(CreateNamespaceRequest.builder().withNamespace(NAMESPACE).build());
      var created =
          wrapper.createTable(
              NAMESPACE,
              CreateTableRequest.builder()
                  .withName(SOURCE.name())
                  .withSchema(SCHEMA)
                  .setProperties(
                      new HashMap<>(
                          Map.of(
                              TableProperties.WRITE_METADATA_LOCATION,
                              dir.resolve("shared_metadata").toUri().toString())))
                  .build());
      IcebergCatalogWrapper lookup = Mockito.spy(wrapper);
      Assertions.assertEquals(
          List.of(SOURCE),
          RegisterTableSourceResolver.findSourceTables(
              lookup, TARGET, created.metadataLocation(), created.tableMetadata()));
      Mockito.verify(lookup, Mockito.never()).loadTableMetadata(Mockito.any());
      Mockito.verify(lookup, Mockito.never()).loadTableMetadataFromLocation(Mockito.any());
      Mockito.verify(lookup, Mockito.never()).listNamespace(Mockito.any());
    }
  }

  private static IcebergCatalogWrapper newCatalog(Path dir) {
    return new IcebergCatalogWrapper(
        new IcebergConfig(
            Map.of(
                "catalog-backend",
                "jdbc",
                "uri",
                "jdbc:sqlite:" + dir.resolve("catalog.db"),
                "jdbc-driver",
                "org.sqlite.JDBC",
                "warehouse",
                dir.resolve("warehouse").toUri().toString())));
  }
}
