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
package org.apache.iceberg.jdbc;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestJdbcMetadataLocationLookup {
  @ParameterizedTest
  @ValueSource(strings = {"V0", "V1"})
  void testBulkLookupDoesNotReadMetadataAndFiltersCatalogAndViews(String version, @TempDir Path dir)
      throws Exception {
    String uri = "jdbc:sqlite:" + dir.resolve("catalog.db");
    try (JdbcCatalogWithMetadataLocationSupport catalog =
        new JdbcCatalogWithMetadataLocationSupport(true)) {
      catalog.initialize(
          "main",
          Map.of(
              CatalogProperties.URI,
              uri,
              CatalogProperties.WAREHOUSE_LOCATION,
              dir.toString(),
              "jdbc.schema-version",
              version));
      try (Connection connection = DriverManager.getConnection(uri)) {
        for (int i = 0; i < 2000; i++) {
          insert(
              connection,
              version,
              "main",
              "unrelated_" + i,
              "s3://b/other/t" + i + "/metadata/v1.json",
              "TABLE");
        }
        insert(connection, version, "main", "source", "s3a://b/a_%!/metadata/v1.json", "TABLE");
        insert(
            connection, version, "main", "child", "s3://b/a_%!/nested/metadata/v1.json", "TABLE");
        insert(connection, version, "main", "sibling", "s3://b/aX123!/metadata/v1.json", "TABLE");
        insert(connection, version, "other", "foreign", "s3://b/a_%!/metadata/v1.json", "TABLE");
        if (version.equals("V1")) {
          insert(connection, version, "main", "view", "s3://b/a_%!/metadata/v1.json", "VIEW");
        }
      }
      Map<TableIdentifier, String> candidates =
          catalog.metadataLocationCandidates("s3://b/a_%!").orElseThrow();
      Assertions.assertEquals(2, candidates.size());
      Assertions.assertTrue(candidates.containsKey(TableIdentifier.of("ns", "source")));
      Assertions.assertTrue(candidates.containsKey(TableIdentifier.of("ns", "child")));
      Assertions.assertEquals(
          1, catalog.metadataLocationCandidates("s3n://b/a_%!/data").orElseThrow().size());
      Assertions.assertTrue(
          catalog.metadataLocationCandidates("s3://b/missing").orElseThrow().isEmpty());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"V0", "V1"})
  void testLookupIncludesFilesystemAliasesAndExactCustomPointer(String version, @TempDir Path dir)
      throws Exception {
    String uri = "jdbc:sqlite:" + dir.resolve("catalog.db");
    try (JdbcCatalogWithMetadataLocationSupport catalog =
        new JdbcCatalogWithMetadataLocationSupport(true)) {
      catalog.initialize(
          "main",
          Map.of(
              CatalogProperties.URI,
              uri,
              CatalogProperties.WAREHOUSE_LOCATION,
              dir.toString(),
              "jdbc.schema-version",
              version));
      try (Connection connection = DriverManager.getConnection(uri)) {
        insert(
            connection,
            version,
            "main",
            "file_alias",
            "file:/warehouse/public/../secret/metadata/v1.json",
            "TABLE");
        insert(
            connection,
            version,
            "main",
            "hdfs_alias",
            "hdfs://node/warehouse//secret/metadata/v1.json",
            "TABLE");
        insert(
            connection, version, "main", "custom", "s3n://b/shared/current.metadata.json", "TABLE");
      }
      Assertions.assertTrue(
          catalog
              .metadataLocationCandidates("file:/warehouse/secret")
              .orElseThrow()
              .containsKey(TableIdentifier.of("ns", "file_alias")));
      Assertions.assertTrue(
          catalog
              .metadataLocationCandidates("hdfs://node/warehouse/secret")
              .orElseThrow()
              .containsKey(TableIdentifier.of("ns", "hdfs_alias")));
      Assertions.assertFalse(
          catalog
              .metadataLocationCandidates("s3://b/private")
              .orElseThrow()
              .containsKey(TableIdentifier.of("ns", "custom")));
      Assertions.assertTrue(
          catalog
              .metadataLocationCandidates("s3://b/private", "s3a://b/shared/current.metadata.json")
              .orElseThrow()
              .containsKey(TableIdentifier.of("ns", "custom")));
    }
  }

  private static void insert(
      Connection connection,
      String version,
      String catalog,
      String table,
      String pointer,
      String type)
      throws Exception {
    String sql =
        "INSERT INTO iceberg_tables (catalog_name,table_namespace,table_name,metadata_location"
            + (version.equals("V1") ? ",iceberg_type" : "")
            + ") VALUES (?,?,?,?"
            + (version.equals("V1") ? ",?" : "")
            + ")";
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setString(1, catalog);
      statement.setString(2, "ns");
      statement.setString(3, table);
      statement.setString(4, pointer);
      if (version.equals("V1")) statement.setString(5, type);
      statement.executeUpdate();
    }
  }
}
