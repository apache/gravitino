/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.iceberg.jdbc;

import com.google.common.collect.ImmutableMap;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class TestJdbcCatalogWithMetadataLocationSupport {

  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

  @Test
  void testLoadFields() {
    JdbcCatalogWithMetadataLocationSupport catalog =
        new JdbcCatalogWithMetadataLocationSupport(false);
    Assertions.assertDoesNotThrow(
        () ->
            catalog.initialize(
                "jdbc",
                ImmutableMap.of(
                    CatalogProperties.URI,
                    "jdbc:h2:mem:test",
                    CatalogProperties.WAREHOUSE_LOCATION,
                    "warehouse")));
  }

  @ParameterizedTest
  @CsvSource({
    "V0, false, JDBC catalog with V0 schema version should not support views",
    "V1, true, JDBC catalog with V1 schema version should support views"
  })
  void testSupportsViewsWithSchemaVersion(
      String schemaVersion, boolean expectedSupportsViews, String message) {
    JdbcCatalogWithMetadataLocationSupport catalog =
        new JdbcCatalogWithMetadataLocationSupport(true);
    Map<String, String> properties = new HashMap<>();
    properties.put(CatalogProperties.URI, "jdbc:sqlite::memory:");
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, "warehouse");
    properties.put("jdbc.schema-version", schemaVersion);

    catalog.initialize("test_jdbc_" + schemaVersion.toLowerCase(), properties);

    Assertions.assertEquals(
        expectedSupportsViews, catalog.supportsViewsWithSchemaVersion(), message);
  }

  @Test
  void testRegisterTableOverwrite() throws Exception {
    String warehouse = Files.createTempDirectory("jdbc-register-overwrite").toString();
    JdbcCatalogWithMetadataLocationSupport catalog =
        new JdbcCatalogWithMetadataLocationSupport(true);
    catalog.initialize(
        "jdbc_overwrite",
        ImmutableMap.of(
            CatalogProperties.URI,
            "jdbc:h2:mem:register_overwrite;DB_CLOSE_DELAY=-1",
            CatalogProperties.WAREHOUSE_LOCATION,
            warehouse));

    Namespace namespace = Namespace.of("ns");
    catalog.createNamespace(namespace);
    TableIdentifier ident = TableIdentifier.of(namespace, "t");

    BaseTable table = (BaseTable) catalog.createTable(ident, SCHEMA);
    String metadataV1 = table.operations().current().metadataFileLocation();

    table.updateProperties().set("version", "v2").commit();
    String metadataV2 = table.operations().current().metadataFileLocation();

    catalog.registerTable(ident, metadataV1, true);
    Assertions.assertEquals(metadataV1, catalog.metadataLocation(ident));
    Assertions.assertFalse(
        ((BaseTable) catalog.loadTable(ident)).properties().containsKey("version"));

    catalog.registerTable(ident, metadataV2, true);
    Assertions.assertEquals(metadataV2, catalog.metadataLocation(ident));
    Assertions.assertEquals(
        "v2", ((BaseTable) catalog.loadTable(ident)).properties().get("version"));

    Assertions.assertThrows(
        AlreadyExistsException.class, () -> catalog.registerTable(ident, metadataV2, false));
  }
}
