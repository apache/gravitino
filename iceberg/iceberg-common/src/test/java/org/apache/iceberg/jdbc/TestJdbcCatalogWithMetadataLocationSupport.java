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
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class TestJdbcCatalogWithMetadataLocationSupport {

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
}
