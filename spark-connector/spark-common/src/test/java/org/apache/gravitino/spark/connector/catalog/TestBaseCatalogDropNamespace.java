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
package org.apache.gravitino.spark.connector.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.SupportsSchemas;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.spark.sql.catalyst.analysis.NonEmptyNamespaceException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link BaseCatalog#dropNamespace}. */
public class TestBaseCatalogDropNamespace {

  private static final String SCHEMA = "db";

  private SupportsSchemas schemas;
  private BaseCatalog catalog;

  @BeforeAll
  static void initCatalogManager() {
    GravitinoClient gravitinoClient = mock(GravitinoClient.class);
    GravitinoCatalogManager.create("user", () -> gravitinoClient);
  }

  @AfterAll
  static void cleanupCatalogManager() {
    GravitinoCatalogManager.get().close();
  }

  @BeforeEach
  void setUp() {
    Catalog gravitinoCatalog = mock(Catalog.class);
    schemas = mock(SupportsSchemas.class);
    when(gravitinoCatalog.asSchemas()).thenReturn(schemas);
    catalog = new DropNamespaceTestCatalog();
    catalog.gravitinoCatalogClient = gravitinoCatalog;
  }

  @Test
  void testDropNamespaceWithoutCascadeThrowsNonEmptyNamespace() {
    when(schemas.dropSchema(anyString(), anyBoolean()))
        .thenThrow(new NonEmptySchemaException("Schema db has tables"));

    assertThrows(
        NonEmptyNamespaceException.class,
        () -> catalog.dropNamespace(new String[] {SCHEMA}, false));
  }

  @Test
  void testDropNamespaceWithCascadeKeepsServerReason() {
    String reason = "Table db.orders is referenced by a table in another schema";
    when(schemas.dropSchema(anyString(), anyBoolean()))
        .thenThrow(new NonEmptySchemaException(reason));

    NonEmptySchemaException e =
        assertThrows(
            NonEmptySchemaException.class,
            () -> catalog.dropNamespace(new String[] {SCHEMA}, true));
    assertEquals(reason, e.getMessage());
  }

  private static class DropNamespaceTestCatalog extends BaseCatalog {
    @Override
    protected TableCatalog createAndInitSparkCatalog(
        String name, CaseInsensitiveStringMap options, Map<String, String> properties) {
      return null;
    }

    @Override
    protected Table createSparkTable(
        Identifier identifier,
        org.apache.gravitino.rel.Table gravitinoTable,
        Table sparkTable,
        TableCatalog sparkCatalog,
        PropertiesConverter propertiesConverter,
        SparkTransformConverter sparkTransformConverter,
        SparkTypeConverter sparkTypeConverter) {
      return null;
    }

    @Override
    protected PropertiesConverter getPropertiesConverter() {
      return null;
    }

    @Override
    protected SparkTransformConverter getSparkTransformConverter() {
      return null;
    }
  }
}
