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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SupportsSchemas;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestBaseCatalog {

  private static final String FORMAT_NAMESPACE = "parquet";
  private static final String TABLE_SCHEMA = "db";

  private SupportsSchemas schemas;
  private org.apache.gravitino.rel.TableCatalog gravitinoTableCatalog;
  private BaseCatalog catalog;

  @BeforeAll
  static void initCatalogManager() {
    GravitinoClient gravitinoClient = mock(GravitinoClient.class);
    GravitinoCatalogManager.create(new SparkConf(false), "user", identity -> gravitinoClient);
  }

  @AfterAll
  static void cleanupCatalogManager() {
    GravitinoCatalogManager.get().close();
  }

  @BeforeEach
  void setUp() {
    Catalog gravitinoCatalog = mock(Catalog.class);
    schemas = mock(SupportsSchemas.class);
    gravitinoTableCatalog = mock(org.apache.gravitino.rel.TableCatalog.class);
    when(gravitinoCatalog.asSchemas()).thenReturn(schemas);
    when(gravitinoCatalog.asTableCatalog()).thenReturn(gravitinoTableCatalog);
    when(schemas.loadSchema(TABLE_SCHEMA)).thenReturn(mock(Schema.class));
    when(schemas.loadSchema(FORMAT_NAMESPACE))
        .thenThrow(new NoSuchSchemaException("Schema parquet does not exist"));
    catalog = new DataSourceTestCatalog(gravitinoCatalog);
  }

  @Test
  void pathLikeNamesWithPathSeparatorsMatchThePrefilter() {
    Identifier[] pathIdents = {
      Identifier.of(new String[] {"parquet"}, "/tmp/data"),
      Identifier.of(new String[] {"csv"}, "s3a://bucket/file.csv"),
      Identifier.of(new String[] {"json"}, "relative/path/to/file"),
      Identifier.of(new String[] {"PARQUET"}, "hdfs://namenode/path"),
      Identifier.of(new String[] {"binaryFile"}, "C:\\Users\\data\\file"),
    };
    for (Identifier ident : pathIdents) {
      assertTrue(
          BaseCatalog.looksLikeDataSourceReference(ident),
          "Expected " + ident + " to match the DataSource prefilter");
    }
  }

  @Test
  void regularTableNamesDoNotMatchThePrefilter() {
    assertFalse(
        BaseCatalog.looksLikeDataSourceReference(Identifier.of(new String[] {"parquet"}, "users")));
    assertFalse(
        BaseCatalog.looksLikeDataSourceReference(
            Identifier.of(new String[] {"my_schema"}, "my_table")));
    assertFalse(
        BaseCatalog.looksLikeDataSourceReference(
            Identifier.of(new String[] {"parquet", "extra"}, "table")));
    assertFalse(
        BaseCatalog.looksLikeDataSourceReference(Identifier.of(new String[] {}, "parquet")));
  }

  @Test
  void missingSchemaAndPathLikeNameIsADataSourceReference() {
    assertTrue(
        catalog.isDataSourceReference(
            Identifier.of(new String[] {FORMAT_NAMESPACE}, "s3a://bucket/file.parquet")));
    assertTrue(
        catalog.isDataSourceReference(
            Identifier.of(new String[] {FORMAT_NAMESPACE}, "C:\\data\\file.parquet")));
  }

  @Test
  void existingSchemaWithSlashInTableNameStaysATableReference() {
    // PostgreSQL and other backends allow '/' in quoted table names; the namespace is a real
    // Gravitino schema, so the identifier must not be hijacked as a DataSource shortcut.
    assertFalse(
        catalog.isDataSourceReference(
            Identifier.of(new String[] {TABLE_SCHEMA}, "table/with/slash")));
  }

  @Test
  void nonPathLikeNameIsDecidedWithoutASchemaLookup() {
    assertFalse(catalog.isDataSourceReference(Identifier.of(new String[] {TABLE_SCHEMA}, "users")));
    verify(schemas, never()).loadSchema(anyString());
  }

  @Test
  void nonNoSuchSchemaErrorFromSchemaLookupPropagates() {
    when(schemas.loadSchema("restricted")).thenThrow(new ForbiddenException("Permission denied"));
    assertThrows(
        ForbiddenException.class,
        () ->
            catalog.isDataSourceReference(Identifier.of(new String[] {"restricted"}, "data/file")));
  }

  @Test
  void loadTableYieldsForDataSourceReferenceWithoutTouchingTableCatalog()
      throws NoSuchTableException {
    Identifier ident = Identifier.of(new String[] {FORMAT_NAMESPACE}, "s3a://bucket/file.parquet");
    assertThrows(NoSuchTableException.class, () -> catalog.loadTable(ident));
    verifyNoInteractions(gravitinoTableCatalog);
  }

  @Test
  void loadTableForWritingYieldsForDataSourceReferenceWithoutTouchingTableCatalog()
      throws NoSuchTableException {
    Identifier ident = Identifier.of(new String[] {FORMAT_NAMESPACE}, "s3a://bucket/file.parquet");
    assertThrows(NoSuchTableException.class, () -> catalog.loadTable(ident, java.util.Set.of()));
    verifyNoInteractions(gravitinoTableCatalog);
  }

  @Test
  void tableExistsReturnsFalseForDataSourceReferenceWithoutTouchingTableCatalog() {
    Identifier ident = Identifier.of(new String[] {FORMAT_NAMESPACE}, "s3a://bucket/file.parquet");
    assertFalse(catalog.tableExists(ident));
    verifyNoInteractions(gravitinoTableCatalog);
  }

  private static class DataSourceTestCatalog extends BaseCatalog {
    private final Catalog catalog;

    private DataSourceTestCatalog(Catalog catalog) {
      this.catalog = catalog;
    }

    @Override
    protected Catalog gravitinoCatalog() {
      return catalog;
    }

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
