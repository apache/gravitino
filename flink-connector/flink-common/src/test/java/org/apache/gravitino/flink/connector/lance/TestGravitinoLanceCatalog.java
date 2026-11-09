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

package org.apache.gravitino.flink.connector.lance;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.flink.connector.DefaultPartitionConverter;
import org.apache.gravitino.flink.connector.utils.CatalogCompat;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.ViewCatalog;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public abstract class TestGravitinoLanceCatalog {

  protected abstract CatalogCompat catalogCompat();

  private static class TestableGravitinoLanceCatalog extends GravitinoLanceCatalog {

    private final CatalogCompat catalogCompat;
    private final Catalog mockGravitinoCatalog;

    TestableGravitinoLanceCatalog(
        String catalogName,
        String defaultDatabase,
        Map<String, String> catalogOptions,
        Catalog mockGravitinoCatalog,
        CatalogCompat catalogCompat) {
      super(
          catalogName,
          defaultDatabase,
          LancePropertiesConverter.INSTANCE,
          DefaultPartitionConverter.INSTANCE,
          catalogOptions);
      this.mockGravitinoCatalog = mockGravitinoCatalog;
      this.catalogCompat = catalogCompat;
    }

    @Override
    protected CatalogCompat catalogCompat() {
      return catalogCompat;
    }

    @Override
    protected Catalog catalog() {
      return mockGravitinoCatalog;
    }
  }

  @Test
  void testOpenWithValidUriSucceeds() {
    Map<String, String> options = ImmutableMap.of("uri", "http://localhost:8090/lance");
    Catalog mockGravitino = Mockito.mock(Catalog.class);
    TestableGravitinoLanceCatalog catalog =
        new TestableGravitinoLanceCatalog(
            "test_lance", "default", options, mockGravitino, catalogCompat());

    Assertions.assertDoesNotThrow(catalog::open);
    Assertions.assertEquals("http://localhost:8090/lance", catalog.getUri());
    Assertions.assertNotNull(catalog.realCatalog());
  }

  @Test
  void testOpenWithMissingUriThrows() {
    Map<String, String> options = Collections.emptyMap();
    Catalog mockGravitino = Mockito.mock(Catalog.class);
    Mockito.when(mockGravitino.properties()).thenReturn(Collections.emptyMap());
    TestableGravitinoLanceCatalog catalog =
        new TestableGravitinoLanceCatalog(
            "test_lance", "default", options, mockGravitino, catalogCompat());

    IllegalArgumentException ex =
        Assertions.assertThrows(IllegalArgumentException.class, catalog::open);
    Assertions.assertTrue(ex.getMessage().contains("requires a non-blank 'uri' property"));
  }

  @Test
  void testOpenWithBlankUriThrows() {
    Map<String, String> options = ImmutableMap.of("uri", "   ");
    Catalog mockGravitino = Mockito.mock(Catalog.class);
    Mockito.when(mockGravitino.properties()).thenReturn(Collections.emptyMap());
    TestableGravitinoLanceCatalog catalog =
        new TestableGravitinoLanceCatalog(
            "test_lance", "default", options, mockGravitino, catalogCompat());

    IllegalArgumentException ex =
        Assertions.assertThrows(IllegalArgumentException.class, catalog::open);
    Assertions.assertTrue(ex.getMessage().contains("requires a non-blank 'uri' property"));
  }

  @Test
  void testOpenWithUriFromGravitinoCatalog() {
    Map<String, String> options = Collections.emptyMap();
    Catalog mockGravitino = Mockito.mock(Catalog.class);
    Mockito.when(mockGravitino.properties())
        .thenReturn(ImmutableMap.of("uri", "http://server-uri:8090/lance"));

    TestableGravitinoLanceCatalog catalog =
        new TestableGravitinoLanceCatalog(
            "test_lance", "default", options, mockGravitino, catalogCompat());

    Assertions.assertDoesNotThrow(catalog::open);
    Assertions.assertEquals("http://server-uri:8090/lance", catalog.getUri());
  }

  @Test
  void testRealCatalogRequiresOpen() {
    Map<String, String> options = ImmutableMap.of("uri", "http://localhost:8090/lance");
    Catalog mockGravitino = Mockito.mock(Catalog.class);
    TestableGravitinoLanceCatalog catalog =
        new TestableGravitinoLanceCatalog(
            "test_lance", "default", options, mockGravitino, catalogCompat());

    IllegalStateException ex =
        Assertions.assertThrows(IllegalStateException.class, catalog::realCatalog);
    Assertions.assertTrue(ex.getMessage().contains("has not been opened"));
  }

  @Test
  void testGetTableInjectsConnectorAndPath() throws Exception {
    Map<String, String> options = ImmutableMap.of("uri", "http://localhost:8090/lance");
    Catalog mockGravitino = Mockito.mock(Catalog.class);
    TableCatalog mockTableCatalog = Mockito.mock(TableCatalog.class);
    Mockito.when(mockGravitino.asTableCatalog()).thenReturn(mockTableCatalog);

    Table mockTable = Mockito.mock(Table.class);
    Mockito.when(mockTable.columns())
        .thenReturn(
            new Column[] {
              Column.of("id", Types.IntegerType.get(), "id column"),
              Column.of("name", Types.StringType.get(), "name column")
            });
    Mockito.when(mockTable.comment()).thenReturn("test table comment");
    Mockito.when(mockTable.index()).thenReturn(new org.apache.gravitino.rel.indexes.Index[0]);
    Mockito.when(mockTable.distribution())
        .thenReturn(org.apache.gravitino.rel.expressions.distributions.Distributions.NONE);
    Mockito.when(mockTable.partitioning())
        .thenReturn(new org.apache.gravitino.rel.expressions.transforms.Transform[0]);
    Mockito.when(mockTable.properties())
        .thenReturn(ImmutableMap.of("location", "s3://lance-bucket/warehouse/my_db/users"));

    NameIdentifier ident = NameIdentifier.of("my_db", "users");
    Mockito.when(mockTableCatalog.loadTable(ident)).thenReturn(mockTable);

    TestableGravitinoLanceCatalog catalog =
        new TestableGravitinoLanceCatalog(
            "test_lance", "default", options, mockGravitino, catalogCompat());
    catalog.open();

    CatalogBaseTable table = catalog.getTable(new ObjectPath("my_db", "users"));
    Assertions.assertNotNull(table);
    Map<String, String> tableOptions = table.getOptions();

    // Verifies that Flink SQL user does not need to configure path or connector!
    Assertions.assertEquals("lance", tableOptions.get("connector"));
    Assertions.assertEquals("s3://lance-bucket/warehouse/my_db/users", tableOptions.get("path"));
    Assertions.assertEquals(
        "s3://lance-bucket/warehouse/my_db/users", tableOptions.get("location"));
    Assertions.assertEquals("test table comment", table.getComment());
  }

  @Test
  void testGetTableDerivesPathFromWarehouseWhenTableHasNoLocation() throws Exception {
    Map<String, String> options =
        ImmutableMap.of(
            "uri", "http://localhost:8090/lance",
            "warehouse", "hdfs://namenode:8020/warehouse");
    Catalog mockGravitino = Mockito.mock(Catalog.class);
    TableCatalog mockTableCatalog = Mockito.mock(TableCatalog.class);
    Mockito.when(mockGravitino.asTableCatalog()).thenReturn(mockTableCatalog);

    Table mockTable = Mockito.mock(Table.class);
    Mockito.when(mockTable.columns())
        .thenReturn(new Column[] {Column.of("val", Types.FloatType.get(), null)});
    Mockito.when(mockTable.index()).thenReturn(new org.apache.gravitino.rel.indexes.Index[0]);
    Mockito.when(mockTable.distribution())
        .thenReturn(org.apache.gravitino.rel.expressions.distributions.Distributions.NONE);
    Mockito.when(mockTable.partitioning())
        .thenReturn(new org.apache.gravitino.rel.expressions.transforms.Transform[0]);
    Mockito.when(mockTable.properties()).thenReturn(Collections.emptyMap());

    NameIdentifier ident = NameIdentifier.of("analytics", "events");
    Mockito.when(mockTableCatalog.loadTable(ident)).thenReturn(mockTable);

    TestableGravitinoLanceCatalog catalog =
        new TestableGravitinoLanceCatalog(
            "test_lance", "default", options, mockGravitino, catalogCompat());
    catalog.open();

    CatalogBaseTable table = catalog.getTable(new ObjectPath("analytics", "events"));
    Assertions.assertNotNull(table);
    Map<String, String> tableOptions = table.getOptions();

    Assertions.assertEquals("lance", tableOptions.get("connector"));
    Assertions.assertEquals(
        "hdfs://namenode:8020/warehouse/analytics/events", tableOptions.get("path"));
    Assertions.assertEquals(
        "hdfs://namenode:8020/warehouse/analytics/events", tableOptions.get("location"));
  }

  @Test
  void testGetTableForwardsStorageOptions() throws Exception {
    Map<String, String> options =
        ImmutableMap.of(
            "uri", "http://localhost:8090/lance",
            "location", "/base/path",
            "lance.storage.s3.endpoint", "http://minio:9000",
            "lance.storage.s3.region", "us-east-1");
    Catalog mockGravitino = Mockito.mock(Catalog.class);
    TableCatalog mockTableCatalog = Mockito.mock(TableCatalog.class);
    Mockito.when(mockGravitino.asTableCatalog()).thenReturn(mockTableCatalog);

    Table mockTable = Mockito.mock(Table.class);
    Mockito.when(mockTable.columns())
        .thenReturn(new Column[] {Column.of("id", Types.LongType.get(), null)});
    Mockito.when(mockTable.index()).thenReturn(new org.apache.gravitino.rel.indexes.Index[0]);
    Mockito.when(mockTable.distribution())
        .thenReturn(org.apache.gravitino.rel.expressions.distributions.Distributions.NONE);
    Mockito.when(mockTable.partitioning())
        .thenReturn(new org.apache.gravitino.rel.expressions.transforms.Transform[0]);
    Mockito.when(mockTable.properties()).thenReturn(Collections.emptyMap());

    NameIdentifier ident = NameIdentifier.of("default", "tbl");
    Mockito.when(mockTableCatalog.loadTable(ident)).thenReturn(mockTable);

    TestableGravitinoLanceCatalog catalog =
        new TestableGravitinoLanceCatalog(
            "test_lance", "default", options, mockGravitino, catalogCompat());
    catalog.open();

    CatalogBaseTable table = catalog.getTable(new ObjectPath("default", "tbl"));
    Map<String, String> tableOptions = table.getOptions();

    Assertions.assertEquals("http://minio:9000", tableOptions.get("lance.storage.s3.endpoint"));
    Assertions.assertEquals("us-east-1", tableOptions.get("lance.storage.s3.region"));
  }

  @Test
  void testDatabaseOperations() throws Exception {
    Map<String, String> options = ImmutableMap.of("uri", "http://localhost:8090/lance");
    Catalog mockGravitino = Mockito.mock(Catalog.class);
    Mockito.when(mockGravitino.asSchemas()).thenReturn(mockGravitino);
    Mockito.when(mockGravitino.listNamespaces())
        .thenReturn(new String[] {"db1", "db2"});
    Mockito.when(mockGravitino.schemaExists("db1")).thenReturn(true);
    Mockito.when(mockGravitino.schemaExists("non_existent")).thenReturn(false);

    org.apache.gravitino.Schema mockSchema = Mockito.mock(org.apache.gravitino.Schema.class);
    Mockito.when(mockSchema.name()).thenReturn("db1");
    Mockito.when(mockSchema.comment()).thenReturn("schema comment");
    Mockito.when(mockSchema.properties()).thenReturn(ImmutableMap.of("prop1", "val1"));
    Mockito.when(mockGravitino.loadSchema("db1")).thenReturn(mockSchema);

    TestableGravitinoLanceCatalog catalog =
        new TestableGravitinoLanceCatalog(
            "test_lance", "default", options, mockGravitino, catalogCompat());
    catalog.open();

    Assertions.assertEquals(2, catalog.listDatabases().size());
    Assertions.assertTrue(catalog.databaseExists("db1"));
    Assertions.assertFalse(catalog.databaseExists("non_existent"));

    CatalogDatabase db = catalog.getDatabase("db1");
    Assertions.assertNotNull(db);
    Assertions.assertEquals("schema comment", db.getComment());
  }

  @Test
  void testTableOperations() throws Exception {
    Map<String, String> options = ImmutableMap.of("uri", "http://localhost:8090/lance");
    Catalog mockGravitino = Mockito.mock(Catalog.class);
    TableCatalog mockTableCatalog = Mockito.mock(TableCatalog.class);
    ViewCatalog mockViewCatalog = Mockito.mock(ViewCatalog.class);
    Mockito.when(mockGravitino.asTableCatalog()).thenReturn(mockTableCatalog);
    Mockito.when(mockGravitino.asViewCatalog()).thenReturn(mockViewCatalog);

    NameIdentifier ident1 = NameIdentifier.of("my_db", "t1");
    NameIdentifier ident2 = NameIdentifier.of("my_db", "t2");
    Mockito.when(mockTableCatalog.listTables(Namespace.of("my_db")))
        .thenReturn(new NameIdentifier[] {ident1, ident2});
    Mockito.when(mockTableCatalog.tableExists(ident1)).thenReturn(true);
    Mockito.when(mockTableCatalog.tableExists(NameIdentifier.of("my_db", "missing"))).thenReturn(false);
    Mockito.when(mockViewCatalog.viewExists(Mockito.any())).thenReturn(false);

    Mockito.when(mockTableCatalog.dropTable(ident1)).thenReturn(true);

    TestableGravitinoLanceCatalog catalog =
        new TestableGravitinoLanceCatalog(
            "test_lance", "default", options, mockGravitino, catalogCompat());
    catalog.open();

    Assertions.assertEquals(2, catalog.listTables("my_db").size());
    Assertions.assertTrue(catalog.tableExists(new ObjectPath("my_db", "t1")));
    Assertions.assertFalse(catalog.tableExists(new ObjectPath("my_db", "missing")));

    catalog.dropTable(new ObjectPath("my_db", "t1"), false);
    Mockito.verify(mockTableCatalog).dropTable(ident1);
  }
}
