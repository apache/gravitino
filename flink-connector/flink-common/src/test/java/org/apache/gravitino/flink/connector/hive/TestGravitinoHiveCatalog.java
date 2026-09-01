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
package org.apache.gravitino.flink.connector.hive;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Map;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.flink.connector.PartitionConverter;
import org.apache.gravitino.flink.connector.SchemaAndTablePropertiesConverter;
import org.apache.gravitino.flink.connector.utils.DefaultCatalogCompat;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestGravitinoHiveCatalog {

  @Test
  public void testGenericTableAlterSkipsCallWhenNoChanges() throws Exception {
    // Existing table and the resolved new table describe the same state (same properties, same
    // comment), so no TableChange is produced. The connector must not forward an empty update
    // list to Gravitino, which would fail server-side with "updates must not be empty".
    Map<String, String> sameProperties =
        ImmutableMap.of("flink.connector", "kafka", "is_generic", "true");

    Catalog gravitinoCatalog = Mockito.mock(Catalog.class);
    TableCatalog tableCatalog = Mockito.mock(TableCatalog.class);
    Mockito.when(gravitinoCatalog.asTableCatalog()).thenReturn(tableCatalog);

    Table existingTable = Mockito.mock(Table.class);
    Mockito.when(existingTable.properties()).thenReturn(sameProperties);
    Mockito.when(existingTable.comment()).thenReturn("same comment");

    ResolvedCatalogTable newTable = resolvedTable("same comment");

    TestableGravitinoHiveCatalog catalog =
        new TestableGravitinoHiveCatalog(gravitinoCatalog, sameProperties);

    catalog.applyGenericTableAlter(new ObjectPath("db", "tbl"), existingTable, newTable);

    // The alter call is skipped entirely because there is nothing to change.
    Mockito.verify(tableCatalog, Mockito.never()).alterTable(Mockito.any(), Mockito.any());
  }

  @Test
  public void testGenericTableAlterCallsAlterWhenPropertiesChange() throws Exception {
    // The resolved new table changes a property, so the connector must forward the update.
    Map<String, String> currentProperties =
        ImmutableMap.of("flink.connector", "kafka", "is_generic", "true");
    Map<String, String> updatedProperties =
        ImmutableMap.of(
            "flink.connector", "kafka", "flink.topic", "new-topic", "is_generic", "true");

    Catalog gravitinoCatalog = Mockito.mock(Catalog.class);
    TableCatalog tableCatalog = Mockito.mock(TableCatalog.class);
    Mockito.when(gravitinoCatalog.asTableCatalog()).thenReturn(tableCatalog);

    Table existingTable = Mockito.mock(Table.class);
    Mockito.when(existingTable.properties()).thenReturn(currentProperties);
    Mockito.when(existingTable.comment()).thenReturn("comment");

    ResolvedCatalogTable newTable = resolvedTable("comment");

    TestableGravitinoHiveCatalog catalog =
        new TestableGravitinoHiveCatalog(gravitinoCatalog, updatedProperties);

    catalog.applyGenericTableAlter(new ObjectPath("db", "tbl"), existingTable, newTable);

    // A real change was present, so the alter call is forwarded to Gravitino.
    Mockito.verify(tableCatalog, Mockito.times(1)).alterTable(Mockito.any(), Mockito.any());
  }

  private static ResolvedCatalogTable resolvedTable(String comment) {
    Schema schema = Schema.newBuilder().column("id", DataTypes.INT()).build();
    CatalogTable table =
        DefaultCatalogCompat.INSTANCE.createCatalogTable(
            schema, comment, Collections.emptyList(), Collections.emptyMap());
    ResolvedSchema resolvedSchema =
        new ResolvedSchema(
            Collections.singletonList(Column.physical("id", DataTypes.INT())),
            Collections.emptyList(),
            null);
    return new ResolvedCatalogTable(table, resolvedSchema);
  }

  private static class TestableGravitinoHiveCatalog extends GravitinoHiveCatalog {
    private final Catalog gravitinoCatalog;
    private final Map<String, String> genericTableProperties;

    TestableGravitinoHiveCatalog(Catalog gravitinoCatalog) {
      this(gravitinoCatalog, Collections.emptyMap());
    }

    TestableGravitinoHiveCatalog(
        Catalog gravitinoCatalog, Map<String, String> genericTableProperties) {
      super(
          "test",
          "default",
          Collections.emptyMap(),
          Mockito.mock(SchemaAndTablePropertiesConverter.class),
          Mockito.mock(PartitionConverter.class),
          hiveConf(),
          null);
      this.gravitinoCatalog = gravitinoCatalog;
      this.genericTableProperties = genericTableProperties;
    }

    @Override
    protected AbstractCatalog realCatalog() {
      return Mockito.mock(AbstractCatalog.class);
    }

    @Override
    protected Catalog catalog() {
      return gravitinoCatalog;
    }

    @Override
    protected Map<String, String> toGravitinoGenericTableProperties(ResolvedCatalogTable table) {
      return genericTableProperties;
    }

    private static HiveConf hiveConf() {
      HiveConf hiveConf = new HiveConf();
      hiveConf.set("hive.metastore.uris", "thrift://localhost:9083");
      return hiveConf;
    }
  }
}
