/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.catalog.lakehouse.iceberg;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.catalog.ManagedSchemaOperations;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.storage.IdGenerator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class TestIcebergTableOperations {

  private IcebergTableOperations icebergTableOps;
  private EntityStore store;
  private ManagedSchemaOperations schemaOps;
  private IdGenerator idGenerator;

  @BeforeEach
  public void setUp() {
    store = mock(EntityStore.class);
    schemaOps = mock(ManagedSchemaOperations.class);
    idGenerator = mock(IdGenerator.class);
    icebergTableOps = spy(new IcebergTableOperations(store, schemaOps, idGenerator));
  }

  @Test
  public void testCreateTableWithoutExternal() {
    NameIdentifier ident = NameIdentifier.of("catalog", "schema", "table");
    Column[] columns = new Column[] {Column.of("id", Types.IntegerType.get(), "id column")};
    Map<String, String> properties = createProperties(false);

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            icebergTableOps.createTable(
                ident,
                columns,
                null,
                properties,
                new Transform[0],
                null,
                new SortOrder[0],
                new Index[0]));
  }

  @Test
  public void testPurgeUnsupported() {
    NameIdentifier ident = NameIdentifier.of("catalog", "schema", "table");
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> icebergTableOps.purgeTable(ident));
  }

  @Test
  public void testCreateExternalTableWithLayout() throws Exception {
    TableEntity tableEntity =
        createTableWithDistribution(
            Distributions.RANGE,
            new Transform[] {Transforms.identity("id")},
            new SortOrder[] {SortOrders.ascending(NamedReference.field("id"))});

    Assertions.assertEquals(Distributions.RANGE, tableEntity.distribution());
    Assertions.assertEquals(1, tableEntity.partitioning().length);
    Assertions.assertEquals(1, tableEntity.sortOrders().length);
    Assertions.assertEquals(1, tableEntity.columns().size());
    Assertions.assertEquals("id", tableEntity.columns().get(0).name());
  }

  @Test
  public void testDefaultDistributionWithSortOrders() throws Exception {
    TableEntity tableEntity =
        createTableWithDistribution(
            Distributions.NONE,
            new Transform[0],
            new SortOrder[] {SortOrders.ascending(NamedReference.field("id"))});
    Assertions.assertEquals(Distributions.RANGE, tableEntity.distribution());
  }

  @Test
  public void testDefaultDistributionWithPartitions() throws Exception {
    TableEntity tableEntity =
        createTableWithDistribution(
            Distributions.NONE, new Transform[] {Transforms.identity("id")}, new SortOrder[0]);
    Assertions.assertEquals(Distributions.HASH, tableEntity.distribution());
  }

  @Test
  public void testDefaultDistributionNone() throws Exception {
    TableEntity tableEntity =
        createTableWithDistribution(Distributions.NONE, new Transform[0], new SortOrder[0]);
    Assertions.assertEquals(Distributions.NONE, tableEntity.distribution());
  }

  private TableEntity createTableWithDistribution(
      Distribution distribution, Transform[] partitions, SortOrder[] sortOrders) throws Exception {
    ArgumentCaptor<TableEntity> tableCaptor = ArgumentCaptor.forClass(TableEntity.class);
    doNothing().when(store).put(tableCaptor.capture(), eq(false));

    NameIdentifier ident = NameIdentifier.of("catalog", "schema", "table");
    Column[] columns = new Column[] {Column.of("id", Types.IntegerType.get(), "id column")};
    Map<String, String> properties = createProperties(true);

    icebergTableOps.createTable(
        ident,
        columns,
        null,
        properties,
        partitions,
        distribution,
        sortOrders,
        Indexes.EMPTY_INDEXES);

    return tableCaptor.getValue();
  }

  private Map<String, String> createProperties(boolean external) {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(Table.PROPERTY_TABLE_FORMAT, "iceberg");
    if (external) {
      properties.put(Table.PROPERTY_EXTERNAL, "true");
    }
    return StringIdentifier.newPropertiesWithId(StringIdentifier.fromId(1L), properties);
  }
}
