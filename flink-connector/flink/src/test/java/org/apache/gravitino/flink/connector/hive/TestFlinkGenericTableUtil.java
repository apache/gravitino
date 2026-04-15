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
import org.apache.flink.table.catalog.CatalogPropertiesUtil;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.factories.ManagedTableFactory;
import org.apache.gravitino.rel.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestFlinkGenericTableUtil {

  @Test
  void testGenericTableWhenCreate() {
    Assertions.assertTrue(FlinkGenericTableUtil.isGenericTableWhenCreate(null));
    Assertions.assertTrue(FlinkGenericTableUtil.isGenericTableWhenCreate(Collections.emptyMap()));
    Assertions.assertFalse(
        FlinkGenericTableUtil.isGenericTableWhenCreate(ImmutableMap.of("connector", "hive")));
    Assertions.assertTrue(
        FlinkGenericTableUtil.isGenericTableWhenCreate(ImmutableMap.of("connector", "kafka")));
  }

  @Test
  void testGenericTableWhenLoad() {
    Assertions.assertFalse(FlinkGenericTableUtil.isGenericTableWhenLoad(null));
    Assertions.assertTrue(
        FlinkGenericTableUtil.isGenericTableWhenLoad(ImmutableMap.of("is_generic", "true")));
    Assertions.assertTrue(
        FlinkGenericTableUtil.isGenericTableWhenLoad(
            ImmutableMap.of("is_generic", "true", "flink.connector", "hive")));
    Assertions.assertFalse(
        FlinkGenericTableUtil.isGenericTableWhenLoad(ImmutableMap.of("is_generic", "false")));
    Assertions.assertFalse(
        FlinkGenericTableUtil.isGenericTableWhenLoad(
            ImmutableMap.of("is_generic", "false", "flink.connector", "kafka")));
    Assertions.assertTrue(
        FlinkGenericTableUtil.isGenericTableWhenLoad(ImmutableMap.of("flink.connector", "kafka")));
    Assertions.assertTrue(
        FlinkGenericTableUtil.isGenericTableWhenLoad(
            ImmutableMap.of("flink.connector.type", "jdbc")));
    Assertions.assertFalse(FlinkGenericTableUtil.isGenericTableWhenLoad(Collections.emptyMap()));
  }

  @Test
  void testGenericTablePropertiesSerializationAddsIsGeneric() {
    ResolvedCatalogTable resolvedTable = createResolvedTable(Collections.emptyMap());

    Map<String, String> properties =
        FlinkGenericTableUtil.toGravitinoGenericTableProperties(resolvedTable);

    Assertions.assertEquals("true", properties.get(CatalogPropertiesUtil.IS_GENERIC));
    Assertions.assertTrue(
        properties.containsKey(CatalogPropertiesUtil.FLINK_PROPERTY_PREFIX + "schema.0.name"));
  }

  @Test
  @SuppressWarnings("deprecation")
  void testGenericTablePropertiesAddsDefaultConnector() {
    ResolvedCatalogTable resolvedTable = createResolvedTable(Collections.emptyMap());

    Map<String, String> properties =
        FlinkGenericTableUtil.toGravitinoGenericTableProperties(resolvedTable);

    Assertions.assertEquals(
        ManagedTableFactory.DEFAULT_IDENTIFIER,
        properties.get(CatalogPropertiesUtil.FLINK_PROPERTY_PREFIX + "connector"));
  }

  @Test
  void testToFlinkGenericTableRemoveDefaultConnector() {
    ResolvedCatalogTable resolvedTable = createResolvedTable(ImmutableMap.of("custom", "value"));
    Map<String, String> properties =
        FlinkGenericTableUtil.toGravitinoGenericTableProperties(resolvedTable);
    Assertions.assertEquals("default", properties.get("flink.connector"));

    CatalogTable catalogTable =
        FlinkGenericTableUtil.toFlinkGenericTable(
            new Table() {
              @Override
              public String name() {
                return "tbl";
              }

              @Override
              public org.apache.gravitino.rel.Column[] columns() {
                return new org.apache.gravitino.rel.Column[0];
              }

              @Override
              public Map<String, String> properties() {
                return properties;
              }

              @Override
              public org.apache.gravitino.Audit auditInfo() {
                return null;
              }
            });

    Assertions.assertFalse(catalogTable.getOptions().containsKey("connector"));
    Assertions.assertEquals("value", catalogTable.getOptions().get("custom"));
  }

  private static ResolvedCatalogTable createResolvedTable(Map<String, String> options) {
    Schema schema = Schema.newBuilder().column("id", DataTypes.INT()).build();
    CatalogTable table = CatalogTable.of(schema, "comment", Collections.emptyList(), options);
    ResolvedSchema resolvedSchema =
        new ResolvedSchema(
            Collections.singletonList(Column.physical("id", DataTypes.INT())),
            Collections.emptyList(),
            null);
    return new ResolvedCatalogTable(table, resolvedSchema);
  }
}
