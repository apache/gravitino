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
package org.apache.gravitino.flink.connector.catalog;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.TableChange;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestBaseCatalog {

  @Test
  public void testHiveSchemaChanges() {
    Map<String, String> currentProperties = ImmutableMap.of("key", "value", "key2", "value2");
    CatalogDatabase current = new CatalogDatabaseImpl(currentProperties, null);

    Map<String, String> newProperties = ImmutableMap.of("key2", "new-value2", "key3", "value3");
    CatalogDatabase updated = new CatalogDatabaseImpl(newProperties, null);

    SchemaChange[] schemaChange = BaseCatalog.getSchemaChange(current, updated);
    Assertions.assertEquals(3, schemaChange.length);
    Assertions.assertInstanceOf(SchemaChange.RemoveProperty.class, schemaChange[0]);
    Assertions.assertEquals("key", ((SchemaChange.RemoveProperty) schemaChange[0]).getProperty());

    Assertions.assertInstanceOf(SchemaChange.SetProperty.class, schemaChange[1]);
    Assertions.assertEquals("key3", ((SchemaChange.SetProperty) schemaChange[1]).getProperty());
    Assertions.assertEquals("value3", ((SchemaChange.SetProperty) schemaChange[1]).getValue());

    Assertions.assertInstanceOf(SchemaChange.SetProperty.class, schemaChange[2]);
    Assertions.assertEquals("key2", ((SchemaChange.SetProperty) schemaChange[2]).getProperty());
    Assertions.assertEquals("new-value2", ((SchemaChange.SetProperty) schemaChange[2]).getValue());
  }

  @Test
  public void testTableChanges() {
    List<TableChange> tableChanges =
        ImmutableList.of(
            TableChange.add(Column.physical("test", DataTypes.INT())),
            TableChange.modifyPhysicalColumnType(
                Column.physical("test", DataTypes.INT()), DataTypes.DOUBLE()),
            TableChange.modifyColumnName(Column.physical("test", DataTypes.INT()), "test2"),
            TableChange.dropColumn("aaa"),
            TableChange.modifyColumnComment(
                Column.physical("test", DataTypes.INT()), "new comment"),
            TableChange.modifyColumnPosition(
                Column.physical("test", DataTypes.INT()),
                TableChange.ColumnPosition.after("test2")),
            TableChange.modifyColumnPosition(
                Column.physical("test", DataTypes.INT()), TableChange.ColumnPosition.first()),
            TableChange.set("key", "value"),
            TableChange.reset("key"));

    List<org.apache.gravitino.rel.TableChange> expected =
        ImmutableList.of(
            org.apache.gravitino.rel.TableChange.addColumn(
                new String[] {"test"}, Types.IntegerType.get()),
            org.apache.gravitino.rel.TableChange.updateColumnType(
                new String[] {"test"}, Types.DoubleType.get()),
            org.apache.gravitino.rel.TableChange.renameColumn(new String[] {"test"}, "test2"),
            org.apache.gravitino.rel.TableChange.deleteColumn(new String[] {"aaa"}, true),
            org.apache.gravitino.rel.TableChange.updateColumnComment(
                new String[] {"test"}, "new comment"),
            org.apache.gravitino.rel.TableChange.updateColumnPosition(
                new String[] {"test"},
                org.apache.gravitino.rel.TableChange.ColumnPosition.after("test2")),
            org.apache.gravitino.rel.TableChange.updateColumnPosition(
                new String[] {"test"}, org.apache.gravitino.rel.TableChange.ColumnPosition.first()),
            org.apache.gravitino.rel.TableChange.setProperty("key", "value"),
            org.apache.gravitino.rel.TableChange.removeProperty("key"));

    org.apache.gravitino.rel.TableChange[] gravitinoTableChanges =
        BaseCatalog.getGravitinoTableChanges(tableChanges);
    Assertions.assertArrayEquals(expected.toArray(), gravitinoTableChanges);
  }

  @Test
  public void testTableChangesWithoutColumnChange() {
    Schema schema = Schema.newBuilder().column("test", "INT").build();
    CatalogBaseTable table =
        CatalogTable.of(
            schema, "test", ImmutableList.of(), ImmutableMap.of("key", "value", "key2", "value2"));
    CatalogBaseTable newTable =
        CatalogTable.of(
            schema, "new comment", ImmutableList.of(), ImmutableMap.of("key", "new value"));
    org.apache.gravitino.rel.TableChange[] tableChanges =
        BaseCatalog.getGravitinoTableChanges(table, newTable);
    List<org.apache.gravitino.rel.TableChange> expected =
        ImmutableList.of(org.apache.gravitino.rel.TableChange.updateComment("new comment"));
    Assertions.assertArrayEquals(expected.toArray(), tableChanges);
  }
}
