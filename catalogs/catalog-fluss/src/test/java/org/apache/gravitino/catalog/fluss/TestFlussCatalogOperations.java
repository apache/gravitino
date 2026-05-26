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

package org.apache.gravitino.catalog.fluss;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableChange.AddColumn;
import org.apache.fluss.metadata.TableChange.After;
import org.apache.fluss.metadata.TableChange.DropColumn;
import org.apache.fluss.metadata.TableChange.First;
import org.apache.fluss.metadata.TableChange.Last;
import org.apache.fluss.metadata.TableChange.ModifyColumn;
import org.apache.fluss.metadata.TableChange.RenameColumn;
import org.apache.fluss.metadata.TableChange.ResetOption;
import org.apache.fluss.metadata.TableChange.SetOption;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Test;

class TestFlussCatalogOperations {

  @Test
  void testToFlussTableChangesMapsSupportedChanges() {
    List<org.apache.fluss.metadata.TableChange> changes =
        FlussCatalogOperations.toFlussTableChanges(
            tableInfo(),
            TableChange.addColumn(
                new String[] {"country"},
                Types.StringType.get(),
                "country",
                TableChange.ColumnPosition.first()),
            TableChange.addColumn(
                new String[] {"city"},
                Types.StringType.get(),
                "city",
                TableChange.ColumnPosition.after("country")),
            TableChange.renameColumn(new String[] {"pv"}, "views"),
            TableChange.updateColumnType(new String[] {"views"}, Types.IntegerType.get()),
            TableChange.updateColumnComment(new String[] {"views"}, "views comment"),
            TableChange.updateColumnNullability(new String[] {"views"}, false),
            TableChange.updateColumnPosition(
                new String[] {"views"}, TableChange.ColumnPosition.after("city")),
            TableChange.deleteColumn(new String[] {"region"}, false),
            TableChange.setProperty("table.log.ttl", "1d"),
            TableChange.removeProperty("old.option"));

    assertEquals(10, changes.size());

    AddColumn country = assertInstanceOf(AddColumn.class, changes.get(0));
    assertEquals("country", country.getName());
    assertEquals(DataTypes.STRING(), country.getDataType());
    assertEquals("country", country.getComment());
    assertInstanceOf(First.class, country.getPosition());

    AddColumn city = assertInstanceOf(AddColumn.class, changes.get(1));
    assertEquals("city", city.getName());
    assertEquals("country", assertInstanceOf(After.class, city.getPosition()).columnName());

    RenameColumn renameColumn = assertInstanceOf(RenameColumn.class, changes.get(2));
    assertEquals("pv", renameColumn.getOldColumnName());
    assertEquals("views", renameColumn.getNewColumnName());

    ModifyColumn typeChange = assertInstanceOf(ModifyColumn.class, changes.get(3));
    assertEquals("views", typeChange.getName());
    assertEquals(DataTypes.INT(), typeChange.getDataType());
    assertEquals("page views", typeChange.getComment());
    assertNull(typeChange.getNewPosition());

    ModifyColumn commentChange = assertInstanceOf(ModifyColumn.class, changes.get(4));
    assertEquals(DataTypes.INT(), commentChange.getDataType());
    assertEquals("views comment", commentChange.getComment());
    assertNull(commentChange.getNewPosition());

    ModifyColumn nullabilityChange = assertInstanceOf(ModifyColumn.class, changes.get(5));
    assertFalse(nullabilityChange.getDataType().isNullable());
    assertEquals("views comment", nullabilityChange.getComment());
    assertNull(nullabilityChange.getNewPosition());

    ModifyColumn positionChange = assertInstanceOf(ModifyColumn.class, changes.get(6));
    assertFalse(positionChange.getDataType().isNullable());
    assertEquals("views comment", positionChange.getComment());
    assertEquals(
        "city", assertInstanceOf(After.class, positionChange.getNewPosition()).columnName());

    assertEquals("region", assertInstanceOf(DropColumn.class, changes.get(7)).getName());

    SetOption setOption = assertInstanceOf(SetOption.class, changes.get(8));
    assertEquals("table.log.ttl", setOption.getKey());
    assertEquals("1d", setOption.getValue());

    assertEquals("old.option", assertInstanceOf(ResetOption.class, changes.get(9)).getKey());
  }

  @Test
  void testDefaultAddColumnPositionMapsToFlussLastPosition() {
    List<org.apache.fluss.metadata.TableChange> changes =
        FlussCatalogOperations.toFlussTableChanges(
            tableInfo(), TableChange.addColumn(new String[] {"country"}, Types.StringType.get()));

    AddColumn addColumn = assertInstanceOf(AddColumn.class, changes.get(0));
    assertEquals("country", addColumn.getName());
    assertInstanceOf(Last.class, addColumn.getPosition());
  }

  @Test
  void testDeleteColumnIfExistsSkipsMissingColumn() {
    List<org.apache.fluss.metadata.TableChange> changes =
        FlussCatalogOperations.toFlussTableChanges(
            tableInfo(), TableChange.deleteColumn(new String[] {"missing"}, true));

    assertTrue(changes.isEmpty());
  }

  @Test
  void testRejectsUnsupportedColumnChanges() {
    UnsupportedOperationException defaultValueException =
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                FlussCatalogOperations.toFlussTableChanges(
                    tableInfo(),
                    TableChange.updateColumnDefaultValue(
                        new String[] {"pv"}, Column.DEFAULT_VALUE_NOT_SET)));
    assertTrue(defaultValueException.getMessage().contains("column default values"));

    UnsupportedOperationException autoIncrementException =
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                FlussCatalogOperations.toFlussTableChanges(
                    tableInfo(), TableChange.updateColumnAutoIncrement(new String[] {"pv"}, true)));
    assertTrue(autoIncrementException.getMessage().contains("auto-increment"));
  }

  @Test
  void testRejectsUnsupportedPositionsAndNestedColumns() {
    IllegalArgumentException positionException =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                FlussCatalogOperations.toFlussTableChanges(
                    tableInfo(),
                    TableChange.updateColumnPosition(
                        new String[] {"pv"}, TableChange.ColumnPosition.after("missing"))));
    assertTrue(positionException.getMessage().contains("Column does not exist"));

    IllegalArgumentException nestedColumnException =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                FlussCatalogOperations.toFlussTableChanges(
                    tableInfo(),
                    TableChange.addColumn(
                        new String[] {"nested", "field"}, Types.StringType.get())));
    assertTrue(nestedColumnException.getMessage().contains("top-level fields"));
  }

  @Test
  void testRejectsUnsupportedTableChanges() {
    UnsupportedOperationException tableCommentException =
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                FlussCatalogOperations.toFlussTableChanges(
                    tableInfo(), TableChange.updateComment("new comment")));
    assertTrue(tableCommentException.getMessage().contains("table comment"));

    UnsupportedOperationException renameTableException =
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                FlussCatalogOperations.toFlussTableChanges(
                    tableInfo(), TableChange.rename("new_table")));
    assertTrue(renameTableException.getMessage().contains("renaming tables"));
  }

  private static TableInfo tableInfo() {
    Schema schema =
        Schema.newBuilder()
            .fromColumns(
                List.of(
                    new Schema.Column("event_day", DataTypes.STRING().copy(false), "event day"),
                    new Schema.Column("region", DataTypes.STRING(), null),
                    new Schema.Column("pv", DataTypes.BIGINT(), "page views")))
            .build();
    TableDescriptor descriptor =
        TableDescriptor.builder().schema(schema).distributedBy(1, List.of()).build();
    return TableInfo.of(TablePath.of("db", "orders"), 1L, 1, descriptor, 10L, 20L);
  }
}
