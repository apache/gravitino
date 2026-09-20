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
package org.apache.gravitino.catalog.hive.integration.test;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance;

@Tag("gravitino-docker-test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class CatalogHive3IT extends CatalogHive2IT {

  @Override
  protected void startNecessaryContainer() {
    hmsCatalog = "hive";
    containerSuite.startHiveContainer(
        ImmutableMap.of(HiveContainer.HIVE_RUNTIME_VERSION, HiveContainer.HIVE3));

    hiveMetastoreUris =
        String.format(
            "thrift://%s:%d",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HIVE_METASTORE_PORT);
  }

  /** Hive 3.x metastores support NOT NULL and DEFAULT constraints, which must round-trip to HMS. */
  @Override
  protected void checkColumnConstraintsOnCreate(
      NameIdentifier nameIdentifier, Map<String, String> properties) {
    NameIdentifier constraintsIdent =
        NameIdentifier.of(schemaName, nameIdentifier.name() + "_constraints");
    Column notNullColumn =
        Column.of("not_null_column", Types.StringType.get(), "not null column", false, false, null);
    Column defaultColumn =
        Column.of(
            "default_column",
            Types.IntegerType.get(),
            "default column",
            true,
            false,
            Literals.integerLiteral(42));
    Column defaultStringColumn =
        Column.of(
            "default_string_column",
            Types.StringType.get(),
            null,
            false,
            false,
            Literals.stringLiteral("it's"));
    Column plainColumn = Column.of("plain_column", Types.StringType.get(), "plain column");

    catalog
        .asTableCatalog()
        .createTable(
            constraintsIdent,
            new Column[] {notNullColumn, defaultColumn, defaultStringColumn, plainColumn},
            TABLE_COMMENT,
            properties,
            Transforms.EMPTY_TRANSFORM);

    Table loaded = catalog.asTableCatalog().loadTable(constraintsIdent);
    assertColumnConstraints(loaded.columns());
    // Read back straight from HMS to make sure the constraints were persisted
    assertColumnConstraints(loadHiveTableColumns(schemaName, constraintsIdent.name()));

    // Property-only alters must leave the constraints untouched
    catalog.asTableCatalog().alterTable(constraintsIdent, TableChange.setProperty("k1", "v1"));
    assertColumnConstraints(loadHiveTableColumns(schemaName, constraintsIdent.name()));

    // Constraints follow the table when it is renamed
    NameIdentifier renamedIdent =
        NameIdentifier.of(schemaName, constraintsIdent.name() + "_renamed");
    catalog.asTableCatalog().alterTable(constraintsIdent, TableChange.rename(renamedIdent.name()));
    assertColumnConstraints(catalog.asTableCatalog().loadTable(renamedIdent).columns());
    assertColumnConstraints(loadHiveTableColumns(schemaName, renamedIdent.name()));

    catalog.asTableCatalog().dropTable(renamedIdent);
  }

  private Column[] loadHiveTableColumns(String schema, String table) {
    try {
      return loadHiveTable(schema, table).columns();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private void assertColumnConstraints(Column[] columns) {
    Assertions.assertEquals(4, columns.length);
    Assertions.assertFalse(columns[0].nullable());
    Assertions.assertEquals(Column.DEFAULT_VALUE_NOT_SET, columns[0].defaultValue());
    Assertions.assertTrue(columns[1].nullable());
    Assertions.assertEquals(Literals.integerLiteral(42), columns[1].defaultValue());
    Assertions.assertFalse(columns[2].nullable());
    Assertions.assertEquals(Literals.stringLiteral("it's"), columns[2].defaultValue());
    Assertions.assertTrue(columns[3].nullable());
    Assertions.assertEquals(Column.DEFAULT_VALUE_NOT_SET, columns[3].defaultValue());
  }

  /** Hive 3.x metastores support NOT NULL and DEFAULT constraints, which are synced on alter. */
  @Override
  protected void checkColumnConstraintsOnAlter(TableCatalog tableCatalog, NameIdentifier id) {
    // add column with default value
    Table altered =
        tableCatalog.alterTable(
            id,
            TableChange.addColumn(
                new String[] {"col_3"}, Types.ByteType.get(), "comment", Literals.NULL));
    Column col3 = findColumn(altered, "col_3");
    Assertions.assertEquals(Literals.NULL, col3.defaultValue());

    // set column NOT NULL and a default value
    altered =
        tableCatalog.alterTable(
            id,
            TableChange.updateColumnNullability(new String[] {HIVE_COL_NAME1}, false),
            TableChange.updateColumnDefaultValue(
                new String[] {HIVE_COL_NAME1}, Literals.integerLiteral(7)));
    Column col1 = findColumn(altered, HIVE_COL_NAME1);
    Assertions.assertFalse(col1.nullable());
    Assertions.assertEquals(Literals.integerLiteral(7), col1.defaultValue());
    col1 = findColumn(tableCatalog.loadTable(id), HIVE_COL_NAME1);
    Assertions.assertFalse(col1.nullable());
    Assertions.assertEquals(Literals.integerLiteral(7), col1.defaultValue());

    // change the default value and make the column nullable again
    tableCatalog.alterTable(
        id,
        TableChange.updateColumnNullability(new String[] {HIVE_COL_NAME1}, true),
        TableChange.updateColumnDefaultValue(
            new String[] {HIVE_COL_NAME1}, Literals.integerLiteral(8)));
    col1 = findColumn(tableCatalog.loadTable(id), HIVE_COL_NAME1);
    Assertions.assertTrue(col1.nullable());
    Assertions.assertEquals(Literals.integerLiteral(8), col1.defaultValue());

    // dropping the column removes its constraints
    tableCatalog.alterTable(id, TableChange.deleteColumn(new String[] {"col_3"}, false));
    Table reloaded = tableCatalog.loadTable(id);
    Assertions.assertTrue(
        Arrays.stream(reloaded.columns()).noneMatch(c -> c.name().equals("col_3")));
  }

  private Column findColumn(Table table, String name) {
    return Arrays.stream(table.columns())
        .filter(c -> c.name().equals(name))
        .findFirst()
        .orElseThrow(() -> new AssertionError("Column not found: " + name));
  }
}
