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
package org.apache.gravitino.catalog.mysql.operation;

import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.JdbcTable;
import org.apache.gravitino.catalog.mysql.converter.MysqlColumnDefaultValueConverter;
import org.apache.gravitino.catalog.mysql.converter.MysqlTypeConverter;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Regression tests for SQL quoting of existing native identifiers. */
public class TestMysqlTableOperationsNativeIdentifiers extends MysqlTableOperations {

  private static final String EXISTING = "na`me\\x; --";
  private static final String QUOTED = "`na``me\\x; --`";

  {
    typeConverter = new MysqlTypeConverter();
    columnDefaultValueConverter = new MysqlColumnDefaultValueConverter();
  }

  @Override
  public JdbcTable load(String databaseName, String tableName) {
    return JdbcTable.builder()
        .withName(tableName)
        .withAuditInfo(AuditInfo.EMPTY)
        .withColumns(
            new JdbcColumn[] {
              JdbcColumn.builder()
                  .withName(EXISTING)
                  .withType(Types.IntegerType.get())
                  .withNullable(true)
                  .build(),
              JdbcColumn.builder()
                  .withName("other")
                  .withType(Types.IntegerType.get())
                  .withNullable(true)
                  .build()
            })
        .withIndexes(Indexes.EMPTY_INDEXES)
        .withDistribution(Distributions.NONE)
        .build();
  }

  @Test
  void testColumnChangesQuoteExistingColumnNames() {
    String[] existing = {EXISTING};
    assertQuoted(
        TableChange.addColumn(
            new String[] {"added"},
            Types.IntegerType.get(),
            null,
            TableChange.ColumnPosition.after(EXISTING),
            true,
            false,
            Column.DEFAULT_VALUE_NOT_SET),
        "AFTER " + QUOTED);
    assertQuoted(
        TableChange.updateColumnPosition(
            new String[] {"other"}, TableChange.ColumnPosition.after(EXISTING)),
        "AFTER " + QUOTED);
    assertQuoted(TableChange.renameColumn(existing, "renamed"), QUOTED + " `renamed`");
    assertQuoted(TableChange.deleteColumn(existing, false), "DROP COLUMN " + QUOTED);
    assertQuoted(
        TableChange.updateColumnType(existing, Types.LongType.get()), "MODIFY COLUMN " + QUOTED);
    assertQuoted(
        TableChange.addIndex(Index.IndexType.UNIQUE_KEY, "idx", new String[][] {existing}),
        "(" + QUOTED + ")");
    assertQuoted(TableChange.updateColumnComment(existing, "c"), "MODIFY COLUMN " + QUOTED);
  }

  private void assertQuoted(TableChange change, String expected) {
    String sql = generateAlterTableSql("db", "tab`le", change);
    Assertions.assertTrue(sql.startsWith("ALTER TABLE `tab``le`"), sql);
    Assertions.assertTrue(sql.contains(expected), sql);
    // The only occurrences of the existing name are fully quoted ones.
    Assertions.assertFalse(sql.replace(QUOTED, "").contains("na`me"), sql);
  }

  @Test
  void testDropAndRenameQuoteExistingIdentifiers() {
    String name = "tab`le\\path; --";
    String quoted = "`tab``le\\path; --`";
    Assertions.assertEquals("DROP TABLE " + quoted, generateDropTableSql(name));
    Assertions.assertEquals(
        "RENAME TABLE " + quoted + " TO `new_table`", generateRenameTableSql(name, "new_table"));
  }
}
