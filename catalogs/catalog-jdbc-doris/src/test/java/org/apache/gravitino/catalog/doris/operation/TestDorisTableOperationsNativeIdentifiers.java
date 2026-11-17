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
package org.apache.gravitino.catalog.doris.operation;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import javax.sql.DataSource;
import org.apache.gravitino.catalog.doris.converter.DorisColumnDefaultValueConverter;
import org.apache.gravitino.catalog.doris.converter.DorisExceptionConverter;
import org.apache.gravitino.catalog.doris.converter.DorisTypeConverter;
import org.apache.gravitino.catalog.doris.utils.DorisUtils;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.JdbcTable;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.partitions.Partitions;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/** Regression tests for SQL quoting of existing native identifiers. */
public class TestDorisTableOperationsNativeIdentifiers extends DorisTableOperations {

  private static final String EXISTING = "na`me\\x; --";
  private static final String QUOTED = "`na``me\\x; --`";

  {
    typeConverter = new DorisTypeConverter();
    exceptionMapper = new DorisExceptionConverter();
    columnDefaultValueConverter = new DorisColumnDefaultValueConverter();
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
    // Doris does not support column rename, so no SQL is generated for it.
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> generateAlterTableSql("db", "t", TableChange.renameColumn(existing, "renamed")));
    assertQuoted(TableChange.deleteColumn(existing, false), "DROP COLUMN " + QUOTED);
    assertQuoted(
        TableChange.updateColumnType(existing, Types.LongType.get()), "MODIFY COLUMN " + QUOTED);
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
  void testAlterStatusEscapesTableLiteral() throws SQLException {
    Connection connection = Mockito.mock(Connection.class);
    PreparedStatement statement = Mockito.mock(PreparedStatement.class);
    Mockito.when(connection.prepareStatement(Mockito.anyString())).thenReturn(statement);
    Mockito.when(statement.executeQuery()).thenReturn(Mockito.mock(ResultSet.class));
    getTableStatus(connection, "database", "tab'le");
    Mockito.verify(connection)
        .prepareStatement(
            "SHOW ALTER TABLE COLUMN WHERE TableName = 'tab''le' ORDER BY JobId DESC limit 1");
  }

  @Test
  void testDropAndRenameQuoteExistingIdentifiers() {
    String name = "tab`le\\path; --";
    String quoted = "`tab``le\\path; --`";
    Assertions.assertEquals("DROP TABLE " + quoted, generateDropTableSql(name));
    Assertions.assertEquals(
        "ALTER TABLE " + quoted + " RENAME `new_table`", generateRenameTableSql(name, "new_table"));
  }

  @Test
  void testPartitionStatementsQuoteExistingNames() throws Exception {
    DataSource dataSource = Mockito.mock(DataSource.class);
    Connection connection = Mockito.mock(Connection.class);
    Statement statement = Mockito.mock(Statement.class);
    ResultSet empty = Mockito.mock(ResultSet.class);
    Mockito.when(dataSource.getConnection()).thenReturn(connection);
    Mockito.when(connection.createStatement()).thenReturn(statement);
    Mockito.when(statement.executeQuery(Mockito.anyString())).thenReturn(empty);
    JdbcTable table =
        JdbcTable.builder()
            .withName("tab`le; --")
            .withDatabaseName("db")
            .withAuditInfo(AuditInfo.EMPTY)
            .withColumns(new JdbcColumn[0])
            .build();
    DorisTablePartitionOperations partitions =
        new DorisTablePartitionOperations(dataSource, table, exceptionMapper, typeConverter);

    partitions.listPartitionNames();
    Mockito.verify(statement).executeQuery("SHOW PARTITIONS FROM `tab``le; --`");
    partitions.dropPartition("p`1");
    Mockito.verify(statement).executeUpdate("ALTER TABLE `tab``le; --` DROP PARTITION `p``1`");
    Assertions.assertTrue(
        DorisUtils.generatePartitionSqlFragment(
                Partitions.list(
                    "p`2",
                    new Literal<?>[][] {{Literals.integerLiteral(1)}},
                    Collections.emptyMap()))
            .startsWith("PARTITION `p``2` VALUES IN"));
  }
}
