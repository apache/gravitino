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
package org.apache.gravitino.catalog.postgresql.operation;

import static org.apache.gravitino.catalog.jdbc.utils.JdbcConnectorUtils.escapeSqlLiteral;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetProvider;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.JdbcTable;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.catalog.jdbc.operation.DatabaseOperation;
import org.apache.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import org.apache.gravitino.catalog.jdbc.operation.RequireDatabaseOperation;
import org.apache.gravitino.catalog.jdbc.utils.JdbcConnectorUtils;
import org.apache.gravitino.catalog.postgresql.converter.PostgreSqlColumnDefaultValueConverter;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NoSuchColumnException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.postgresql.PGConnection;

/** Table operations for PostgreSQL. */
public class PostgreSqlTableOperations extends JdbcTableOperations
    implements RequireDatabaseOperation {

  public static final String PG_QUOTE = "\"";
  public static final String NEW_LINE = "\n";
  public static final String ALTER_TABLE = "ALTER TABLE ";
  public static final String ALTER_COLUMN = "ALTER COLUMN ";
  public static final String IS = " IS E'";
  public static final String COLUMN_COMMENT = "COMMENT ON COLUMN ";
  public static final String TABLE_COMMENT = "COMMENT ON TABLE ";

  private static final String STANDALONE_UNIQUE_INDEX_SQL =
      "SELECT 1 FROM pg_catalog.pg_index i "
          + "JOIN pg_catalog.pg_class idx ON idx.oid = i.indexrelid "
          + "JOIN pg_catalog.pg_class t ON t.oid = i.indrelid "
          + "JOIN pg_catalog.pg_namespace s ON s.oid = t.relnamespace "
          + "WHERE s.nspname = ? AND t.relname = ? AND idx.relname = ? AND i.indisunique "
          + "AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_constraint c "
          + "WHERE c.conindid = i.indexrelid AND c.conrelid = i.indrelid "
          + "AND c.contype IN ('p', 'u'))";

  private static final String POSTGRESQL_NOT_SUPPORT_NESTED_COLUMN_MSG =
      "PostgreSQL does not support nested column names.";

  private String database;
  private PostgreSqlSchemaOperations schemaOperations;

  @Override
  public void initialize(
      DataSource dataSource,
      JdbcExceptionConverter exceptionMapper,
      JdbcTypeConverter jdbcTypeConverter,
      JdbcColumnDefaultValueConverter jdbcColumnDefaultValueConverter,
      Map<String, String> conf) {
    super.initialize(
        dataSource, exceptionMapper, jdbcTypeConverter, jdbcColumnDefaultValueConverter, conf);
    database = new JdbcConfig(conf).getJdbcDatabase();
    Preconditions.checkArgument(
        StringUtils.isNotBlank(database),
        "The `jdbc-database` configuration item is mandatory in PostgreSQL.");
  }

  @Override
  public void setDatabaseOperation(DatabaseOperation databaseOperation) {
    this.schemaOperations = (PostgreSqlSchemaOperations) databaseOperation;
  }

  /** {@inheritDoc} */
  @Override
  public void create(
      String schema,
      String tableName,
      JdbcColumn[] columns,
      @Nullable String comment,
      Map<String, String> properties,
      @Nullable Transform[] partitioning,
      Distribution distribution,
      Index[] indexes,
      @Nullable SortOrder[] sortOrders) {
    try {
      super.create(
          schema,
          tableName,
          columns,
          comment,
          properties,
          partitioning,
          distribution,
          indexes,
          sortOrders);
    } catch (NoSuchSchemaException e) {
      // PostgreSQL removes unusable schemas from search_path. An unqualified CREATE can therefore
      // report 3F000 even when the requested schema exists but the source role has no USAGE.
      boolean lacksUsage;
      try {
        lacksUsage = schemaLacksUsage(schema);
      } catch (RuntimeException diagnosisFailure) {
        diagnosisFailure.addSuppressed(e);
        throw diagnosisFailure;
      }
      if (lacksUsage) {
        throw new ForbiddenException(
            e, "Insufficient source privileges for schema %s: USAGE is required", schema);
      }
      throw e;
    }
  }

  @Override
  public List<String> listTables(String schemaName) throws NoSuchSchemaException {
    try (Connection connection = getConnection(schemaName)) {
      if (!schemaOperations.schemaExists(connection, schemaName)) {
        throw new NoSuchSchemaException("No such schema: %s", schemaName);
      }
      final List<String> names = Lists.newArrayList();
      try (ResultSet tables = getTables(connection)) {
        while (tables.next()) {
          if (Objects.equals(tables.getString("TABLE_SCHEM"), schemaName)) {
            names.add(tables.getString("TABLE_NAME"));
          }
        }
      }
      LOG.info("Finished listing tables size {} for database name {} ", names.size(), schemaName);
      return names;
    } catch (final SQLException se) {
      throw this.exceptionMapper.toGravitinoException(se);
    }
  }

  @Override
  protected JdbcTable.Builder getTableBuilder(
      ResultSet tablesResult, String databaseName, String tableName) throws SQLException {
    boolean found = false;
    JdbcTable.Builder builder = null;
    while (tablesResult.next() && !found) {
      String tableNameInResult = tablesResult.getString("TABLE_NAME");
      String tableSchemaInResultLowerCase = tablesResult.getString("TABLE_SCHEM");
      if (Objects.equals(tableNameInResult, tableName)
          && Objects.equals(tableSchemaInResultLowerCase, databaseName)) {
        builder = getBasicJdbcTableInfo(tablesResult);
        found = true;
      }
    }

    if (!found) {
      throw new NoSuchTableException("Table %s does not exist in %s.", tableName, databaseName);
    }

    return builder;
  }

  @Override
  protected JdbcColumn.Builder getColumnBuilder(
      ResultSet columnsResult, String databaseName, String tableName) throws SQLException {
    JdbcColumn.Builder builder = null;
    if (Objects.equals(columnsResult.getString("TABLE_NAME"), tableName)
        && Objects.equals(columnsResult.getString("TABLE_SCHEM"), databaseName)) {
      builder = getBasicJdbcColumnInfo(columnsResult);
    }
    return builder;
  }

  @Override
  protected String generateCreateTableSql(
      String tableName,
      JdbcColumn[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitioning,
      Distribution distribution,
      Index[] indexes) {
    if (ArrayUtils.isNotEmpty(partitioning)) {
      throw new UnsupportedOperationException(
          "Currently we do not support Partitioning in PostgreSQL");
    }
    Preconditions.checkArgument(
        Distributions.isNone(distribution), "PostgreSQL does not support distribution");

    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder
        .append("CREATE TABLE ")
        .append(quotedIdentifier(tableName))
        .append(" (")
        .append(NEW_LINE);

    // Add columns
    for (int i = 0; i < columns.length; i++) {
      JdbcColumn column = columns[i];
      sqlBuilder.append("    ").append(quotedIdentifier(column.name()));

      appendColumnDefinition(column, sqlBuilder);
      // Add a comma for the next column, unless it's the last one
      if (i < columns.length - 1) {
        sqlBuilder.append(",").append(NEW_LINE);
      }
    }
    appendIndexesSql(indexes, sqlBuilder);
    sqlBuilder.append(NEW_LINE).append(")");
    // Add table properties if any
    if (MapUtils.isNotEmpty(properties)) {
      // TODO #804 will add properties
      throw new IllegalArgumentException("Properties are not supported yet");
    }

    sqlBuilder.append(";");

    // Add table comment if specified
    if (StringUtils.isNotEmpty(comment)) {
      sqlBuilder
          .append(NEW_LINE)
          .append(TABLE_COMMENT)
          .append(quotedIdentifier(tableName))
          .append(IS)
          .append(escapeSqlLiteral(validateText(comment, "table comment"), '\''))
          .append("';");
    }
    Arrays.stream(columns)
        .filter(jdbcColumn -> StringUtils.isNotEmpty(jdbcColumn.comment()))
        .forEach(
            jdbcColumn ->
                sqlBuilder
                    .append(NEW_LINE)
                    .append(COLUMN_COMMENT)
                    .append(quotedIdentifier(tableName))
                    .append(".")
                    .append(quotedIdentifier(jdbcColumn.name()))
                    .append(IS)
                    .append(
                        escapeSqlLiteral(
                            validateText(
                                jdbcColumn.comment(), "comment for column " + jdbcColumn.name()),
                            '\''))
                    .append("';"));

    // Return the generated SQL statement
    String result = sqlBuilder.toString();

    LOG.info("Generated create table:{} sql: {}", tableName, result);
    return result;
  }

  @VisibleForTesting
  static void appendIndexesSql(Index[] indexes, StringBuilder sqlBuilder) {
    for (Index index : indexes) {
      validateIndexName(index.name());
      String fieldStr = getIndexFieldStr(index.fieldNames());
      sqlBuilder.append(",").append(NEW_LINE);
      switch (index.type()) {
        case PRIMARY_KEY:
          if (StringUtils.isNotEmpty(index.name())) {
            sqlBuilder.append("CONSTRAINT ").append(quotedIdentifier(index.name()));
          }
          sqlBuilder.append(" PRIMARY KEY (").append(fieldStr).append(")");
          break;
        case UNIQUE_KEY:
          if (StringUtils.isNotEmpty(index.name())) {
            sqlBuilder.append("CONSTRAINT ").append(quotedIdentifier(index.name()));
          }
          sqlBuilder.append(" UNIQUE (").append(fieldStr).append(")");
          break;
        default:
          throw new IllegalArgumentException("PostgreSQL doesn't support index : " + index.type());
      }
    }
  }

  protected static String getIndexFieldStr(String[][] fieldNames) {
    return Arrays.stream(fieldNames)
        .map(
            colNames -> {
              if (colNames.length > 1) {
                throw new IllegalArgumentException(
                    "Index does not support complex fields in PostgreSQL");
              }
              return quotedIdentifier(colNames[0]);
            })
        .collect(Collectors.joining(", "));
  }

  private static boolean allowsAutoIncrement(Type type) {
    return type instanceof Types.ShortType || Types.allowAutoIncrement(type);
  }

  private void appendColumnDefinition(JdbcColumn column, StringBuilder sqlBuilder) {
    // Add data type
    sqlBuilder.append(SPACE).append(typeConverter.fromGravitino(column.dataType())).append(SPACE);

    if (column.autoIncrement()) {
      if (!allowsAutoIncrement(column.dataType())) {
        throw new IllegalArgumentException(
            "Unsupported auto-increment , column: "
                + column.name()
                + ", type: "
                + column.dataType());
      }
      sqlBuilder.append("GENERATED BY DEFAULT AS IDENTITY ");
    }

    // Add NOT NULL if the column is marked as such
    if (column.nullable()) {
      sqlBuilder.append("NULL ");
    } else {
      sqlBuilder.append("NOT NULL ");
    }
    // Add DEFAULT value if specified
    appendDefaultValue(column, sqlBuilder);
  }

  @Override
  protected String generateRenameTableSql(String oldTableName, String newTableName) {
    return ALTER_TABLE
        + quotedIdentifier(oldTableName)
        + " RENAME TO "
        + quotedIdentifier(newTableName);
  }

  @Override
  protected String generateDropTableSql(String tableName) {
    return "DROP TABLE " + quotedIdentifier(tableName);
  }

  /**
   * Drops the table by its schema-qualified name. An unqualified name is resolved through the
   * search path, and PostgreSQL leaves out a schema the catalog user has no USAGE privilege on. The
   * drop would then report the table as missing instead of refusing it with 42501.
   *
   * <p>On {@code branch-1.3}, that missing-table result can cause Gravitino to remove the
   * registration of a table that still exists. On {@code main}, the registration is already
   * preserved when the connector returns {@code false}; schema qualification ensures the permission
   * failure is reported as 403.
   */
  @Override
  protected void dropTable(String schemaName, String tableName) {
    try (Connection connection = getConnection(schemaName)) {
      JdbcConnectorUtils.executeUpdate(
          connection,
          "DROP TABLE " + quotedIdentifier(schemaName) + "." + quotedIdentifier(tableName));
    } catch (final SQLException se) {
      throw this.exceptionMapper.toGravitinoException(se);
    }
  }

  @Override
  protected String generatePurgeTableSql(String tableName) {
    throw new UnsupportedOperationException(
        "PostgreSQL does not support purge table in Gravitino, please use drop table");
  }

  @Override
  protected String generateAlterTableSql(
      String schemaName, String tableName, TableChange... changes) {
    // Not all operations require the original table information, so lazy loading is used here
    JdbcTable lazyLoadTable = null;
    List<String> alterSql = new ArrayList<>();
    for (TableChange change : changes) {
      if (change instanceof TableChange.UpdateComment) {
        lazyLoadTable = getOrCreateTable(schemaName, tableName, lazyLoadTable);
        alterSql.add(updateCommentDefinition((TableChange.UpdateComment) change, lazyLoadTable));
      } else if (change instanceof TableChange.SetProperty) {
        throw new IllegalArgumentException("Set property is not supported yet");
      } else if (change instanceof TableChange.RemoveProperty) {
        // PostgreSQL does not support deleting table attributes, it can be replaced by Set Property
        throw new IllegalArgumentException("Remove property is not supported yet");
      } else if (change instanceof TableChange.AddColumn) {
        TableChange.AddColumn addColumn = (TableChange.AddColumn) change;
        lazyLoadTable = getOrCreateTable(schemaName, tableName, lazyLoadTable);
        alterSql.addAll(addColumnFieldDefinition(addColumn, lazyLoadTable));
      } else if (change instanceof TableChange.RenameColumn) {
        TableChange.RenameColumn renameColumn = (TableChange.RenameColumn) change;
        alterSql.add(renameColumnFieldDefinition(renameColumn, tableName));
      } else if (change instanceof TableChange.UpdateColumnDefaultValue) {
        lazyLoadTable = getOrCreateTable(schemaName, tableName, lazyLoadTable);
        TableChange.UpdateColumnDefaultValue updateColumnDefaultValue =
            (TableChange.UpdateColumnDefaultValue) change;
        alterSql.add(
            updateColumnDefaultValueFieldDefinition(updateColumnDefaultValue, lazyLoadTable));
      } else if (change instanceof TableChange.UpdateColumnType) {
        lazyLoadTable = getOrCreateTable(schemaName, tableName, lazyLoadTable);
        TableChange.UpdateColumnType updateColumnType = (TableChange.UpdateColumnType) change;
        alterSql.add(updateColumnTypeFieldDefinition(schemaName, updateColumnType, lazyLoadTable));
      } else if (change instanceof TableChange.UpdateColumnComment) {
        alterSql.add(
            updateColumnCommentFieldDefinition(
                (TableChange.UpdateColumnComment) change, tableName));
      } else if (change instanceof TableChange.UpdateColumnPosition) {
        throw new IllegalArgumentException("PostgreSQL does not support column position.");
      } else if (change instanceof TableChange.DeleteColumn) {
        lazyLoadTable = getOrCreateTable(schemaName, tableName, lazyLoadTable);
        TableChange.DeleteColumn deleteColumn = (TableChange.DeleteColumn) change;
        String deleteColSql = deleteColumnFieldDefinition(deleteColumn, lazyLoadTable);
        if (StringUtils.isNotEmpty(deleteColSql)) {
          alterSql.add(deleteColSql);
        }
      } else if (change instanceof TableChange.UpdateColumnNullability) {
        TableChange.UpdateColumnNullability updateColumnNullability =
            (TableChange.UpdateColumnNullability) change;

        lazyLoadTable = getOrCreateTable(schemaName, tableName, lazyLoadTable);
        JdbcColumn column = validateUpdateColumnNullable(updateColumnNullability, lazyLoadTable);

        alterSql.add(updateColumnNullabilityDefinition(updateColumnNullability, tableName));
        // A NULL default conflicts with NOT NULL, so drop it when the column becomes non-nullable,
        // unless the same request sets a new default value for the column.
        JdbcColumn pendingColumn = withPendingDefaultValueAndNullability(column, changes);
        if (Literals.NULL.equals(column.defaultValue())
            && Column.DEFAULT_VALUE_NOT_SET.equals(pendingColumn.defaultValue())) {
          alterSql.add(
              ALTER_TABLE
                  + quotedIdentifier(tableName)
                  + " "
                  + ALTER_COLUMN
                  + quotedIdentifier(updateColumnNullability.fieldName()[0])
                  + " DROP DEFAULT;");
        }
      } else if (change instanceof TableChange.AddIndex) {
        alterSql.add(addIndexDefinition(tableName, (TableChange.AddIndex) change));
      } else if (change instanceof TableChange.DeleteIndex) {
        TableChange.DeleteIndex deleteIndex = (TableChange.DeleteIndex) change;
        rejectStandaloneUniqueIndex(schemaName, tableName, deleteIndex.getName());
        alterSql.add(deleteIndexDefinition(tableName, deleteIndex));
      } else if (change instanceof TableChange.UpdateColumnAutoIncrement) {
        alterSql.add(
            updateColumnAutoIncrementDefinition(
                (TableChange.UpdateColumnAutoIncrement) change, tableName));
      } else {
        throw new IllegalArgumentException(
            "Unsupported table change type: " + change.getClass().getName());
      }
    }

    // If there is no change, return directly
    if (alterSql.isEmpty()) {
      return "";
    }

    // Return the generated SQL statement
    String result = String.join("\n", alterSql);
    LOG.info("Generated alter table:{}.{} sql: {}", schemaName, tableName, result);
    return result;
  }

  @VisibleForTesting
  static String updateColumnAutoIncrementDefinition(
      TableChange.UpdateColumnAutoIncrement change, String tableName) {
    if (change.fieldName().length > 1) {
      throw new UnsupportedOperationException(POSTGRESQL_NOT_SUPPORT_NESTED_COLUMN_MSG);
    }
    String fieldName = change.fieldName()[0];
    String action =
        change.isAutoIncrement() ? "ADD GENERATED BY DEFAULT AS IDENTITY" : "DROP IDENTITY";

    return String.format(
        "ALTER TABLE %s %s %s %s;",
        quotedIdentifier(tableName), ALTER_COLUMN, quotedIdentifier(fieldName), action);
  }

  @VisibleForTesting
  static String deleteIndexDefinition(String tableName, TableChange.DeleteIndex deleteIndex) {
    // Gravitino creates primary and unique indexes as constraints. Dropping the constraint
    // also removes its backing index, so a second DROP INDEX would fail after a successful drop.
    return String.format(
        "ALTER TABLE %s DROP CONSTRAINT %s%s;",
        quotedIdentifier(tableName),
        deleteIndex.isIfExists() ? "IF EXISTS " : "",
        quotedIdentifier(deleteIndex.getName()));
  }

  @VisibleForTesting
  static String addIndexDefinition(String tableName, TableChange.AddIndex addIndex) {
    validateIndexName(addIndex.getName());
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append("ALTER TABLE ").append(quotedIdentifier(tableName)).append(" ADD");
    if (StringUtils.isNotEmpty(addIndex.getName())) {
      sqlBuilder.append(" CONSTRAINT ").append(quotedIdentifier(addIndex.getName()));
    }
    switch (addIndex.getType()) {
      case PRIMARY_KEY:
        sqlBuilder.append(" PRIMARY KEY ");
        break;
      case UNIQUE_KEY:
        sqlBuilder.append(" UNIQUE ");
        break;
      default:
        throw new IllegalArgumentException("Unsupported index type: " + addIndex.getType());
    }
    sqlBuilder.append("(").append(getIndexFieldStr(addIndex.getFieldNames())).append(");");
    return sqlBuilder.toString();
  }

  private void rejectStandaloneUniqueIndex(String schema, String table, String index) {
    // The table-scoped constraint drop cannot remove a standalone index. Refuse both strict and
    // IF EXISTS requests rather than silently succeeding or dropping a schema-scoped index name.
    // This lookup runs during SQL generation, before borrowing the DDL execution connection.
    try (Connection connection = getConnection(schema);
        PreparedStatement statement = connection.prepareStatement(STANDALONE_UNIQUE_INDEX_SQL)) {
      statement.setString(1, schema);
      statement.setString(2, table);
      statement.setString(3, index);
      try (ResultSet result = statement.executeQuery()) {
        Preconditions.checkArgument(
            !result.next(),
            "Index %s is a standalone unique index, not a table constraint; deleting it through the table index API is not supported",
            index);
      }
    } catch (SQLException e) {
      throw exceptionMapper.toGravitinoException(e);
    }
  }

  private String updateColumnNullabilityDefinition(
      TableChange.UpdateColumnNullability updateColumnNullability, String tableName) {
    if (updateColumnNullability.fieldName().length > 1) {
      throw new UnsupportedOperationException(POSTGRESQL_NOT_SUPPORT_NESTED_COLUMN_MSG);
    }
    String col = updateColumnNullability.fieldName()[0];
    if (updateColumnNullability.nullable()) {
      return ALTER_TABLE
          + quotedIdentifier(tableName)
          + " "
          + ALTER_COLUMN
          + quotedIdentifier(col)
          + " DROP NOT NULL;";
    } else {
      return ALTER_TABLE
          + quotedIdentifier(tableName)
          + " "
          + ALTER_COLUMN
          + quotedIdentifier(col)
          + " SET NOT NULL;";
    }
  }

  private String updateCommentDefinition(
      TableChange.UpdateComment updateComment, JdbcTable jdbcTable) {
    String newComment =
        StringUtils.defaultString(validateText(updateComment.getNewComment(), "table comment"));
    if (null == StringIdentifier.fromComment(newComment)) {
      // Detect and add Gravitino id.
      if (StringUtils.isNotEmpty(jdbcTable.comment())) {
        StringIdentifier identifier = StringIdentifier.fromComment(jdbcTable.comment());
        if (null != identifier) {
          newComment = StringIdentifier.addToComment(identifier, newComment);
        }
      }
    }
    return TABLE_COMMENT
        + quotedIdentifier(jdbcTable.name())
        + IS
        + escapeSqlLiteral(validateText(newComment, "table comment"), '\'')
        + "';";
  }

  private String deleteColumnFieldDefinition(
      TableChange.DeleteColumn deleteColumn, JdbcTable table) {
    if (deleteColumn.fieldName().length > 1) {
      throw new UnsupportedOperationException(POSTGRESQL_NOT_SUPPORT_NESTED_COLUMN_MSG);
    }
    String col = deleteColumn.fieldName()[0];
    boolean colExists =
        Arrays.stream(table.columns()).anyMatch(s -> StringUtils.equals(col, s.name()));
    if (!colExists) {
      if (BooleanUtils.isTrue(deleteColumn.getIfExists())) {
        return "";
      } else {
        throw new IllegalArgumentException("Delete column does not exist: " + col);
      }
    }
    return ALTER_TABLE
        + quotedIdentifier(table.name())
        + " DROP COLUMN "
        + quotedIdentifier(deleteColumn.fieldName()[0])
        + ";";
  }

  private String updateColumnDefaultValueFieldDefinition(
      TableChange.UpdateColumnDefaultValue updateColumnDefaultValue, JdbcTable jdbcTable) {
    if (updateColumnDefaultValue.fieldName().length > 1) {
      throw new UnsupportedOperationException(POSTGRESQL_NOT_SUPPORT_NESTED_COLUMN_MSG);
    }
    String col = updateColumnDefaultValue.fieldName()[0];
    JdbcColumn column =
        (JdbcColumn)
            Arrays.stream(jdbcTable.columns())
                .filter(c -> c.name().equals(col))
                .findFirst()
                .orElse(null);
    if (null == column) {
      throw new NoSuchColumnException("Column %s does not exist.", col);
    }

    StringBuilder sqlBuilder = new StringBuilder(ALTER_TABLE + quotedIdentifier(jdbcTable.name()));
    sqlBuilder
        .append("\n")
        .append(ALTER_COLUMN)
        .append(quotedIdentifier(col))
        .append(" SET DEFAULT ")
        .append(
            columnDefaultValueConverter.fromGravitino(
                updateColumnDefaultValue.getNewDefaultValue()));
    return sqlBuilder.append(";").toString();
  }

  private String updateColumnTypeFieldDefinition(
      String schemaName, TableChange.UpdateColumnType updateColumnType, JdbcTable jdbcTable) {
    if (updateColumnType.fieldName().length > 1) {
      throw new UnsupportedOperationException(POSTGRESQL_NOT_SUPPORT_NESTED_COLUMN_MSG);
    }
    String col = updateColumnType.fieldName()[0];
    JdbcColumn column =
        (JdbcColumn)
            Arrays.stream(jdbcTable.columns())
                .filter(c -> c.name().equals(col))
                .findFirst()
                .orElse(null);
    if (null == column) {
      throw new NoSuchColumnException("Column %s does not exist.", col);
    }
    Type newType = updateColumnType.getNewDataType();
    StringBuilder sqlBuilder = new StringBuilder(ALTER_TABLE + quotedIdentifier(jdbcTable.name()));
    sqlBuilder
        .append("\n")
        .append(ALTER_COLUMN)
        .append(quotedIdentifier(col))
        .append(" SET DATA TYPE ")
        .append(typeConverter.fromGravitino(newType));
    // SET DATA TYPE resets the column to the new type's default collation. When the column has an
    // explicit non-default collation and the new type is still collatable text, re-apply it so a
    // type change does not silently drop the collation. Non-text targets must not carry COLLATE;
    // PostgreSQL rejects it.
    if (isCollatableTextType(newType)) {
      String collation = explicitColumnCollation(schemaName, jdbcTable.name(), col);
      if (collation != null) {
        sqlBuilder.append(" COLLATE ").append(collation);
      }
    }
    if (!column.nullable()) {
      sqlBuilder
          .append(",\n")
          .append(ALTER_COLUMN)
          .append(quotedIdentifier(col))
          .append(" SET NOT NULL");
    }
    return sqlBuilder.append(";").toString();
  }

  private static boolean isCollatableTextType(Type type) {
    return type instanceof Types.StringType
        || type instanceof Types.VarCharType
        || type instanceof Types.FixedCharType;
  }

  /**
   * Returns the explicit collation of a column as a schema-qualified, quoted identifier (for
   * example {@code "pg_catalog"."C"}), or null when the column uses the type's default collation.
   *
   * <p>A collation is a schema object, so both its schema and name are read and quoted separately.
   * An unqualified name would fail when the collation's schema is not on the connection's search
   * path, and would silently rebind the column to a same-named collation in another schema. {@code
   * information_schema.columns.collation_name} is null for the type default, so a non-null value is
   * always an explicit collation worth preserving across a type change.
   */
  @Nullable
  private String explicitColumnCollation(String schemaName, String tableName, String columnName) {
    // No data source means SQL generation without a live connection (for example unit tests that
    // only assert the generated statement); there is nothing to look up.
    if (dataSource == null) {
      return null;
    }
    String sql =
        "SELECT collation_schema, collation_name FROM information_schema.columns "
            + "WHERE table_schema = ? AND table_name = ? AND column_name = ?";
    try (Connection connection = dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setString(1, schemaName);
      statement.setString(2, tableName);
      statement.setString(3, columnName);
      try (ResultSet rows = statement.executeQuery()) {
        if (!rows.next()) {
          return null;
        }
        String collationName = rows.getString("collation_name");
        if (collationName == null) {
          return null;
        }
        String collationSchema = rows.getString("collation_schema");
        // collation_schema is non-null whenever collation_name is, but quote defensively and fall
        // back to the bare name if it is ever absent.
        return collationSchema == null
            ? quotedIdentifier(collationName)
            : quotedIdentifier(collationSchema) + "." + quotedIdentifier(collationName);
      }
    } catch (SQLException e) {
      throw exceptionMapper.toGravitinoException(e);
    }
  }

  private String renameColumnFieldDefinition(
      TableChange.RenameColumn renameColumn, String tableName) {
    if (renameColumn.fieldName().length > 1) {
      throw new UnsupportedOperationException(POSTGRESQL_NOT_SUPPORT_NESTED_COLUMN_MSG);
    }
    return ALTER_TABLE
        + quotedIdentifier(tableName)
        + " RENAME COLUMN "
        + quotedIdentifier(renameColumn.fieldName()[0])
        + SPACE
        + "TO"
        + SPACE
        + quotedIdentifier(renameColumn.getNewName())
        + ";";
  }

  private List<String> addColumnFieldDefinition(
      TableChange.AddColumn addColumn, JdbcTable lazyLoadTable) {
    if (addColumn.fieldName().length > 1) {
      throw new UnsupportedOperationException(POSTGRESQL_NOT_SUPPORT_NESTED_COLUMN_MSG);
    }
    List<String> result = new ArrayList<>();
    String col = addColumn.fieldName()[0];

    StringBuilder columnDefinition = new StringBuilder();
    columnDefinition
        .append(ALTER_TABLE)
        .append(quotedIdentifier(lazyLoadTable.name()))
        .append(SPACE)
        .append("ADD COLUMN ")
        .append(quotedIdentifier(col))
        .append(SPACE)
        .append(typeConverter.fromGravitino(addColumn.getDataType()))
        .append(SPACE);

    if (addColumn.isAutoIncrement()) {
      if (!allowsAutoIncrement(addColumn.getDataType())) {
        throw new IllegalArgumentException(
            "Unsupported auto-increment , column: "
                + Arrays.toString(addColumn.getFieldName())
                + ", type: "
                + addColumn.getDataType());
      }
      columnDefinition.append("GENERATED BY DEFAULT AS IDENTITY ");
    }

    // Add NOT NULL if the column is marked as such
    if (!addColumn.isNullable()) {
      columnDefinition.append("NOT NULL ");
    }

    // Append default value if available
    if (!Column.DEFAULT_VALUE_NOT_SET.equals(addColumn.getDefaultValue())) {
      columnDefinition
          .append("DEFAULT ")
          .append(columnDefaultValueConverter.fromGravitino(addColumn.getDefaultValue()))
          .append(SPACE);
    }

    // Append position if available
    if (!(addColumn.getPosition() instanceof TableChange.Default)) {
      throw new IllegalArgumentException(
          "PostgreSQL does not support column position in Gravitino.");
    }
    result.add(columnDefinition.append(";").toString());

    // Append comment if available
    if (StringUtils.isNotEmpty(addColumn.getComment())) {
      result.add(
          COLUMN_COMMENT
              + quotedIdentifier(lazyLoadTable.name())
              + "."
              + quotedIdentifier(col)
              + IS
              + escapeSqlLiteral(
                  validateText(addColumn.getComment(), "comment for column " + col), '\'')
              + "';");
    }
    return result;
  }

  private String updateColumnCommentFieldDefinition(
      TableChange.UpdateColumnComment updateColumnComment, String tableName) {
    String newComment = StringUtils.defaultString(updateColumnComment.getNewComment());
    if (updateColumnComment.fieldName().length > 1) {
      throw new UnsupportedOperationException(POSTGRESQL_NOT_SUPPORT_NESTED_COLUMN_MSG);
    }
    String col = updateColumnComment.fieldName()[0];
    return COLUMN_COMMENT
        + quotedIdentifier(tableName)
        + "."
        + quotedIdentifier(col)
        + IS
        + escapeSqlLiteral(validateText(newComment, "comment for column " + col), '\'')
        + "';";
  }

  @Override
  protected ResultSet getIndexInfo(String schemaName, String tableName, DatabaseMetaData metaData)
      throws SQLException {
    return metaData.getIndexInfo(
        metaData.getConnection().getCatalog(), schemaName, tableName, false, false);
  }

  @Override
  protected ResultSet getPrimaryKeys(String schemaName, String tableName, DatabaseMetaData metaData)
      throws SQLException {
    return metaData.getPrimaryKeys(metaData.getConnection().getCatalog(), schemaName, tableName);
  }

  /**
   * {@inheritDoc}
   *
   * <p>PostgreSQL uses the duplicate-relation SQLSTATE for index and constraint name conflicts as
   * well as table conflicts. During an alter this is an invalid change, not an existing table.
   */
  @Override
  public void alterTable(String schema, String tableName, TableChange... changes) {
    try {
      super.alterTable(schema, tableName, changes);
    } catch (TableAlreadyExistsException e) {
      throw new IllegalArgumentException(
          "PostgreSQL relation name conflict while altering table "
              + schema
              + "."
              + tableName
              + ": an index or constraint may already use this name. Choose an unused name. "
              + "PostgreSQL reason: "
              + e.getMessage(),
          e);
    }
  }

  @Override
  protected Connection getConnection(String schema) throws SQLException {
    Connection connection = dataSource.getConnection();
    connection.setCatalog(database);
    connection.setSchema(schema);
    return connection;
  }

  /** {@inheritDoc} */
  @Override
  protected ResultSet getTables(Connection connection) throws SQLException {
    DatabaseMetaData metadata = connection.getMetaData();
    return metadata.getTables(
        connection.getCatalog(),
        metadataPattern(metadata, connection.getSchema()),
        null,
        JdbcConnectorUtils.getTableTypes());
  }

  @Override
  protected ResultSet getTable(Connection connection, String schema, String tableName)
      throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    return metaData.getTables(
        connection.getCatalog(),
        metadataPattern(metaData, schema),
        metadataPattern(metaData, tableName),
        null);
  }

  @Override
  protected ResultSet getColumns(Connection connection, String schema, String tableName)
      throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    CachedRowSet columns = RowSetProvider.newFactory().createCachedRowSet();
    try (ResultSet metadata =
        metaData.getColumns(
            connection.getCatalog(),
            metadataPattern(metaData, schema),
            metadataPattern(metaData, tableName),
            null)) {
      columns.populate(metadata);
    }

    boolean needsDeclarations = false;
    Boolean standardConformingStrings = null;
    while (columns.next()) {
      String defaultValue = columns.getString("COLUMN_DEF");
      String escaped =
          PostgreSqlColumnDefaultValueConverter.withExplicitBackslashEscapes(defaultValue);
      if (!Objects.equals(defaultValue, escaped)) {
        if (standardConformingStrings == null) {
          // pgJDBC's literal escaper uses the cached standard_conforming_strings setting.
          // A single backslash stays unchanged only in standard mode. This public API also works
          // with older drivers without the parameter-status API, with no query or session change.
          standardConformingStrings =
              "\\".equals(connection.unwrap(PGConnection.class).escapeLiteral("\\"));
        }
        if (!standardConformingStrings) {
          columns.updateString("COLUMN_DEF", escaped);
          columns.updateRow();
        }
      }
      String type = columns.getString("TYPE_NAME");
      if ("varchar".equals(type)
          || "bpchar".equals(type)
          || "_varchar".equals(type)
          || "_bpchar".equals(type)
          || (getAutoIncrementInfo(columns) && columns.getString("COLUMN_DEF") != null)) {
        needsDeclarations = true;
      }
    }
    columns.beforeFirst();
    if (!needsDeclarations) {
      return columns;
    }

    // COLUMN_SIZE substitutes the configurable pgjdbc unknownLength for unbounded character
    // types. Serial defaults also need their declaration: JDBC marks shared sequence defaults as
    // auto-incrementing too. One catalog query resolves both without querying per column.
    Set<String> unboundedColumns = new HashSet<>();
    Set<String> serialColumns = new HashSet<>();
    // Match only a bare nextval call and verify its dependency on the owned sequence. Ownership
    // alone would also match custom defaults such as nextval(owned_sequence) + 1.
    // Resolve ownership by OID: pg_get_serial_sequence would resolve the table name and require
    // schema USAGE even when JDBC and catalog metadata are readable without that privilege.
    String sql =
        "SELECT a.attname, a.atttypmod = -1 AND a.atttypid IN "
            + "('pg_catalog.varchar'::pg_catalog.regtype, 'pg_catalog.bpchar'::pg_catalog.regtype, "
            + "'pg_catalog.varchar[]'::pg_catalog.regtype, "
            + "'pg_catalog.bpchar[]'::pg_catalog.regtype) AS unbounded_character, "
            + "a.attnotnull AND a.atttypid IN ('pg_catalog.int2'::pg_catalog.regtype, "
            + "'pg_catalog.int4'::pg_catalog.regtype, 'pg_catalog.int8'::pg_catalog.regtype) "
            + "AND pg_catalog.pg_get_expr(d.adbin, d.adrelid) ~ "
            + "'^nextval[(]''([^'']|'''')*''::regclass[)]$' "
            + "AND EXISTS (SELECT 1 FROM pg_catalog.pg_depend dep "
            + "WHERE dep.classid = 'pg_catalog.pg_attrdef'::pg_catalog.regclass "
            + "AND dep.objid = d.oid AND dep.refclassid = 'pg_catalog.pg_class'::pg_catalog.regclass "
            + "AND dep.deptype = 'n' "
            + "AND EXISTS (SELECT 1 FROM pg_catalog.pg_depend owned "
            + "JOIN pg_catalog.pg_class seq ON seq.oid = owned.objid AND seq.relkind = 'S' "
            + "WHERE owned.classid = 'pg_catalog.pg_class'::pg_catalog.regclass "
            + "AND owned.objid = dep.refobjid AND owned.objsubid = 0 "
            + "AND owned.refclassid = 'pg_catalog.pg_class'::pg_catalog.regclass "
            + "AND owned.refobjid = a.attrelid AND owned.refobjsubid = a.attnum "
            + "AND owned.deptype IN ('a', 'i'))) AS owned_serial "
            + "FROM pg_catalog.pg_attribute a "
            + "JOIN pg_catalog.pg_class c ON a.attrelid = c.oid "
            + "JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid "
            + "LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum "
            + "WHERE n.nspname = ? AND c.relname = ? AND a.attnum > 0 AND NOT a.attisdropped";
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setString(1, schema);
      statement.setString(2, tableName);
      try (ResultSet declarations = statement.executeQuery()) {
        while (declarations.next()) {
          if (declarations.getBoolean("unbounded_character")) {
            unboundedColumns.add(declarations.getString("attname"));
          }
          if (declarations.getBoolean("owned_serial")) {
            serialColumns.add(declarations.getString("attname"));
          }
        }
      }
    }
    while (columns.next()) {
      boolean updated = false;
      String name = columns.getString("COLUMN_NAME");
      if (unboundedColumns.contains(name)) {
        // Zero is the converter's canonical unknown length, independent of driver configuration.
        columns.updateInt("COLUMN_SIZE", 0);
        updated = true;
      }
      if (getAutoIncrementInfo(columns) && serialColumns.contains(name)) {
        // Preserve basic auto-increment semantics using a new IDENTITY on recreation, rather
        // than copying a default that still depends on the source table's owned sequence.
        // Shared sequences and custom expressions deliberately retain their original defaults.
        columns.updateNull("COLUMN_DEF");
        updated = true;
      }
      if (updated) {
        columns.updateRow();
      }
    }
    columns.beforeFirst();
    return columns;
  }

  /**
   * Reads the JDBC YES/NO marker without relying on pgjdbc's boolean coercion, which a cached
   * metadata row set does not provide.
   *
   * @param resultSet the column metadata positioned on the current column
   * @return whether the column is auto-incrementing
   * @throws SQLException if the metadata cannot be read
   */
  @Override
  protected boolean getAutoIncrementInfo(ResultSet resultSet) throws SQLException {
    return "YES".equals(resultSet.getString("IS_AUTOINCREMENT"));
  }

  @Override
  public Integer calculateDatetimePrecision(String typeName, int columnSize, int scale) {
    String upperTypeName = typeName.toUpperCase(Locale.ROOT);
    if (upperTypeName.startsWith("_")) {
      upperTypeName = upperTypeName.substring(1);
    }
    switch (upperTypeName) {
      case "TIME":
      case "TIMETZ":
      case "TIMESTAMP":
      case "TIMESTAMPTZ":
        return Math.max(scale, 0);
      default:
        return null;
    }
  }

  private boolean schemaLacksUsage(String schema) {
    try (Connection connection = dataSource.getConnection();
        PreparedStatement statement =
            connection.prepareStatement(
                "SELECT pg_catalog.has_schema_privilege(n.oid, 'USAGE') "
                    + "FROM pg_catalog.pg_namespace n WHERE n.nspname = ?")) {
      statement.setString(1, schema);
      try (ResultSet rows = statement.executeQuery()) {
        return rows.next() && !rows.getBoolean(1);
      }
    } catch (SQLException e) {
      throw exceptionMapper.toGravitinoException(e);
    }
  }

  @Nullable
  private static String validateText(@Nullable String text, String field) {
    if (text != null && text.indexOf('\0') >= 0) {
      throw new IllegalArgumentException("PostgreSQL " + field + " must not contain U+0000 (NUL)");
    }
    return text;
  }

  private static String quotedIdentifier(String name) {
    return PG_QUOTE + name.replace(PG_QUOTE, PG_QUOTE + PG_QUOTE) + PG_QUOTE;
  }

  private static void validateIndexName(@Nullable String name) {
    if (name != null && name.getBytes(StandardCharsets.UTF_8).length > 63) {
      throw new IllegalArgumentException(
          "PostgreSQL index name must not exceed 63 UTF-8 bytes: " + name);
    }
  }

  @Nullable
  private static String metadataPattern(DatabaseMetaData metadata, @Nullable String name)
      throws SQLException {
    if (name == null) {
      return null;
    }
    String escape = metadata.getSearchStringEscape();
    return name.replace(escape, escape + escape)
        .replace("%", escape + "%")
        .replace("_", escape + "_");
  }
}
