/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.postgresql.operation;

import static com.datastrato.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;

import com.datastrato.gravitino.StringIdentifier;
import com.datastrato.gravitino.catalog.jdbc.JdbcColumn;
import com.datastrato.gravitino.catalog.jdbc.JdbcTable;
import com.datastrato.gravitino.catalog.jdbc.config.JdbcConfig;
import com.datastrato.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import com.datastrato.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import com.datastrato.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import com.datastrato.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import com.datastrato.gravitino.exceptions.NoSuchColumnException;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.indexes.Index;
import com.datastrato.gravitino.rel.types.Types;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

/** Table operations for PostgreSQL. */
public class PostgreSqlTableOperations extends JdbcTableOperations {

  public static final String PG_QUOTE = "\"";
  public static final String NEW_LINE = "\n";
  public static final String ALTER_TABLE = "ALTER TABLE ";
  public static final String ALTER_COLUMN = "ALTER COLUMN ";
  public static final String IS = " IS '";
  public static final String COLUMN_COMMENT = "COMMENT ON COLUMN ";
  public static final String TABLE_COMMENT = "COMMENT ON TABLE ";

  private static final String POSTGRESQL_NOT_SUPPORT_NESTED_COLUMN_MSG =
      "PostgreSQL does not support nested column names.";

  private String database;

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
  protected String generateCreateTableSql(
      String tableName,
      JdbcColumn[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitioning,
      Index[] indexes) {
    if (ArrayUtils.isNotEmpty(partitioning)) {
      throw new UnsupportedOperationException(
          "Currently we do not support Partitioning in PostgreSQL");
    }
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder
        .append("CREATE TABLE ")
        .append(PG_QUOTE)
        .append(tableName)
        .append(PG_QUOTE)
        .append(" (")
        .append(NEW_LINE);

    // Add columns
    for (int i = 0; i < columns.length; i++) {
      JdbcColumn column = columns[i];
      sqlBuilder.append("    ").append(PG_QUOTE).append(column.name()).append(PG_QUOTE);

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
          .append(tableName)
          .append(IS)
          .append(comment)
          .append("';");
    }
    Arrays.stream(columns)
        .filter(jdbcColumn -> StringUtils.isNotEmpty(jdbcColumn.comment()))
        .forEach(
            jdbcColumn ->
                sqlBuilder
                    .append(NEW_LINE)
                    .append(COLUMN_COMMENT)
                    .append(tableName)
                    .append(".")
                    .append(jdbcColumn.name())
                    .append(IS)
                    .append(jdbcColumn.comment())
                    .append("';"));

    // Return the generated SQL statement
    String result = sqlBuilder.toString();

    LOG.info("Generated create table:{} sql: {}", tableName, result);
    return result;
  }

  @VisibleForTesting
  static void appendIndexesSql(Index[] indexes, StringBuilder sqlBuilder) {
    for (Index index : indexes) {
      String fieldStr = getIndexFieldStr(index.fieldNames());
      sqlBuilder.append(",").append(NEW_LINE);
      switch (index.type()) {
        case PRIMARY_KEY:
          if (StringUtils.isNotEmpty(index.name())) {
            sqlBuilder.append("CONSTRAINT ").append(PG_QUOTE).append(index.name()).append(PG_QUOTE);
          }
          sqlBuilder.append(" PRIMARY KEY (").append(fieldStr).append(")");
          break;
        case UNIQUE_KEY:
          if (StringUtils.isNotEmpty(index.name())) {
            sqlBuilder.append("CONSTRAINT ").append(PG_QUOTE).append(index.name()).append(PG_QUOTE);
          }
          sqlBuilder.append(" UNIQUE (").append(fieldStr).append(")");
          break;
        default:
          throw new IllegalArgumentException("PostgreSQL doesn't support index : " + index.type());
      }
    }
  }

  private static String getIndexFieldStr(String[][] fieldNames) {
    return Arrays.stream(fieldNames)
        .map(
            colNames -> {
              if (colNames.length > 1) {
                throw new IllegalArgumentException(
                    "Index does not support complex fields in PostgreSQL");
              }
              return PG_QUOTE + colNames[0] + PG_QUOTE;
            })
        .collect(Collectors.joining(", "));
  }

  private void appendColumnDefinition(JdbcColumn column, StringBuilder sqlBuilder) {
    // Add data type
    sqlBuilder
        .append(SPACE)
        .append(typeConverter.fromGravitinoType(column.dataType()))
        .append(SPACE);

    if (column.autoIncrement()) {
      if (!Types.allowAutoIncrement(column.dataType())) {
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
    if (!DEFAULT_VALUE_NOT_SET.equals(column.defaultValue())) {
      sqlBuilder
          .append("DEFAULT ")
          .append(columnDefaultValueConverter.fromGravitino(column.defaultValue()))
          .append(SPACE);
    }
  }

  @Override
  protected String generateRenameTableSql(String oldTableName, String newTableName) {
    return ALTER_TABLE + PG_QUOTE + oldTableName + PG_QUOTE + " RENAME TO " + newTableName;
  }

  @Override
  protected String generateDropTableSql(String tableName) {
    return "DROP TABLE " + PG_QUOTE + tableName + PG_QUOTE;
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
      } else if (change instanceof TableChange.UpdateColumnType) {
        lazyLoadTable = getOrCreateTable(schemaName, tableName, lazyLoadTable);
        TableChange.UpdateColumnType updateColumnType = (TableChange.UpdateColumnType) change;
        alterSql.add(updateColumnTypeFieldDefinition(updateColumnType, lazyLoadTable));
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
        validateUpdateColumnNullable(updateColumnNullability, lazyLoadTable);

        alterSql.add(updateColumnNullabilityDefinition(updateColumnNullability, tableName));
      } else if (change instanceof TableChange.AddIndex) {
        alterSql.add(addIndexDefinition(tableName, (TableChange.AddIndex) change));
      } else if (change instanceof TableChange.DeleteIndex) {
        alterSql.add(deleteIndexDefinition(tableName, (TableChange.DeleteIndex) change));
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
        PG_QUOTE + tableName + PG_QUOTE, ALTER_COLUMN, PG_QUOTE + fieldName + PG_QUOTE, action);
  }

  @VisibleForTesting
  static String deleteIndexDefinition(String tableName, TableChange.DeleteIndex deleteIndex) {
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder
        .append("ALTER TABLE ")
        .append(PG_QUOTE)
        .append(tableName)
        .append(PG_QUOTE)
        .append(" DROP CONSTRAINT ")
        .append(PG_QUOTE)
        .append(deleteIndex.getName())
        .append(PG_QUOTE)
        .append(";\n");
    if (deleteIndex.isIfExists()) {
      sqlBuilder
          .append("DROP INDEX IF EXISTS ")
          .append(PG_QUOTE)
          .append(deleteIndex.getName())
          .append(PG_QUOTE)
          .append(";");
    } else {
      sqlBuilder
          .append("DROP INDEX ")
          .append(PG_QUOTE)
          .append(deleteIndex.getName())
          .append(PG_QUOTE)
          .append(";");
    }
    return sqlBuilder.toString();
  }

  @VisibleForTesting
  static String addIndexDefinition(String tableName, TableChange.AddIndex addIndex) {
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder
        .append("ALTER TABLE ")
        .append(PG_QUOTE)
        .append(tableName)
        .append(PG_QUOTE)
        .append(" ADD CONSTRAINT ")
        .append(PG_QUOTE)
        .append(addIndex.getName())
        .append(PG_QUOTE);
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

  private String updateColumnNullabilityDefinition(
      TableChange.UpdateColumnNullability updateColumnNullability, String tableName) {
    if (updateColumnNullability.fieldName().length > 1) {
      throw new UnsupportedOperationException(POSTGRESQL_NOT_SUPPORT_NESTED_COLUMN_MSG);
    }
    String col = updateColumnNullability.fieldName()[0];
    if (updateColumnNullability.nullable()) {
      return ALTER_TABLE
          + PG_QUOTE
          + tableName
          + PG_QUOTE
          + " "
          + ALTER_COLUMN
          + PG_QUOTE
          + col
          + PG_QUOTE
          + " DROP NOT NULL;";
    } else {
      return ALTER_TABLE
          + PG_QUOTE
          + tableName
          + PG_QUOTE
          + " "
          + ALTER_COLUMN
          + PG_QUOTE
          + col
          + PG_QUOTE
          + " SET NOT NULL;";
    }
  }

  private String updateCommentDefinition(
      TableChange.UpdateComment updateComment, JdbcTable jdbcTable) {
    String newComment = updateComment.getNewComment();
    if (null == StringIdentifier.fromComment(newComment)) {
      // Detect and add gravitino id.
      if (StringUtils.isNotEmpty(jdbcTable.comment())) {
        StringIdentifier identifier = StringIdentifier.fromComment(jdbcTable.comment());
        if (null != identifier) {
          newComment = StringIdentifier.addToComment(identifier, newComment);
        }
      }
    }
    return TABLE_COMMENT + PG_QUOTE + jdbcTable.name() + PG_QUOTE + IS + newComment + "';";
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
        + PG_QUOTE
        + table.name()
        + PG_QUOTE
        + " DROP COLUMN "
        + PG_QUOTE
        + deleteColumn.fieldName()[0]
        + PG_QUOTE
        + ";";
  }

  private String updateColumnTypeFieldDefinition(
      TableChange.UpdateColumnType updateColumnType, JdbcTable jdbcTable) {
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
    StringBuilder sqlBuilder = new StringBuilder(ALTER_TABLE + jdbcTable.name());
    sqlBuilder
        .append("\n")
        .append(ALTER_COLUMN)
        .append(PG_QUOTE)
        .append(col)
        .append(PG_QUOTE)
        .append(" SET DATA TYPE ")
        .append(typeConverter.fromGravitinoType(updateColumnType.getNewDataType()));
    if (!column.nullable()) {
      sqlBuilder
          .append(",\n")
          .append(ALTER_COLUMN)
          .append(PG_QUOTE)
          .append(col)
          .append(PG_QUOTE)
          .append(" SET NOT NULL");
    }
    return sqlBuilder.append(";").toString();
  }

  private String renameColumnFieldDefinition(
      TableChange.RenameColumn renameColumn, String tableName) {
    if (renameColumn.fieldName().length > 1) {
      throw new UnsupportedOperationException(POSTGRESQL_NOT_SUPPORT_NESTED_COLUMN_MSG);
    }
    return ALTER_TABLE
        + tableName
        + " RENAME COLUMN "
        + PG_QUOTE
        + renameColumn.fieldName()[0]
        + PG_QUOTE
        + SPACE
        + "TO"
        + SPACE
        + PG_QUOTE
        + renameColumn.getNewName()
        + PG_QUOTE
        + ";";
  }

  public JdbcTable getOrCreateTable(
      String databaseName, String tableName, JdbcTable lazyLoadTable) {
    if (null == lazyLoadTable) {
      return load(databaseName, tableName);
    }
    return lazyLoadTable;
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
        .append(lazyLoadTable.name())
        .append(SPACE)
        .append("ADD COLUMN ")
        .append(PG_QUOTE)
        .append(col)
        .append(PG_QUOTE)
        .append(SPACE)
        .append(typeConverter.fromGravitinoType(addColumn.getDataType()))
        .append(SPACE);

    if (addColumn.isAutoIncrement()) {
      if (!Types.allowAutoIncrement(addColumn.getDataType())) {
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

    // Append position if available
    if (!(addColumn.getPosition() instanceof TableChange.Default)) {
      throw new IllegalArgumentException(
          "PostgreSQL does not support column position in gravitino.");
    }
    result.add(columnDefinition.append(";").toString());

    // Append comment if available
    if (StringUtils.isNotEmpty(addColumn.getComment())) {
      result.add(
          COLUMN_COMMENT
              + PG_QUOTE
              + lazyLoadTable.name()
              + PG_QUOTE
              + "."
              + PG_QUOTE
              + col
              + PG_QUOTE
              + IS
              + addColumn.getComment()
              + "';");
    }
    return result;
  }

  private String updateColumnCommentFieldDefinition(
      TableChange.UpdateColumnComment updateColumnComment, String tableName) {
    String newComment = updateColumnComment.getNewComment();
    if (updateColumnComment.fieldName().length > 1) {
      throw new UnsupportedOperationException(POSTGRESQL_NOT_SUPPORT_NESTED_COLUMN_MSG);
    }
    String col = updateColumnComment.fieldName()[0];
    return COLUMN_COMMENT
        + PG_QUOTE
        + tableName
        + PG_QUOTE
        + "."
        + PG_QUOTE
        + col
        + PG_QUOTE
        + IS
        + newComment
        + "';";
  }

  @Override
  protected ResultSet getIndexInfo(String schemaName, String tableName, DatabaseMetaData metaData)
      throws SQLException {
    return metaData.getIndexInfo(database, schemaName, tableName, false, false);
  }

  @Override
  protected ResultSet getPrimaryKeys(String schemaName, String tableName, DatabaseMetaData metaData)
      throws SQLException {
    return metaData.getPrimaryKeys(database, schemaName, tableName);
  }

  @Override
  protected Connection getConnection(String schema) throws SQLException {
    Connection connection = dataSource.getConnection();
    connection.setCatalog(database);
    connection.setSchema(schema);
    return connection;
  }

  @Override
  protected ResultSet getTable(Connection connection, String schema, String tableName)
      throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    return metaData.getTables(database, schema, tableName, null);
  }

  @Override
  protected ResultSet getColumns(Connection connection, String schema, String tableName)
      throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    return metaData.getColumns(database, schema, tableName, null);
  }
}
