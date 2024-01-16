/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.postgresql.operation;

import com.datastrato.gravitino.StringIdentifier;
import com.datastrato.gravitino.catalog.jdbc.JdbcColumn;
import com.datastrato.gravitino.catalog.jdbc.JdbcTable;
import com.datastrato.gravitino.catalog.jdbc.config.JdbcConfig;
import com.datastrato.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import com.datastrato.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import com.datastrato.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import com.datastrato.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import com.datastrato.gravitino.exceptions.NoSuchColumnException;
import com.datastrato.gravitino.exceptions.NoSuchTableException;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.expressions.literals.Literals;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.types.Types;
import com.google.common.base.Preconditions;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import javax.sql.DataSource;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

/** Table operations for PostgreSQL. */
public class PostgreSqlTableOperations extends JdbcTableOperations {

  public static final String PG_QUOTE = "\"";

  private static final String SHOW_COLUMN_COMMENT_SQL =
      "SELECT \n"
          + "    a.attname as col_name,\n"
          + "    col_description(a.attrelid, a.attnum) as comment\n"
          + "FROM \n"
          + "    pg_class AS c\n"
          + "JOIN \n"
          + "    pg_attribute AS a ON a.attrelid = c.oid\n"
          + "JOIN\n"
          + "    pg_namespace AS n ON n.oid = c.relnamespace\n"
          + "WHERE \n"
          + "    a.attnum > 0 \n"
          + "    AND c.relname = ? AND n.nspname = ?";

  private static final String SHOW_COLUMN_INFO_SQL =
      "select * FROM information_schema.columns WHERE table_name = ? AND table_schema = ? order by ordinal_position";

  private static final String SHOW_TABLE_COMMENT_SQL =
      "SELECT tb.table_name, d.description\n"
          + "FROM information_schema.tables tb\n"
          + "         JOIN pg_class c ON c.relname = tb.table_name\n"
          + "         LEFT JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = '0'\n"
          + "WHERE tb.table_name = ? AND table_schema = ?;";

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
  public JdbcTable load(String schema, String tableName) throws NoSuchTableException {
    try (Connection connection = getConnection(schema)) {
      // The first step is to obtain the comment information of the column.
      Map<String, String> columnCommentMap = selectColumnComment(schema, tableName, connection);

      // The second step is to obtain the column information of the table.
      List<JdbcColumn> jdbcColumns =
          selectColumnInfoAndExecute(
              schema,
              tableName,
              connection,
              (builder, s) -> builder.withComment(columnCommentMap.get(s)));

      // The third step is to obtain the comment information of the table.
      String comment = selectTableComment(schema, tableName, connection);
      return new JdbcTable.Builder()
          .withName(tableName)
          .withColumns(jdbcColumns.toArray(new JdbcColumn[0]))
          .withComment(comment)
          .withAuditInfo(AuditInfo.EMPTY)
          .withProperties(Collections.emptyMap())
          .build();
    } catch (SQLException e) {
      throw this.exceptionMapper.toGravitinoException(e);
    }
  }

  private List<JdbcColumn> selectColumnInfoAndExecute(
      String schemaName,
      String tableName,
      Connection connection,
      BiConsumer<JdbcColumn.Builder, String> builderConsumer)
      throws SQLException {
    List<JdbcColumn> jdbcColumns = new ArrayList<>();
    try (PreparedStatement preparedStatement = connection.prepareStatement(SHOW_COLUMN_INFO_SQL)) {
      preparedStatement.setString(1, tableName);
      preparedStatement.setString(2, schemaName);
      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        while (resultSet.next()) {
          ColDataType colDataType = new ColDataType();
          colDataType.setDataType(resultSet.getString("data_type"));
          colDataType.setArgumentsStringList(getArgList(resultSet));
          String columnName = resultSet.getString("column_name");
          boolean nullable = "YES".equalsIgnoreCase(resultSet.getString("is_nullable"));
          String defaultValue = extractDefaultValue(resultSet.getString("column_default"));
          JdbcColumn.Builder builder =
              new JdbcColumn.Builder()
                  .withName(columnName)
                  .withDefaultValue(
                      (nullable && defaultValue == null)
                          ? Literals.NULL
                          : columnDefaultValueConverter.toGravitino(colDataType, defaultValue))
                  .withNullable("YES".equalsIgnoreCase(resultSet.getString("is_nullable")))
                  .withType(typeConverter.toGravitinoType(colDataType))
                  .withAutoIncrement("YES".equalsIgnoreCase(resultSet.getString("is_identity")));
          builderConsumer.accept(builder, columnName);
          jdbcColumns.add(builder.build());
        }
      }
    }
    if (jdbcColumns.isEmpty()) {
      throw new NoSuchTableException("Table " + tableName + " does not exist.");
    }
    return jdbcColumns;
  }

  private static List<String> getArgList(ResultSet resultSet) throws SQLException {
    List<String> result = new ArrayList<>();
    String characterMaximumLength = resultSet.getString("character_maximum_length");
    if (StringUtils.isNotEmpty(characterMaximumLength)) {
      result.add(characterMaximumLength);
    }
    String numericPrecision = resultSet.getString("numeric_precision");
    if (StringUtils.isNotEmpty(numericPrecision)) {
      result.add(numericPrecision);
    }
    String numericScale = resultSet.getString("numeric_scale");
    if (StringUtils.isNotEmpty(numericScale)) {
      result.add(numericScale);
    }
    String datetimePrecision = resultSet.getString("datetime_precision");
    if (StringUtils.isNotEmpty(datetimePrecision)) {
      result.add(datetimePrecision);
    }
    return result;
  }

  private String selectTableComment(String schema, String tableName, Connection connection)
      throws SQLException {
    try (PreparedStatement preparedStatement =
        connection.prepareStatement(SHOW_TABLE_COMMENT_SQL)) {
      preparedStatement.setString(1, tableName);
      preparedStatement.setString(2, schema);
      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        if (resultSet.next()) {
          return resultSet.getString("description");
        }
      }
    }
    return null;
  }

  /**
   * @return Returns the column names and comments of the table
   * @throws SQLException
   */
  private Map<String, String> selectColumnComment(
      String schema, String tableName, Connection connection) throws SQLException {
    Map<String, String> columnCommentMap = new HashMap<>();

    try (PreparedStatement preparedStatement =
        connection.prepareStatement(SHOW_COLUMN_COMMENT_SQL)) {
      preparedStatement.setString(1, tableName);
      preparedStatement.setString(2, schema);
      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        while (resultSet.next()) {
          String comment = resultSet.getString("comment");
          if (null != comment) {
            String columnName = resultSet.getString("col_name");
            columnCommentMap.put(columnName, comment);
          }
        }
      }
    }
    return columnCommentMap;
  }

  private static String extractDefaultValue(String columnDefault) {
    if (columnDefault == null) {
      return null;
    }

    // Remove single quotes and '::'
    int i = columnDefault.indexOf("::");
    if (-1 != i) {
      columnDefault = columnDefault.substring(0, i);
    }
    return columnDefault;
  }

  @Override
  protected String generateCreateTableSql(
      String tableName,
      JdbcColumn[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitioning) {
    if (ArrayUtils.isNotEmpty(partitioning)) {
      throw new UnsupportedOperationException(
          "Currently we do not support Partitioning in PostgreSQL");
    }
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append("CREATE TABLE ").append(tableName).append(" (\n");

    // Add columns
    for (int i = 0; i < columns.length; i++) {
      JdbcColumn column = columns[i];
      sqlBuilder.append("    \"").append(column.name()).append(PG_QUOTE);

      appendColumnDefinition(column, sqlBuilder);
      // Add a comma for the next column, unless it's the last one
      if (i < columns.length - 1) {
        sqlBuilder.append(",\n");
      }
    }
    sqlBuilder.append("\n)");
    // Add table properties if any
    if (MapUtils.isNotEmpty(properties)) {
      // TODO #804 will add properties
      throw new IllegalArgumentException("Properties are not supported yet");
    }

    sqlBuilder.append(";");

    // Add table comment if specified
    if (StringUtils.isNotEmpty(comment)) {
      sqlBuilder
          .append("\nCOMMENT ON TABLE ")
          .append(tableName)
          .append(" IS '")
          .append(comment)
          .append("';");
    }
    Arrays.stream(columns)
        .filter(jdbcColumn -> StringUtils.isNotEmpty(jdbcColumn.comment()))
        .forEach(
            jdbcColumn ->
                sqlBuilder
                    .append("\nCOMMENT ON COLUMN ")
                    .append(tableName)
                    .append(".")
                    .append(jdbcColumn.name())
                    .append(" IS '")
                    .append(jdbcColumn.comment())
                    .append("';"));

    // Return the generated SQL statement
    String result = sqlBuilder.toString();

    LOG.info("Generated create table:{} sql: {}", tableName, result);
    return result;
  }

  private StringBuilder appendColumnDefinition(JdbcColumn column, StringBuilder sqlBuilder) {
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
    if (!JdbcColumn.DEFAULT_VALUE_NOT_SET.equals(column.defaultValue())) {
      sqlBuilder
          .append("DEFAULT")
          .append(columnDefaultValueConverter.fromGravitino(column))
          .append(SPACE);
    }

    // Add column properties if specified
    if (CollectionUtils.isNotEmpty(column.getProperties())) {
      // TODO #804 will add properties
      throw new IllegalArgumentException("Properties are not supported yet");
    }
    return sqlBuilder;
  }

  @Override
  protected String generateRenameTableSql(String oldTableName, String newTableName) {
    return "ALTER TABLE " + oldTableName + " RENAME TO " + newTableName;
  }

  @Override
  protected String generateDropTableSql(String tableName) {
    throw new UnsupportedOperationException(
        "PostgreSQL does not support drop operation in Gravitino, please use purge operation");
  }

  @Override
  protected String generatePurgeTableSql(String tableName) {
    return "DROP TABLE " + tableName;
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
        alterSql.add(
            updateColumnNullabilityDefinition(
                (TableChange.UpdateColumnNullability) change, tableName));
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

  private String updateColumnNullabilityDefinition(
      TableChange.UpdateColumnNullability updateColumnNullability, String tableName) {
    if (updateColumnNullability.fieldName().length > 1) {
      throw new UnsupportedOperationException("PostgreSQL does not support nested column names.");
    }
    String col = updateColumnNullability.fieldName()[0];
    if (updateColumnNullability.nullable()) {
      return "ALTER TABLE " + tableName + " ALTER COLUMN " + col + " DROP NOT NULL;";
    } else {
      return "ALTER TABLE " + tableName + " ALTER COLUMN " + col + " SET NOT NULL;";
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
    return "COMMENT ON TABLE " + jdbcTable.name() + " IS '" + newComment + "';";
  }

  private String deleteColumnFieldDefinition(
      TableChange.DeleteColumn deleteColumn, JdbcTable table) {
    if (deleteColumn.fieldName().length > 1) {
      throw new UnsupportedOperationException("PostgreSQL does not support nested column names.");
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
    return "ALTER TABLE " + table.name() + " DROP COLUMN " + deleteColumn.fieldName()[0] + ";";
  }

  private String updateColumnTypeFieldDefinition(
      TableChange.UpdateColumnType updateColumnType, JdbcTable jdbcTable) {
    if (updateColumnType.fieldName().length > 1) {
      throw new UnsupportedOperationException("PostgreSQL does not support nested column names.");
    }
    String col = updateColumnType.fieldName()[0];
    JdbcColumn column =
        (JdbcColumn)
            Arrays.stream(jdbcTable.columns())
                .filter(c -> c.name().equals(col))
                .findFirst()
                .orElse(null);
    if (null == column) {
      throw new NoSuchColumnException("Column " + col + " does not exist.");
    }
    StringBuilder sqlBuilder = new StringBuilder("ALTER TABLE " + jdbcTable.name());
    sqlBuilder
        .append("\n")
        .append("ALTER COLUMN ")
        .append(col)
        .append(" SET DATA TYPE ")
        .append(typeConverter.fromGravitinoType(updateColumnType.getNewDataType()));
    if (!column.nullable()) {
      sqlBuilder.append(",\n").append("ALTER COLUMN ").append(col).append(" SET NOT NULL");
    }
    return sqlBuilder.append(";").toString();
  }

  private String renameColumnFieldDefinition(
      TableChange.RenameColumn renameColumn, String tableName) {
    if (renameColumn.fieldName().length > 1) {
      throw new UnsupportedOperationException("PostgreSQL does not support nested column names.");
    }
    return "ALTER TABLE "
        + tableName
        + " RENAME COLUMN "
        + renameColumn.fieldName()[0]
        + SPACE
        + "TO"
        + SPACE
        + renameColumn.getNewName()
        + ";";
  }

  private JdbcTable getOrCreateTable(
      String databaseName, String tableName, JdbcTable lazyLoadTable) {
    if (null == lazyLoadTable) {
      return load(databaseName, tableName);
    }
    return lazyLoadTable;
  }

  private List<String> addColumnFieldDefinition(
      TableChange.AddColumn addColumn, JdbcTable lazyLoadTable) {
    if (addColumn.fieldName().length > 1) {
      throw new UnsupportedOperationException("PostgreSQL does not support nested column names.");
    }
    List<String> result = new ArrayList<>();
    String col = addColumn.fieldName()[0];

    StringBuilder columnDefinition = new StringBuilder();
    columnDefinition
        .append("ALTER TABLE ")
        .append(lazyLoadTable.name())
        .append(SPACE)
        .append("ADD COLUMN ")
        .append(col)
        .append(SPACE)
        .append(typeConverter.fromGravitinoType(addColumn.getDataType()))
        .append(SPACE);

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
          "COMMENT ON COLUMN "
              + lazyLoadTable.name()
              + "."
              + col
              + " IS '"
              + addColumn.getComment()
              + "';");
    }
    return result;
  }

  private String updateColumnCommentFieldDefinition(
      TableChange.UpdateColumnComment updateColumnComment, String tableName) {
    String newComment = updateColumnComment.getNewComment();
    if (updateColumnComment.fieldName().length > 1) {
      throw new UnsupportedOperationException("PostgreSQL does not support nested column names.");
    }
    String col = updateColumnComment.fieldName()[0];
    return "COMMENT ON COLUMN " + tableName + "." + col + " IS '" + newComment + "';";
  }

  @Override
  protected Map<String, String> extractPropertiesFromResultSet(ResultSet table) {
    // We have rewritten the `load` method, so there is no need to implement this method
    throw new UnsupportedOperationException("Extracting table columns is not supported yet");
  }

  @Override
  protected JdbcColumn extractJdbcColumnFromResultSet(ResultSet column) {
    // We have rewritten the `load` method, so there is no need to implement this method
    throw new UnsupportedOperationException("Extracting table columns is not supported yet");
  }

  @Override
  protected Connection getConnection(String schema) throws SQLException {
    Connection connection = dataSource.getConnection();
    connection.setCatalog(database);
    connection.setSchema(schema);
    return connection;
  }
}
