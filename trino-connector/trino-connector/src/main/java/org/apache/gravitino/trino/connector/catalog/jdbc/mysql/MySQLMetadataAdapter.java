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
package org.apache.gravitino.trino.connector.catalog.jdbc.mysql;

import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;
import static org.apache.gravitino.trino.connector.catalog.jdbc.mysql.MySQLPropertyMeta.TABLE_PRIMARY_KEY;
import static org.apache.gravitino.trino.connector.catalog.jdbc.mysql.MySQLPropertyMeta.TABLE_UNIQUE_KEY;
import static org.apache.gravitino.trino.connector.catalog.jdbc.mysql.MySQLPropertyMeta.filterColumnProperties;
import static org.apache.gravitino.trino.connector.catalog.jdbc.mysql.MySQLPropertyMeta.getDefaultValue;
import static org.apache.gravitino.trino.connector.catalog.jdbc.mysql.MySQLPropertyMeta.getPrimaryKey;
import static org.apache.gravitino.trino.connector.catalog.jdbc.mysql.MySQLPropertyMeta.getUniqueKey;
import static org.apache.gravitino.trino.connector.catalog.jdbc.mysql.MySQLPropertyMeta.isAutoIncrement;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.session.PropertyMetadata;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.property.PropertyConverter;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import org.apache.gravitino.trino.connector.catalog.jdbc.JdbcColumnDefaultValueConverter;
import org.apache.gravitino.trino.connector.metadata.GravitinoColumn;
import org.apache.gravitino.trino.connector.metadata.GravitinoTable;
import org.apache.logging.log4j.util.Strings;

/** Transforming Apache Gravitino MySQL metadata to Trino. */
public class MySQLMetadataAdapter extends CatalogConnectorMetadataAdapter {

  private final PropertyConverter tableConverter;
  private final JdbcColumnDefaultValueConverter columnDefaultValueConverter;

  /**
   * Constructs a new MySQLMetadataAdapter.
   *
   * @param schemaProperties The list of schema property metadata
   * @param tableProperties The list of table property metadata
   * @param columnProperties The list of column property metadata
   */
  public MySQLMetadataAdapter(
      List<PropertyMetadata<?>> schemaProperties,
      List<PropertyMetadata<?>> tableProperties,
      List<PropertyMetadata<?>> columnProperties) {

    super(schemaProperties, tableProperties, columnProperties, new MySQLDataTypeTransformer());
    this.tableConverter = new MySQLTablePropertyConverter();
    this.columnDefaultValueConverter = new MysqlColumnDefaultValueConverter();
  }

  @Override
  public Map<String, String> toGravitinoTableProperties(Map<String, Object> properties) {
    Map<String, Object> stringMap = tableConverter.engineToGravitinoProperties(properties);
    return super.toGravitinoTableProperties(stringMap);
  }

  @Override
  public Map<String, Object> toTrinoTableProperties(Map<String, String> properties) {
    Map<String, String> objectMap = tableConverter.gravitinoToEngineProperties(properties);
    return super.toTrinoTableProperties(objectMap);
  }

  /** Transform Trino ConnectorTableMetadata to Gravitino table metadata */
  @Override
  public GravitinoTable createTable(ConnectorTableMetadata tableMetadata) {
    String tableName = tableMetadata.getTableSchema().getTable().getTableName();
    String schemaName = tableMetadata.getTableSchema().getTable().getSchemaName();
    String comment = tableMetadata.getComment().orElse("");
    Map<String, Object> tableProperties = tableMetadata.getProperties();
    Map<String, String> properties = toGravitinoTableProperties(tableProperties);

    Set<String> primaryKeyList = getPrimaryKey(tableProperties);
    Map<String, Set<String>> uniqueKeyMap = getUniqueKey(tableProperties);

    List<GravitinoColumn> columns = new ArrayList<>();
    ImmutableList.Builder<GravitinoColumn> incrementColumnListBuilder = ImmutableList.builder();
    ImmutableSet.Builder<String> columnNamesBuilder = ImmutableSet.builder();
    for (int i = 0; i < tableMetadata.getColumns().size(); i++) {
      ColumnMetadata column = tableMetadata.getColumns().get(i);
      if (primaryKeyList.contains(column.getName()) && column.isNullable()) {
        throw new TrinoException(NOT_SUPPORTED, "Primary key must be NOT NULL in MySQL");
      }

      Map<String, Object> columnProperties = column.getProperties();
      boolean autoIncrement = isAutoIncrement(columnProperties);
      String defaultValue = getDefaultValue(columnProperties);

      Type gravitinoType = dataTypeTransformer.getGravitinoType(column.getType());
      GravitinoColumn gravitinoColumn =
          new GravitinoColumn(
              column.getName(),
              gravitinoType,
              i,
              column.getComment(),
              column.isNullable(),
              autoIncrement,
              columnDefaultValueConverter.toGravitino(
                  gravitinoType, defaultValue, column.isNullable()),
              filterColumnProperties(columnProperties));

      columns.add(gravitinoColumn);

      if (autoIncrement) {
        incrementColumnListBuilder.add(gravitinoColumn);
      }

      columnNamesBuilder.add(column.getName());
    }

    Index[] indexes = buildIndexes(primaryKeyList, uniqueKeyMap, columnNamesBuilder.build());

    validateIncrementCol(incrementColumnListBuilder.build(), indexes);

    return new GravitinoTable(schemaName, tableName, columns, comment, properties, indexes);
  }

  private static Index[] buildIndexes(
      Set<String> primaryKeyList, Map<String, Set<String>> uniqueKeyMap, Set<String> columnNames) {
    ImmutableList.Builder<Index> builder = ImmutableList.builder();
    if (!primaryKeyList.isEmpty()) {
      builder.add(convertPrimaryKey(primaryKeyList, columnNames));
    }

    if (!uniqueKeyMap.isEmpty()) {
      builder.addAll(convertUniqueKey(uniqueKeyMap, columnNames));
    }

    List<Index> indexList = builder.build();
    return indexList.toArray(new Index[indexList.size()]);
  }

  private static Index convertPrimaryKey(Set<String> primaryKeys, Set<String> columnNames) {
    for (String primaryKeyColumn : primaryKeys) {
      if (!columnNames.contains(primaryKeyColumn)) {
        throw new TrinoException(
            INVALID_TABLE_PROPERTY,
            format(
                "Column '%s' specified in property '%s' doesn't exist in table",
                primaryKeyColumn, TABLE_PRIMARY_KEY));
      }
    }
    return Indexes.createMysqlPrimaryKey(convertIndexFieldNames(primaryKeys));
  }

  private static List<Index> convertUniqueKey(
      Map<String, Set<String>> uniqueKeys, Set<String> columnNames) {
    ImmutableList.Builder<Index> builder = ImmutableList.builder();
    for (String uniqueKey : uniqueKeys.keySet()) {
      Set<String> uniqueKeyColumns = uniqueKeys.get(uniqueKey);
      for (String uniqueKeyColumn : uniqueKeyColumns) {
        if (!columnNames.contains(uniqueKeyColumn)) {
          throw new TrinoException(
              INVALID_TABLE_PROPERTY,
              format(
                  "Column '%s' specified in property '%s' doesn't exist in table",
                  uniqueKeyColumn, TABLE_UNIQUE_KEY));
        }
      }
      builder.add(Indexes.unique(uniqueKey, convertIndexFieldNames(uniqueKeyColumns)));
    }
    return builder.build();
  }

  private static String[][] convertIndexFieldNames(Set<String> fieldNames) {
    return fieldNames.stream().map(colName -> new String[] {colName}).toArray(String[][]::new);
  }

  /**
   * The auto-increment column will be verified. There can only be one auto-increment column and it
   * must be the primary key or unique index.
   *
   * @param columns table auto increment columns
   * @param indexes table indexes
   */
  protected static void validateIncrementCol(List<GravitinoColumn> columns, Index[] indexes) {
    // check not exists multi auto-increment column
    if (columns.size() > 1) {
      throw new TrinoException(NOT_SUPPORTED, "Only one column can be auto-incremented in MySQL");
    }
    if (!columns.isEmpty()) {
      GravitinoColumn incrementColumn = columns.get(0);
      // check auto-increment column must be integer type
      if (!(incrementColumn.getType() instanceof Type.IntegralType)) {
        throw new TrinoException(
            NOT_SUPPORTED, "Auto increment column must be integer type in MySQL");
      }
      // check auto-increment column must be not null
      if (incrementColumn.isNullable()) {
        throw new TrinoException(NOT_SUPPORTED, "Auto increment column must be not null in MySQL");
      }
      // check auto-increment column have no default value
      if (DEFAULT_VALUE_NOT_SET != incrementColumn.getDefaultValue()) {
        throw new TrinoException(
            NOT_SUPPORTED, "Auto increment column cannot have a default value in MySQL");
      }
      // check the only one auto-increment column can only be defined in primary key or unique
      // key.
      // auto-increment column can exist in both primary key and unique key
      String colName = incrementColumn.getName();
      Optional<Index> existAutoIncrementColIndexOptional =
          Arrays.stream(indexes)
              .filter(
                  index ->
                      Arrays.stream(index.fieldNames())
                          .flatMap(Arrays::stream)
                          .anyMatch(s -> StringUtils.equalsIgnoreCase(colName, s)))
              .filter(
                  index ->
                      index.type() == Index.IndexType.PRIMARY_KEY
                          || index.type() == Index.IndexType.UNIQUE_KEY)
              .findAny();
      if (!existAutoIncrementColIndexOptional.isPresent()) {
        throw new TrinoException(
            NOT_SUPPORTED, "Auto increment column must be defined as a key in MySQL");
      }
    }
  }

  @Override
  public ConnectorTableMetadata getTableMetadata(GravitinoTable gravitinoTable) {
    SchemaTableName schemaTableName =
        new SchemaTableName(gravitinoTable.getSchemaName(), gravitinoTable.getName());
    ArrayList<ColumnMetadata> columnMetadataList = new ArrayList<>();
    for (GravitinoColumn column : gravitinoTable.getColumns()) {
      columnMetadataList.add(getColumnMetadata(column));
    }

    Map<String, Object> properties = toTrinoTableProperties(gravitinoTable.getProperties());

    ImmutableMap.Builder<String, Object> propertiesBuilder = ImmutableMap.builder();
    propertiesBuilder.putAll(properties);

    Index[] indexes = gravitinoTable.getIndexes();
    if (ArrayUtils.isNotEmpty(indexes)) {
      List<String> primaryKeys = new ArrayList<>();
      List<String> uniqueKeys = new ArrayList<>();
      for (int i = 0; i < indexes.length; i++) {
        Index index = indexes[i];
        switch (index.type()) {
          case PRIMARY_KEY:
            Arrays.stream(index.fieldNames())
                .flatMap(Arrays::stream)
                .forEach(col -> primaryKeys.add(col));
            break;

          case UNIQUE_KEY:
            List<String> columns =
                Arrays.stream(index.fieldNames())
                    .flatMap(Arrays::stream)
                    .collect(Collectors.toUnmodifiableList());
            uniqueKeys.add(String.format("%s:%s", index.name(), Strings.join(columns, ',')));
            break;
        }
      }
      if (!primaryKeys.isEmpty()) {
        propertiesBuilder.put(TABLE_PRIMARY_KEY, primaryKeys);
      }
      if (!uniqueKeys.isEmpty()) {
        propertiesBuilder.put(TABLE_UNIQUE_KEY, uniqueKeys);
      }
    }
    return new ConnectorTableMetadata(
        schemaTableName,
        columnMetadataList,
        propertiesBuilder.build(),
        Optional.ofNullable(gravitinoTable.getComment()));
  }

  @Override
  public ColumnMetadata getColumnMetadata(GravitinoColumn column) {
    Map<String, Object> propertyMap = Maps.newHashMap(column.getProperties());
    if (column.isAutoIncrement()) {
      propertyMap.put(MySQLPropertyMeta.AUTO_INCREMENT, true);
    }

    String defaultValue = columnDefaultValueConverter.fromGravitino(column.getDefaultValue());
    if (defaultValue != null) {
      propertyMap.put(MySQLPropertyMeta.DEFAULT, defaultValue);
    }

    return ColumnMetadata.builder()
        .setName(column.getName())
        .setType(dataTypeTransformer.getTrinoType(column.getType()))
        .setComment(Optional.ofNullable(column.getComment()))
        .setNullable(column.isNullable())
        .setHidden(column.isHidden())
        .setProperties(propertyMap)
        .build();
  }
}
