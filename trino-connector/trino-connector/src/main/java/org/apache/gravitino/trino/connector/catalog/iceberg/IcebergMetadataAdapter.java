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
package org.apache.gravitino.trino.connector.catalog.iceberg;

import static org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants.FORMAT;
import static org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants.FORMAT_VERSION;
import static org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants.PROVIDER;
import static org.apache.gravitino.trino.connector.catalog.iceberg.IcebergPropertyMeta.ICEBERG_FORMAT_PROPERTY;
import static org.apache.gravitino.trino.connector.catalog.iceberg.IcebergPropertyMeta.ICEBERG_FORMAT_VERSION_PROPERTY;
import static org.apache.gravitino.trino.connector.catalog.iceberg.IcebergTablePropertyConverter.convertTableFormatToTrino;

import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.session.PropertyMetadata;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.gravitino.catalog.property.PropertyConverter;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import org.apache.gravitino.trino.connector.metadata.GravitinoColumn;
import org.apache.gravitino.trino.connector.metadata.GravitinoTable;

/** Transforming Apache Gravitino Iceberg metadata to Trino. */
public class IcebergMetadataAdapter extends CatalogConnectorMetadataAdapter {

  // Move all this logic to CatalogConnectorMetadataAdapter
  private final PropertyConverter tableConverter;
  private final PropertyConverter schemaConverter;

  private static final Set<String> ICEBERG_PROPERTIES_TO_REMOVE =
      ImmutableSet.of(
          IcebergPropertyMeta.ICEBERG_PARTITIONING_PROPERTY,
          IcebergPropertyMeta.ICEBERG_SORTED_BY_PROPERTY);

  /**
   * Constructs a new IcebergMetadataAdapter.
   *
   * @param schemaProperties the list of schema property metadata
   * @param tableProperties the list of table property metadata
   * @param columnProperties the list of column property metadata
   */
  public IcebergMetadataAdapter(
      List<PropertyMetadata<?>> schemaProperties,
      List<PropertyMetadata<?>> tableProperties,
      List<PropertyMetadata<?>> columnProperties) {
    super(schemaProperties, tableProperties, columnProperties, new IcebergDataTypeTransformer());
    this.tableConverter = new IcebergTablePropertyConverter();
    this.schemaConverter = new IcebergSchemaPropertyConverter();
  }

  @Override
  public Map<String, Object> toTrinoTableProperties(Map<String, String> properties) {
    Map<String, String> objectMap = tableConverter.gravitinoToEngineProperties(properties);
    return super.toTrinoTableProperties(objectMap);
  }

  @Override
  public Map<String, Object> toTrinoSchemaProperties(Map<String, String> properties) {
    Map<String, String> objectMap = schemaConverter.gravitinoToEngineProperties(properties);
    return super.toTrinoSchemaProperties(objectMap);
  }

  @Override
  public Map<String, String> toGravitinoTableProperties(Map<String, Object> properties) {
    Map<String, Object> stringMap = tableConverter.engineToGravitinoProperties(properties);
    return super.toGravitinoTableProperties(stringMap);
  }

  @Override
  public Map<String, String> toGravitinoSchemaProperties(Map<String, Object> properties) {
    Map<String, Object> stringMap = schemaConverter.engineToGravitinoProperties(properties);
    return super.toGravitinoSchemaProperties(stringMap);
  }

  @Override
  public GravitinoTable createTable(ConnectorTableMetadata tableMetadata) {
    String tableName = tableMetadata.getTableSchema().getTable().getTableName();
    String schemaName = tableMetadata.getTableSchema().getTable().getSchemaName();
    String comment = tableMetadata.getComment().orElse("");

    Map<String, Object> propertyMap = tableMetadata.getProperties();
    List<String> partitionColumns =
        propertyMap.containsKey(IcebergPropertyMeta.ICEBERG_PARTITIONING_PROPERTY)
            ? (List<String>) propertyMap.get(IcebergPropertyMeta.ICEBERG_PARTITIONING_PROPERTY)
            : Collections.emptyList();

    List<String> sortColumns =
        propertyMap.containsKey(IcebergPropertyMeta.ICEBERG_SORTED_BY_PROPERTY)
            ? (List<String>) propertyMap.get(IcebergPropertyMeta.ICEBERG_SORTED_BY_PROPERTY)
            : Collections.emptyList();

    Map<String, String> properties =
        toGravitinoTableProperties(
            removeKeys(tableMetadata.getProperties(), ICEBERG_PROPERTIES_TO_REMOVE));

    if (propertyMap.containsKey(ICEBERG_FORMAT_PROPERTY)) {
      String format = propertyMap.get(ICEBERG_FORMAT_PROPERTY).toString();
      properties.put(PROVIDER, format);
      if (propertyMap.containsKey((ICEBERG_FORMAT_VERSION_PROPERTY))) {
        properties.put(FORMAT_VERSION, propertyMap.get(ICEBERG_FORMAT_VERSION_PROPERTY).toString());
      }
    }

    List<GravitinoColumn> columns = new ArrayList<>();
    for (int i = 0; i < tableMetadata.getColumns().size(); i++) {
      ColumnMetadata column = tableMetadata.getColumns().get(i);
      columns.add(
          new GravitinoColumn(
              column.getName(),
              dataTypeTransformer.getGravitinoType(column.getType()),
              i,
              column.getComment(),
              column.isNullable(),
              false,
              column.getProperties()));
    }
    GravitinoTable gravitinoTable =
        new GravitinoTable(schemaName, tableName, columns, comment, properties);

    if (!partitionColumns.isEmpty()) {
      Transform[] partitioning = ExpressionUtil.partitionFiledToExpression(partitionColumns);
      gravitinoTable.setPartitioning(partitioning);
    }

    if (!sortColumns.isEmpty()) {
      SortOrder[] sorting = ExpressionUtil.sortOrderFiledToExpression(sortColumns);
      gravitinoTable.setSortOrders(sorting);
    }

    return gravitinoTable;
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

    properties.put(
        ICEBERG_FORMAT_PROPERTY,
        convertTableFormatToTrino(gravitinoTable.getProperties().get(FORMAT)));
    properties.put(
        ICEBERG_FORMAT_VERSION_PROPERTY, gravitinoTable.getProperties().get(FORMAT_VERSION));

    if (ArrayUtils.isNotEmpty(gravitinoTable.getPartitioning())) {
      properties.put(
          IcebergPropertyMeta.ICEBERG_PARTITIONING_PROPERTY,
          ExpressionUtil.expressionToPartitionFiled(gravitinoTable.getPartitioning()));
    }

    if (ArrayUtils.isNotEmpty(gravitinoTable.getSortOrders())) {
      properties.put(
          IcebergPropertyMeta.ICEBERG_SORTED_BY_PROPERTY,
          ExpressionUtil.expressionToSortOrderFiled(gravitinoTable.getSortOrders()));
    }

    return new ConnectorTableMetadata(
        schemaTableName,
        columnMetadataList,
        properties,
        Optional.ofNullable(gravitinoTable.getComment()));
  }
}
