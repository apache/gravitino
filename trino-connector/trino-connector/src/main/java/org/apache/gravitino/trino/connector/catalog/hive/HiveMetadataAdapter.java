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
package org.apache.gravitino.trino.connector.catalog.hive;

import com.google.common.collect.ImmutableSet;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.session.PropertyMetadata;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.gravitino.catalog.property.PropertyConverter;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.distributions.Strategy;
import org.apache.gravitino.rel.expressions.sorts.SortDirection;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.trino.connector.GravitinoErrorCode;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import org.apache.gravitino.trino.connector.catalog.hive.SortingColumn.Order;
import org.apache.gravitino.trino.connector.metadata.GravitinoColumn;
import org.apache.gravitino.trino.connector.metadata.GravitinoTable;

/** Transforming Apache Gravitino Hive metadata to Trino. */
public class HiveMetadataAdapter extends CatalogConnectorMetadataAdapter {

  private final PropertyConverter tableConverter;
  private final PropertyConverter schemaConverter;

  private static final Set<String> HIVE_PROPERTIES_TO_REMOVE =
      ImmutableSet.of(
          HivePropertyMeta.HIVE_PARTITION_KEY,
          HivePropertyMeta.HIVE_BUCKET_KEY,
          HivePropertyMeta.HIVE_BUCKET_COUNT_KEY,
          HivePropertyMeta.HIVE_SORT_ORDER_KEY);

  /**
   * Constructs a new HiveMetadataAdapter.
   *
   * @param schemaProperties the schema properties metadata
   * @param tableProperties the table properties metadata
   * @param columnProperties the column properties metadata
   */
  public HiveMetadataAdapter(
      List<PropertyMetadata<?>> schemaProperties,
      List<PropertyMetadata<?>> tableProperties,
      List<PropertyMetadata<?>> columnProperties) {
    super(schemaProperties, tableProperties, columnProperties, new HiveDataTypeTransformer());
    this.tableConverter = new HiveTablePropertyConverter();
    this.schemaConverter = new HiveSchemaPropertyConverter();
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
        propertyMap.containsKey(HivePropertyMeta.HIVE_PARTITION_KEY)
            ? (List<String>) propertyMap.get(HivePropertyMeta.HIVE_PARTITION_KEY)
            : Collections.emptyList();
    List<String> bucketColumns =
        propertyMap.containsKey(HivePropertyMeta.HIVE_BUCKET_KEY)
            ? (List<String>) propertyMap.get(HivePropertyMeta.HIVE_BUCKET_KEY)
            : Collections.emptyList();
    int bucketCount =
        propertyMap.containsKey(HivePropertyMeta.HIVE_BUCKET_COUNT_KEY)
            ? (int) propertyMap.get(HivePropertyMeta.HIVE_BUCKET_COUNT_KEY)
            : 0;
    List<SortingColumn> sortColumns =
        propertyMap.containsKey(HivePropertyMeta.HIVE_SORT_ORDER_KEY)
            ? (List<SortingColumn>) propertyMap.get(HivePropertyMeta.HIVE_SORT_ORDER_KEY)
            : Collections.emptyList();

    if (!sortColumns.isEmpty() && (bucketColumns.isEmpty() || bucketCount == 0)) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT,
          "Sort columns can only be set when bucket columns and bucket count are set");
    }

    Map<String, String> properties =
        toGravitinoTableProperties(
            removeKeys(tableMetadata.getProperties(), HIVE_PROPERTIES_TO_REMOVE));

    List<GravitinoColumn> columns = new ArrayList<>();
    for (int i = 0; i < tableMetadata.getColumns().size(); i++) {
      ColumnMetadata column = tableMetadata.getColumns().get(i);
      columns.add(
          new GravitinoColumn(
              column.getName(),
              dataTypeTransformer.getGravitinoType(column.getType()),
              i,
              column.getComment(),
              column.isNullable()));
    }
    GravitinoTable gravitinoTable =
        new GravitinoTable(schemaName, tableName, columns, comment, properties);

    if (!partitionColumns.isEmpty()) {
      Transform[] partitioning =
          partitionColumns.stream().map(Transforms::identity).toArray(Transform[]::new);
      gravitinoTable.setPartitioning(partitioning);
    }

    if (!bucketColumns.isEmpty()) {
      Expression[] bucketing =
          bucketColumns.stream().map(NamedReference::field).toArray(Expression[]::new);
      gravitinoTable.setDistribution(Distributions.of(Strategy.HASH, bucketCount, bucketing));
    }

    if (!sortColumns.isEmpty()) {
      SortOrder[] sorting =
          sortColumns.stream()
              .map(
                  sortingColumn -> {
                    Expression expression = NamedReference.field(sortingColumn.getColumnName());
                    SortDirection sortDirection =
                        sortingColumn.getOrder() == Order.ASCENDING
                            ? SortDirection.ASCENDING
                            : SortDirection.DESCENDING;
                    return SortOrders.of(expression, sortDirection);
                  })
              .toArray(SortOrder[]::new);
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

    if (ArrayUtils.isNotEmpty(gravitinoTable.getPartitioning())) {
      // Only support simple partition now like partition by a, b, c.
      // Format like partition like partition by year(a), b, c is NOT supported now.
      properties.put(
          HivePropertyMeta.HIVE_PARTITION_KEY,
          gravitinoTable.getPartitioning().length > 0
              ? Arrays.stream(gravitinoTable.getPartitioning())
                  .map(
                      ts ->
                          ((Transform.SingleFieldTransform) ts)
                              .fieldName()[0].toLowerCase(Locale.ENGLISH))
                  .collect(Collectors.toList())
              : Collections.emptyList());
    }

    if (gravitinoTable.getDistribution() != null
        && !Distributions.NONE.equals(gravitinoTable.getDistribution())) {
      properties.put(
          HivePropertyMeta.HIVE_BUCKET_KEY,
          Arrays.stream(gravitinoTable.getDistribution().expressions())
              .map(ts -> ((NamedReference) ts).fieldName()[0].toLowerCase(Locale.ENGLISH))
              .collect(Collectors.toList()));

      properties.put(
          HivePropertyMeta.HIVE_BUCKET_COUNT_KEY, gravitinoTable.getDistribution().number());
    }

    if (ArrayUtils.isNotEmpty(gravitinoTable.getSortOrders())) {
      // Only support the simple format
      properties.put(
          HivePropertyMeta.HIVE_SORT_ORDER_KEY,
          Arrays.stream(gravitinoTable.getSortOrders())
              .map(
                  sortOrder -> {
                    Expression expression = sortOrder.expression();
                    SortDirection sortDirection =
                        sortOrder.direction() == SortDirection.ASCENDING
                            ? SortDirection.ASCENDING
                            : SortDirection.DESCENDING;
                    Order order =
                        sortDirection == SortDirection.ASCENDING
                            ? Order.ASCENDING
                            : Order.DESCENDING;
                    return new SortingColumn(
                        ((NamedReference) expression).fieldName()[0].toLowerCase(Locale.ENGLISH),
                        order);
                  })
              .collect(Collectors.toList()));
    }

    return new ConnectorTableMetadata(
        schemaTableName,
        columnMetadataList,
        properties,
        Optional.ofNullable(gravitinoTable.getComment()));
  }
}
