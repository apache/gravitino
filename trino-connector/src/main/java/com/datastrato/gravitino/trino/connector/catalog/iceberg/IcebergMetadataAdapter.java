/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.catalog.iceberg;

import static com.datastrato.gravitino.trino.connector.catalog.iceberg.IcebergPropertyMeta.ICEBERG_PARTITIONING_PROPERTY;
import static com.datastrato.gravitino.trino.connector.catalog.iceberg.IcebergPropertyMeta.ICEBERG_SORTED_BY_PROPERTY;

import com.datastrato.gravitino.catalog.property.PropertyConverter;
import com.datastrato.gravitino.dto.rel.partitioning.Partitioning;
import com.datastrato.gravitino.rel.expressions.Expression;
import com.datastrato.gravitino.rel.expressions.NamedReference;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrder;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrders;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.expressions.transforms.Transforms;
import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import com.datastrato.gravitino.trino.connector.metadata.GravitinoColumn;
import com.datastrato.gravitino.trino.connector.metadata.GravitinoTable;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.session.PropertyMetadata;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;

/** Transforming gravitino Iceberg metadata to trino. */
public class IcebergMetadataAdapter extends CatalogConnectorMetadataAdapter {

  // Move all this logic to CatalogConnectorMetadataAdapter
  private final PropertyConverter tableConverter;
  private final PropertyConverter schemaConverter;

  private static final Set<String> ICEBERG_PROPERTIES_TO_REMOVE =
      ImmutableSet.of(ICEBERG_PARTITIONING_PROPERTY, ICEBERG_SORTED_BY_PROPERTY);

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
        propertyMap.containsKey(ICEBERG_PARTITIONING_PROPERTY)
            ? (List<String>) propertyMap.get(ICEBERG_PARTITIONING_PROPERTY)
            : Collections.EMPTY_LIST;

    List<String> sortColumns =
        propertyMap.containsKey(ICEBERG_SORTED_BY_PROPERTY)
            ? (List<String>) propertyMap.get(ICEBERG_SORTED_BY_PROPERTY)
            : Collections.EMPTY_LIST;

    Map<String, String> properties =
        toGravitinoTableProperties(
            removeKeys(tableMetadata.getProperties(), ICEBERG_PROPERTIES_TO_REMOVE));

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
      Transform[] partitioning =
          partitionColumns.stream().map(Transforms::identity).toArray(Transform[]::new);
      gravitinoTable.setPartitioning(partitioning);
    }

    if (!sortColumns.isEmpty()) {
      SortOrder[] sorting =
          sortColumns.stream()
              .map(
                  sortingColumn -> {
                    Expression expression = NamedReference.field(sortingColumn);
                    return SortOrders.ascending(expression);
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
          ICEBERG_PARTITIONING_PROPERTY,
          gravitinoTable.getPartitioning().length > 0
              ? Arrays.stream(gravitinoTable.getPartitioning())
                  .map(ts -> ((Partitioning.SingleFieldPartitioning) ts).fieldName()[0])
                  .collect(Collectors.toList())
              : Collections.EMPTY_LIST);
    }

    if (ArrayUtils.isNotEmpty(gravitinoTable.getSortOrders())) {
      // Only support the simple format
      properties.put(
          ICEBERG_SORTED_BY_PROPERTY,
          Arrays.stream(gravitinoTable.getSortOrders())
              .map(
                  sortOrder -> {
                    Expression expression = sortOrder.expression();
                    return ((NamedReference) expression).fieldName()[0];
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
