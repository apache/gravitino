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
package org.apache.gravitino.trino.connector.catalog.glue;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.session.PropertyMetadata.integerProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.type.VarcharType.VARCHAR;

import com.google.common.collect.ImmutableList;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.gravitino.catalog.glue.GlueConstants;
import org.apache.gravitino.catalog.hive.StorageFormat;
import org.apache.gravitino.catalog.property.PropertyConverter;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.trino.connector.GravitinoErrorCode;
import org.apache.gravitino.trino.connector.catalog.hive.HiveMetadataAdapter;
import org.apache.gravitino.trino.connector.catalog.hive.HivePropertyMeta;
import org.apache.gravitino.trino.connector.catalog.hive.SortingColumn;
import org.apache.gravitino.trino.connector.catalog.iceberg.ExpressionUtil;
import org.apache.gravitino.trino.connector.metadata.GravitinoColumn;
import org.apache.gravitino.trino.connector.metadata.GravitinoTable;

/**
 * Transforming Apache Gravitino Glue metadata to Trino. This adapter handles properties that are
 * specific to the Glue catalog and the lakehouse connector, excluding properties that conflict with
 * the lakehouse connector's native properties.
 */
public class GlueMetadataAdapter extends HiveMetadataAdapter {

  /** The table type property for lakehouse connector (ICEBERG, HIVE, DELTA). */
  static final String LAKEHOUSE_TABLE_TYPE = "type";

  /** Value of {@link #LAKEHOUSE_TABLE_TYPE} for Hive-format tables. */
  static final String HIVE_TABLE_TYPE = "HIVE";

  /** Value of {@link #LAKEHOUSE_TABLE_TYPE} for Iceberg tables. */
  static final String ICEBERG_TABLE_TYPE = "ICEBERG";

  private static final String FORMAT = "format";

  private static final List<PropertyMetadata<?>> GLUE_TABLE_PROPERTY_META =
      ImmutableList.of(
          stringProperty(LAKEHOUSE_TABLE_TYPE, "The type of table (ICEBERG, HIVE)", null, false),
          new PropertyMetadata<>(
              HivePropertyMeta.HIVE_PARTITION_KEY,
              "Partition columns",
              new ArrayType(VARCHAR),
              List.class,
              ImmutableList.of(),
              false,
              value ->
                  ((List<?>) value)
                      .stream()
                          .map(name -> ((String) name).toLowerCase(Locale.ROOT))
                          .collect(ImmutableList.toImmutableList()),
              value -> value),
          new PropertyMetadata<>(
              HivePropertyMeta.HIVE_BUCKET_KEY,
              "Bucketing columns",
              new ArrayType(VARCHAR),
              List.class,
              ImmutableList.of(),
              false,
              value ->
                  ((List<?>) value)
                      .stream()
                          .map(name -> ((String) name).toLowerCase(Locale.ROOT))
                          .collect(ImmutableList.toImmutableList()),
              value -> value),
          integerProperty(
              HivePropertyMeta.HIVE_BUCKET_COUNT_KEY,
              "The number of buckets for the table",
              null,
              false),
          new PropertyMetadata<>(
              HivePropertyMeta.HIVE_SORT_ORDER_KEY,
              "Bucket sorting columns",
              new ArrayType(VARCHAR),
              List.class,
              ImmutableList.of(),
              false,
              value ->
                  ((List<?>) value)
                      .stream()
                          .map(String.class::cast)
                          .map(name -> name.toLowerCase(Locale.ROOT))
                          .map(SortingColumn::sortingColumnFromString)
                          .collect(toImmutableList()),
              // Hive tables carry SortingColumn values; Iceberg tables carry Iceberg sort fields
              // such as "id DESC NULLS LAST" as strings.
              value ->
                  ((List<?>) value)
                      .stream()
                          .map(
                              v ->
                                  v instanceof SortingColumn
                                      ? SortingColumn.sortingColumnToString((SortingColumn) v)
                                      : v.toString())
                          .collect(toImmutableList())),
          stringProperty(FORMAT, "The format of the data files (PARQUET, ORC, etc.)", null, false),
          stringProperty("location", "The S3 storage location for the table", null, false));

  private final PropertyConverter tableConverter = new GlueTablePropertyConverter();

  /** Table type a table gets when the statement sets no {@code type}. */
  private final String defaultTableType;

  /**
   * Constructs a new GlueMetadataAdapter for a catalog whose tables default to Hive format.
   *
   * @param schemaProperties the schema properties metadata
   */
  public GlueMetadataAdapter(List<PropertyMetadata<?>> schemaProperties) {
    this(schemaProperties, HIVE_TABLE_TYPE);
  }

  /**
   * Constructs a new GlueMetadataAdapter.
   *
   * @param schemaProperties the schema properties metadata
   * @param defaultTableType the table type of tables created without {@code type}
   */
  public GlueMetadataAdapter(List<PropertyMetadata<?>> schemaProperties, String defaultTableType) {
    super(
        schemaProperties,
        GLUE_TABLE_PROPERTY_META,
        ImmutableList.of(),
        new GlueDataTypeTransformer());
    this.defaultTableType = defaultTableType.toUpperCase(Locale.ROOT);
  }

  @Override
  public Map<String, Object> toTrinoTableProperties(Map<String, String> properties) {
    // Convert Gravitino keys (e.g. table-format) to Trino keys (e.g. type),
    // then filter to only properties declared in GLUE_TABLE_PROPERTY_META.
    // We must NOT call super here because HiveMetadataAdapter would apply
    // HiveTablePropertyConverter a second time, corrupting the already-converted keys.
    Map<String, String> converted = tableConverter.gravitinoToEngineProperties(properties);
    Map<String, Object> result = new HashMap<>();
    for (PropertyMetadata<?> meta : GLUE_TABLE_PROPERTY_META) {
      String key = meta.getName();
      if (converted.containsKey(key)) {
        result.put(key, converted.get(key));
      }
    }
    // Glue keeps the storage format of a Hive-format table as its storage classes only. The format
    // is derived from them so that SHOW CREATE TABLE recreates a table of the same format.
    if (isIcebergTable(properties)) {
      result.putIfAbsent(LAKEHOUSE_TABLE_TYPE, ICEBERG_TABLE_TYPE);
    } else {
      StorageFormat format = storageFormat(properties);
      if (format != null) {
        result.putIfAbsent(LAKEHOUSE_TABLE_TYPE, HIVE_TABLE_TYPE);
        result.putIfAbsent(FORMAT, format.name());
      }
    }
    return result;
  }

  @Override
  public Map<String, String> toGravitinoTableProperties(Map<String, Object> properties) {
    // Convert Trino keys (e.g. type) to Gravitino keys (e.g. table-format),
    // then drop null values. Must NOT call super for the same reason as above.
    Map<String, Object> converted = tableConverter.engineToGravitinoProperties(properties);
    return converted.entrySet().stream()
        .filter(e -> e.getValue() != null)
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));
  }

  @Override
  public GravitinoTable createTable(ConnectorTableMetadata tableMetadata) {
    @SuppressWarnings("unchecked")
    List<String> partitionExpressions =
        tableMetadata.getProperties().containsKey(HivePropertyMeta.HIVE_PARTITION_KEY)
            ? (List<String>) tableMetadata.getProperties().get(HivePropertyMeta.HIVE_PARTITION_KEY)
            : Collections.emptyList();

    GravitinoTable table = super.createTable(tableMetadata);

    Object type = tableMetadata.getProperties().get(LAKEHOUSE_TABLE_TYPE);
    String tableType = type == null ? defaultTableType : type.toString();
    if (!ICEBERG_TABLE_TYPE.equalsIgnoreCase(tableType)) {
      for (GravitinoColumn column : table.getColumns()) {
        checkHiveColumnType(column.getName(), column.getType());
      }
    }

    if (!partitionExpressions.isEmpty()) {
      Transform[] transforms = ExpressionUtil.partitionFiledToExpression(partitionExpressions);
      table.setPartitioning(transforms);
    }

    return table;
  }

  /**
   * Iceberg tables keep their partitioning and sort order as Iceberg transforms, which the Hive
   * conversion cannot express, so they are converted the way the Iceberg connector shows them.
   */
  @Override
  public ConnectorTableMetadata getTableMetadata(GravitinoTable gravitinoTable) {
    if (!isIcebergTable(gravitinoTable.getProperties())) {
      return super.getTableMetadata(gravitinoTable);
    }
    Map<String, Object> properties = toTrinoTableProperties(gravitinoTable.getProperties());
    if (ArrayUtils.isNotEmpty(gravitinoTable.getPartitioning())) {
      properties.put(
          HivePropertyMeta.HIVE_PARTITION_KEY,
          ExpressionUtil.expressionToPartitionFiled(gravitinoTable.getPartitioning()));
    }
    // The sorted_by property of Trino's Iceberg connector takes columns only, so sort terms over a
    // transform, such as day(ts), are left out.
    SortOrder[] columnSortOrders =
        gravitinoTable.getSortOrders() == null
            ? new SortOrder[0]
            : Arrays.stream(gravitinoTable.getSortOrders())
                .filter(order -> order.expression() instanceof NamedReference)
                .toArray(SortOrder[]::new);
    if (columnSortOrders.length > 0) {
      properties.put(
          HivePropertyMeta.HIVE_SORT_ORDER_KEY,
          ExpressionUtil.expressionToSortOrderFiled(columnSortOrders));
    }
    return new ConnectorTableMetadata(
        new SchemaTableName(gravitinoTable.getSchemaName(), gravitinoTable.getName()),
        getColumnMetadataList(gravitinoTable),
        properties,
        Optional.ofNullable(gravitinoTable.getComment()));
  }

  /**
   * The lakehouse connector decodes table properties into its own types, while the properties of a
   * CREATE TABLE AS SELECT statement are decoded with the Glue property metadata, so it cannot
   * plan the layout of the new table.
   */
  @Override
  public boolean skipsInternalNewTableLayout() {
    return true;
  }

  @Override
  protected List<String> toTrinoPartitionKeys(Transform[] partitioning) {
    return ExpressionUtil.expressionToPartitionFiled(partitioning);
  }

  private static boolean isIcebergTable(Map<String, String> properties) {
    return ICEBERG_TABLE_TYPE.equalsIgnoreCase(properties.get(GlueConstants.TABLE_TYPE_PARAM))
        || ICEBERG_TABLE_TYPE.equalsIgnoreCase(properties.get(GlueConstants.TABLE_FORMAT));
  }

  /** Returns the storage format whose classes the table uses, or null if there is none. */
  private static StorageFormat storageFormat(Map<String, String> properties) {
    String inputFormat = properties.get(GlueConstants.INPUT_FORMAT_CLASS);
    String outputFormat = properties.get(GlueConstants.OUTPUT_FORMAT);
    String serde = properties.get(GlueConstants.SERDE_LIB);
    for (StorageFormat format : StorageFormat.values()) {
      // Trino has no format named RCFILE; it tells RCTEXT and RCBINARY apart by SerDe.
      if (format != StorageFormat.RCFILE
          && format.getInputFormat().equals(inputFormat)
          && format.getOutputFormat().equals(outputFormat)
          && format.getSerde().equals(serde)) {
        return format;
      }
    }
    return null;
  }

  /**
   * Hive-format tables keep timestamps at the millisecond precision Trino reads them with, so a
   * column, or a field nested in it, of a higher timestamp precision is refused.
   */
  private static void checkHiveColumnType(String column, Type type) {
    if (type instanceof Types.TimestampType) {
      Types.TimestampType timestamp = (Types.TimestampType) type;
      if (!timestamp.hasTimeZone() && timestamp.hasPrecisionSet()) {
        throw new TrinoException(
            GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT,
            String.format(
                "Column %s: Hive-format tables support timestamp(3) only, got timestamp(%d); use"
                    + " type = 'ICEBERG' for timestamp(6)",
                column, timestamp.precision()));
      }
    } else if (type instanceof Types.ListType) {
      checkHiveColumnType(column, ((Types.ListType) type).elementType());
    } else if (type instanceof Types.MapType) {
      checkHiveColumnType(column, ((Types.MapType) type).keyType());
      checkHiveColumnType(column, ((Types.MapType) type).valueType());
    } else if (type instanceof Types.StructType) {
      for (Types.StructType.Field field : ((Types.StructType) type).fields()) {
        checkHiveColumnType(column, field.type());
      }
    }
  }

  /** Returns the table property metadata for Glue catalogs. */
  public static List<PropertyMetadata<?>> getTablePropertyMetadata() {
    return GLUE_TABLE_PROPERTY_META;
  }
}
