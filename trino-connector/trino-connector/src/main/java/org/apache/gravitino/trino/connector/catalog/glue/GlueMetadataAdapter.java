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
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.gravitino.catalog.property.PropertyConverter;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.trino.connector.catalog.hive.HiveMetadataAdapter;
import org.apache.gravitino.trino.connector.catalog.hive.HivePropertyMeta;
import org.apache.gravitino.trino.connector.catalog.hive.SortingColumn;
import org.apache.gravitino.trino.connector.catalog.iceberg.ExpressionUtil;
import org.apache.gravitino.trino.connector.metadata.GravitinoTable;

/**
 * Transforming Apache Gravitino Glue metadata to Trino. This adapter handles properties that are
 * specific to the Glue catalog and the lakehouse connector, excluding properties that conflict with
 * the lakehouse connector's native properties.
 */
public class GlueMetadataAdapter extends HiveMetadataAdapter {

  /** The table type property for lakehouse connector (ICEBERG, HIVE, DELTA). */
  static final String LAKEHOUSE_TABLE_TYPE = "type";

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
              value ->
                  ((List<?>) value)
                      .stream()
                          .map(SortingColumn.class::cast)
                          .map(SortingColumn::sortingColumnToString)
                          .collect(toImmutableList())),
          stringProperty(
              "format", "The format of the data files (PARQUET, ORC, etc.)", null, false),
          stringProperty("location", "The S3 storage location for the table", null, false));

  private final PropertyConverter tableConverter = new GlueTablePropertyConverter();

  /**
   * Constructs a new GlueMetadataAdapter.
   *
   * @param schemaProperties the schema properties metadata
   */
  public GlueMetadataAdapter(List<PropertyMetadata<?>> schemaProperties) {
    super(
        schemaProperties,
        GLUE_TABLE_PROPERTY_META,
        ImmutableList.of(),
        new GlueDataTypeTransformer());
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

    if (!partitionExpressions.isEmpty()) {
      Transform[] transforms = ExpressionUtil.partitionFiledToExpression(partitionExpressions);
      table.setPartitioning(transforms);
    }

    return table;
  }

  @Override
  protected List<String> toTrinoPartitionKeys(Transform[] partitioning) {
    return ExpressionUtil.expressionToPartitionFiled(partitioning);
  }

  /** Returns the table property metadata for Glue catalogs. */
  public static List<PropertyMetadata<?>> getTablePropertyMetadata() {
    return GLUE_TABLE_PROPERTY_META;
  }
}
