/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector.utils;

import com.datastrato.gravitino.rel.expressions.distributions.Distribution;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrder;
import com.datastrato.gravitino.spark.connector.ConnectorConstants;
import com.datastrato.gravitino.spark.connector.PropertiesConverter;
import com.datastrato.gravitino.spark.connector.SparkTransformConverter;
import com.datastrato.gravitino.spark.connector.SparkTypeConverter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * GravitinoTableInfoHelper is a common helper class that is used to retrieve table info from the
 * Gravitino Server
 */
public class GravitinoTableInfoHelper {

  private Identifier identifier;
  private com.datastrato.gravitino.rel.Table gravitinoTable;
  private PropertiesConverter propertiesConverter;
  private SparkTransformConverter sparkTransformConverter;

  public GravitinoTableInfoHelper(
      Identifier identifier,
      com.datastrato.gravitino.rel.Table gravitinoTable,
      PropertiesConverter propertiesConverter,
      SparkTransformConverter sparkTransformConverter) {
    this.identifier = identifier;
    this.gravitinoTable = gravitinoTable;
    this.propertiesConverter = propertiesConverter;
    this.sparkTransformConverter = sparkTransformConverter;
  }

  public String name(boolean isCaseSensitive) {
    return getNormalizedIdentifier(identifier, gravitinoTable.name(), isCaseSensitive);
  }

  public StructType schema() {
    List<StructField> structs =
        Arrays.stream(gravitinoTable.columns())
            .map(
                column -> {
                  String comment = column.comment();
                  Metadata metadata = Metadata.empty();
                  if (comment != null) {
                    metadata =
                        new MetadataBuilder()
                            .putString(ConnectorConstants.COMMENT, comment)
                            .build();
                  }
                  return StructField.apply(
                      column.name(),
                      SparkTypeConverter.toSparkType(column.dataType()),
                      column.nullable(),
                      metadata);
                })
            .collect(Collectors.toList());
    return DataTypes.createStructType(structs);
  }

  public Map<String, String> properties() {
    Map properties = new HashMap();
    if (gravitinoTable.properties() != null) {
      properties.putAll(gravitinoTable.properties());
    }

    properties = propertiesConverter.toSparkTableProperties(properties);

    // Spark will retrieve comment from properties.
    String comment = gravitinoTable.comment();
    if (comment != null) {
      properties.put(ConnectorConstants.COMMENT, comment);
    }
    return properties;
  }

  public Transform[] partitioning() {
    com.datastrato.gravitino.rel.expressions.transforms.Transform[] partitions =
        gravitinoTable.partitioning();
    Distribution distribution = gravitinoTable.distribution();
    SortOrder[] sortOrders = gravitinoTable.sortOrder();
    return sparkTransformConverter.toSparkTransform(partitions, distribution, sortOrders);
  }

  public SparkTransformConverter getSparkTransformConverter() {
    return sparkTransformConverter;
  }

  // The underlying catalogs may not case-sensitive, to keep consistent with the action of SparkSQL,
  // we should return normalized identifiers.
  private String getNormalizedIdentifier(
      Identifier tableIdentifier, String gravitinoTableName, boolean isCaseSensitive) {
    if (tableIdentifier.namespace().length == 0) {
      return gravitinoTableName;
    }

    String databaseName = tableIdentifier.namespace()[0];
    if (!isCaseSensitive) {
      databaseName = databaseName.toLowerCase(Locale.ROOT);
    }

    return String.join(".", databaseName, gravitinoTableName);
  }
}
