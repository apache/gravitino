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

package org.apache.gravitino.spark.connector.utils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.spark.connector.ConnectorConstants;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * GravitinoTableInfoHelper is a common helper class that is used to retrieve table info from the
 * Apache Gravitino Server
 */
public class GravitinoTableInfoHelper {

  private boolean isCaseSensitive;
  private Identifier identifier;
  private org.apache.gravitino.rel.Table gravitinoTable;
  private PropertiesConverter propertiesConverter;
  private SparkTransformConverter sparkTransformConverter;
  private SparkTypeConverter sparkTypeConverter;

  public GravitinoTableInfoHelper(
      boolean isCaseSensitive,
      Identifier identifier,
      org.apache.gravitino.rel.Table gravitinoTable,
      PropertiesConverter propertiesConverter,
      SparkTransformConverter sparkTransformConverter,
      SparkTypeConverter sparkTypeConverter) {
    this.isCaseSensitive = isCaseSensitive;
    this.identifier = identifier;
    this.gravitinoTable = gravitinoTable;
    this.propertiesConverter = propertiesConverter;
    this.sparkTransformConverter = sparkTransformConverter;
    this.sparkTypeConverter = sparkTypeConverter;
  }

  public String name() {
    return getNormalizedIdentifier(identifier, gravitinoTable.name());
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
                      sparkTypeConverter.toSparkType(column.dataType()),
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
    org.apache.gravitino.rel.expressions.transforms.Transform[] partitions =
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
  private String getNormalizedIdentifier(Identifier tableIdentifier, String gravitinoTableName) {
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
