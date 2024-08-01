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

package org.apache.gravitino.spark.connector.hive;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.hive.TableType;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.spark.sql.connector.catalog.TableCatalog;

/** Transform Apache Hive catalog properties between Apache Spark and Apache Gravitino. */
public class HivePropertiesConverter implements PropertiesConverter {
  public static class HivePropertiesConverterHolder {
    private static final HivePropertiesConverter INSTANCE = new HivePropertiesConverter();
  }

  private HivePropertiesConverter() {}

  public static HivePropertiesConverter getInstance() {
    return HivePropertiesConverterHolder.INSTANCE;
  }

  // Transform Spark Hive file format to Gravitino hive file format
  static final Map<String, String> fileFormatMap =
      ImmutableMap.of(
          "sequencefile", HivePropertiesConstants.GRAVITINO_HIVE_FORMAT_SEQUENCEFILE,
          "rcfile", HivePropertiesConstants.GRAVITINO_HIVE_FORMAT_RCFILE,
          "orc", HivePropertiesConstants.GRAVITINO_HIVE_FORMAT_ORC,
          "parquet", HivePropertiesConstants.GRAVITINO_HIVE_FORMAT_PARQUET,
          "textfile", HivePropertiesConstants.GRAVITINO_HIVE_FORMAT_TEXTFILE,
          "hive", HivePropertiesConstants.GRAVITINO_HIVE_FORMAT_TEXTFILE,
          "json", HivePropertiesConstants.GRAVITINO_HIVE_FORMAT_JSON,
          "csv", HivePropertiesConstants.GRAVITINO_HIVE_FORMAT_CSV,
          "avro", HivePropertiesConstants.GRAVITINO_HIVE_FORMAT_AVRO);

  static final Map<String, String> sparkToGravitinoPropertyMap =
      ImmutableMap.of(
          "hive.output-format",
          HivePropertiesConstants.GRAVITINO_HIVE_OUTPUT_FORMAT,
          "hive.input-format",
          HivePropertiesConstants.GRAVITINO_HIVE_INPUT_FORMAT,
          "hive.serde",
          HivePropertiesConstants.GRAVITINO_HIVE_SERDE_LIB,
          HivePropertiesConstants.SPARK_HIVE_LOCATION,
          HivePropertiesConstants.GRAVITINO_HIVE_TABLE_LOCATION);

  static final Map<String, String> gravitinoToSparkPropertyMap =
      ImmutableMap.of(
          HivePropertiesConstants.GRAVITINO_HIVE_TABLE_LOCATION,
          HivePropertiesConstants.SPARK_HIVE_LOCATION);

  @Override
  public Map<String, String> toSparkCatalogProperties(Map<String, String> properties) {
    Preconditions.checkArgument(properties != null, "Hive Catalog properties should not be null");
    String metastoreUri = properties.get(GravitinoSparkConfig.GRAVITINO_HIVE_METASTORE_URI);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(metastoreUri),
        "Couldn't get "
            + GravitinoSparkConfig.GRAVITINO_HIVE_METASTORE_URI
            + " from Hive catalog properties");
    HashMap<String, String> all = new HashMap<>();
    all.put(GravitinoSparkConfig.SPARK_HIVE_METASTORE_URI, metastoreUri);
    return all;
  }

  /**
   * CREATE TABLE xxx STORED AS PARQUET will save "hive.stored-as" = "PARQUET" in property.
   *
   * <p>CREATE TABLE xxx USING PARQUET will save "provider" = "PARQUET" in property.
   *
   * <p>CREATE TABLE xxx ROW FORMAT SERDE xx STORED AS INPUTFORMAT xx OUTPUTFORMAT xx will save
   * "hive.input-format", "hive.output-format", "hive.serde" in property.
   *
   * <p>CREATE TABLE xxx ROW FORMAT DELIMITED FIELDS TERMINATED xx will save "option.field.delim" in
   * property.
   *
   * <p>Please refer to
   * https://github.com/apache/spark/blob/7d87a94dd77f43120701e48a371324a4f5f2064b/sql/catalyst/src/main/scala/org/apache/spark/sql/connector/catalog/CatalogV2Util.scala#L397
   * for more details.
   */
  @Override
  public Map<String, String> toGravitinoTableProperties(Map<String, String> properties) {
    Map<String, String> gravitinoTableProperties = fromOptionProperties(properties);
    String provider = gravitinoTableProperties.get(TableCatalog.PROP_PROVIDER);
    String storeAs = gravitinoTableProperties.get(HivePropertiesConstants.SPARK_HIVE_STORED_AS);
    String fileFormat = Optional.ofNullable(storeAs).orElse(provider);
    String isExternal =
        Optional.ofNullable(gravitinoTableProperties.get(TableCatalog.PROP_EXTERNAL))
            .orElse("false");

    if (fileFormat != null) {
      String gravitinoFormat = fileFormatMap.get(fileFormat.toLowerCase(Locale.ROOT));
      if (gravitinoFormat != null) {
        gravitinoTableProperties.put(
            HivePropertiesConstants.GRAVITINO_HIVE_FORMAT, gravitinoFormat);
      } else {
        throw new UnsupportedOperationException("Doesn't support hive file format: " + fileFormat);
      }
    }

    if (isExternal.equalsIgnoreCase("true")) {
      gravitinoTableProperties.put(
          HivePropertiesConstants.GRAVITINO_HIVE_TABLE_TYPE, TableType.EXTERNAL_TABLE.name());
    }

    sparkToGravitinoPropertyMap.forEach(
        (sparkProperty, gravitinoProperty) -> {
          if (gravitinoTableProperties.containsKey(sparkProperty)) {
            String value = gravitinoTableProperties.remove(sparkProperty);
            gravitinoTableProperties.put(gravitinoProperty, value);
          }
        });

    return gravitinoTableProperties;
  }

  @Override
  public Map<String, String> toSparkTableProperties(Map<String, String> properties) {
    Map<String, String> sparkTableProperties = toOptionProperties(properties);
    String hiveTableType =
        sparkTableProperties.get(HivePropertiesConstants.GRAVITINO_HIVE_TABLE_TYPE);

    if (HivePropertiesConstants.GRAVITINO_HIVE_EXTERNAL_TABLE.equalsIgnoreCase(hiveTableType)) {
      sparkTableProperties.remove(HivePropertiesConstants.GRAVITINO_HIVE_TABLE_TYPE);
      sparkTableProperties.put(HivePropertiesConstants.SPARK_HIVE_EXTERNAL, "true");
    }

    gravitinoToSparkPropertyMap.forEach(
        (gravitinoProperty, sparkProperty) -> {
          if (sparkTableProperties.containsKey(gravitinoProperty)) {
            String value = sparkTableProperties.remove(gravitinoProperty);
            sparkTableProperties.put(sparkProperty, value);
          }
        });

    return sparkTableProperties;
  }

  @VisibleForTesting
  static Map<String, String> toOptionProperties(Map<String, String> properties) {
    return properties.entrySet().stream()
        .collect(
            Collectors.toMap(
                entry -> {
                  String key = entry.getKey();
                  if (key.startsWith(
                      HivePropertiesConstants.GRAVITINO_HIVE_SERDE_PARAMETER_PREFIX)) {
                    return TableCatalog.OPTION_PREFIX
                        + key.substring(
                            HivePropertiesConstants.GRAVITINO_HIVE_SERDE_PARAMETER_PREFIX.length());
                  } else {
                    return key;
                  }
                },
                entry -> entry.getValue(),
                (existingValue, newValue) -> newValue));
  }

  @VisibleForTesting
  static Map<String, String> fromOptionProperties(Map<String, String> properties) {
    return properties.entrySet().stream()
        .collect(
            Collectors.toMap(
                entry -> {
                  String key = entry.getKey();
                  if (key.startsWith(TableCatalog.OPTION_PREFIX)) {
                    return HivePropertiesConstants.GRAVITINO_HIVE_SERDE_PARAMETER_PREFIX
                        + key.substring(TableCatalog.OPTION_PREFIX.length());
                  } else {
                    return key;
                  }
                },
                entry -> entry.getValue(),
                (existingValue, newValue) -> newValue));
  }
}
