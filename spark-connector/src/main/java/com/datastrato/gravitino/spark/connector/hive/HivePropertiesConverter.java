/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector.hive;

import com.datastrato.gravitino.spark.connector.PropertiesConverter;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import javax.ws.rs.NotSupportedException;
import org.apache.spark.sql.connector.catalog.TableCatalog;

/** Transform hive catalog properties between Spark and Gravitino. */
public class HivePropertiesConverter implements PropertiesConverter {

  // Transform Spark format to Gravitino format
  static final Map<String, String> hiveTableFormatMap =
      ImmutableMap.of(
          "sequencefile", "SEQUENCEFILE",
          "rcfile", "RCFILE",
          "orc", "ORC",
          "parquet", "PARQUET",
          "textfile", "TEXTFILE",
          "avro", "AVRO");

  static final Map<String, String> sparkToGravitinoPropertyMap =
      ImmutableMap.of(
          "hive.output-format",
          HivePropertyConstants.GRAVITINO_HIVE_OUTPUT_FORMAT,
          "hive.input-format",
          HivePropertyConstants.GRAVITINO_HIVE_INPUT_FORMAT,
          "hive.serde",
          HivePropertyConstants.GRAVITINO_HIVE_SERDE_LIB);

  /**
   * CREATE TABLE xxx STORED AS PARQUET will save "hive.stored.as" = "PARQUET" in property. CREATE
   * TABLE xxx USING PARQUET will save "provider" = "PARQUET" in property. CREATE TABLE xxx ROW
   * FORMAT SERDE xx STORED AS INPUTFORMAT xx OUTPUTFORMAT xx will save "hive.input-format",
   * "hive.output-format", "hive.serde" in property. CREATE TABLE xxx ROW FORMAT DELIMITED FIELDS
   * TERMINATED xx will save "option.xx" in property.
   */
  @Override
  public Map<String, String> toGravitinoTableProperties(Map<String, String> properties) {
    Map<String, String> gravitinoTableProperties =
        PropertiesConverter.transformOptionProperties(properties);
    String provider = gravitinoTableProperties.get(TableCatalog.PROP_PROVIDER);
    String storeAs = gravitinoTableProperties.get(HivePropertyConstants.SPARK_HIVE_STORED_AS);
    String sparkHiveTableFormat = Optional.ofNullable(storeAs).orElse(provider);
    if (sparkHiveTableFormat != null) {
      String gravitinoFormat =
          hiveTableFormatMap.get(sparkHiveTableFormat.toLowerCase(Locale.ROOT));
      if (gravitinoFormat != null) {
        gravitinoTableProperties.put(HivePropertyConstants.GRAVITINO_HIVE_FORMAT, gravitinoFormat);
      } else {
        throw new NotSupportedException(
            "Doesn't support spark hive table format: " + sparkHiveTableFormat);
      }
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
    return new HashMap<>(properties);
  }
}
