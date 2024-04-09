/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector.hive;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import javax.ws.rs.NotSupportedException;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
public class TestHivePropertiesConverter {
  HivePropertiesConverter hivePropertiesConverter = new HivePropertiesConverter();

  @Test
  void testTableFormat() {
    // stored as
    Map<String, String> hiveProperties =
        hivePropertiesConverter.toGravitinoTableProperties(
            ImmutableMap.of(HivePropertiesConstants.SPARK_HIVE_STORED_AS, "PARQUET"));
    Assertions.assertEquals(
        hiveProperties.get(HivePropertiesConstants.GRAVITINO_HIVE_FORMAT), "PARQUET");
    Assertions.assertThrowsExactly(
        NotSupportedException.class,
        () ->
            hivePropertiesConverter.toGravitinoTableProperties(
                ImmutableMap.of(HivePropertiesConstants.SPARK_HIVE_STORED_AS, "notExists")));

    // using
    hiveProperties =
        hivePropertiesConverter.toGravitinoTableProperties(
            ImmutableMap.of(TableCatalog.PROP_PROVIDER, "PARQUET"));
    Assertions.assertEquals(
        hiveProperties.get(HivePropertiesConstants.GRAVITINO_HIVE_FORMAT), "PARQUET");
    Assertions.assertThrowsExactly(
        NotSupportedException.class,
        () ->
            hivePropertiesConverter.toGravitinoTableProperties(
                ImmutableMap.of(TableCatalog.PROP_PROVIDER, "notExists")));

    // row format
    hiveProperties =
        hivePropertiesConverter.toGravitinoTableProperties(
            ImmutableMap.of(
                "hive.input-format", "a", "hive.output-format", "b", "hive.serde", "c"));
    Assertions.assertEquals(
        ImmutableMap.of(
            HivePropertiesConstants.GRAVITINO_HIVE_INPUT_FORMAT,
            "a",
            HivePropertiesConstants.GRAVITINO_HIVE_OUTPUT_FORMAT,
            "b",
            HivePropertiesConstants.GRAVITINO_HIVE_SERDE_LIB,
            "c"),
        hiveProperties);

    hiveProperties =
        hivePropertiesConverter.toGravitinoTableProperties(
            ImmutableMap.of(TableCatalog.OPTION_PREFIX + "a", "a", "b", "b"));
    Assertions.assertEquals(
        ImmutableMap.of(
            HivePropertiesConstants.GRAVITINO_HIVE_SERDE_PARAMETER_PREFIX + "a", "a", "b", "b"),
        hiveProperties);

    hiveProperties =
        hivePropertiesConverter.toSparkTableProperties(
            ImmutableMap.of(
                HivePropertiesConstants.GRAVITINO_HIVE_SERDE_PARAMETER_PREFIX + "a",
                "a",
                "b",
                "b"));
    Assertions.assertEquals(
        ImmutableMap.of(TableCatalog.OPTION_PREFIX + "a", "a", "b", "b"), hiveProperties);
  }

  @Test
  void testExternalTable() {
    Map<String, String> hiveProperties =
        hivePropertiesConverter.toGravitinoTableProperties(
            ImmutableMap.of(HivePropertiesConstants.SPARK_HIVE_EXTERNAL, "true"));
    Assertions.assertEquals(
        hiveProperties.get(HivePropertiesConstants.GRAVITINO_HIVE_TABLE_TYPE),
        HivePropertiesConstants.GRAVITINO_HIVE_EXTERNAL_TABLE);

    hiveProperties =
        hivePropertiesConverter.toSparkTableProperties(
            ImmutableMap.of(
                HivePropertiesConstants.GRAVITINO_HIVE_TABLE_TYPE,
                HivePropertiesConstants.GRAVITINO_HIVE_EXTERNAL_TABLE));
    Assertions.assertEquals(
        ImmutableMap.of(HivePropertiesConstants.SPARK_HIVE_EXTERNAL, "true"), hiveProperties);
  }

  @Test
  void testLocation() {
    String location = "/user/hive/external_db";

    Map<String, String> hiveProperties =
        hivePropertiesConverter.toGravitinoTableProperties(
            ImmutableMap.of(HivePropertiesConstants.SPARK_HIVE_LOCATION, location));
    Assertions.assertEquals(
        hiveProperties.get(HivePropertiesConstants.GRAVITINO_HIVE_TABLE_LOCATION), location);

    hiveProperties =
        hivePropertiesConverter.toSparkTableProperties(
            ImmutableMap.of(HivePropertiesConstants.GRAVITINO_HIVE_TABLE_LOCATION, location));
    Assertions.assertEquals(
        ImmutableMap.of(HivePropertiesConstants.SPARK_HIVE_LOCATION, location), hiveProperties);
  }

  @Test
  void testOptionProperties() {
    Map<String, String> properties =
        HivePropertiesConverter.fromOptionProperties(
            ImmutableMap.of(TableCatalog.OPTION_PREFIX + "a", "1", "b", "2"));
    Assertions.assertEquals(
        ImmutableMap.of(
            HivePropertiesConstants.GRAVITINO_HIVE_SERDE_PARAMETER_PREFIX + "a", "1", "b", "2"),
        properties);

    properties =
        HivePropertiesConverter.toOptionProperties(
            ImmutableMap.of(
                HivePropertiesConstants.GRAVITINO_HIVE_SERDE_PARAMETER_PREFIX + "a",
                "1",
                "b",
                "2"));
    Assertions.assertEquals(
        ImmutableMap.of(TableCatalog.OPTION_PREFIX + "a", "1", "b", "2"), properties);
  }
}
