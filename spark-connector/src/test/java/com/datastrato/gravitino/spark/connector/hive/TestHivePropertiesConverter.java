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
            ImmutableMap.of(HivePropertyConstants.SPARK_HIVE_STORED_AS, "PARQUET"));
    Assertions.assertEquals(
        ImmutableMap.of(HivePropertyConstants.GRAVITINO_HIVE_FORMAT, "PARQUET"), hiveProperties);
    Assertions.assertThrowsExactly(
        NotSupportedException.class,
        () ->
            hivePropertiesConverter.toGravitinoTableProperties(
                ImmutableMap.of(HivePropertyConstants.SPARK_HIVE_STORED_AS, "notExists")));

    // using
    hiveProperties =
        hivePropertiesConverter.toGravitinoTableProperties(
            ImmutableMap.of(TableCatalog.PROP_PROVIDER, "PARQUET"));
    Assertions.assertEquals(
        hiveProperties.get(HivePropertyConstants.GRAVITINO_HIVE_FORMAT), "PARQUET");
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
            HivePropertyConstants.GRAVITINO_HIVE_INPUT_FORMAT,
            "a",
            HivePropertyConstants.GRAVITINO_HIVE_OUTPUT_FORMAT,
            "b",
            HivePropertyConstants.GRAVITINO_HIVE_SERDE_LIB,
            "c"),
        hiveProperties);

    hiveProperties =
        hivePropertiesConverter.toGravitinoTableProperties(
            ImmutableMap.of("option.a", "a", "b", "b"));
    Assertions.assertEquals(ImmutableMap.of("a", "a", "b", "b"), hiveProperties);
  }
}
