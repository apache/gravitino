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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
public class TestHivePropertiesConverter {
  private final HivePropertiesConverter hivePropertiesConverter =
      HivePropertiesConverter.getInstance();

  @Test
  void testTableFormat() {
    // stored as
    Map<String, String> hiveProperties =
        hivePropertiesConverter.toGravitinoTableProperties(
            ImmutableMap.of(HivePropertiesConstants.SPARK_HIVE_STORED_AS, "PARQUET"));
    Assertions.assertEquals(
        "PARQUET", hiveProperties.get(HivePropertiesConstants.GRAVITINO_HIVE_FORMAT));
    Assertions.assertThrowsExactly(
        UnsupportedOperationException.class,
        () ->
            hivePropertiesConverter.toGravitinoTableProperties(
                ImmutableMap.of(HivePropertiesConstants.SPARK_HIVE_STORED_AS, "notExists")));

    // using
    hiveProperties =
        hivePropertiesConverter.toGravitinoTableProperties(
            ImmutableMap.of(TableCatalog.PROP_PROVIDER, "PARQUET"));
    Assertions.assertEquals(
        "PARQUET", hiveProperties.get(HivePropertiesConstants.GRAVITINO_HIVE_FORMAT));
    hiveProperties =
        hivePropertiesConverter.toGravitinoTableProperties(
            ImmutableMap.of(TableCatalog.PROP_PROVIDER, "HIVE"));
    Assertions.assertEquals(
        HivePropertiesConstants.GRAVITINO_HIVE_FORMAT_TEXTFILE,
        hiveProperties.get(HivePropertiesConstants.GRAVITINO_HIVE_FORMAT));
    Assertions.assertThrowsExactly(
        UnsupportedOperationException.class,
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
        HivePropertiesConstants.GRAVITINO_HIVE_EXTERNAL_TABLE,
        hiveProperties.get(HivePropertiesConstants.GRAVITINO_HIVE_TABLE_TYPE));

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

  @Test
  void testCatalogProperties() {
    CaseInsensitiveStringMap caseInsensitiveStringMap =
        new CaseInsensitiveStringMap(ImmutableMap.of("option-key", "option-value"));
    Map<String, String> properties =
        hivePropertiesConverter.toSparkCatalogProperties(
            caseInsensitiveStringMap,
            ImmutableMap.of(
                GravitinoSparkConfig.GRAVITINO_HIVE_METASTORE_URI,
                "hive-uri",
                PropertiesConverter.SPARK_PROPERTY_PREFIX + "bypass-key",
                "bypass-value",
                "key1",
                "value1"));
    Assertions.assertEquals(
        ImmutableMap.of(
            GravitinoSparkConfig.SPARK_HIVE_METASTORE_URI,
            "hive-uri",
            "option-key",
            "option-value",
            "bypass-key",
            "bypass-value"),
        properties);

    // test overwrite
    caseInsensitiveStringMap =
        new CaseInsensitiveStringMap(
            ImmutableMap.of(
                "bypass-key",
                "overwrite-value",
                GravitinoSparkConfig.SPARK_HIVE_METASTORE_URI,
                "hive-uri2"));
    properties =
        hivePropertiesConverter.toSparkCatalogProperties(
            caseInsensitiveStringMap,
            ImmutableMap.of(
                GravitinoSparkConfig.GRAVITINO_HIVE_METASTORE_URI,
                "hive-uri",
                PropertiesConverter.SPARK_PROPERTY_PREFIX + "bypass-key",
                "bypass-value"));

    Assertions.assertEquals(
        ImmutableMap.of(
            GravitinoSparkConfig.SPARK_HIVE_METASTORE_URI,
            "hive-uri2",
            "bypass-key",
            "overwrite-value"),
        properties);
  }
}
