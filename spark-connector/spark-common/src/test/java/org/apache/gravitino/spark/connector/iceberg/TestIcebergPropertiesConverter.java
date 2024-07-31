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

package org.apache.gravitino.spark.connector.iceberg;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergPropertiesConverter {
  private final IcebergPropertiesConverter icebergPropertiesConverter =
      IcebergPropertiesConverter.getInstance();

  @Test
  void testCatalogPropertiesWithHiveBackend() {
    Map<String, String> properties =
        icebergPropertiesConverter.toSparkCatalogProperties(
            ImmutableMap.of(
                IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND,
                IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND_HIVE,
                IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_URI,
                "hive-uri",
                IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_WAREHOUSE,
                "hive-warehouse",
                "key1",
                "value1"));
    Assertions.assertEquals(
        ImmutableMap.of(
            IcebergPropertiesConstants.ICEBERG_CATALOG_CACHE_ENABLED,
            "FALSE",
            IcebergPropertiesConstants.ICEBERG_CATALOG_TYPE,
            IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_HIVE,
            IcebergPropertiesConstants.ICEBERG_CATALOG_URI,
            "hive-uri",
            IcebergPropertiesConstants.ICEBERG_CATALOG_WAREHOUSE,
            "hive-warehouse"),
        properties);
  }

  @Test
  void testCatalogPropertiesWithJdbcBackend() {
    Map<String, String> properties =
        icebergPropertiesConverter.toSparkCatalogProperties(
            ImmutableMap.of(
                IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND,
                IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_JDBC,
                IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_URI,
                "jdbc-uri",
                IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_WAREHOUSE,
                "jdbc-warehouse",
                IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_JDBC_USER,
                "user",
                IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_JDBC_PASSWORD,
                "passwd",
                "key1",
                "value1"));
    Assertions.assertEquals(
        ImmutableMap.of(
            IcebergPropertiesConstants.ICEBERG_CATALOG_CACHE_ENABLED,
            "FALSE",
            IcebergPropertiesConstants.ICEBERG_CATALOG_TYPE,
            IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_JDBC,
            IcebergPropertiesConstants.ICEBERG_CATALOG_URI,
            "jdbc-uri",
            IcebergPropertiesConstants.ICEBERG_CATALOG_WAREHOUSE,
            "jdbc-warehouse",
            IcebergPropertiesConstants.ICEBERG_CATALOG_JDBC_USER,
            "user",
            IcebergPropertiesConstants.ICEBERG_CATALOG_JDBC_PASSWORD,
            "passwd"),
        properties);
  }

  @Test
  void testCatalogPropertiesWithRestBackend() {
    Map<String, String> properties =
        icebergPropertiesConverter.toSparkCatalogProperties(
            ImmutableMap.of(
                IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND,
                IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_REST,
                IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_URI,
                "rest-uri",
                IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_WAREHOUSE,
                "rest-warehouse",
                "key1",
                "value1"));
    Assertions.assertEquals(
        ImmutableMap.of(
            IcebergPropertiesConstants.ICEBERG_CATALOG_CACHE_ENABLED,
            "FALSE",
            IcebergPropertiesConstants.ICEBERG_CATALOG_TYPE,
            IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_REST,
            IcebergPropertiesConstants.ICEBERG_CATALOG_URI,
            "rest-uri",
            IcebergPropertiesConstants.ICEBERG_CATALOG_WAREHOUSE,
            "rest-warehouse"),
        properties);
  }
}
