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
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergCatalogBackend;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
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

  @Test
  void testCatalogPropertiesWithCustomBackend() {
    Map<String, String> properties =
        icebergPropertiesConverter.toSparkCatalogProperties(
            ImmutableMap.of(
                IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND,
                IcebergCatalogBackend.CUSTOM.name(),
                IcebergConstants.CATALOG_BACKEND_IMPL,
                "CustomCatalog",
                IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_WAREHOUSE,
                "custom-warehouse",
                "key1",
                "value1"));

    Assertions.assertEquals(
        ImmutableMap.of(
            IcebergPropertiesConstants.ICEBERG_CATALOG_CACHE_ENABLED,
            "FALSE",
            IcebergPropertiesConstants.ICEBERG_CATALOG_IMPL,
            "CustomCatalog",
            IcebergPropertiesConstants.ICEBERG_CATALOG_WAREHOUSE,
            "custom-warehouse"),
        properties);
  }

  @Test
  void testIcebergRestPropertiesFromHiveBackendExcludesStaticCredentials() {
    Map<String, String> gravitinoProperties =
        ImmutableMap.of(
            IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND,
            IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND_HIVE,
            IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_URI,
            "thrift://hive-metastore:9083",
            IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_WAREHOUSE,
            "s3://bucket/warehouse",
            "s3-access-key-id",
            "AKIDEXAMPLE",
            "s3-secret-access-key",
            "secret");

    Map<String, String> properties =
        icebergPropertiesConverter.buildIcebergRestProperties(
            "my_catalog", "http://gravitino:9001/iceberg", gravitinoProperties, ImmutableMap.of());

    Assertions.assertEquals(
        "rest", properties.get(IcebergPropertiesConstants.ICEBERG_CATALOG_TYPE));
    Assertions.assertEquals(
        "http://gravitino:9001/iceberg",
        properties.get(IcebergPropertiesConstants.ICEBERG_CATALOG_URI));
    Assertions.assertEquals(
        "my_catalog", properties.get(IcebergPropertiesConstants.ICEBERG_CATALOG_WAREHOUSE));
    Assertions.assertEquals("my_catalog", properties.get("prefix"));
    Assertions.assertEquals(
        "vended-credentials", properties.get("header.X-Iceberg-Access-Delegation"));
    Assertions.assertEquals(
        "org.apache.iceberg.aws.s3.S3FileIO", properties.get(IcebergConstants.IO_IMPL));

    // The REST protocol vends its own credentials, so no static secret should be present.
    Assertions.assertFalse(properties.containsKey(IcebergConstants.ICEBERG_S3_ACCESS_KEY_ID));
    Assertions.assertFalse(properties.containsKey(IcebergConstants.ICEBERG_S3_SECRET_ACCESS_KEY));
    Assertions.assertFalse(properties.containsKey(IcebergConstants.ICEBERG_JDBC_USER));
    Assertions.assertFalse(properties.containsKey(IcebergConstants.ICEBERG_JDBC_PASSWORD));
  }

  @Test
  void testIcebergRestPropertiesFromJdbcBackendExcludesStaticCredentials() {
    Map<String, String> gravitinoProperties =
        ImmutableMap.of(
            IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND,
            IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_JDBC,
            IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_URI,
            "jdbc:postgresql://db:5432/iceberg",
            IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_JDBC_USER,
            "user",
            IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_JDBC_PASSWORD,
            "passwd");

    Map<String, String> properties =
        icebergPropertiesConverter.buildIcebergRestProperties(
            "my_catalog", "http://gravitino:9001/iceberg", gravitinoProperties, ImmutableMap.of());

    Assertions.assertEquals(
        "rest", properties.get(IcebergPropertiesConstants.ICEBERG_CATALOG_TYPE));
    Assertions.assertFalse(properties.containsKey(IcebergConstants.ICEBERG_JDBC_USER));
    Assertions.assertFalse(properties.containsKey(IcebergConstants.ICEBERG_JDBC_PASSWORD));
  }

  @Test
  void testIcebergRestPropertiesRespectsExplicitIoImpl() {
    Map<String, String> gravitinoProperties =
        ImmutableMap.of(
            IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND,
            IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND_HIVE,
            IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_WAREHOUSE,
            "s3://bucket/warehouse",
            IcebergConstants.IO_IMPL,
            "com.example.CustomFileIO");

    Map<String, String> properties =
        icebergPropertiesConverter.buildIcebergRestProperties(
            "my_catalog", "http://gravitino:9001/iceberg", gravitinoProperties, ImmutableMap.of());

    Assertions.assertEquals("com.example.CustomFileIO", properties.get(IcebergConstants.IO_IMPL));
  }

  @Test
  void testIcebergRestPropertiesWarehouseSchemeWithoutNativeFileIo() {
    Map<String, String> gravitinoProperties =
        ImmutableMap.of(
            IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND,
            IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND_HIVE,
            IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_WAREHOUSE,
            "hdfs://namenode/warehouse");

    Map<String, String> properties =
        icebergPropertiesConverter.buildIcebergRestProperties(
            "my_catalog", "http://gravitino:9001/iceberg", gravitinoProperties, ImmutableMap.of());

    Assertions.assertFalse(properties.containsKey(IcebergConstants.IO_IMPL));
  }

  @Test
  void testIcebergRestPropertiesBypassAndReservedKeyOverride() {
    Map<String, String> gravitinoProperties =
        ImmutableMap.of(
            IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND,
            IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND_HIVE,
            "spark.bypass.some-custom-property",
            "custom-value",
            // Attempting to override a reserved routing key via spark.bypass must be ignored.
            "spark.bypass." + IcebergPropertiesConstants.ICEBERG_CATALOG_URI,
            "http://attacker-controlled/iceberg");

    Map<String, String> properties =
        icebergPropertiesConverter.buildIcebergRestProperties(
            "my_catalog", "http://gravitino:9001/iceberg", gravitinoProperties, ImmutableMap.of());

    Assertions.assertEquals("custom-value", properties.get("some-custom-property"));
    Assertions.assertEquals(
        "http://gravitino:9001/iceberg",
        properties.get(IcebergPropertiesConstants.ICEBERG_CATALOG_URI));
  }

  @Test
  void testIcebergRestPropertiesRestClientConfigPassthrough() {
    Map<String, String> gravitinoProperties =
        ImmutableMap.of(
            IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND,
            IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND_HIVE);

    Map<String, String> properties =
        icebergPropertiesConverter.buildIcebergRestProperties(
            "my_catalog",
            "http://gravitino:9001/iceberg",
            gravitinoProperties,
            ImmutableMap.of("rest.auth.type", "basic", "rest.auth.basic.username", "admin"));

    Assertions.assertEquals("basic", properties.get("rest.auth.type"));
    Assertions.assertEquals("admin", properties.get("rest.auth.basic.username"));
  }
}
