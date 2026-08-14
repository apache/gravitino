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

package org.apache.gravitino.flink.connector.iceberg;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.iceberg.rest.auth.AuthProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestGravitinoIcebergCatalogFactory {

  private final GravitinoIcebergCatalogFactory factory = new GravitinoIcebergCatalogFactory();

  @Test
  void testJdbcBackendTranslatedToCatalogImpl() {
    Map<String, String> options =
        ImmutableMap.of(
            IcebergPropertiesConstants.ICEBERG_CATALOG_TYPE,
            IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_JDBC,
            IcebergPropertiesConstants.ICEBERG_CATALOG_URI,
            "jdbc:mysql://localhost:3306/gravitino",
            IcebergPropertiesConstants.ICEBERG_CATALOG_WAREHOUSE,
            "hdfs://localhost:9000/user/hive/warehouse",
            "jdbc.user",
            "iceberg",
            "jdbc.password",
            "iceberg");

    Map<String, String> result = factory.toIcebergCatalogOptions(options);

    // JDBC backend must be loaded through catalog-impl, not catalog-type.
    Assertions.assertEquals(
        IcebergPropertiesConstants.ICEBERG_JDBC_CATALOG_IMPL,
        result.get(IcebergPropertiesConstants.ICEBERG_CATALOG_IMPL));
    Assertions.assertFalse(
        result.containsKey(IcebergPropertiesConstants.ICEBERG_CATALOG_TYPE),
        "catalog-type and catalog-impl are mutually exclusive");
    // JDBC connection properties are preserved.
    Assertions.assertEquals("iceberg", result.get("jdbc.user"));
    Assertions.assertEquals("iceberg", result.get("jdbc.password"));
    Assertions.assertEquals(
        "jdbc:mysql://localhost:3306/gravitino",
        result.get(IcebergPropertiesConstants.ICEBERG_CATALOG_URI));
    Assertions.assertEquals("iceberg", result.get(CommonCatalogOptions.CATALOG_TYPE.key()));
  }

  @Test
  void testExplicitCatalogImplIsRespected() {
    Map<String, String> options =
        ImmutableMap.of(
            IcebergPropertiesConstants.ICEBERG_CATALOG_TYPE,
            IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_JDBC,
            IcebergPropertiesConstants.ICEBERG_CATALOG_IMPL,
            "com.example.CustomJdbcCatalog");

    Map<String, String> result = factory.toIcebergCatalogOptions(options);

    // An explicitly provided catalog-impl must not be overwritten, and catalog-type is dropped.
    Assertions.assertEquals(
        "com.example.CustomJdbcCatalog",
        result.get(IcebergPropertiesConstants.ICEBERG_CATALOG_IMPL));
    Assertions.assertFalse(result.containsKey(IcebergPropertiesConstants.ICEBERG_CATALOG_TYPE));
  }

  @Test
  void testHiveBackendKeepsCatalogType() {
    Map<String, String> options =
        ImmutableMap.of(
            IcebergPropertiesConstants.ICEBERG_CATALOG_TYPE,
            IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_HIVE,
            IcebergPropertiesConstants.ICEBERG_CATALOG_URI,
            "thrift://localhost:9083");

    Map<String, String> result = factory.toIcebergCatalogOptions(options);

    Assertions.assertEquals(
        IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_HIVE,
        result.get(IcebergPropertiesConstants.ICEBERG_CATALOG_TYPE));
    Assertions.assertFalse(result.containsKey(IcebergPropertiesConstants.ICEBERG_CATALOG_IMPL));
    Assertions.assertEquals("iceberg", result.get(CommonCatalogOptions.CATALOG_TYPE.key()));
  }

  @Test
  void testRestBackendKeepsCatalogType() {
    Map<String, String> options =
        ImmutableMap.of(
            IcebergPropertiesConstants.ICEBERG_CATALOG_TYPE,
            IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_REST,
            IcebergPropertiesConstants.ICEBERG_CATALOG_URI,
            "http://localhost:9001/iceberg/",
            // Pre-set so the REST auth-propagation branch (which needs a live
            // GravitinoCatalogManager) is skipped; that branch is covered separately below.
            AuthProperties.AUTH_TYPE,
            "none");

    Map<String, String> result = factory.toIcebergCatalogOptions(options);

    Assertions.assertEquals(
        IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_REST,
        result.get(IcebergPropertiesConstants.ICEBERG_CATALOG_TYPE));
    Assertions.assertFalse(result.containsKey(IcebergPropertiesConstants.ICEBERG_CATALOG_IMPL));
    Assertions.assertEquals("iceberg", result.get(CommonCatalogOptions.CATALOG_TYPE.key()));
  }

  @Test
  void testRestAuthPropagatedWhenLoadedFromCatalogStore() {
    // Simulates the catalog-store load path (`USE CATALOG ...`): by the time options reach this
    // factory, IcebergPropertiesConverter has already renamed `catalog-backend` -> `catalog-type`
    // during property conversion, so only `catalog-type=rest` is present, no `catalog-backend`.
    Map<String, String> options =
        ImmutableMap.of(
            IcebergPropertiesConstants.ICEBERG_CATALOG_TYPE,
            IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_REST,
            IcebergPropertiesConstants.ICEBERG_CATALOG_URI,
            "http://localhost:9001/iceberg/");

    // Regression check for #11601: the REST auth-propagation branch must be gated on the
    // normalized `catalog-type`, not the renamed-away `catalog-backend` key, otherwise it would
    // silently skip auth propagation on this path. GravitinoCatalogManager isn't initialized in
    // this unit test, so reaching it (and failing there) proves the gate now fires correctly.
    IllegalStateException exception =
        Assertions.assertThrows(
            IllegalStateException.class, () -> factory.toIcebergCatalogOptions(options));
    String msg = exception.getMessage();
    Assertions.assertTrue(msg != null && msg.contains("GravitinoCatalogManager"));
  }
}
