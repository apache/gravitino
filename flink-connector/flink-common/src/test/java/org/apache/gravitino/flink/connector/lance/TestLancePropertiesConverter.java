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

package org.apache.gravitino.flink.connector.lance;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.catalog.ObjectPath;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestLancePropertiesConverter {

  private static final LancePropertiesConverter CONVERTER = LancePropertiesConverter.INSTANCE;

  @Test
  void testTransformPropertyToGravitinoCatalog() {
    Assertions.assertEquals("uri", CONVERTER.transformPropertyToGravitinoCatalog("uri"));
    Assertions.assertEquals(
        "catalog-backend", CONVERTER.transformPropertyToGravitinoCatalog("catalog-backend"));
    Assertions.assertEquals("impl", CONVERTER.transformPropertyToGravitinoCatalog("impl"));
    Assertions.assertEquals("warehouse", CONVERTER.transformPropertyToGravitinoCatalog("warehouse"));
    Assertions.assertEquals("location", CONVERTER.transformPropertyToGravitinoCatalog("location"));
    Assertions.assertEquals(
        "lance.storage.s3.endpoint",
        CONVERTER.transformPropertyToGravitinoCatalog("lance.storage.s3.endpoint"));
    Assertions.assertNull(CONVERTER.transformPropertyToGravitinoCatalog("other.property"));
  }

  @Test
  void testTransformPropertyToFlinkCatalog() {
    Assertions.assertEquals("uri", CONVERTER.transformPropertyToFlinkCatalog("uri"));
    Assertions.assertEquals(
        "catalog-backend", CONVERTER.transformPropertyToFlinkCatalog("catalog-backend"));
    Assertions.assertEquals("impl", CONVERTER.transformPropertyToFlinkCatalog("impl"));
    Assertions.assertEquals("warehouse", CONVERTER.transformPropertyToFlinkCatalog("warehouse"));
    Assertions.assertEquals("location", CONVERTER.transformPropertyToFlinkCatalog("location"));
    Assertions.assertEquals(
        "lance.storage.s3.region",
        CONVERTER.transformPropertyToFlinkCatalog("lance.storage.s3.region"));
    Assertions.assertNull(CONVERTER.transformPropertyToFlinkCatalog("unknown.key"));
  }

  @Test
  void testToFlinkCatalogPropertiesDefaults() {
    Map<String, String> gravitinoProps = ImmutableMap.of("uri", "http://localhost:8090/lance");
    Map<String, String> flinkProps = CONVERTER.toFlinkCatalogProperties(gravitinoProps);

    Assertions.assertEquals(
        GravitinoLanceCatalogFactoryOptions.IDENTIFIER,
        flinkProps.get(CommonCatalogOptions.CATALOG_TYPE.key()));
    Assertions.assertEquals("http://localhost:8090/lance", flinkProps.get("uri"));
    Assertions.assertEquals(
        LancePropertiesConstants.LANCE_CATALOG_BACKEND_REST,
        flinkProps.get(LancePropertiesConstants.GRAVITINO_LANCE_CATALOG_BACKEND));
    Assertions.assertEquals(
        LancePropertiesConstants.LANCE_CATALOG_BACKEND_REST,
        flinkProps.get(LancePropertiesConstants.GRAVITINO_LANCE_CATALOG_IMPL));
  }

  @Test
  void testToGravitinoCatalogProperties() {
    Map<String, String> configMap =
        ImmutableMap.of(
            "uri", "http://localhost:8090/lance",
            "impl", "rest",
            "warehouse", "s3://my-bucket/warehouse",
            "lance.storage.s3.endpoint", "http://s3.amazonaws.com",
            "custom.option", "custom_val",
            "flink.bypass.already.bypassed", "val2");
    Map<String, String> gravitinoProps =
        CONVERTER.toGravitinoCatalogProperties(Configuration.fromMap(configMap));

    Assertions.assertEquals("http://localhost:8090/lance", gravitinoProps.get("uri"));
    Assertions.assertEquals("rest", gravitinoProps.get("impl"));
    Assertions.assertEquals("s3://my-bucket/warehouse", gravitinoProps.get("warehouse"));
    Assertions.assertEquals(
        "http://s3.amazonaws.com", gravitinoProps.get("lance.storage.s3.endpoint"));
    Assertions.assertEquals("custom_val", gravitinoProps.get("flink.bypass.custom.option"));
    Assertions.assertEquals("val2", gravitinoProps.get("flink.bypass.already.bypassed"));
  }

  @Test
  void testToFlinkTablePropertiesWithExplicitLocation() {
    Map<String, String> catalogProps = ImmutableMap.of("uri", "http://localhost:8090/lance");
    Map<String, String> tableProps = ImmutableMap.of("location", "s3://bucket/lance/db/tbl");
    ObjectPath tablePath = new ObjectPath("db", "tbl");

    Map<String, String> flinkTableProps =
        CONVERTER.toFlinkTableProperties(catalogProps, tableProps, tablePath);

    Assertions.assertEquals("lance", flinkTableProps.get("connector"));
    Assertions.assertEquals("s3://bucket/lance/db/tbl", flinkTableProps.get("path"));
    Assertions.assertEquals("s3://bucket/lance/db/tbl", flinkTableProps.get("location"));
  }

  @Test
  void testToFlinkTablePropertiesWithPathFallback() {
    Map<String, String> catalogProps = ImmutableMap.of("uri", "http://localhost:8090/lance");
    Map<String, String> tableProps = ImmutableMap.of("path", "file:///tmp/lance_tbl");
    ObjectPath tablePath = new ObjectPath("default", "lance_tbl");

    Map<String, String> flinkTableProps =
        CONVERTER.toFlinkTableProperties(catalogProps, tableProps, tablePath);

    Assertions.assertEquals("lance", flinkTableProps.get("connector"));
    Assertions.assertEquals("file:///tmp/lance_tbl", flinkTableProps.get("path"));
    Assertions.assertEquals("file:///tmp/lance_tbl", flinkTableProps.get("location"));
  }

  @Test
  void testToFlinkTablePropertiesWithWarehouseDerivation() {
    Map<String, String> catalogProps =
        ImmutableMap.of(
            "uri", "http://localhost:8090/lance",
            "warehouse", "s3://my-warehouse/data/");
    Map<String, String> tableProps = Collections.emptyMap();
    ObjectPath tablePath = new ObjectPath("my_db", "my_table");

    Map<String, String> flinkTableProps =
        CONVERTER.toFlinkTableProperties(catalogProps, tableProps, tablePath);

    Assertions.assertEquals("lance", flinkTableProps.get("connector"));
    Assertions.assertEquals("s3://my-warehouse/data/my_db/my_table", flinkTableProps.get("path"));
    Assertions.assertEquals("s3://my-warehouse/data/my_db/my_table", flinkTableProps.get("location"));
  }

  @Test
  void testToFlinkTablePropertiesWithCatalogLocationDerivation() {
    Map<String, String> catalogProps =
        ImmutableMap.of(
            "uri", "http://localhost:8090/lance",
            "location", "hdfs://namenode:9000/base");
    Map<String, String> tableProps = Collections.emptyMap();
    ObjectPath tablePath = new ObjectPath("finance", "transactions");

    Map<String, String> flinkTableProps =
        CONVERTER.toFlinkTableProperties(catalogProps, tableProps, tablePath);

    Assertions.assertEquals("lance", flinkTableProps.get("connector"));
    Assertions.assertEquals(
        "hdfs://namenode:9000/base/finance/transactions", flinkTableProps.get("path"));
    Assertions.assertEquals(
        "hdfs://namenode:9000/base/finance/transactions", flinkTableProps.get("location"));
  }

  @Test
  void testToFlinkTablePropertiesForwardingStorageOptions() {
    Map<String, String> catalogProps =
        ImmutableMap.of(
            "uri", "http://localhost:8090/lance",
            "lance.storage.s3.endpoint", "http://minio:9000",
            "lance.storage.s3.access_key_id", "minioadmin",
            "location", "/tmp/lance");
    Map<String, String> tableProps =
        ImmutableMap.of("lance.storage.s3.secret_access_key", "miniosecret");
    ObjectPath tablePath = new ObjectPath("default", "test_tbl");

    Map<String, String> flinkTableProps =
        CONVERTER.toFlinkTableProperties(catalogProps, tableProps, tablePath);

    Assertions.assertEquals("lance", flinkTableProps.get("connector"));
    Assertions.assertEquals("http://minio:9000", flinkTableProps.get("lance.storage.s3.endpoint"));
    Assertions.assertEquals(
        "minioadmin", flinkTableProps.get("lance.storage.s3.access_key_id"));
    Assertions.assertEquals(
        "miniosecret", flinkTableProps.get("lance.storage.s3.secret_access_key"));
  }

  @Test
  void testToGravitinoTableProperties() {
    Map<String, String> flinkProps =
        ImmutableMap.of(
            "connector", "lance",
            "path", "s3://bucket/lance/db/table1",
            "custom.key", "custom.val");

    Map<String, String> gravitinoTableProps = CONVERTER.toGravitinoTableProperties(flinkProps);

    Assertions.assertEquals("s3://bucket/lance/db/table1", gravitinoTableProps.get("location"));
    Assertions.assertEquals("s3://bucket/lance/db/table1", gravitinoTableProps.get("path"));
    Assertions.assertEquals("lance", gravitinoTableProps.get("lance"));
    Assertions.assertEquals("custom.val", gravitinoTableProps.get("custom.key"));
  }

  @Test
  void testGetFlinkCatalogType() {
    Assertions.assertEquals("gravitino-lance", CONVERTER.getFlinkCatalogType());
  }
}
