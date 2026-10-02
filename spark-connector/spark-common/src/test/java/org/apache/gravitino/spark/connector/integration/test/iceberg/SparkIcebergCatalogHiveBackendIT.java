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
package org.apache.gravitino.spark.connector.integration.test.iceberg;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.apache.gravitino.spark.connector.iceberg.IcebergPropertiesConstants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** This class use Apache Iceberg HiveCatalog for backend catalog. */
@Tag("gravitino-docker-test")
public abstract class SparkIcebergCatalogHiveBackendIT extends SparkIcebergCatalogIT {

  /** Verifies that Hive table aliases differing only in case read and write the same table. */
  @Test
  public void testCaseInsensitiveTableName() {
    String tableName = "test_case_insensitive_table";
    String uppercaseName = tableName.toUpperCase(Locale.ROOT);
    dropTableIfExists(tableName);
    try {
      createSimpleTable(tableName);
      sql(String.format("INSERT INTO %s VALUES(1, 'lowercase', 10)", tableName));
      sql(String.format("INSERT INTO %s VALUES(2, 'uppercase', 20)", uppercaseName));

      List<String> rows = getQueryData(String.format("SELECT * FROM %s ORDER BY id", tableName));
      Assertions.assertEquals(List.of("1,lowercase,10", "2,uppercase,20"), rows);
      Assertions.assertEquals(
          rows, getQueryData(String.format("SELECT * FROM %s ORDER BY id", uppercaseName)));
    } finally {
      dropTableIfExists(tableName);
    }
  }

  @Override
  protected Map<String, String> getExtraSparkConfigs() {
    // This class deliberately exercises the legacy Hive backend without a discoverable Iceberg
    // REST endpoint (useDynamicIcebergRestConfigProvider() is false), so routing must be disabled
    // explicitly.
    return ImmutableMap.of(GravitinoSparkConfig.GRAVITINO_ICEBERG_REST_ROUTING_ENABLED, "false");
  }

  @Override
  protected Map<String, String> getCatalogConfigs() {
    Map<String, String> catalogProperties = Maps.newHashMap();
    catalogProperties.put(
        IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND,
        IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_HIVE);
    catalogProperties.put(
        IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_WAREHOUSE, warehouse);
    catalogProperties.put(
        IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_URI, hiveMetastoreUri);
    catalogProperties.put(
        IcebergConstants.TABLE_METADATA_CACHE_IMPL,
        "org.apache.gravitino.iceberg.common.cache.LocalTableMetadataCache");
    catalogProperties.put(IcebergConstants.IO_IMPL, "org.apache.iceberg.hadoop.HadoopFileIO");

    return catalogProperties;
  }
}
