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

import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.apache.gravitino.spark.connector.iceberg.IcebergPropertiesConstants;
import org.apache.gravitino.spark.connector.integration.test.SparkEnvIT;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;

/** Tests Spark native Iceberg REST catalog registration through Gravitino Spark plugin. */
@Tag("gravitino-docker-test")
@DisabledIf("org.apache.gravitino.integration.test.util.ITUtils#isEmbedded")
public abstract class SparkIcebergNativeRestCatalogIT extends SparkEnvIT {

  private static final String DATABASE = "native_iceberg_rest_db";

  @Override
  protected String getCatalogName() {
    return "iceberg_native";
  }

  @Override
  protected String getProvider() {
    return "lakehouse-iceberg";
  }

  @Override
  protected Map<String, String> getCatalogConfigs() {
    Map<String, String> catalogProperties = Maps.newHashMap();
    catalogProperties.put(
        IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND,
        IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_REST);
    catalogProperties.put(
        IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_URI, icebergRestServiceUri);
    return catalogProperties;
  }

  @Override
  protected boolean supportsFunction() {
    return false;
  }

  @Override
  protected void customizeSparkConf(SparkConf sparkConf) {
    sparkConf.set(GravitinoSparkConfig.engineAccessModeConfig(getProvider()), "native");
  }

  @BeforeAll
  void initDefaultDatabase() {
    sql("USE " + getCatalogName());
    sql("CREATE DATABASE IF NOT EXISTS " + DATABASE);
  }

  @BeforeEach
  void init() {
    sql("USE " + getCatalogName());
    sql("USE " + DATABASE);
  }

  @AfterAll
  void cleanUp() throws IOException {
    if (getSparkSession() != null) {
      sql("USE " + getCatalogName());
      sql("DROP TABLE IF EXISTS " + DATABASE + ".native_table");
      sql("DROP DATABASE IF EXISTS " + DATABASE);
    }
  }

  @Test
  void testSparkUsesNativeIcebergCatalog() {
    CatalogPlugin catalogPlugin =
        getSparkSession().sessionState().catalogManager().catalog(getCatalogName());
    Assertions.assertInstanceOf(SparkCatalog.class, catalogPlugin);
  }

  @Test
  void testCreateAndQueryTableWithNativeIcebergRestCatalog() {
    sql("DROP TABLE IF EXISTS native_table");
    sql("CREATE TABLE native_table (id INT, name STRING) USING iceberg");
    sql("INSERT INTO native_table VALUES (1, 'a'), (2, 'b')");

    List<String> rows = getQueryData("SELECT id, name FROM native_table ORDER BY id");
    Assertions.assertEquals(2, rows.size());
    Assertions.assertEquals("1,a;2,b", String.join(";", rows));
  }
}
