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
import java.util.Map;
import org.apache.gravitino.spark.connector.iceberg.GravitinoIcebergCatalog;
import org.apache.gravitino.spark.connector.iceberg.IcebergPropertiesConstants;
import org.apache.iceberg.catalog.Catalog;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;

/**
 * This class configures the Gravitino Iceberg catalog with a Hive backend, but runs the Iceberg
 * REST auxiliary service in {@code dynamic-config-provider} mode so the Spark connector
 * auto-discovers it and routes the catalog through the Iceberg REST protocol instead of talking to
 * the Hive metastore directly. All test cases inherited from {@link SparkIcebergCatalogIT} exercise
 * this REST-routed path end to end.
 */
@Tag("gravitino-docker-test")
// Spark connector uses a low Iceberg version, couldn't work with Iceberg REST server with high
// Iceberg version in embedded mode.
@DisabledIf("org.apache.gravitino.integration.test.util.ITUtils#isEmbedded")
public abstract class SparkIcebergCatalogRestRoutingIT extends SparkIcebergCatalogIT {

  @Override
  protected boolean useDynamicIcebergRestConfigProvider() {
    return true;
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
    return catalogProperties;
  }

  @Test
  void testCatalogIsRoutedThroughIcebergRestServer() {
    CatalogPlugin catalogPlugin =
        getSparkSession().sessionState().catalogManager().catalog(getCatalogName());
    Assertions.assertInstanceOf(GravitinoIcebergCatalog.class, catalogPlugin);

    Catalog icebergCatalog = ((GravitinoIcebergCatalog) catalogPlugin).icebergCatalog();
    Assertions.assertEquals(
        "org.apache.iceberg.rest.RESTCatalog",
        icebergCatalog.getClass().getName(),
        "The hive-backed catalog should have been routed through the discovered Iceberg REST "
            + "server, instead of Iceberg's native HiveCatalog.");
  }
}
