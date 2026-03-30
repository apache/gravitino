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

package org.apache.gravitino.flink.connector.integration.test.iceberg;

import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import org.apache.flink.table.api.TableResult;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.flink.connector.iceberg.GravitinoIcebergCatalog;
import org.apache.gravitino.flink.connector.iceberg.GravitinoIcebergCatalogFactoryOptions;
import org.apache.gravitino.flink.connector.iceberg.IcebergPropertiesConstants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FlinkIcebergRestCatalogIT119 extends FlinkIcebergRestCatalogIT {

  @Override
  @Test
  public void testCreateGravitinoIcebergUsingSQL() {
    tableEnv.useCatalog(DEFAULT_CATALOG);
    int numCatalogs = tableEnv.listCatalogs().length;

    String catalogName = "gravitino_iceberg_using_sql";
    tableEnv.executeSql(
        String.format(
            "create catalog %s with ("
                + "'type'='%s', "
                + "'catalog-backend'='%s',"
                + "'uri'='%s'"
                + ")",
            catalogName,
            GravitinoIcebergCatalogFactoryOptions.IDENTIFIER,
            getCatalogBackend(),
            getUri()));
    Assertions.assertTrue(metalake.catalogExists(catalogName));

    Catalog gravitinoCatalog = metalake.loadCatalog(catalogName);
    Map<String, String> properties = gravitinoCatalog.properties();
    Assertions.assertEquals(
        getUri(), properties.get(IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_URI));
    Assertions.assertFalse(
        properties.containsKey(IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_WAREHOUSE));

    Optional<org.apache.flink.table.catalog.Catalog> catalog = tableEnv.getCatalog(catalogName);
    Assertions.assertTrue(catalog.isPresent());
    Assertions.assertInstanceOf(GravitinoIcebergCatalog.class, catalog.get());

    String[] catalogs = tableEnv.listCatalogs();
    Assertions.assertEquals(numCatalogs + 1, catalogs.length, "Should create a new catalog");
    Assertions.assertTrue(
        Arrays.asList(catalogs).contains(catalogName), "Should create the correct catalog.");

    TableResult result = tableEnv.executeSql("show catalogs");
    Assertions.assertEquals(
        numCatalogs + 1, Lists.newArrayList(result.collect()).size(), "Should have 2 catalogs");

    Assertions.assertEquals(
        DEFAULT_CATALOG,
        tableEnv.getCurrentCatalog(),
        "Current catalog should be default_catalog in flink");

    tableEnv.useCatalog(catalogName);
    Assertions.assertEquals(
        catalogName,
        tableEnv.getCurrentCatalog(),
        "Current catalog should be the one that is created just now.");

    tableEnv.useCatalog(DEFAULT_CATALOG);
    tableEnv.executeSql("drop catalog " + catalogName);
    Assertions.assertFalse(metalake.catalogExists(catalogName));

    Optional<org.apache.flink.table.catalog.Catalog> droppedCatalog =
        tableEnv.getCatalog(catalogName);
    Assertions.assertFalse(droppedCatalog.isPresent(), "Catalog should be dropped");
  }
}
