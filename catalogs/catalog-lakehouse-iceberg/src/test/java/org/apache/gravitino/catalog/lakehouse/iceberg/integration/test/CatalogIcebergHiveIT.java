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
package org.apache.gravitino.catalog.lakehouse.iceberg.integration.test;

import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-test")
public class CatalogIcebergHiveIT extends CatalogIcebergBaseIT {

  @Override
  protected void initIcebergCatalogProperties() {
    URIS =
        String.format(
            "thrift://%s:%d",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HIVE_METASTORE_PORT);
    TYPE = "hive";
    WAREHOUSE =
        String.format(
            "hdfs://%s:%d/user/hive/warehouse-catalog-iceberg/",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HDFS_DEFAULTFS_PORT);
  }

  @Test
  void testAlterCatalogProperties() {
    Map<String, String> catalogProperties = Maps.newHashMap();
    catalogProperties.put("key1", "val1");
    catalogProperties.put("key2", "val2");
    String icebergCatalogBackendName = "iceberg-catalog-name-test";

    String wrongURIS = URIS + "wrong";
    catalogProperties.put(IcebergConfig.CATALOG_BACKEND.getKey(), TYPE);
    catalogProperties.put(IcebergConfig.CATALOG_URI.getKey(), wrongURIS);
    catalogProperties.put(IcebergConfig.CATALOG_WAREHOUSE.getKey(), WAREHOUSE);
    catalogProperties.put(IcebergConfig.CATALOG_BACKEND_NAME.getKey(), icebergCatalogBackendName);

    String catalogNm = GravitinoITUtils.genRandomName("iceberg_it_catalog");
    Catalog createdCatalog =
        metalake.createCatalog(
            catalogNm, Catalog.Type.RELATIONAL, "lakehouse-iceberg", "comment", catalogProperties);
    Catalog loadCatalog = metalake.loadCatalog(catalogNm);
    Assertions.assertEquals(createdCatalog, loadCatalog);

    Assertions.assertThrows(
        Exception.class, () -> createdCatalog.asSchemas().createSchema("schema1", "", null));

    metalake.alterCatalog(
        catalogNm, CatalogChange.setProperty(IcebergConfig.CATALOG_URI.getKey(), URIS));

    Assertions.assertDoesNotThrow(
        () -> createdCatalog.asSchemas().createSchema("schema1", "", null));

    createdCatalog.asSchemas().dropSchema("schema1", false);
    metalake.dropCatalog(catalogNm, true);
  }
}
