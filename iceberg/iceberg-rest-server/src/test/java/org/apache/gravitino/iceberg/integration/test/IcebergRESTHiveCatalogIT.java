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
package org.apache.gravitino.iceberg.integration.test;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergCatalogBackend;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.rest.RESTCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@Tag("gravitino-docker-test")
@TestInstance(Lifecycle.PER_CLASS)
public class IcebergRESTHiveCatalogIT extends IcebergRESTServiceIT {
  protected static final ContainerSuite containerSuite = ContainerSuite.getInstance();

  @Override
  protected boolean supportsNestedNamespaces() {
    return false;
  }

  public IcebergRESTHiveCatalogIT() {
    catalogType = IcebergCatalogBackend.HIVE;
  }

  @Override
  void initEnv() {
    containerSuite.startHiveContainer();
  }

  @Override
  Map<String, String> getCatalogConfig() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + IcebergConfig.CATALOG_BACKEND.getKey(),
        IcebergCatalogBackend.HIVE.toString().toLowerCase());

    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + IcebergConfig.CATALOG_URI.getKey(),
        String.format(
            "thrift://%s:%d",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HIVE_METASTORE_PORT));

    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + IcebergConfig.CATALOG_WAREHOUSE.getKey(),
        GravitinoITUtils.genRandomName(
            String.format(
                "hdfs://%s:%d/user/hive/warehouse-hive",
                containerSuite.getHiveContainer().getContainerIpAddress(),
                HiveContainer.HDFS_DEFAULTFS_PORT)));
    return configMap;
  }

  @Test
  void testRegisterTableOverwrite() throws Exception {
    String namespace = getTestNamespace();
    sql(
        "CREATE TABLE %s.register_overwrite_t (id bigint COMMENT 'unique id', data string) USING iceberg",
        namespace);
    sql("ALTER TABLE %s.register_overwrite_t SET TBLPROPERTIES ('version' = 'v2')", namespace);

    List<String> metadataLocations =
        convertToStringList(
            sql("SELECT file FROM %s.register_overwrite_t.metadata_log_entries", namespace), 0);
    Assertions.assertTrue(metadataLocations.size() >= 2);
    String metadataV1 = metadataLocations.get(0);
    String metadataV2 = metadataLocations.get(metadataLocations.size() - 1);

    Namespace icebergNamespace = Namespace.of(namespace);
    TableIdentifier tableIdent = TableIdentifier.of(icebergNamespace, "register_overwrite_t");

    try (RESTCatalog catalog = new RESTCatalog()) {
      catalog.initialize(
          "rest",
          ImmutableMap.of(
              CatalogProperties.URI,
              String.format("http://127.0.0.1:%d/iceberg/", getServerPort())));
      catalog.registerTable(tableIdent, metadataV1, true);
      Table table = catalog.loadTable(tableIdent);
      Assertions.assertFalse(table.properties().containsKey("version"));

      catalog.registerTable(tableIdent, metadataV2, true);
      table = catalog.loadTable(tableIdent);
      Assertions.assertEquals("v2", table.properties().get("version"));

      Assertions.assertThrows(
          AlreadyExistsException.class, () -> catalog.registerTable(tableIdent, metadataV2, false));
    }
  }
}
