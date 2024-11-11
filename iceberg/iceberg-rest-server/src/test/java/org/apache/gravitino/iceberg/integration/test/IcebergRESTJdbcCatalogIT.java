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

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.common.IcebergCatalogBackend;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@Tag("gravitino-docker-test")
@TestInstance(Lifecycle.PER_CLASS)
public class IcebergRESTJdbcCatalogIT extends IcebergRESTServiceIT {

  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  private boolean hiveStarted = false;

  public IcebergRESTJdbcCatalogIT() {
    catalogType = IcebergCatalogBackend.JDBC;
  }

  @Override
  void initEnv() {
    containerSuite.startHiveContainer();
    hiveStarted = true;
  }

  @Override
  public Map<String, String> getCatalogConfig() {
    return getCatalogJdbcConfig();
  }

  protected Map<String, String> getCatalogJdbcConfig() {
    Map<String, String> configMap = new HashMap<>();

    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + IcebergConfig.CATALOG_BACKEND.getKey(),
        IcebergCatalogBackend.JDBC.toString().toLowerCase());

    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + IcebergConfig.JDBC_DRIVER.getKey(),
        "org.sqlite.JDBC");

    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + IcebergConfig.CATALOG_URI.getKey(),
        "jdbc:sqlite::memory:");

    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + IcebergConstants.ICEBERG_JDBC_USER, "iceberg");

    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + IcebergConstants.ICEBERG_JDBC_PASSWORD, "iceberg");

    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + IcebergConfig.JDBC_INIT_TABLES.getKey(), "true");

    configMap.put(IcebergConfig.ICEBERG_CONFIG_PREFIX + "jdbc.schema-version", "V1");

    if (hiveStarted) {
      configMap.put(
          IcebergConfig.ICEBERG_CONFIG_PREFIX + IcebergConfig.CATALOG_WAREHOUSE.getKey(),
          GravitinoITUtils.genRandomName(
              String.format(
                  "hdfs://%s:%d/user/hive/warehouse-jdbc-sqlite",
                  containerSuite.getHiveContainer().getContainerIpAddress(),
                  HiveContainer.HDFS_DEFAULTFS_PORT)));
    }
    return configMap;
  }
}
