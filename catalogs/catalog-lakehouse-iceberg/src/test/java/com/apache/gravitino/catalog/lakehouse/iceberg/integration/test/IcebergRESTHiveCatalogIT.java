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
package com.apache.gravitino.catalog.lakehouse.iceberg.integration.test;

import com.apache.gravitino.auxiliary.AuxiliaryServiceManager;
import com.apache.gravitino.catalog.lakehouse.iceberg.IcebergCatalogBackend;
import com.apache.gravitino.catalog.lakehouse.iceberg.IcebergConfig;
import com.apache.gravitino.catalog.lakehouse.iceberg.IcebergRESTService;
import com.apache.gravitino.integration.test.container.ContainerSuite;
import com.apache.gravitino.integration.test.container.HiveContainer;
import com.apache.gravitino.integration.test.util.GravitinoITUtils;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

// Hive&Jdbc catalog must be tested with gravitino-docker-test env,
// so we should create a separate class instead using junit `parameterized test`
// to auto-generate catalog type
@Tag("gravitino-docker-test")
@TestInstance(Lifecycle.PER_CLASS)
public class IcebergRESTHiveCatalogIT extends IcebergRESTServiceIT {
  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();

  public IcebergRESTHiveCatalogIT() {
    catalogType = IcebergCatalogBackend.HIVE;
  }

  @Override
  void initEnv() {
    containerSuite.startHiveContainer();
  }

  @Override
  Map<String, String> getCatalogConfig() {
    Map<String, String> customConfigs = new HashMap<>();
    customConfigs.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + IcebergRESTService.SERVICE_NAME
            + "."
            + IcebergConfig.CATALOG_BACKEND.getKey(),
        IcebergCatalogBackend.HIVE.toString().toLowerCase());

    customConfigs.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + IcebergRESTService.SERVICE_NAME
            + "."
            + IcebergConfig.CATALOG_URI.getKey(),
        String.format(
            "thrift://%s:%d",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HIVE_METASTORE_PORT));

    customConfigs.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + IcebergRESTService.SERVICE_NAME
            + "."
            + IcebergConfig.CATALOG_WAREHOUSE.getKey(),
        GravitinoITUtils.genRandomName(
            String.format(
                "hdfs://%s:%d/user/hive/warehouse-hive",
                containerSuite.getHiveContainer().getContainerIpAddress(),
                HiveContainer.HDFS_DEFAULTFS_PORT)));
    return customConfigs;
  }
}
