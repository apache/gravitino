/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.iceberg.integration.test;

import com.datastrato.gravitino.auxiliary.AuxiliaryServiceManager;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergCatalogBackend;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergConfig;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergRESTService;
import com.datastrato.gravitino.integration.test.container.ContainerSuite;
import com.datastrato.gravitino.integration.test.container.HiveContainer;
import com.datastrato.gravitino.integration.test.util.GravitinoITUtils;
import com.datastrato.gravitino.server.web.JettyServerConfig;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@Tag("gravitino-docker-it")
@TestInstance(Lifecycle.PER_CLASS)
public class IcebergRESTRESTCatalogHiveBackendIT extends IcebergRESTServiceIT {
  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();

  public IcebergRESTRESTCatalogHiveBackendIT() {
    catalogType = IcebergCatalogBackend.REST;
    backendCatalogType = IcebergCatalogBackend.HIVE;
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
        IcebergCatalogBackend.REST.toString().toLowerCase());

    customConfigs.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + IcebergRESTService.SERVICE_NAME
            + "."
            + IcebergConfig.CATALOG_URI.getKey(),
        String.format(
            "http://%s:%d/iceberg/",
            "127.0.0.1", JettyServerConfig.DEFAULT_ICEBERG_REST_SERVICE_HTTP_PORT));

    customConfigs.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + IcebergRESTService.SERVICE_NAME
            + "."
            + IcebergConfig.REAL_CATALOG_BACKEND.getKey(),
        IcebergCatalogBackend.HIVE.toString().toLowerCase());

    customConfigs.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + IcebergRESTService.SERVICE_NAME
            + "."
            + IcebergConfig.CATALOG_BACKEND_URI.getKey(),
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
