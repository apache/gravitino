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
public class IcebergRESTRESTCatalogJdbcBackendIT extends IcebergRESTServiceIT {
  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();

  public IcebergRESTRESTCatalogJdbcBackendIT() {
    catalogType = IcebergCatalogBackend.REST;
    backendCatalogType = IcebergCatalogBackend.JDBC;
  }

  @Override
  void initEnv() {
    containerSuite.startHiveContainer();
  }

  public Map<String, String> getCatalogConfig() {
    Map<String, String> configMap = new HashMap<>();

    configMap.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + IcebergRESTService.SERVICE_NAME
            + "."
            + IcebergConfig.CATALOG_BACKEND.getKey(),
        IcebergCatalogBackend.REST.toString().toLowerCase());

    configMap.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + IcebergRESTService.SERVICE_NAME
            + "."
            + IcebergConfig.CATALOG_URI.getKey(),
        String.format(
            "http://%s:%d/iceberg/",
            "127.0.0.1", JettyServerConfig.DEFAULT_ICEBERG_REST_SERVICE_HTTP_PORT));

    configMap.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + IcebergRESTService.SERVICE_NAME
            + "."
            + IcebergConfig.REAL_CATALOG_BACKEND.getKey(),
        IcebergCatalogBackend.JDBC.toString().toLowerCase());

    configMap.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + IcebergRESTService.SERVICE_NAME
            + "."
            + IcebergConfig.JDBC_DRIVER.getKey(),
        "org.sqlite.JDBC");

    configMap.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + IcebergRESTService.SERVICE_NAME
            + "."
            + IcebergConfig.CATALOG_BACKEND_URI.getKey(),
        "jdbc:sqlite::memory:");

    configMap.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + IcebergRESTService.SERVICE_NAME
            + "."
            + IcebergConfig.JDBC_USER.getKey(),
        "iceberg");

    configMap.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + IcebergRESTService.SERVICE_NAME
            + "."
            + IcebergConfig.JDBC_PASSWORD.getKey(),
        "iceberg");

    configMap.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + IcebergRESTService.SERVICE_NAME
            + "."
            + IcebergConfig.JDBC_INIT_TABLES.getKey(),
        "true");

    configMap.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + IcebergRESTService.SERVICE_NAME
            + "."
            + IcebergConfig.CATALOG_WAREHOUSE.getKey(),
        GravitinoITUtils.genRandomName(
            String.format(
                "hdfs://%s:%d/user/hive/warehouse-jdbc-sqlite",
                containerSuite.getHiveContainer().getContainerIpAddress(),
                HiveContainer.HDFS_DEFAULTFS_PORT)));
    return configMap;
  }
}
