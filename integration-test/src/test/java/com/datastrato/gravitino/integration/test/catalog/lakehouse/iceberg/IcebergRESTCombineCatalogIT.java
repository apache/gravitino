/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.integration.test.catalog.lakehouse.iceberg;

import com.datastrato.gravitino.aux.AuxiliaryServiceManager;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergCatalogBackend;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergConfig;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergRESTService;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.combine.CombineCatalog;
import com.datastrato.gravitino.integration.test.container.ContainerSuite;
import com.datastrato.gravitino.integration.test.container.HiveContainer;
import com.datastrato.gravitino.integration.test.util.GravitinoITUtils;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

/**
 * IcebergRESTCombineCatalogIT starts Gravitino Iceberg server with combine catalog, in which the
 * primary catalog is JDBC catalog and the secondary catalog is Hive catalog. The Spark session
 * starts with two catalog, the first is rest catalog (refer to Gravitino Iceberg REST server) which
 * is used to run most tests, the second is a Hive catalog named `secondary` which is used to
 * double-check the table information.
 */
@Tag("gravitino-docker-it")
@TestInstance(Lifecycle.PER_CLASS)
public class IcebergRESTCombineCatalogIT extends IcebergRESTServiceIT {
  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();

  public IcebergRESTCombineCatalogIT() {
    catalogType = IcebergCatalogBackend.COMBINE;
  }

  @Override
  void initEnv() {
    containerSuite.startHiveContainer();
  }

  /** Get table info from the secondary catalog and check the value */
  @Override
  void doubleCheckTableInfo(
      String tableIdentifier, Map<String, String> contains, Set<String> notContains) {
    Map<String, String> secondaryTableInfo = getTableInfo("secondary." + tableIdentifier);
    checkMapContains(contains, secondaryTableInfo);

    Set<String> tmp = new HashSet<>(notContains);
    tmp.retainAll(secondaryTableInfo.keySet());
    Assertions.assertEquals(0, tmp.size());
  }

  @Override
  public Map<String, String> getCatalogConfig() {
    Map<String, String> configMap = new HashMap<>();

    configMap.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + IcebergRESTService.SERVICE_NAME
            + "."
            + IcebergConfig.CATALOG_BACKEND.getKey(),
        IcebergCatalogBackend.COMBINE.toString().toLowerCase());

    // primary catalog config
    configMap.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + IcebergRESTService.SERVICE_NAME
            + "."
            + CombineCatalog.PRIMARY
            + "."
            + IcebergConfig.CATALOG_BACKEND.getKey(),
        IcebergCatalogBackend.JDBC.toString().toLowerCase());

    configMap.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + IcebergRESTService.SERVICE_NAME
            + "."
            + CombineCatalog.PRIMARY
            + "."
            + IcebergConfig.CATALOG_URI.getKey(),
        "jdbc:sqlite::memory:");

    configMap.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + IcebergRESTService.SERVICE_NAME
            + "."
            + CombineCatalog.PRIMARY
            + "."
            + IcebergConfig.JDBC_DRIVER.getKey(),
        "org.sqlite.JDBC");

    configMap.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + IcebergRESTService.SERVICE_NAME
            + "."
            + CombineCatalog.PRIMARY
            + "."
            + IcebergConfig.JDBC_USER.getKey(),
        "iceberg");

    configMap.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + IcebergRESTService.SERVICE_NAME
            + "."
            + CombineCatalog.PRIMARY
            + "."
            + IcebergConfig.JDBC_PASSWORD.getKey(),
        "iceberg");

    configMap.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + IcebergRESTService.SERVICE_NAME
            + "."
            + CombineCatalog.PRIMARY
            + "."
            + IcebergConfig.JDBC_INIT_TABLES.getKey(),
        "true");

    configMap.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + IcebergRESTService.SERVICE_NAME
            + "."
            + CombineCatalog.PRIMARY
            + "."
            + IcebergConfig.CATALOG_WAREHOUSE.getKey(),
        GravitinoITUtils.genRandomName(
            String.format(
                "hdfs://%s:%d/user/hive/warehouse-jdbc-sqlite",
                containerSuite.getHiveContainer().getContainerIpAddress(),
                HiveContainer.HDFS_DEFAULTFS_PORT)));

    // secondary catalog config
    configMap.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + IcebergRESTService.SERVICE_NAME
            + "."
            + CombineCatalog.SECONDARY
            + "."
            + IcebergConfig.CATALOG_BACKEND.getKey(),
        IcebergCatalogBackend.HIVE.toString().toLowerCase());

    configMap.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + IcebergRESTService.SERVICE_NAME
            + "."
            + CombineCatalog.SECONDARY
            + "."
            + IcebergConfig.CATALOG_URI.getKey(),
        String.format(
            "thrift://%s:%d",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HIVE_METASTORE_PORT));

    configMap.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + IcebergRESTService.SERVICE_NAME
            + "."
            + CombineCatalog.SECONDARY
            + "."
            + IcebergConfig.CATALOG_WAREHOUSE.getKey(),
        GravitinoITUtils.genRandomName(
            String.format(
                "hdfs://%s:%d/user/hive/warehouse-hive",
                containerSuite.getHiveContainer().getContainerIpAddress(),
                HiveContainer.HDFS_DEFAULTFS_PORT)));

    return configMap;
  }
}
