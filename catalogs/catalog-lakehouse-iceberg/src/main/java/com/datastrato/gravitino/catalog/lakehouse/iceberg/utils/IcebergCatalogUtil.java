/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.iceberg.utils;

import static com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergCatalogPropertiesMetadata.ICEBERG_JDBC_INITIALIZE;

import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergCatalogBackend;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergConfig;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.combine.CombineCatalog;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.combine.hive.CombineHiveCatalog;
import com.datastrato.gravitino.utils.MapUtils;
import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergCatalogUtil {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergCatalogUtil.class);

  private static InMemoryCatalog loadMemoryCatalog(Map<String, String> properties) {
    InMemoryCatalog memoryCatalog = new InMemoryCatalog();
    Map<String, String> resultProperties = new HashMap<>(properties);
    resultProperties.put(CatalogProperties.WAREHOUSE_LOCATION, "/tmp");
    memoryCatalog.initialize("memory", resultProperties);
    return memoryCatalog;
  }

  private static HiveCatalog loadHiveCatalog(Map<String, String> properties) {
    HiveCatalog hiveCatalog = new HiveCatalog();
    HdfsConfiguration hdfsConfiguration = new HdfsConfiguration();
    properties.forEach(hdfsConfiguration::set);
    hiveCatalog.setConf(hdfsConfiguration);
    hiveCatalog.initialize("hive", properties);
    return hiveCatalog;
  }

  private static HiveCatalog loadCombineHiveCatalog(Map<String, String> properties) {
    HiveCatalog hiveCatalog = new CombineHiveCatalog();
    HdfsConfiguration hdfsConfiguration = new HdfsConfiguration();
    properties.forEach(hdfsConfiguration::set);
    hiveCatalog.setConf(hdfsConfiguration);
    hiveCatalog.initialize("hive", properties);
    return hiveCatalog;
  }

  private static CombineCatalog loadCombineCatalog(Map<String, String> properties) {

    Map<String, String> primaryProperties = MapUtils.getPrefixMap(properties, "primary.");
    String primaryCatalogType = primaryProperties.get(IcebergConfig.CATALOG_BACKEND.getKey());
    Catalog primaryCatalog = loadCatalogBackend(primaryCatalogType, primaryProperties);

    Map<String, String> secondaryProperties = MapUtils.getPrefixMap(properties, "secondary.");
    String secondaryCatalogType = secondaryProperties.get(IcebergConfig.CATALOG_BACKEND.getKey());
    Preconditions.checkArgument(
        "hive".equalsIgnoreCase(secondaryCatalogType),
        "Only support Hive as secondary catalog backend");
    // todo support JDBC ?
    // Catalog secondaryCatalog = loadCatalogBackend(secondaryCatalogType, secondaryProperties);
    Catalog secondaryCatalog = loadCombineHiveCatalog(secondaryProperties);

    CombineCatalog combineCatalog =
        new CombineCatalog(
            (BaseMetastoreCatalog) primaryCatalog, (BaseMetastoreCatalog) secondaryCatalog);
    combineCatalog.initialize("combine", properties);
    return combineCatalog;
  }

  private static JdbcCatalog loadJdbcCatalog(Map<String, String> properties) {
    IcebergConfig icebergConfig = new IcebergConfig(properties);
    String driverClassName = icebergConfig.getJdbcDriver();

    icebergConfig.get(IcebergConfig.JDBC_USER);
    icebergConfig.get(IcebergConfig.JDBC_PASSWORD);

    try {
      // Load the jdbc driver
      Class.forName(driverClassName);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Couldn't load jdbc driver " + driverClassName);
    }
    JdbcCatalog jdbcCatalog =
        new JdbcCatalog(
            null,
            null,
            Boolean.parseBoolean(properties.getOrDefault(ICEBERG_JDBC_INITIALIZE, "true")));
    HdfsConfiguration hdfsConfiguration = new HdfsConfiguration();
    properties.forEach(hdfsConfiguration::set);
    jdbcCatalog.setConf(hdfsConfiguration);
    jdbcCatalog.initialize("jdbc", properties);
    return jdbcCatalog;
  }

  public static Catalog loadCatalogBackend(String catalogType) {
    return loadCatalogBackend(catalogType, Collections.emptyMap());
  }

  public static Catalog loadCatalogBackend(String catalogType, Map<String, String> properties) {
    LOG.info("Load catalog backend of {}", catalogType);
    switch (IcebergCatalogBackend.valueOf(catalogType.toUpperCase())) {
      case MEMORY:
        return loadMemoryCatalog(properties);
      case HIVE:
        return loadHiveCatalog(properties);
      case JDBC:
        return loadJdbcCatalog(properties);
      case COMBINE:
        return loadCombineCatalog(properties);
      default:
        throw new RuntimeException(
            catalogType
                + " catalog is not supported yet, supported catalogs: [memory]"
                + catalogType);
    }
  }
}
