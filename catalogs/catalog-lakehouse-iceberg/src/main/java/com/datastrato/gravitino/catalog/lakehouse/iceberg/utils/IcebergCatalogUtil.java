/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.iceberg.utils;

import static com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergCatalogPropertiesMetadata.ICEBERG_JDBC_INITIALIZE;

import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergCatalogBackend;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergCatalogPropertiesMetadata;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergConfig;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.rest.RESTCatalog;
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

  private static Catalog loadRestCatalog(
      Map<String, String> properties, boolean buildForIcebergRestService) {
    IcebergConfig icebergConfig = new IcebergConfig(properties);
    if (buildForIcebergRestService) {
      Map<String, String> copiedProperties = new HashMap<>(properties);
      String realCatalogBackend = icebergConfig.get(IcebergConfig.REAL_CATALOG_BACKEND);
      Preconditions.checkArgument(
          StringUtils.isNotBlank(realCatalogBackend),
          "The real catalog backend of Iceberg RESTCatalog is not set.");
      String backend_catalog_uri = icebergConfig.get(IcebergConfig.CATALOG_BACKEND_URI);
      Preconditions.checkArgument(
          StringUtils.isNotBlank(backend_catalog_uri),
          "The backend catalog URI of Iceberg RESTCatalog is not set.");
      copiedProperties.put(IcebergCatalogPropertiesMetadata.URI, backend_catalog_uri);
      return loadCatalogBackend(realCatalogBackend, copiedProperties);
    } else {
      RESTCatalog restCatalog = new RESTCatalog();
      HdfsConfiguration hdfsConfiguration = new HdfsConfiguration();
      properties.forEach(hdfsConfiguration::set);
      restCatalog.setConf(hdfsConfiguration);
      restCatalog.initialize("rest", properties);
      return restCatalog;
    }
  }

  public static Catalog loadCatalogBackend(String catalogType, Map<String, String> properties) {
    return loadCatalogBackend(catalogType, properties, false);
  }

  public static Catalog loadCatalogBackend(
      String catalogType, Map<String, String> properties, boolean buildForIcebergRestService) {
    LOG.info("Load catalog backend of {}", catalogType);
    switch (IcebergCatalogBackend.valueOf(catalogType.toUpperCase())) {
      case MEMORY:
        return loadMemoryCatalog(properties);
      case HIVE:
        return loadHiveCatalog(properties);
      case JDBC:
        return loadJdbcCatalog(properties);
      case REST:
        return loadRestCatalog(properties, buildForIcebergRestService);
      default:
        throw new RuntimeException(
            catalogType
                + " catalog is not supported yet, supported catalogs: [memory]"
                + catalogType);
    }
  }

  private IcebergCatalogUtil() {}
}
