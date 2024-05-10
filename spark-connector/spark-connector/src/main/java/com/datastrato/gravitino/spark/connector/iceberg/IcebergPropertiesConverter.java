/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector.iceberg;

import com.datastrato.gravitino.spark.connector.PropertiesConverter;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/** Transform Iceberg catalog properties between Spark and Gravitino. */
public class IcebergPropertiesConverter implements PropertiesConverter {

  public static class IcebergPropertiesConverterHolder {
    private static final IcebergPropertiesConverter INSTANCE = new IcebergPropertiesConverter();
  }

  private IcebergPropertiesConverter() {}

  public static IcebergPropertiesConverter getInstance() {
    return IcebergPropertiesConverter.IcebergPropertiesConverterHolder.INSTANCE;
  }

  @Override
  public Map<String, String> toSparkCatalogProperties(Map<String, String> properties) {
    Preconditions.checkArgument(
        properties != null, "Iceberg Catalog properties should not be null");

    String catalogBackend =
        properties.get(IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(catalogBackend), "Iceberg Catalog backend should not be empty.");

    HashMap<String, String> all = new HashMap<>();

    switch (catalogBackend.toLowerCase(Locale.ROOT)) {
      case IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND_HIVE:
        initHiveProperties(properties, all);
        break;
      case IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND_JDBC:
        initJdbcProperties(properties, all);
        break;
      default:
        // SparkCatalog does not support Memory type catalog
        throw new IllegalArgumentException(
            "Unsupported Iceberg Catalog backend: " + catalogBackend);
    }
    return all;
  }

  @Override
  public Map<String, String> toGravitinoTableProperties(Map<String, String> properties) {
    return new HashMap<>(properties);
  }

  @Override
  public Map<String, String> toSparkTableProperties(Map<String, String> properties) {
    return new HashMap<>(properties);
  }

  private void initHiveProperties(
      Map<String, String> gravitinoProperties, HashMap<String, String> icebergProperties) {
    String metastoreUri =
        gravitinoProperties.get(IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_URI);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(metastoreUri),
        "Couldn't get "
            + IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_URI
            + " from Iceberg Catalog properties");
    String hiveWarehouse =
        gravitinoProperties.get(IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_WAREHOUSE);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(hiveWarehouse),
        "Couldn't get "
            + IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_WAREHOUSE
            + " from Iceberg Catalog properties");
    icebergProperties.put(
        IcebergPropertiesConstants.ICEBERG_CATALOG_TYPE,
        IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_HIVE);
    icebergProperties.put(IcebergPropertiesConstants.ICEBERG_CATALOG_URI, metastoreUri);
    icebergProperties.put(IcebergPropertiesConstants.ICEBERG_CATALOG_WAREHOUSE, hiveWarehouse);
  }

  private void initJdbcProperties(
      Map<String, String> gravitinoProperties, HashMap<String, String> icebergProperties) {
    String jdbcUri =
        gravitinoProperties.get(IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_URI);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(jdbcUri),
        "Couldn't get "
            + IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_URI
            + " from Iceberg Catalog properties");
    String jdbcWarehouse =
        gravitinoProperties.get(IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_WAREHOUSE);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(jdbcWarehouse),
        "Couldn't get "
            + IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_WAREHOUSE
            + " from Iceberg Catalog properties");
    String jdbcUser =
        gravitinoProperties.get(IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_JDBC_USER);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(jdbcUser),
        "Couldn't get "
            + IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_JDBC_USER
            + " from Iceberg Catalog properties");
    String jdbcPassword =
        gravitinoProperties.get(IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_JDBC_PASSWORD);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(jdbcPassword),
        "Couldn't get "
            + IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_JDBC_PASSWORD
            + " from Iceberg Catalog properties");
    icebergProperties.put(
        IcebergPropertiesConstants.ICEBERG_CATALOG_TYPE,
        IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_JDBC);
    icebergProperties.put(IcebergPropertiesConstants.ICEBERG_CATALOG_URI, jdbcUri);
    icebergProperties.put(IcebergPropertiesConstants.ICEBERG_CATALOG_WAREHOUSE, jdbcWarehouse);
    icebergProperties.put(IcebergPropertiesConstants.ICEBERG_CATALOG_JDBC_USER, jdbcUser);
    icebergProperties.put(IcebergPropertiesConstants.ICEBERG_CATALOG_JDBC_PASSWORD, jdbcPassword);
  }
}
