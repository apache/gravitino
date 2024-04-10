/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector.iceberg;

import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.spark.connector.GravitinoCatalogAdaptor;
import com.datastrato.gravitino.spark.connector.PropertiesConverter;
import com.datastrato.gravitino.spark.connector.table.SparkBaseTable;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/** IcebergAdaptor provides specific operations for Iceberg Catalog to adapt to GravitinoCatalog. */
public class IcebergAdaptor implements GravitinoCatalogAdaptor {

  private void initHiveProperties(
      String catalogBackend,
      Map<String, String> gravitinoProperties,
      HashMap<String, String> icebergProperties) {
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
        IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_TYPE,
        catalogBackend.toLowerCase(Locale.ENGLISH));
    icebergProperties.put(IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_URI, metastoreUri);
    icebergProperties.put(
        IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_WAREHOUSE, hiveWarehouse);
  }

  private void initJdbcProperties(
      String catalogBackend,
      Map<String, String> gravitinoProperties,
      HashMap<String, String> icebergProperties) {
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
    String jdbcUser = gravitinoProperties.get(IcebergPropertiesConstants.GRAVITINO_JDBC_USER);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(jdbcUser),
        "Couldn't get "
            + IcebergPropertiesConstants.GRAVITINO_JDBC_USER
            + " from Iceberg Catalog properties");
    String jdbcPassword =
        gravitinoProperties.get(IcebergPropertiesConstants.GRAVITINO_JDBC_PASSWORD);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(jdbcPassword),
        "Couldn't get "
            + IcebergPropertiesConstants.GRAVITINO_JDBC_PASSWORD
            + " from Iceberg Catalog properties");
    String jdbcDriver =
        gravitinoProperties.get(IcebergPropertiesConstants.GRAVITINO_ICEBERG_JDBC_DRIVER);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(jdbcDriver),
        "Couldn't get "
            + IcebergPropertiesConstants.GRAVITINO_ICEBERG_JDBC_DRIVER
            + " from Iceberg Catalog properties");
    icebergProperties.put(
        IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_TYPE,
        catalogBackend.toLowerCase(Locale.ROOT));
    icebergProperties.put(IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_URI, jdbcUri);
    icebergProperties.put(
        IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_WAREHOUSE, jdbcWarehouse);
    icebergProperties.put(IcebergPropertiesConstants.GRAVITINO_ICEBERG_JDBC_USER, jdbcUser);
    icebergProperties.put(IcebergPropertiesConstants.GRAVITINO_ICEBERG_JDBC_PASSWORD, jdbcPassword);
    icebergProperties.put(IcebergPropertiesConstants.GRAVITINO_ICEBERG_JDBC_DRIVER, jdbcDriver);
  }

  @Override
  public PropertiesConverter getPropertiesConverter() {
    return new IcebergPropertiesConverter();
  }

  @Override
  public SparkBaseTable createSparkTable(
      Identifier identifier,
      Table gravitinoTable,
      TableCatalog sparkCatalog,
      PropertiesConverter propertiesConverter) {
    return new SparkIcebergTable(identifier, gravitinoTable, sparkCatalog, propertiesConverter);
  }

  @Override
  public TableCatalog createAndInitSparkCatalog(
      String name, CaseInsensitiveStringMap options, Map<String, String> properties) {
    Preconditions.checkArgument(
        properties != null, "Iceberg Catalog properties should not be null");

    String catalogBackend =
        properties.get(IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(catalogBackend), "Iceberg Catalog backend should not be empty.");

    HashMap<String, String> all = new HashMap<>(options);

    switch (catalogBackend.toLowerCase(Locale.ENGLISH)) {
      case IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND_HIVE:
        initHiveProperties(catalogBackend, properties, all);
        break;
      case IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND_JDBC:
        initJdbcProperties(catalogBackend, properties, all);
        break;
      default:
        // SparkCatalog does not support Memory type catalog
        throw new IllegalArgumentException(
            "Unsupported Iceberg Catalog backend: " + catalogBackend);
    }

    TableCatalog icebergCatalog = new SparkCatalog();
    icebergCatalog.initialize(name, new CaseInsensitiveStringMap(all));

    return icebergCatalog;
  }
}
