/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector.iceberg;

import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.spark.connector.GravitinoCatalogAdaptor;
import com.datastrato.gravitino.spark.connector.GravitinoSparkConfig;
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
        gravitinoProperties.get(GravitinoSparkConfig.LAKEHOUSE_ICEBERG_CATALOG_URI);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(metastoreUri),
        "Couldn't get "
            + GravitinoSparkConfig.LAKEHOUSE_ICEBERG_CATALOG_URI
            + " from iceberg catalog properties");
    String hiveWarehouse =
        gravitinoProperties.get(GravitinoSparkConfig.LAKEHOUSE_ICEBERG_CATALOG_WAREHOUSE);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(hiveWarehouse),
        "Couldn't get "
            + GravitinoSparkConfig.LAKEHOUSE_ICEBERG_CATALOG_WAREHOUSE
            + " from iceberg catalog properties");
    icebergProperties.put(
        GravitinoSparkConfig.LAKEHOUSE_ICEBERG_CATALOG_TYPE,
        catalogBackend.toLowerCase(Locale.ENGLISH));
    icebergProperties.put(GravitinoSparkConfig.LAKEHOUSE_ICEBERG_CATALOG_URI, metastoreUri);
    icebergProperties.put(GravitinoSparkConfig.LAKEHOUSE_ICEBERG_CATALOG_WAREHOUSE, hiveWarehouse);
  }

  private void initJdbcProperties(
      String catalogBackend,
      Map<String, String> gravitinoProperties,
      HashMap<String, String> icebergProperties) {
    String jdbcUri = gravitinoProperties.get(GravitinoSparkConfig.LAKEHOUSE_ICEBERG_CATALOG_URI);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(jdbcUri),
        "Couldn't get "
            + GravitinoSparkConfig.LAKEHOUSE_ICEBERG_CATALOG_URI
            + " from iceberg catalog properties");
    String jdbcWarehouse =
        gravitinoProperties.get(GravitinoSparkConfig.LAKEHOUSE_ICEBERG_CATALOG_WAREHOUSE);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(jdbcWarehouse),
        "Couldn't get "
            + GravitinoSparkConfig.LAKEHOUSE_ICEBERG_CATALOG_WAREHOUSE
            + " from iceberg catalog properties");
    String jdbcUser = gravitinoProperties.get(GravitinoSparkConfig.GRAVITINO_JDBC_USER);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(jdbcUser),
        "Couldn't get "
            + GravitinoSparkConfig.GRAVITINO_JDBC_USER
            + " from iceberg catalog properties");
    String jdbcPasswrod = gravitinoProperties.get(GravitinoSparkConfig.GRAVITINO_JDBC_PASSWORD);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(jdbcPasswrod),
        "Couldn't get "
            + GravitinoSparkConfig.GRAVITINO_JDBC_PASSWORD
            + " from iceberg catalog properties");
    String jdbcDriver =
        gravitinoProperties.get(GravitinoSparkConfig.LAKEHOUSE_ICEBERG_CATALOG_JDBC_DRIVER);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(jdbcDriver),
        "Couldn't get "
            + GravitinoSparkConfig.LAKEHOUSE_ICEBERG_CATALOG_JDBC_DRIVER
            + " from iceberg catalog properties");
    icebergProperties.put(
        GravitinoSparkConfig.LAKEHOUSE_ICEBERG_CATALOG_TYPE,
        catalogBackend.toLowerCase(Locale.ENGLISH));
    icebergProperties.put(GravitinoSparkConfig.LAKEHOUSE_ICEBERG_CATALOG_URI, jdbcUri);
    icebergProperties.put(GravitinoSparkConfig.LAKEHOUSE_ICEBERG_CATALOG_WAREHOUSE, jdbcWarehouse);
    icebergProperties.put(GravitinoSparkConfig.LAKEHOUSE_ICEBERG_CATALOG_JDBC_USER, jdbcUser);
    icebergProperties.put(
        GravitinoSparkConfig.LAKEHOUSE_ICEBERG_CATALOG_JDBC_PASSWORD, jdbcPasswrod);
    icebergProperties.put(GravitinoSparkConfig.LAKEHOUSE_ICEBERG_CATALOG_JDBC_DRIVER, jdbcDriver);
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

    String catalogBackend = properties.get(GravitinoSparkConfig.LAKEHOUSE_ICEBERG_CATALOG_BACKEND);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(catalogBackend), "Iceberg Catalog backend should not be empty.");

    HashMap<String, String> all = new HashMap<>(options);

    switch (catalogBackend.toLowerCase(Locale.ENGLISH)) {
      case GravitinoSparkConfig.LAKEHOUSE_ICEBERG_CATALOG_BACKEND_HIVE:
        initHiveProperties(catalogBackend, properties, all);
        break;
      case GravitinoSparkConfig.LAKEHOUSE_ICEBERG_CATALOG_BACKEND_JDBC:
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
