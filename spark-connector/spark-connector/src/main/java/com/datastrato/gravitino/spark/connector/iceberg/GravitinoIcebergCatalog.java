/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector.iceberg;

import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.spark.connector.PropertiesConverter;
import com.datastrato.gravitino.spark.connector.SparkTransformConverter;
import com.datastrato.gravitino.spark.connector.catalog.BaseCatalog;
import com.datastrato.gravitino.spark.connector.table.SparkBaseTable;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.spark.sql.catalyst.analysis.NoSuchFunctionException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.connector.catalog.FunctionCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * The GravitinoIcebergCatalog class extends the BaseCatalog to integrate with the Iceberg table
 * format, providing specialized support for Iceberg-specific functionalities within Spark's
 * ecosystem. This implementation can further adapt to specific interfaces such as
 * StagingTableCatalog and FunctionCatalog, allowing for advanced operations like table staging and
 * function management tailored to the needs of Iceberg tables.
 */
public class GravitinoIcebergCatalog extends BaseCatalog implements FunctionCatalog {

  @Override
  protected boolean supportsBucketTransfrom() {
    return true;
  }

  @Override
  protected TableCatalog createAndInitSparkCatalog(
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

  @Override
  protected SparkBaseTable createSparkTable(
      Identifier identifier,
      Table gravitinoTable,
      TableCatalog sparkCatalog,
      PropertiesConverter propertiesConverter,
      SparkTransformConverter sparkTransformConverter) {
    return new SparkIcebergTable(
        identifier, gravitinoTable, sparkCatalog, propertiesConverter, sparkTransformConverter);
  }

  @Override
  protected PropertiesConverter getPropertiesConverter() {
    return new IcebergPropertiesConverter();
  }

  @Override
  protected SparkTransformConverter getSparkTransformConverter() {
    return new SparkTransformConverter(true);
  }

  @Override
  public Identifier[] listFunctions(String[] namespace) throws NoSuchNamespaceException {
    return ((SparkCatalog) sparkCatalog).listFunctions(namespace);
  }

  @Override
  public UnboundFunction loadFunction(Identifier ident) throws NoSuchFunctionException {
    return ((SparkCatalog) sparkCatalog).loadFunction(ident);
  }

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
}
