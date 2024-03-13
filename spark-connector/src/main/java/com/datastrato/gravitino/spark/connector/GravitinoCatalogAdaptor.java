/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector;

import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.spark.connector.table.SparkBaseTable;
import java.util.Map;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * GravitinoCatalogAdaptor provides a unified interface for different catalogs to adapt to
 * GravitinoCatalog.
 */
public interface GravitinoCatalogAdaptor {

  /**
   * Get a PropertiesConverter to transform properties between Gravitino and Spark.
   *
   * @return an PropertiesConverter
   */
  PropertiesConverter getPropertiesConverter();

  /**
   * Create a specific Spark table, combined with gravitinoTable to do DML operations and
   * sparkCatalog to do IO operations.
   *
   * @param identifier Spark's table identifier
   * @param gravitinoTable Gravitino table to do DDL operations
   * @param sparkCatalog specific Spark catalog to do IO operations
   * @param propertiesConverter transform properties between Gravitino and Spark
   * @return a specific Spark table
   */
  SparkBaseTable createSparkTable(
      Identifier identifier,
      Table gravitinoTable,
      TableCatalog sparkCatalog,
      PropertiesConverter propertiesConverter);

  /**
   * Create a specific Spark catalog, mainly used to create Spark table.
   *
   * @param name catalog name
   * @param options catalog options from configuration
   * @param properties catalog properties from Gravitino
   * @return a specific Spark catalog
   */
  TableCatalog createAndInitSparkCatalog(
      String name, CaseInsensitiveStringMap options, Map<String, String> properties);
}
