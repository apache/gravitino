/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector.hive;

import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.spark.connector.PropertiesConverter;
import com.datastrato.gravitino.spark.connector.SparkTransformConverter;
import com.datastrato.gravitino.spark.connector.catalog.BaseCatalog;
import com.datastrato.gravitino.spark.connector.table.SparkBaseTable;
import java.util.Map;
import org.apache.kyuubi.spark.connector.hive.HiveTableCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class GravitinoHiveCatalog extends BaseCatalog {

  @Override
  protected TableCatalog createAndInitSparkCatalog(
      String name, CaseInsensitiveStringMap options, Map<String, String> properties) {
    TableCatalog hiveCatalog = new HiveTableCatalog();
    Map<String, String> all =
        HivePropertiesConverter.getInstance().toSparkCatalogProperties(options, properties);
    hiveCatalog.initialize(name, new CaseInsensitiveStringMap(all));

    return hiveCatalog;
  }

  @Override
  protected SparkBaseTable createSparkTable(
      Identifier identifier,
      Table gravitinoTable,
      TableCatalog sparkCatalog,
      PropertiesConverter propertiesConverter,
      SparkTransformConverter sparkTransformConverter) {
    return new SparkHiveTable(
        identifier, gravitinoTable, sparkCatalog, propertiesConverter, sparkTransformConverter);
  }

  @Override
  protected PropertiesConverter getPropertiesConverter() {
    return HivePropertiesConverter.getInstance();
  }

  @Override
  protected SparkTransformConverter getSparkTransformConverter() {
    return new SparkTransformConverter(false);
  }
}
