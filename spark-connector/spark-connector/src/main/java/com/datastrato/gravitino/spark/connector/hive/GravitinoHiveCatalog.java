/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector.hive;

import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.spark.connector.GravitinoSparkConfig;
import com.datastrato.gravitino.spark.connector.PropertiesConverter;
import com.datastrato.gravitino.spark.connector.SparkTransformConverter;
import com.datastrato.gravitino.spark.connector.catalog.BaseCatalog;
import java.util.Map;
import org.apache.kyuubi.spark.connector.hive.HiveTable;
import org.apache.kyuubi.spark.connector.hive.HiveTableCatalog;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class GravitinoHiveCatalog extends BaseCatalog {

  @Override
  protected TableCatalog createAndInitSparkCatalog(
      String name, CaseInsensitiveStringMap options, Map<String, String> properties) {
    Preconditions.checkArgument(properties != null, "Hive Catalog properties should not be null");
    String metastoreUri = properties.get(GravitinoSparkConfig.GRAVITINO_HIVE_METASTORE_URI);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(metastoreUri),
        "Couldn't get "
            + GravitinoSparkConfig.GRAVITINO_HIVE_METASTORE_URI
            + " from hive catalog properties");

    TableCatalog hiveCatalog = new HiveTableCatalog();
    HashMap<String, String> all = new HashMap<>(options);
    all.put(GravitinoSparkConfig.SPARK_HIVE_METASTORE_URI, metastoreUri);
    hiveCatalog.initialize(name, new CaseInsensitiveStringMap(all));

    return hiveCatalog;
  }

  @Override
  protected org.apache.spark.sql.connector.catalog.Table createSparkTable(
      Identifier identifier,
      Table gravitinoTable,
      TableCatalog sparkHiveCatalog,
      PropertiesConverter propertiesConverter,
      SparkTransformConverter sparkTransformConverter) {
    org.apache.spark.sql.connector.catalog.Table sparkTable;
    try {
      sparkTable = sparkHiveCatalog.loadTable(identifier);
    } catch (NoSuchTableException e) {
      throw new RuntimeException(
          String.format(
              "Failed to load the real sparkTable: %s",
              String.join(".", getDatabase(identifier), identifier.name())),
          e);
    }
    return new SparkHiveTable(
        identifier,
        gravitinoTable,
        (HiveTable) sparkTable,
        (HiveTableCatalog) sparkHiveCatalog,
        propertiesConverter,
        sparkTransformConverter);
  }

  @Override
  protected PropertiesConverter getPropertiesConverter() {
    return new HivePropertiesConverter();
  }

  @Override
  protected SparkTransformConverter getSparkTransformConverter() {
    return new SparkTransformConverter(false);
  }
}
