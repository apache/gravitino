/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector.hive;

import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.spark.connector.GravitinoSparkConfig;
import com.datastrato.gravitino.spark.connector.PropertiesConverter;
import com.datastrato.gravitino.spark.connector.catalog.BaseCatalog;
import com.datastrato.gravitino.spark.connector.table.SparkBaseTable;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.kyuubi.spark.connector.hive.HiveTableCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class GravitinoHiveCatalog extends BaseCatalog {

  @Override
  protected boolean supportsBucketTransfrom() {
    return false;
  }

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
  protected SparkBaseTable createSparkTable(
      Identifier identifier,
      Table gravitinoTable,
      TableCatalog sparkCatalog,
      PropertiesConverter propertiesConverter) {
    return new SparkHiveTable(identifier, gravitinoTable, sparkCatalog, propertiesConverter);
  }

  @Override
  protected PropertiesConverter getPropertiesConverter() {
    return new HivePropertiesConverter();
  }
}
