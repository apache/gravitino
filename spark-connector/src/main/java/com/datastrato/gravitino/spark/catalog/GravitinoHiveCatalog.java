/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.catalog;

import com.datastrato.gravitino.spark.GravitinoSparkConfig;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import javax.ws.rs.NotSupportedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.kyuubi.spark.connector.hive.HiveTableCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/** GravitinoHiveCatalog represents a hive catalog in Gravitino */
public class GravitinoHiveCatalog extends BaseCatalog {

  @Override
  public Table createSparkTable(
      Identifier identifier, com.datastrato.gravitino.rel.Table gravitinoTable) {
    throw new NotSupportedException("Doesn't support creating spark hive table");
  }

  @Override
  public TableCatalog createAndInitSparkCatalog(String name, CaseInsensitiveStringMap options) {
    Preconditions.checkArgument(
        gravitinoCatalog.properties() != null, "Hive Catalog properties should not be null");
    String metastoreUri =
        gravitinoCatalog.properties().get(GravitinoSparkConfig.GRAVITINO_HIVE_METASTORE_URI);
    Preconditions.checkArgument(
        StringUtils.isNoneBlank(metastoreUri),
        "Couldn't get "
            + GravitinoSparkConfig.GRAVITINO_HIVE_METASTORE_URI
            + " from catalog properties");

    TableCatalog hiveCatalog = new HiveTableCatalog();
    HashMap all = new HashMap(options);
    all.put(GravitinoSparkConfig.SPARK_HIVE_METASTORE_URI, metastoreUri);
    hiveCatalog.initialize(name, new CaseInsensitiveStringMap(all));

    return hiveCatalog;
  }
}
