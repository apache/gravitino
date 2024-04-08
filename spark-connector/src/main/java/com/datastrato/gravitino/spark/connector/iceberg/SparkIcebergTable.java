/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector.iceberg;

import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.spark.connector.PropertiesConverter;
import com.datastrato.gravitino.spark.connector.table.SparkBaseTable;
import com.google.common.base.Preconditions;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsDelete;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.expressions.SortValue;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.RequiresDistributionAndOrdering;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.sources.Filter;

public class SparkIcebergTable extends SparkBaseTable implements SupportsDelete {

  public SparkIcebergTable(
      Identifier identifier,
      Table gravitinoTable,
      TableCatalog sparkIcebergCatalog,
      PropertiesConverter propertiesConverter) {
    super(identifier, gravitinoTable, sparkIcebergCatalog, propertiesConverter);
  }

  @Override
  public boolean canDeleteWhere(Filter[] filters) {
    return ((SupportsDelete) getSparkTable()).canDeleteWhere(filters);
  }

  @Override
  public void deleteWhere(Filter[] filters) {
    ((SupportsDelete) getSparkTable()).deleteWhere(filters);
  }

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    WriteBuilder writeBuilder = ((SupportsWrite) getSparkTable()).newWriteBuilder(info);
    RequiresDistributionAndOrdering write = (RequiresDistributionAndOrdering) writeBuilder.build();
    SortOrder[] sortOrders = write.requiredOrdering();
    Preconditions.checkArgument(
        sortOrders[0] instanceof SortValue,
        "The iceberg connector does not support custom sort orders. "
            + "Please use the default sort order provided by the iceberg connector.");
    return writeBuilder;
  }
}
