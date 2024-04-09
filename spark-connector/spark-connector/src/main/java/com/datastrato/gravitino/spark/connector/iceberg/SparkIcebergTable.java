/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector.iceberg;

import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.spark.connector.PropertiesConverter;
import com.datastrato.gravitino.spark.connector.table.SparkBaseTable;
import com.google.common.base.Preconditions;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsDelete;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.RequiresDistributionAndOrdering;
import org.apache.spark.sql.connector.write.Write;
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
    SparkTable sparkTable = (SparkTable) getSparkTable();
    WriteBuilder writeBuilder = sparkTable.newWriteBuilder(info);
    Write write = writeBuilder.build();
    RequiresDistributionAndOrdering distributionAndOrdering =
        (RequiresDistributionAndOrdering) write;
    SortOrder[] sortOrders = distributionAndOrdering.requiredOrdering();
    Preconditions.checkArgument(sortOrders.length == 1, "Only one sort order is supported");
    Preconditions.checkArgument(sortOrders[0].expression() != null, "Sort order should have child");
    return writeBuilder;
  }
}
