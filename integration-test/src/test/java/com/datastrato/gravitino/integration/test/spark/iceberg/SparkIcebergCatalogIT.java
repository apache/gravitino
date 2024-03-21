/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.spark.iceberg;

import com.datastrato.gravitino.integration.test.spark.SparkCommonIT;
import com.datastrato.gravitino.spark.connector.iceberg.SparkIcebergTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.catalyst.analysis.ResolvedTable;
import org.apache.spark.sql.catalyst.plans.logical.CommandResult;
import org.apache.spark.sql.catalyst.plans.logical.DescribeRelation;
import org.apache.spark.sql.connector.catalog.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@Tag("gravitino-docker-it")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SparkIcebergCatalogIT extends SparkCommonIT {

  @Override
  protected String getCatalogName() {
    return "iceberg";
  }

  @Override
  protected String getProvider() {
    return "lakehouse-iceberg";
  }

  // TODO
  @Test
  void testMetadataColumns() {
    String tableName = "test_metadata_columns";
    dropTableIfExists(tableName);
    createSimpleTable(tableName);

    Dataset ds = getSparkSession().sql("DESC TABLE EXTENDED " + tableName);
    CommandResult result = (CommandResult) ds.logicalPlan();
    DescribeRelation relation = (DescribeRelation) result.commandLogicalPlan();
    ResolvedTable table = (ResolvedTable) relation.child();
    Table table1 = table.table();
    Assertions.assertTrue(table1 instanceof SparkIcebergTable);
    SparkIcebergTable icebergTable = (SparkIcebergTable) table1;
  }
}
