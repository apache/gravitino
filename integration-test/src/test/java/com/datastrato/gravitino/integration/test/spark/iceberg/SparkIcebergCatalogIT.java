/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.spark.iceberg;

import com.datastrato.gravitino.integration.test.spark.SparkCommonIT;
import com.datastrato.gravitino.integration.test.util.spark.SparkTableInfo;
import java.util.List;
import java.util.Map;

import com.datastrato.gravitino.spark.connector.iceberg.IcebergPropertiesConstants;
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

  @Override
  protected boolean supportsSparkSQLClusteredBy() {
    return false;
  }

  @Override
  protected boolean supportsPartition() {
    return false;
  }

  @Test
  void testIcebergFileLevelDeleteOperation() {
    String tableName = "test_delete_table";
    dropTableIfExists(tableName);
    createSimpleTable(tableName);

    SparkTableInfo table = getTableInfo(tableName);
    checkTableColumns(tableName, getSimpleTableColumn(), table);
    sql(
        String.format(
            "INSERT INTO %s VALUES (1, '1', 1),(2, '2', 2),(3, '3', 3),(4, '4', 4),(5, '5', 5)",
            tableName));
    List<String> queryResult1 = getTableData(tableName);
    Assertions.assertEquals(5, queryResult1.size());
    Assertions.assertEquals("1,1,1;2,2,2;3,3,3;4,4,4;5,5,5", String.join(";", queryResult1));
    sql(getDeleteSql(tableName, "id < 10"));
    List<String> queryResult2 = getTableData(tableName);
    Assertions.assertEquals(0, queryResult2.size());
  }

  @Test
  void testIcebergTableReservedPropertiesWhenLoad() {
    String tableName = "test_iceberg_table_loaded_properties";
    dropTableIfExists(tableName);
    createSimpleTable(tableName);
    SparkTableInfo table = getTableInfo(tableName);
    checkTableColumns(tableName, getSimpleTableColumn(), table);
    Map<String, String> tableProperties = table.getTableProperties();
    Assertions.assertNotNull(tableProperties);
    Assertions.assertEquals("iceberg/parquet", tableProperties.get(IcebergPropertiesConstants.GRAVITINO_ICEBERG_FILE_FORMAT));
    Assertions.assertEquals("iceberg", tableProperties.get(IcebergPropertiesConstants.GRAVITINO_ICEBERG_PROVIDER));
    Assertions.assertEquals("none", tableProperties.get(IcebergPropertiesConstants.GRAVITINO_ICEBERG_CURRENT_SNAPSHOT_ID));
    Assertions.assertTrue(tableProperties.get(IcebergPropertiesConstants.GRAVITINO_ICEBERG_LOCATION).contains(tableName));
    Assertions.assertEquals("1", tableProperties.get(IcebergPropertiesConstants.GRAVITINO_ICEBERG_FORMAT_VERSION));
    /**  */
    Assertions.assertFalse(tableProperties.containsKey(IcebergPropertiesConstants.GRAVITINO_ICEBERG_SORT_ORDER));
    Assertions.assertFalse(tableProperties.containsKey(IcebergPropertiesConstants.GRAVITINO_ICEBERG_IDENTIFIER_FIELDS));
  }
}
