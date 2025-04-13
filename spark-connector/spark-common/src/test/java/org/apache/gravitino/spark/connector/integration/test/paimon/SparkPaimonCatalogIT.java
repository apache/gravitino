/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.spark.connector.integration.test.paimon;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.spark.connector.integration.test.SparkCommonIT;
import org.apache.gravitino.spark.connector.integration.test.util.SparkTableInfo;
import org.apache.gravitino.spark.connector.integration.test.util.SparkTableInfoChecker;
import org.apache.gravitino.spark.connector.paimon.PaimonPropertiesConstants;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public abstract class SparkPaimonCatalogIT extends SparkCommonIT {

  @Override
  protected String getCatalogName() {
    return "paimon";
  }

  @Override
  protected String getProvider() {
    return "lakehouse-paimon";
  }

  @Override
  protected boolean supportsSparkSQLClusteredBy() {
    return false;
  }

  @Override
  protected boolean supportsPartition() {
    return true;
  }

  @Override
  protected boolean supportsDelete() {
    return false;
  }

  @Override
  protected boolean supportsSchemaEvolution() {
    return true;
  }

  @Override
  protected boolean supportsSchemaAndTableProperties() {
    return true;
  }

  @Override
  protected boolean supportsComplexType() {
    return true;
  }

  @Override
  protected boolean supportsUpdateColumnPosition() {
    return true;
  }

  @Override
  protected boolean supportsReplaceColumns() {
    // Paimon doesn't support replace columns, because it doesn't support drop all fields in table.
    // And `ALTER TABLE REPLACE COLUMNS` statement will remove all existing columns at first and
    // then adds the new set of columns.
    return false;
  }

  @Override
  protected String getTableLocation(SparkTableInfo table) {
    Map<String, String> tableProperties = table.getTableProperties();
    return tableProperties.get(PaimonPropertiesConstants.PAIMON_TABLE_LOCATION);
  }

  @Test
  void testPaimonPartitions() {
    String partitionPathString = "name=a/address=beijing";

    String tableName = "test_paimon_partition_table";
    dropTableIfExists(tableName);
    String createTableSQL = getCreatePaimonSimpleTableString(tableName);
    createTableSQL = createTableSQL + " PARTITIONED BY (name, address);";
    sql(createTableSQL);
    SparkTableInfo tableInfo = getTableInfo(tableName);
    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withColumns(getPaimonSimpleTableColumn())
            .withIdentifyPartition(Collections.singletonList("name"))
            .withIdentifyPartition(Collections.singletonList("address"));
    checker.check(tableInfo);

    String insertData = String.format("INSERT into %s values(2,'a','beijing');", tableName);
    sql(insertData);
    List<String> queryResult = getTableData(tableName);
    Assertions.assertEquals(1, queryResult.size());
    Assertions.assertEquals("2,a,beijing", queryResult.get(0));
    Path partitionPath = new Path(getTableLocation(tableInfo), partitionPathString);
    checkDirExists(partitionPath);
  }

  @Test
  void testPaimonPartitionManagement() {
    testPaimonListAndDropPartition();
    // TODO: replace, add and load partition operations are unsupported now.
  }

  private void testPaimonListAndDropPartition() {
    String tableName = "test_paimon_drop_partition";
    dropTableIfExists(tableName);
    String createTableSQL = getCreatePaimonSimpleTableString(tableName);
    createTableSQL = createTableSQL + " PARTITIONED BY (name);";
    sql(createTableSQL);

    String insertData =
        String.format(
            "INSERT into %s values(1,'a','beijing'), (2,'b','beijing'), (3,'c','beijing');",
            tableName);
    sql(insertData);
    List<String> queryResult = getTableData(tableName);
    Assertions.assertEquals(3, queryResult.size());

    List<String> partitions = getQueryData(String.format("show partitions %s", tableName));
    Assertions.assertEquals(3, partitions.size());
    Assertions.assertEquals("name=a;name=b;name=c", String.join(";", partitions));

    sql(String.format("ALTER TABLE %s DROP PARTITION (`name`='a')", tableName));
    partitions = getQueryData(String.format("show partitions %s", tableName));
    Assertions.assertEquals(2, partitions.size());
    Assertions.assertEquals("name=b;name=c", String.join(";", partitions));
  }

  private String getCreatePaimonSimpleTableString(String tableName) {
    return String.format(
        "CREATE TABLE %s (id INT COMMENT 'id comment', name STRING COMMENT '', address STRING COMMENT '') USING paimon",
        tableName);
  }

  private List<SparkTableInfo.SparkColumnInfo> getPaimonSimpleTableColumn() {
    return Arrays.asList(
        SparkTableInfo.SparkColumnInfo.of("id", DataTypes.IntegerType, "id comment"),
        SparkTableInfo.SparkColumnInfo.of("name", DataTypes.StringType, ""),
        SparkTableInfo.SparkColumnInfo.of("address", DataTypes.StringType, ""));
  }
}
