/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.datastrato.gravitino.integration.test.util.spark;

import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.datastrato.gravitino.spark.connector.table.SparkBaseTable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.ResolvedTable;
import org.apache.spark.sql.catalyst.plans.logical.CommandResult;
import org.apache.spark.sql.catalyst.plans.logical.DescribeRelation;
import org.junit.jupiter.api.Assertions;

/**
 * Provides helper methods to execute SparkSQL and get SparkSQL result, will be reused by SparkIT
 * and IcebergRESTServiceIT
 *
 * <p>Referred from spark/v3.4/spark/src/test/java/org/apache/iceberg/spark/SparkTestBase.java
 */
public abstract class SparkUtilIT extends AbstractIT {

  protected abstract SparkSession getSparkSession();

  protected Set<String> getCatalogs() {
    return convertToStringSet(sql("SHOW CATALOGS"), 0);
  }

  protected Set<String> getDatabases() {
    return convertToStringSet(sql("SHOW DATABASES"), 0);
  }

  protected Set<String> listTableNames() {
    // the first column is namespace, the second column is table name
    return convertToStringSet(sql("SHOW TABLES"), 1);
  }

  protected Set<String> listTableNames(String database) {
    return convertToStringSet(sql("SHOW TABLES in " + database), 1);
  }

  protected void dropDatabaseIfExists(String database) {
    sql("DROP DATABASE IF EXISTS " + database);
  }

  // Specify Location explicitly because the default location is local HDFS, Spark will expand the
  // location to HDFS.
  protected void createDatabaseIfNotExists(String database) {
    sql(
        String.format(
            "CREATE DATABASE IF NOT EXISTS %s LOCATION '/user/hive/%s'", database, database));
  }

  protected Map<String, String> getDatabaseMetadata(String database) {
    return convertToStringMap(sql("DESC DATABASE EXTENDED " + database));
  }

  protected List<Object[]> sql(String query) {
    List<Row> rows = getSparkSession().sql(query).collectAsList();
    return rowsToJava(rows);
  }

  // Create SparkTableInfo from SparkBaseTable retrieved from LogicalPlan.
  protected SparkTableInfo getTableInfo(String tableName) {
    Dataset ds = getSparkSession().sql("DESC TABLE EXTENDED " + tableName);
    CommandResult result = (CommandResult) ds.logicalPlan();
    DescribeRelation relation = (DescribeRelation) result.commandLogicalPlan();
    ResolvedTable table = (ResolvedTable) relation.child();
    SparkBaseTable baseTable = (SparkBaseTable) table.table();
    return SparkTableInfo.create(baseTable);
  }

  protected void dropTableIfExists(String tableName) {
    sql("DROP TABLE IF EXISTS " + tableName);
  }

  protected boolean tableExists(String tableName) {
    try {
      SparkTableInfo tableInfo = getTableInfo(tableName);
      Assertions.assertEquals(tableName, tableInfo.getTableName());
      return true;
    } catch (Exception e) {
      if (e instanceof AnalysisException) {
        return false;
      }
      throw e;
    }
  }

  private List<Object[]> rowsToJava(List<Row> rows) {
    return rows.stream().map(this::toJava).collect(Collectors.toList());
  }

  private Object[] toJava(Row row) {
    return IntStream.range(0, row.size())
        .mapToObj(
            pos -> {
              if (row.isNullAt(pos)) {
                return null;
              }
              Object value = row.get(pos);
              if (value instanceof Row) {
                return toJava((Row) value);
              } else if (value instanceof scala.collection.Seq) {
                return row.getList(pos);
              } else if (value instanceof scala.collection.Map) {
                return row.getJavaMap(pos);
              }
              return value;
            })
        .toArray(Object[]::new);
  }

  private static Set<String> convertToStringSet(List<Object[]> objects, int index) {
    return objects.stream().map(row -> String.valueOf(row[index])).collect(Collectors.toSet());
  }

  private static Map<String, String> convertToStringMap(List<Object[]> objects) {
    return objects.stream()
        .collect(
            Collectors.toMap(
                row -> String.valueOf(row[0]),
                row -> String.valueOf(row[1]),
                (oldValue, newValue) -> oldValue));
  }
}
