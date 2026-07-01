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

import java.util.List;
import org.apache.gravitino.spark.connector.paimon.GravitinoPaimonCatalogSpark35;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SparkPaimonCatalogFilesystemBackendIT35 extends SparkPaimonCatalogFilesystemBackendIT {

  @Test
  void testCatalogClassName() {
    String catalogClass =
        getSparkSession()
            .sessionState()
            .conf()
            .getConfString("spark.sql.catalog." + getCatalogName());
    Assertions.assertEquals(GravitinoPaimonCatalogSpark35.class.getName(), catalogClass);
  }

  @Test
  void testPaimonCompactProcedure() {
    String tableName = "test_paimon_compact";
    dropTableIfExists(tableName);
    sql(
        String.format(
            "CREATE TABLE %s (id INT COMMENT 'id comment', name STRING COMMENT '', address STRING COMMENT '') USING paimon",
            tableName));

    sql(String.format("INSERT INTO %s VALUES(1, 'a', 'beijing')", tableName));
    sql(String.format("INSERT INTO %s VALUES(2, 'b', 'shanghai')", tableName));

    String fullTableName =
        String.format("%s.%s.%s", getCatalogName(), getDefaultDatabase(), tableName);

    sql(String.format("USE %s.%s", getCatalogName(), getDefaultDatabase()));

    List<Row> result =
        getSparkSession()
            .sql(String.format("CALL system.compact(table => '%s')", fullTableName))
            .collectAsList();
    Assertions.assertFalse(result.isEmpty());
  }
}
