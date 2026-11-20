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
package org.apache.gravitino.spark.connector.integration.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.withSettings;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/** Checks the SQL issued by Spark suite teardown against a shared catalog. */
class TestSparkCommonITCleanup {

  // Expose the SQL boundary so the test needs neither a Spark session nor a database server.
  abstract static class Fixture extends SparkCommonIT {
    @Override
    protected abstract List<Object[]> sql(String query);
  }

  @Test
  void dropsOnlyTheCurrentSuitesDatabase() {
    Fixture suite =
        mock(Fixture.class, withSettings().useConstructor().defaultAnswer(CALLS_REAL_METHODS));
    Fixture other =
        mock(Fixture.class, withSettings().useConstructor().defaultAnswer(CALLS_REAL_METHODS));
    String owned = suite.getDefaultDatabase();
    doReturn("jdbc_mysql").when(suite).getCatalogName();
    doReturn(
            List.of(
                new Object[] {"MYSQL_JDBC_BACKEND"},
                new Object[] {"default"},
                new Object[] {"default_db"},
                new Object[] {other.getDefaultDatabase()},
                new Object[] {owned}))
        .when(suite)
        .sql("SHOW DATABASES IN jdbc_mysql");
    doReturn(List.<Object[]>of(new Object[] {owned, "fixture_table"}))
        .when(suite)
        .sql("SHOW TABLES");

    suite.cleanUp();

    ArgumentCaptor<String> statements = ArgumentCaptor.forClass(String.class);
    verify(suite, times(5)).sql(statements.capture());
    assertEquals(
        List.of(
            "SHOW DATABASES IN jdbc_mysql",
            "USE jdbc_mysql." + owned,
            "SHOW TABLES",
            "DROP TABLE IF EXISTS fixture_table",
            "DROP DATABASE IF EXISTS " + owned),
        statements.getAllValues());
  }
}
