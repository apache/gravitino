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
package org.apache.gravitino.trino.connector.integration.test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

class TestTrinoQueryIT {
  private static final String TABLES = "\"tb01\"\n\"tb02\"";
  private static final String STAGING_TABLES = TABLES + "\n\"tmp_trino_staging\"";
  private static final Duration TIMEOUT = Duration.ofSeconds(2);

  @Test
  void testSqlFileWaitsForCleanup(@TempDir Path directory) throws Exception {
    Files.writeString(
        directory.resolve("rollback.sql"),
        "-- Wait for transaction rollback.\n<RETRY_UNTIL_MATCH> SHOW TABLES;\n");
    Files.writeString(directory.resolve("rollback.txt"), TABLES + "\n\n");
    TrinoQueryIT.getTesterCount(directory.toString(), "pg", "rollback");
    TrinoQueryRunner runner = Mockito.mock(TrinoQueryRunner.class);
    Mockito.when(runner.runQuery("SHOW TABLES")).thenReturn(STAGING_TABLES, TABLES);
    new TrinoQueryIT()
        .executeSqlFileWithCheckResult(directory.toString(), "rollback.sql", runner, "pg");
    Mockito.verify(runner, Mockito.times(2)).runQuery("SHOW TABLES");
  }

  @Test
  void testWaitsForStagingTableCleanup() {
    TrinoQueryRunner runner = Mockito.mock(TrinoQueryRunner.class);
    Mockito.when(runner.runQuery("SHOW TABLES")).thenReturn(STAGING_TABLES, TABLES);
    Assertions.assertEquals(
        TABLES,
        TrinoQueryIT.executeQuery(runner, "<RETRY_UNTIL_MATCH> SHOW TABLES", TABLES, TIMEOUT));
    Mockito.verify(runner, Mockito.times(2)).runQuery("SHOW TABLES");
  }

  @Test
  void testPersistentLeakRemainsAMismatch() {
    TrinoQueryRunner runner = Mockito.mock(TrinoQueryRunner.class);
    Mockito.when(runner.runQuery("SHOW TABLES")).thenReturn(STAGING_TABLES);
    String result =
        TrinoQueryIT.executeQuery(runner, "<RETRY_UNTIL_MATCH> SHOW TABLES", TABLES, TIMEOUT);
    Assertions.assertEquals(STAGING_TABLES, result);
    Assertions.assertFalse(TrinoQueryIT.match(TABLES, result));
    Mockito.verify(runner, Mockito.atLeast(2)).runQuery("SHOW TABLES");
  }

  @Test
  void testUnmarkedQueryRunsOnce() {
    TrinoQueryRunner runner = Mockito.mock(TrinoQueryRunner.class);
    Mockito.when(runner.runQuery("SHOW TABLES")).thenReturn(STAGING_TABLES, TABLES);
    Assertions.assertEquals(
        STAGING_TABLES, TrinoQueryIT.executeQuery(runner, "SHOW TABLES", TABLES, TIMEOUT));
    Mockito.verify(runner).runQuery("SHOW TABLES");
  }

  @Test
  void testRejectsRetryingWrites() {
    TrinoQueryRunner runner = Mockito.mock(TrinoQueryRunner.class);
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            TrinoQueryIT.executeQuery(
                runner, "<RETRY_UNTIL_MATCH> INSERT INTO t VALUES (1)", "", TIMEOUT));
    Mockito.verifyNoInteractions(runner);
  }
}
