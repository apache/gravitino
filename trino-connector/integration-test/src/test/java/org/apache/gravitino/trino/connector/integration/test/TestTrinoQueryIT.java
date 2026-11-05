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
  void testGenerationWaitsForCleanup(@TempDir Path directory) throws Exception {
    Files.writeString(directory.resolve("rollback.sql"), "<RETRY_UNTIL_MATCH> SHOW TABLES;\n");
    Files.writeString(directory.resolve("rollback.txt"), TABLES + "\n\n");
    TrinoQueryRunner runner = Mockito.mock(TrinoQueryRunner.class);
    Mockito.when(runner.runQuery("SHOW TABLES")).thenReturn(STAGING_TABLES, TABLES);
    new TrinoQueryIT().executeSqlFileWithGenOutput(directory.toString(), "rollback.sql", runner);
    Assertions.assertEquals(TABLES + "\n", Files.readString(directory.resolve("rollback.txt")));
    Mockito.verify(runner, Mockito.times(2)).runQuery("SHOW TABLES");
  }

  @Test
  void testGenerationCanReuseFinalExpectedResult(@TempDir Path directory) throws Exception {
    Files.writeString(directory.resolve("rollback.sql"), "<RETRY_UNTIL_MATCH> SHOW TABLES;\n");
    for (int trailingNewlines = 0; trailingNewlines <= 2; trailingNewlines++) {
      Files.writeString(directory.resolve("rollback.txt"), TABLES + "\n".repeat(trailingNewlines));
      TrinoQueryRunner runner = Mockito.mock(TrinoQueryRunner.class);
      Mockito.when(runner.runQuery("SHOW TABLES"))
          .thenReturn(STAGING_TABLES, TABLES, STAGING_TABLES, TABLES);
      TrinoQueryIT test = new TrinoQueryIT();
      for (int generation = 0; generation < 2; generation++) {
        test.executeSqlFileWithGenOutput(directory.toString(), "rollback.sql", runner);
        Assertions.assertEquals(TABLES + "\n", Files.readString(directory.resolve("rollback.txt")));
      }
      Mockito.verify(runner, Mockito.times(4)).runQuery("SHOW TABLES");
    }
  }

  @Test
  void testGenerationRequiresExistingRetryExpectation(@TempDir Path directory) throws Exception {
    Files.writeString(
        directory.resolve("rollback.sql"),
        "CREATE TABLE t (i INT);\n<RETRY_UNTIL_MATCH> SHOW TABLES;\n");
    TrinoQueryRunner runner = Mockito.mock(TrinoQueryRunner.class);
    Mockito.when(runner.runQuery(Mockito.anyString())).thenReturn("ok");
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            new TrinoQueryIT()
                .executeSqlFileWithGenOutput(directory.toString(), "rollback.sql", runner));
    Mockito.verifyNoInteractions(runner);
    Assertions.assertFalse(Files.exists(directory.resolve("rollback.txt")));
  }

  @Test
  void testGenerationTimeoutPreservesGoldenFile(@TempDir Path directory) throws Exception {
    Files.writeString(directory.resolve("rollback.sql"), "<RETRY_UNTIL_MATCH> SHOW TABLES;\n");
    String previous = TABLES + "\n\n";
    Files.writeString(directory.resolve("rollback.txt"), previous);
    TrinoQueryRunner runner = Mockito.mock(TrinoQueryRunner.class);
    Mockito.when(runner.runQuery("SHOW TABLES")).thenReturn(STAGING_TABLES);
    IllegalStateException error =
        Assertions.assertThrows(
            IllegalStateException.class,
            () ->
                new TrinoQueryIT()
                    .executeSqlFileWithGenOutput(
                        directory.toString(), "rollback.sql", runner, Duration.ofMillis(300)));
    Assertions.assertTrue(error.getMessage().contains("golden file unchanged"));
    Assertions.assertEquals(previous, Files.readString(directory.resolve("rollback.txt")));
    Mockito.verify(runner, Mockito.atLeast(2)).runQuery("SHOW TABLES");
  }

  @Test
  void testGenerationKeepsStatementExpectationsAligned(@TempDir Path directory) throws Exception {
    Files.writeString(
        directory.resolve("rollback.sql"), "SHOW SCHEMAS;\n<RETRY_UNTIL_MATCH> SHOW TABLES;\n");
    Files.writeString(directory.resolve("rollback.txt"), "\"old_schema\"\n\n" + TABLES + "\n\n");
    TrinoQueryRunner runner = Mockito.mock(TrinoQueryRunner.class);
    Mockito.when(runner.runQuery("SHOW SCHEMAS")).thenReturn("\"new_schema\"");
    Mockito.when(runner.runQuery("SHOW TABLES")).thenReturn(STAGING_TABLES, TABLES);
    new TrinoQueryIT().executeSqlFileWithGenOutput(directory.toString(), "rollback.sql", runner);
    Assertions.assertEquals(
        "\"new_schema\"\n\n" + TABLES + "\n", Files.readString(directory.resolve("rollback.txt")));
    Mockito.verify(runner).runQuery("SHOW SCHEMAS");
    Mockito.verify(runner, Mockito.times(2)).runQuery("SHOW TABLES");
  }

  @Test
  void testUnmarkedGenerationNeedsNoExistingGolden(@TempDir Path directory) throws Exception {
    Files.writeString(directory.resolve("plain.sql"), "SHOW TABLES;\n");
    TrinoQueryRunner runner = Mockito.mock(TrinoQueryRunner.class);
    Mockito.when(runner.runQuery("SHOW TABLES")).thenReturn(TABLES);
    new TrinoQueryIT().executeSqlFileWithGenOutput(directory.toString(), "plain.sql", runner);
    Assertions.assertEquals(TABLES + "\n", Files.readString(directory.resolve("plain.txt")));
    Mockito.verify(runner).runQuery("SHOW TABLES");
  }

  @Test
  void testGenerationRejectsMarkedWritesBeforeExecutingSql(@TempDir Path directory)
      throws Exception {
    Files.writeString(
        directory.resolve("writes.sql"),
        "SHOW TABLES;\n<RETRY_UNTIL_MATCH> INSERT INTO t VALUES (1);\n");
    String previous = TABLES + "\n\n1\n\n";
    Files.writeString(directory.resolve("writes.txt"), previous);
    TrinoQueryRunner runner = Mockito.mock(TrinoQueryRunner.class);
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            new TrinoQueryIT()
                .executeSqlFileWithGenOutput(directory.toString(), "writes.sql", runner));
    Mockito.verifyNoInteractions(runner);
    Assertions.assertEquals(previous, Files.readString(directory.resolve("writes.txt")));
  }

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
