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
package org.apache.gravitino.cli.integration.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import org.apache.gravitino.cli.GravitinoOptions;
import org.apache.gravitino.cli.Main;
import org.apache.gravitino.cli.commands.Command;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Checks the CLI's {@code --output table} text end to end: argument parsing, the REST round trip to
 * a real server, and the printed table.
 *
 * <p>The JDBC catalogs are created with {@code verify-on-create=false} and no database behind them,
 * on purpose. The table output reads only catalog metadata (name, type, provider, comment), so a
 * real database adds cost without adding coverage, and the setting only skips the connection check
 * at create time, which the list and details commands never reach. JDBC connectivity is tested by
 * the JDBC catalog integration tests.
 */
public class TableFormatOutputIT extends BaseIT {
  private String gravitinoUrl;

  @BeforeAll
  public void startUp() {
    // Fail the test, rather than end the test JVM, when a command fails.
    Main.useExit = false;
    gravitinoUrl = String.format("http://127.0.0.1:%d", getGravitinoServerPort());

    // No database behind these catalogs; see the class comment.
    String postgresProperties =
        "jdbc-url=jdbc:postgresql://localhost:5432/mydb,jdbc-user=user,jdbc-password=password,"
            + "jdbc-database=mydb,jdbc-driver=org.postgresql.Driver,verify-on-create=false";
    String mysqlProperties =
        "jdbc-url=jdbc:mysql://localhost:3306,jdbc-user=user,jdbc-password=password,"
            + "jdbc-driver=com.mysql.cj.jdbc.Driver,verify-on-create=false";
    String[] create_metalake_args = {
      "metalake",
      "create",
      commandArg(GravitinoOptions.METALAKE),
      "my_metalake",
      commandArg(GravitinoOptions.COMMENT),
      "my metalake",
      commandArg(GravitinoOptions.URL),
      gravitinoUrl
    };
    Main.main(create_metalake_args);

    String[] create_catalog_args = {
      "catalog",
      "create",
      commandArg(GravitinoOptions.METALAKE),
      "my_metalake",
      commandArg(GravitinoOptions.NAME),
      "postgres",
      commandArg(GravitinoOptions.PROVIDER),
      "postgres",
      commandArg(GravitinoOptions.PROPERTIES),
      postgresProperties,
      commandArg(GravitinoOptions.URL),
      gravitinoUrl
    };
    Main.main(create_catalog_args);

    String[] create_catalog_with_comment_args = {
      "catalog",
      "create",
      commandArg(GravitinoOptions.METALAKE),
      "my_metalake",
      commandArg(GravitinoOptions.NAME),
      "postgres2",
      commandArg(GravitinoOptions.PROVIDER),
      "postgres",
      commandArg(GravitinoOptions.PROPERTIES),
      postgresProperties,
      commandArg(GravitinoOptions.URL),
      gravitinoUrl,
      commandArg(GravitinoOptions.COMMENT),
      "catalog, 用于测试"
    };
    Main.main(create_catalog_with_comment_args);

    String[] create_mysql_catalog_args = {
      "catalog",
      "create",
      commandArg(GravitinoOptions.METALAKE),
      "my_metalake",
      commandArg(GravitinoOptions.NAME),
      "mysql",
      commandArg(GravitinoOptions.PROVIDER),
      "mysql",
      commandArg(GravitinoOptions.PROPERTIES),
      mysqlProperties,
      commandArg(GravitinoOptions.URL),
      gravitinoUrl
    };
    Main.main(create_mysql_catalog_args);
  }

  @AfterAll
  public void restoreExit() {
    Main.useExit = true;
  }

  @Test
  public void testMetalakeListCommand() {
    // Create a byte array output stream to capture the output of the command
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    PrintStream originalOut = System.out;
    System.setOut(new PrintStream(outputStream));

    String[] args = {
      "metalake",
      "list",
      commandArg(GravitinoOptions.OUTPUT),
      Command.OUTPUT_FORMAT_TABLE,
      commandArg(GravitinoOptions.URL),
      gravitinoUrl
    };
    Main.main(args);

    // Restore the original System.out
    System.setOut(originalOut);
    // Get the output and verify it
    String output = new String(outputStream.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(
        "+-------------+\n"
            + "|  Metalake   |\n"
            + "+-------------+\n"
            + "| my_metalake |\n"
            + "+-------------+",
        output);
  }

  @Test
  public void testMetalakeDetailsCommand() {
    // Create a byte array output stream to capture the output of the command
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    PrintStream originalOut = System.out;
    System.setOut(new PrintStream(outputStream));

    String[] args = {
      "metalake",
      "details",
      commandArg(GravitinoOptions.METALAKE),
      "my_metalake",
      commandArg(GravitinoOptions.OUTPUT),
      Command.OUTPUT_FORMAT_TABLE,
      commandArg(GravitinoOptions.URL),
      gravitinoUrl
    };
    Main.main(args);

    // Restore the original System.out
    System.setOut(originalOut);
    // Get the output and verify it
    String output = new String(outputStream.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(
        "+-------------+-------------+\n"
            + "|  Metalake   |   Comment   |\n"
            + "+-------------+-------------+\n"
            + "| my_metalake | my metalake |\n"
            + "+-------------+-------------+",
        output);
  }

  @Test
  public void testCatalogListCommand() {
    // Create a byte array output stream to capture the output of the command
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    PrintStream originalOut = System.out;
    System.setOut(new PrintStream(outputStream));

    String[] args = {
      "catalog",
      "list",
      commandArg(GravitinoOptions.METALAKE),
      "my_metalake",
      commandArg(GravitinoOptions.OUTPUT),
      Command.OUTPUT_FORMAT_TABLE,
      commandArg(GravitinoOptions.URL),
      gravitinoUrl
    };
    Main.main(args);

    // Restore the original System.out
    System.setOut(originalOut);
    // Get the output and verify it
    String output = new String(outputStream.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(
        "+-----------+\n"
            + "|  Catalog  |\n"
            + "+-----------+\n"
            + "| mysql     |\n"
            + "| postgres  |\n"
            + "| postgres2 |\n"
            + "+-----------+",
        output);
  }

  @Test
  public void testCatalogDetailsCommand() {
    // Create a byte array output stream to capture the output of the command
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    PrintStream originalOut = System.out;
    System.setOut(new PrintStream(outputStream));

    String[] args = {
      "catalog",
      "details",
      commandArg(GravitinoOptions.METALAKE),
      "my_metalake",
      commandArg(GravitinoOptions.NAME),
      "postgres",
      commandArg(GravitinoOptions.OUTPUT),
      "table",
      commandArg(GravitinoOptions.URL),
      gravitinoUrl
    };
    Main.main(args);

    // Restore the original System.out
    System.setOut(originalOut);
    // Get the output and verify it
    String output = new String(outputStream.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(
        "+----------+------------+-----------------+---------+\n"
            + "| Catalog  |    Type    |    Provider     | Comment |\n"
            + "+----------+------------+-----------------+---------+\n"
            + "| postgres | RELATIONAL | jdbc-postgresql | null    |\n"
            + "+----------+------------+-----------------+---------+",
        output);
  }

  @Test
  public void testCatalogDetailsCommandFullCornerCharacter() {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    PrintStream originalOut = System.out;
    System.setOut(new PrintStream(outputStream));

    String[] args = {
      "catalog",
      "details",
      commandArg(GravitinoOptions.METALAKE),
      "my_metalake",
      commandArg(GravitinoOptions.NAME),
      "postgres2",
      commandArg(GravitinoOptions.OUTPUT),
      "table",
      commandArg(GravitinoOptions.URL),
      gravitinoUrl
    };
    Main.main(args);
    // Restore the original System.out
    System.setOut(originalOut);
    // Get the output and verify it
    String output = new String(outputStream.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(
        "+-----------+------------+-----------------+-------------------+\n"
            + "|  Catalog  |    Type    |    Provider     |      Comment      |\n"
            + "+-----------+------------+-----------------+-------------------+\n"
            + "| postgres2 | RELATIONAL | jdbc-postgresql | catalog, 用于测试 |\n"
            + "+-----------+------------+-----------------+-------------------+",
        output);
  }

  @Test
  public void testMySQLCatalogDetailsCommand() {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    PrintStream originalOut = System.out;
    System.setOut(new PrintStream(outputStream));

    String[] args = {
      "catalog",
      "details",
      commandArg(GravitinoOptions.METALAKE),
      "my_metalake",
      commandArg(GravitinoOptions.NAME),
      "mysql",
      commandArg(GravitinoOptions.OUTPUT),
      "table",
      commandArg(GravitinoOptions.URL),
      gravitinoUrl
    };
    Main.main(args);

    System.setOut(originalOut);
    String output = new String(outputStream.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(
        "+---------+------------+------------+---------+\n"
            + "| Catalog |    Type    |  Provider  | Comment |\n"
            + "+---------+------------+------------+---------+\n"
            + "| mysql   | RELATIONAL | jdbc-mysql | null    |\n"
            + "+---------+------------+------------+---------+",
        output);
  }

  private String commandArg(String arg) {
    return String.format("--%s", arg);
  }
}
