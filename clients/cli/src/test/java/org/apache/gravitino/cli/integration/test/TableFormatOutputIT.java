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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TableFormatOutputIT extends BaseIT {
  private String gravitinoUrl;

  @BeforeAll
  public void startUp() {
    gravitinoUrl = String.format("http://127.0.0.1:%d", getGravitinoServerPort());
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
      "jdbc-url=jdbc:postgresql://postgresql-host/mydb,jdbc-user=user,jdbc-password=password,jdbc-database=db,jdbc-driver=org.postgresql.Driver",
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
      "jdbc-url=jdbc:postgresql://postgresql-host/mydb,jdbc-user=user,jdbc-password=password,jdbc-database=db,jdbc-driver=org.postgresql.Driver",
      commandArg(GravitinoOptions.URL),
      gravitinoUrl,
      commandArg(GravitinoOptions.COMMENT),
      "catalog, 用于测试"
    };
    Main.main(create_catalog_with_comment_args);
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

  private String commandArg(String arg) {
    return String.format("--%s", arg);
  }
}
