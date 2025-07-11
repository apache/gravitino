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

package org.apache.gravitino.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestMain {

  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;

  @BeforeEach
  public void setUpStreams() {
    System.setOut(new PrintStream(outContent));
    System.setErr(new PrintStream(errContent));
  }

  @AfterEach
  public void restoreStreams() {
    System.setOut(originalOut);
    System.setErr(originalErr);
  }

  @Test
  public void withTwoArgs() throws ParseException {
    Options options = new GravitinoOptions().options();
    CommandLineParser parser = new DefaultParser();
    String[] args = {"metalake", "details"};
    CommandLine line = parser.parse(options, args);

    String command = Main.resolveCommand(line);
    assertEquals(CommandActions.DETAILS, command);
    String entity = Main.resolveEntity(line);
    assertEquals(CommandEntities.METALAKE, entity);
  }

  @Test
  public void defaultToDetailsOneArg() throws ParseException {
    Options options = new GravitinoOptions().options();
    CommandLineParser parser = new DefaultParser();
    String[] args = {"metalake"};
    CommandLine line = parser.parse(options, args);

    Assertions.assertThrows(IllegalArgumentException.class, () -> Main.resolveCommand(line));
    Assertions.assertEquals(CommandEntities.METALAKE, Main.resolveEntity(line));
  }

  @Test
  public void withNoArgs() throws ParseException {
    Options options = new GravitinoOptions().options();
    CommandLineParser parser = new DefaultParser();
    String[] args = {};
    CommandLine line = parser.parse(options, args);

    String command = Main.resolveCommand(line);
    assertNull(command);
    String entity = Main.resolveEntity(line);
    assertNull(entity);
  }

  @Test
  public void withNoArgsAndOptions() throws ParseException {
    Options options = new GravitinoOptions().options();
    CommandLineParser parser = new DefaultParser();
    String[] args = {"--name", "metalake_demo"};
    CommandLine line = parser.parse(options, args);

    String command = Main.resolveCommand(line);
    assertNull(command);
    String entity = Main.resolveEntity(line);
    assertNull(entity);
  }

  @Test
  @SuppressWarnings("DefaultCharset")
  public void parseError() throws UnsupportedEncodingException {
    String[] args = {"--invalidOption"};

    Main.useExit = false;
    assertThrows(
        RuntimeException.class,
        () -> {
          Main.main(args);
        });

    assertTrue(errContent.toString().contains("Error parsing command line")); // Expect error
  }

  @Test
  public void catalogWithOneArg() throws ParseException {
    Options options = new GravitinoOptions().options();
    CommandLineParser parser = new DefaultParser();
    String[] args = {"catalog", "--name", "catalog_postgres"};
    CommandLine line = parser.parse(options, args);

    Assertions.assertThrows(IllegalArgumentException.class, () -> Main.resolveCommand(line));
  }

  @Test
  public void metalakeWithHelpOption() throws ParseException {
    Options options = new GravitinoOptions().options();
    CommandLineParser parser = new DefaultParser();
    String[] args = {"metalake", "--help"};
    CommandLine line = parser.parse(options, args);

    assertEquals(Main.resolveEntity(line), CommandEntities.METALAKE);
    assertNull(Main.resolveCommand(line));
  }

  @Test
  public void catalogWithHelpOption() throws ParseException {
    Options options = new GravitinoOptions().options();
    CommandLineParser parser = new DefaultParser();
    String[] args = {"catalog", "--help"};
    CommandLine line = parser.parse(options, args);

    assertEquals(Main.resolveEntity(line), CommandEntities.CATALOG);
    assertNull(Main.resolveCommand(line));
  }

  @Test
  public void schemaWithHelpOption() throws ParseException {
    Options options = new GravitinoOptions().options();
    CommandLineParser parser = new DefaultParser();
    String[] args = {"schema", "--help"};
    CommandLine line = parser.parse(options, args);

    assertEquals(Main.resolveEntity(line), CommandEntities.SCHEMA);
    assertNull(Main.resolveCommand(line));
  }

  @Test
  void testCliHelpWithoutArgs() {
    String[] args = {};
    Main.main(args);

    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        "Usage: ./bin/gcli.sh [gravitino-options] <entity> <command> [command-options]\n"
            + "Entities: schema, role, metalake, catalog, column, topic, model, tag, fileset, user, table, group\n"
            + "Gravitino Options:\n"
            + "  --url Gravitino URL (default: http://localhost:8090)\n"
            + "  --login user name\n"
            + "  --ignore ignore client/sever version check\n"
            + "  --quiet quiet mode\n"
            + "For detailed help on entity specific operations, use ./bin/gcli.sh <entity> --help",
        output);
  }

  @Test
  void testCliHelpWithHelpOption() {
    String[] args = {"--help"};
    Main.main(args);

    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        "Usage: ./bin/gcli.sh [gravitino-options] <entity> <command> [command-options]\n"
            + "Entities: schema, role, metalake, catalog, column, topic, model, tag, fileset, user, table, group\n"
            + "Gravitino Options:\n"
            + "  --url Gravitino URL (default: http://localhost:8090)\n"
            + "  --login user name\n"
            + "  --ignore ignore client/sever version check\n"
            + "  --quiet quiet mode\n"
            + "For detailed help on entity specific operations, use ./bin/gcli.sh <entity> --help",
        output);
  }

  @Test
  void testMetalakeHelp() {
    String[] args = {"metalake", "--help"};
    Main.main(args);

    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        "Usage: ./bin/gcli.sh [gravitino-options] metalake <command> [command-options]\n"
            + "Commands:\n"
            + "  -- list\n"
            + "  -- details\n"
            + "  -- create\n"
            + "  -- delete\n"
            + "  -- set\n"
            + "  -- remove\n"
            + "  -- properties\n"
            + "  -- update\n"
            + "For detailed help on sub commands specific operations, use ./bin/gcli.sh metalake <command> --help",
        output);
  }

  @Test
  void testTableHelp() {
    String[] args = {"table", "--help"};
    Main.main(args);

    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        "Usage: ./bin/gcli.sh [gravitino-options] table <command> [command-options]\n"
            + "Commands:\n"
            + "  -- list\n"
            + "  -- details\n"
            + "  -- delete\n"
            + "  -- set\n"
            + "  -- remove\n"
            + "  -- properties\n"
            + "  -- update\n"
            + "For detailed help on sub commands specific operations, use ./bin/gcli.sh table <command> --help",
        output);
  }

  @Test
  void testListMetalakeHelp() {
    String[] args = {"metalake", CommandActions.LIST, "--help"};
    Main.main(args);

    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        "Usage: ./bin/gcli.sh [gravitino-options] metalake list <required args> [optional args]\n"
            + "Required Args:\n"
            + "  N/A\n"
            + "Optional Args:\n"
            + "  --output: output format (plain/table)\n"
            + "Gravitino Options:\n"
            + "  --url: Gravitino URL (default: http://localhost:8090)\n"
            + "  --login: user name\n"
            + "  --ignore: ignore client/sever version check\n"
            + "  --quiet: quiet mode",
        output);
  }

  @Test
  void testMetalakeDetailsHelp() {
    String[] args = {"metalake", CommandActions.DETAILS, "--help"};
    Main.main(args);

    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        "Usage: ./bin/gcli.sh [gravitino-options] metalake details <required args> [optional args]\n"
            + "Required Args:\n"
            + "  N/A\n"
            + "Optional Args:\n"
            + "  --audit: display audit information\n"
            + "  --output: output format (plain/table)\n"
            + "Gravitino Options:\n"
            + "  --url: Gravitino URL (default: http://localhost:8090)\n"
            + "  --login: user name\n"
            + "  --ignore: ignore client/sever version check\n"
            + "  --quiet: quiet mode",
        output);
  }

  @Test
  void testListTableHelp() {
    String[] args = {"table", CommandActions.LIST, "--help"};
    Main.main(args);

    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        "Usage: ./bin/gcli.sh [gravitino-options] table list <required args> [optional args]\n"
            + "Required Args:\n"
            + "  --metalake: metalake name\n"
            + "  --name: full entity name (dot separated)\n"
            + "Optional Args:\n"
            + "  --output: output format (plain/table)\n"
            + "Gravitino Options:\n"
            + "  --url: Gravitino URL (default: http://localhost:8090)\n"
            + "  --login: user name\n"
            + "  --ignore: ignore client/sever version check\n"
            + "  --quiet: quiet mode",
        output);
  }

  @Test
  void testTableDetailsHelp() {
    String[] args = {"table", CommandActions.DETAILS, "--help"};
    Main.main(args);

    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        "Usage: ./bin/gcli.sh [gravitino-options] table details <required args> [optional args]\n"
            + "Required Args:\n"
            + "  --metalake: metalake name\n"
            + "  --name: full entity name (dot separated)\n"
            + "Optional Args:\n"
            + "  --audit: display audit information\n"
            + "  --index: display index information\n"
            + "  --distribution: display distribution information\n"
            + "  --partition: display partition information\n"
            + "  --sortorder: display sortorder information\n"
            + "  --output: output format (plain/table)\n"
            + "Gravitino Options:\n"
            + "  --url: Gravitino URL (default: http://localhost:8090)\n"
            + "  --login: user name\n"
            + "  --ignore: ignore client/sever version check\n"
            + "  --quiet: quiet mode",
        output);
  }

  @Test
  void testEntityHelpVerbose() {
    String[] args = {"table", "--help", "--verbose"};
    Main.main(args);

    String output = new String(outContent.toByteArray(), StandardCharsets.UTF_8).trim();
    Assertions.assertEquals(
        "Usage: ./bin/gcli.sh [gravitino-options] table <command> [command-options]\n"
            + "Commands:\n"
            + "  -- list\n"
            + "  -- details\n"
            + "  -- delete\n"
            + "  -- set\n"
            + "  -- remove\n"
            + "  -- properties\n"
            + "  -- update\n"
            + "For detailed help on sub commands specific operations, use ./bin/gcli.sh table <command> --help\n"
            + "\n"
            + "gcli table [list|details|create|delete|properties|set|remove]\n"
            + "\n"
            + "Please set the metalake in the Gravitino configuration file or the environment variable before running any of these commands.\n"
            + "\n"
            + "When creating a table the columns are specified in CSV file specifying the name of the column, the datatype, a comment, true or false if the column is nullable, true or false if the column is auto incremented, a default value and a default type. Not all of the columns need to be specifed just the name and datatype columns. If not specified comment default to null, nullability to true and auto increment to false. If only the default value is specified it defaults to the same data type as the column.\n"
            + "\n"
            + "Example CSV file\n"
            + "Name,Datatype,Comment,Nullable,AutoIncrement,DefaultValue,DefaultType\n"
            + "name,String,person's name\n"
            + "ID,Integer,unique id,false,true\n"
            + "location,String,city they work in,false,false,Sydney,String\n"
            + "\n"
            + "Example commands\n"
            + "\n"
            + "Show all tables\n"
            + "gcli table list --name catalog_postgres.hr\n"
            + "\n"
            + "Show tables details\n"
            + "gcli table details --name catalog_postgres.hr.departments\n"
            + "\n"
            + "Show tables audit information\n"
            + "gcli table details --name catalog_postgres.hr.departments --audit\n"
            + "\n"
            + "Show tables distribution information\n"
            + "gcli table details --name catalog_postgres.hr.departments --distribution\n"
            + "\n"
            + "Show tables partition information\n"
            + "gcli table details --name catalog_postgres.hr.departments --partition\n"
            + "\n"
            + "Show tables sort order information\n"
            + "gcli table details --name catalog_postgres.hr.departments --sortorder\n"
            + "\n"
            + "Show table indexes\n"
            + "gcli table details --name catalog_mysql.db.iceberg_namespace_properties --index\n"
            + "\n"
            + "Create a table\n"
            + "gcli table create --name catalog_postgres.hr.salaries --comment \"comment\" --columnfile ~/table.csv\n"
            + "\n"
            + "Delete a table\n"
            + "gcli table delete --name catalog_postgres.hr.salaries\n"
            + "\n"
            + "Display a tables's properties\n"
            + "gcli table properties --name catalog_postgres.hr.salaries\n"
            + "\n"
            + "Set a tables's property\n"
            + "gcli table set --name catalog_postgres.hr.salaries --property test --value value\n"
            + "\n"
            + "Remove a tables's property\n"
            + "gcli table remove --name catalog_postgres.hr.salaries --property test",
        output);
  }

  @SuppressWarnings("DefaultCharset")
  public void CreateTagWithNoTag() {
    String[] args = {"tag", "create", "--metalake", "metalake_test_no_tag"};

    Main.main(args);

    assertTrue(errContent.toString().contains(ErrorMessages.MISSING_TAG)); // Expect error
  }

  @SuppressWarnings("DefaultCharset")
  public void DeleteTagWithNoTag() {
    String[] args = {"tag", "delete", "--metalake", "metalake_test_no_tag", "-f"};

    Main.main(args);

    assertTrue(errContent.toString().contains(ErrorMessages.MISSING_TAG)); // Expect error
  }
}
