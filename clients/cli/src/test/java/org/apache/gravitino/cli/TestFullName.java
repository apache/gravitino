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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestFullName {

  private Options options;
  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;

  @BeforeEach
  public void setUp() {
    Main.useExit = false;
    options = new GravitinoOptions().options();
    System.setOut(new PrintStream(outContent));
    System.setErr(new PrintStream(errContent));
  }

  @AfterEach
  public void restoreStreams() {
    System.setOut(originalOut);
    System.setErr(originalErr);
  }

  @Test
  public void entityFromFullNameOption() throws Exception {
    String[] args = {"--metalake", "metalakeA", "--name", "catalogB.schemaC.tableD.columnE"};
    CommandLine commandLine = new DefaultParser().parse(options, args);
    FullName fullName = new FullName(commandLine);

    String metalakeName = fullName.getMetalakeName();
    assertEquals("metalakeA", metalakeName);
    String catalogName = fullName.getCatalogName();
    assertEquals("catalogB", catalogName);
    String schemaName = fullName.getSchemaName();
    assertEquals("schemaC", schemaName);
    String tableName = fullName.getTableName();
    assertEquals("tableD", tableName);
    String columnName = fullName.getColumnName();
    assertEquals("columnE", columnName);
  }

  @Test
  public void entityNotFound() throws Exception {
    String[] args = {};
    CommandLine commandLine = new DefaultParser().parse(options, args);
    FullName fullName = new FullName(commandLine);

    assertThrows(RuntimeException.class, fullName::getMetalakeName);
  }

  @Test
  public void malformedName() throws Exception {
    String[] args = {"--name", "catalog.schema"};
    CommandLine commandLine = new DefaultParser().parse(options, args);
    FullName fullName = new FullName(commandLine);
    String tableName = fullName.getTableName();
    assertNull(tableName);
  }

  @Test
  public void missingName() throws Exception {
    String[] args = {"catalog", "--name"};
    assertThrows(MissingArgumentException.class, () -> new DefaultParser().parse(options, args));
  }

  @Test
  public void missingArgs() throws Exception {
    String[] args = {}; // No name provided
    CommandLine commandLine = new DefaultParser().parse(options, args);
    FullName fullName = new FullName(commandLine);

    String namePart = fullName.getNamePart(3);
    assertNull(namePart);
  }

  @Test
  public void hasPartName() throws ParseException {
    String[] argsWithoutName = {"catalog", "details", "--metalake", "metalake"};
    CommandLine commandLineWithoutName = new DefaultParser().parse(options, argsWithoutName);
    FullName fullNameWithoutName = new FullName(commandLineWithoutName);
    assertFalse(fullNameWithoutName.hasName());

    String[] argsWithName = {
      "catalog", "details", "--metalake", "metalake", "--name", "Hive_catalog"
    };
    CommandLine commandLineWithName = new DefaultParser().parse(options, argsWithName);
    FullName fullNameWithName = new FullName(commandLineWithName);
    assertTrue(fullNameWithName.hasName());
  }

  @Test
  public void hasPartNameMetalake() throws Exception {
    String[] args = {"metalake", "details", "--metalake", "metalake"};
    CommandLine commandLine = new DefaultParser().parse(options, args);
    FullName fullName = new FullName(commandLine);
    assertFalse(fullName.hasCatalogName());
    assertFalse(fullName.hasSchemaName());
    assertFalse(fullName.hasTableName());
    assertFalse(fullName.hasColumnName());
  }

  @Test
  public void hasPartNameCatalog() throws Exception {
    String[] args = {"catalog", "details", "--metalake", "metalake", "--name", "catalog"};
    CommandLine commandLine = new DefaultParser().parse(options, args);
    FullName fullName = new FullName(commandLine);
    assertTrue(fullName.hasCatalogName());
    assertFalse(fullName.hasSchemaName());
    assertFalse(fullName.hasTableName());
    assertFalse(fullName.hasColumnName());
  }

  @Test
  public void hasPartNameSchema() throws Exception {
    String[] args = {"schema", "details", "--metalake", "metalake", "--name", "catalog.schema"};
    CommandLine commandLine = new DefaultParser().parse(options, args);
    FullName fullName = new FullName(commandLine);
    assertTrue(fullName.hasCatalogName());
    assertTrue(fullName.hasSchemaName());
    assertFalse(fullName.hasTableName());
    assertFalse(fullName.hasColumnName());
  }

  @Test
  public void hasPartNameTable() throws Exception {
    String[] args = {
      "table", "details", "--metalake", "metalake", "--name", "catalog.schema.table"
    };
    CommandLine commandLine = new DefaultParser().parse(options, args);
    FullName fullName = new FullName(commandLine);
    assertTrue(fullName.hasCatalogName());
    assertTrue(fullName.hasSchemaName());
    assertTrue(fullName.hasTableName());
    assertFalse(fullName.hasColumnName());
  }

  @Test
  public void hasPartNameColumn() throws Exception {
    String[] args = {
      "table", "details", "--metalake", "metalake", "--name", "catalog.schema.table.column"
    };
    CommandLine commandLine = new DefaultParser().parse(options, args);
    FullName fullName = new FullName(commandLine);
    assertTrue(fullName.hasCatalogName());
    assertTrue(fullName.hasSchemaName());
    assertTrue(fullName.hasTableName());
    assertTrue(fullName.hasColumnName());
  }

  @Test
  @SuppressWarnings("DefaultCharset")
  public void testMissingName() throws ParseException {
    String[] args = {"column", "list", "-m", "demo_metalake", "-i"};
    CommandLine commandLine = new DefaultParser().parse(options, args);
    FullName fullName = new FullName(commandLine);
    fullName.getCatalogName();
    fullName.getSchemaName();
    fullName.getTableName();
    fullName.getColumnName();
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(output, ErrorMessages.MISSING_NAME);
  }

  @Test
  @SuppressWarnings("DefaultCharset")
  public void testMalformedName() throws ParseException {
    String[] args = {"column", "list", "-m", "demo_metalake", "-i", "--name", "Hive_catalog"};
    CommandLine commandLine = new DefaultParser().parse(options, args);
    FullName fullName = new FullName(commandLine);
    fullName.getCatalogName();
    fullName.getSchemaName();
    fullName.getTableName();
    fullName.getColumnName();
    String output = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(output, ErrorMessages.MALFORMED_NAME);
  }

  @Test
  @SuppressWarnings("DefaultCharset")
  public void testGetMetalake() throws ParseException {
    String[] args = {
      "table", "list", "-i", "-m", "demo_metalake", "--name", "Hive_catalog.default"
    };
    CommandLine commandLine = new DefaultParser().parse(options, args);
    FullName fullName = new FullName(commandLine);
    String metalakeName = fullName.getMetalakeName();
    assertEquals(metalakeName, "demo_metalake");
  }

  @Test
  @SuppressWarnings("DefaultCharset")
  public void testGetMetalakeWithoutMetalakeOption() throws ParseException {
    String[] args = {"table", "list", "-i", "--name", "Hive_catalog.default"};
    CommandLine commandLine = new DefaultParser().parse(options, args);
    FullName fullName = new FullName(commandLine);
    assertThrows(RuntimeException.class, fullName::getMetalakeName);
    String errOutput = new String(errContent.toByteArray(), StandardCharsets.UTF_8).trim();
    assertEquals(errOutput, ErrorMessages.MISSING_METALAKE);
  }

  @Test
  @SuppressWarnings("DefaultCharset")
  void testGetLevelFromCatalog() throws ParseException {
    String[] args = {"table", "list", "-i", "--name", "Hive_catalog"};
    CommandLine commandLine = new DefaultParser().parse(options, args);
    FullName fullName = new FullName(commandLine);
    assertEquals(1, fullName.getLevel());
  }

  @Test
  @SuppressWarnings("DefaultCharset")
  void testGetLevelFromSchema() throws ParseException {
    String[] args = {"table", "list", "-i", "--name", "Hive_catalog.default"};
    CommandLine commandLine = new DefaultParser().parse(options, args);
    FullName fullName = new FullName(commandLine);
    assertEquals(2, fullName.getLevel());
  }

  @Test
  @SuppressWarnings("DefaultCharset")
  void testGetLevelFromTable() throws ParseException {
    String[] args = {"table", "list", "-i", "--name", "Hive_catalog.default.sales"};
    CommandLine commandLine = new DefaultParser().parse(options, args);
    FullName fullName = new FullName(commandLine);
    assertEquals(3, fullName.getLevel());
  }

  @Test
  @SuppressWarnings("DefaultCharset")
  void testGetLevelFromColumn() throws ParseException {
    String[] args = {"table", "list", "-i", "--name", "Hive_catalog.default.sales.columns"};
    CommandLine commandLine = new DefaultParser().parse(options, args);
    FullName fullName = new FullName(commandLine);
    assertEquals(4, fullName.getLevel());
  }

  @Test
  void testHasNamePartWithNegativeIndex() throws Exception {
    String[] args = {"--name", "catalog.schema.table.column"};
    CommandLine commandLine = new DefaultParser().parse(options, args);
    FullName fullName = new FullName(commandLine);

    assertFalse(fullName.hasNamePart(0));

    assertTrue(fullName.hasNamePart(1));
    assertTrue(fullName.hasNamePart(2));
    assertTrue(fullName.hasNamePart(3));
    assertTrue(fullName.hasNamePart(4));

    assertFalse(fullName.hasNamePart(5));

    assertFalse(fullName.hasNamePart(-1));
    assertFalse(fullName.hasNamePart(-10));
  }
}
