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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.Options;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestFulllName {

  private Options options;

  @BeforeEach
  public void setUp() {
    options = new GravitinoOptions().options();
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

    String metalakeName = fullName.getMetalakeName();
    assertNull(metalakeName);
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
}
