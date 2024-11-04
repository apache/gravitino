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
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.gravitino.rel.TableChange;
import org.junit.jupiter.api.Test;

public class TestPositionConverter {

  @Test
  public void testConvertFirstPosition() {
    TableChange.ColumnPosition position = PositionConverter.convert("first");
    assertNotNull(position);
    assertEquals(TableChange.ColumnPosition.first(), position);
  }

  @Test
  public void testConvertNullPosition() {
    TableChange.ColumnPosition position = PositionConverter.convert(null);
    assertNotNull(position);
    assertEquals(TableChange.ColumnPosition.defaultPos(), position);
  }

  @Test
  public void testConvertEmptyPosition() {
    TableChange.ColumnPosition position = PositionConverter.convert("");
    assertNotNull(position);
    assertEquals(TableChange.ColumnPosition.defaultPos(), position);
  }

  @Test
  public void testConvertValidColumnName() {
    String columnName = "column1";
    TableChange.ColumnPosition position = PositionConverter.convert(columnName);
    assertNotNull(position);
    assertEquals(TableChange.ColumnPosition.after(columnName), position);
  }

  @Test
  public void testConvertCaseInsensitiveColumnName() {
    String columnName = "COLUMN2";
    TableChange.ColumnPosition position = PositionConverter.convert(columnName);
    assertNotNull(position);
    assertEquals(TableChange.ColumnPosition.after(columnName), position);
  }

  @Test
  public void testConvertWhitespaceColumnName() {
    String columnName = "   column3   ";
    TableChange.ColumnPosition position = PositionConverter.convert(columnName.trim());
    assertNotNull(position);
    assertEquals(TableChange.ColumnPosition.after("column3"), position);
  }

  @Test
  public void testConvertUnrecognizedPosition() {
    String unrecognized = "unrecognized";
    TableChange.ColumnPosition position = PositionConverter.convert(unrecognized);
    assertNotNull(position);
    assertEquals(TableChange.ColumnPosition.after(unrecognized), position);
  }
}
