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
package org.apache.gravitino.trino.connector.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Test;

public class TestGravitinoColumn {

  @Test
  public void testGravitinoColumn() {
    Column column =
        Column.of(
            "f1", Types.StringType.get(), "test column", false, true, Literals.stringLiteral("1"));
    GravitinoColumn gravitinoColumn = new GravitinoColumn(column, 0);

    assertEquals(gravitinoColumn.getName(), column.name());
    assertEquals(gravitinoColumn.getIndex(), 0);
    assertEquals(gravitinoColumn.getComment(), column.comment());
    assertEquals(gravitinoColumn.getType(), column.dataType());
    assertEquals(gravitinoColumn.isNullable(), column.nullable());
    assertEquals(gravitinoColumn.isAutoIncrement(), column.autoIncrement());
    assertEquals(gravitinoColumn.getDefaultValue(), column.defaultValue());
  }
}
