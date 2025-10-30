/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.catalog.lakehouse.utils;

import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;

import java.util.List;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestEntityConverter {

  @Test
  void testToColumns() {
    AuditInfo auditInfo = AuditInfo.builder().build();
    List<ColumnEntity> columnEntities =
        List.of(
            ColumnEntity.builder()
                .withName("id")
                .withId(1L)
                .withDataType(Types.IntegerType.get())
                .withComment("Identifier")
                .withAutoIncrement(true)
                .withNullable(false)
                .withDefaultValue(DEFAULT_VALUE_NOT_SET)
                .withAuditInfo(auditInfo)
                .withPosition(1)
                .build(),
            ColumnEntity.builder()
                .withName("name")
                .withId(2L)
                .withDataType(Types.StringType.get())
                .withComment("Name of the entity")
                .withAutoIncrement(false)
                .withNullable(true)
                .withDefaultValue(DEFAULT_VALUE_NOT_SET)
                .withPosition(2)
                .withAuditInfo(auditInfo)
                .build());
    var columns = EntityConverter.toColumns(columnEntities);
    Assertions.assertEquals(2, columns.length);
    for (var column : columns) {
      if (column.name().equals("id")) {
        Assertions.assertEquals(Types.IntegerType.get(), column.dataType());
        Assertions.assertEquals("Identifier", column.comment());
        Assertions.assertTrue(column.autoIncrement());
        Assertions.assertFalse(column.nullable());
        Assertions.assertEquals(DEFAULT_VALUE_NOT_SET, column.defaultValue());
      } else if (column.name().equals("name")) {
        Assertions.assertEquals(Types.StringType.get(), column.dataType());
        Assertions.assertEquals("Name of the entity", column.comment());
        Assertions.assertFalse(column.autoIncrement());
        Assertions.assertTrue(column.nullable());
        Assertions.assertEquals(DEFAULT_VALUE_NOT_SET, column.defaultValue());
      }
    }
  }
}
