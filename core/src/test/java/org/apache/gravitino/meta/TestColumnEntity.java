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
package org.apache.gravitino.meta;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestColumnEntity {

  @Test
  public void testColumnEntityFields() {
    ColumnEntity columnEntity =
        ColumnEntity.builder()
            .withId(1L)
            .withName("test")
            .withPosition(1)
            .withComment("test comment")
            .withDataType(Types.IntegerType.get())
            .withNullable(true)
            .withAutoIncrement(true)
            .withDefaultValue(Literals.integerLiteral(1))
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();

    Assertions.assertEquals(1L, columnEntity.id());
    Assertions.assertEquals("test", columnEntity.name());
    Assertions.assertEquals(1, columnEntity.position());
    Assertions.assertEquals("test comment", columnEntity.comment());
    Assertions.assertEquals(Types.IntegerType.get(), columnEntity.dataType());
    Assertions.assertTrue(columnEntity.nullable());
    Assertions.assertTrue(columnEntity.autoIncrement());
    Assertions.assertEquals(Literals.integerLiteral(1), columnEntity.defaultValue());

    ColumnEntity columnEntity2 =
        ColumnEntity.builder()
            .withId(1L)
            .withName("test")
            .withPosition(1)
            .withDataType(Types.IntegerType.get())
            .withNullable(true)
            .withAutoIncrement(true)
            .withDefaultValue(Literals.integerLiteral(1))
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();
    Assertions.assertNull(columnEntity2.comment());

    ColumnEntity columnEntity3 =
        ColumnEntity.builder()
            .withId(1L)
            .withName("test")
            .withPosition(1)
            .withComment("test comment")
            .withDataType(Types.IntegerType.get())
            .withNullable(true)
            .withAutoIncrement(true)
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();
    Assertions.assertEquals(Column.DEFAULT_VALUE_NOT_SET, columnEntity3.defaultValue());
  }

  @Test
  public void testWithoutRequiredFields() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          ColumnEntity.builder()
              .withId(1L)
              .withPosition(1)
              .withName("test")
              .withNullable(true)
              .withAutoIncrement(true)
              .withDefaultValue(Literals.integerLiteral(1))
              .withAuditInfo(
                  AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
              .build();
        });

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          ColumnEntity.builder()
              .withId(1L)
              .withPosition(1)
              .withComment("test comment")
              .withDataType(Types.IntegerType.get())
              .withAutoIncrement(true)
              .withDefaultValue(Literals.integerLiteral(1))
              .withAuditInfo(
                  AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
              .build();
        });

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          ColumnEntity.builder()
              .withId(1L)
              .withName("test")
              .withComment("test comment")
              .withDataType(Types.IntegerType.get())
              .withNullable(true)
              .withDefaultValue(Literals.integerLiteral(1))
              .withAuditInfo(
                  AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
              .build();
        });

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          ColumnEntity.builder()
              .withId(1L)
              .withComment("test comment")
              .withDataType(Types.IntegerType.get())
              .withNullable(true)
              .withAutoIncrement(true)
              .build();
        });
  }

  @Test
  public void testTableColumnEntity() {
    ColumnEntity columnEntity1 =
        ColumnEntity.builder()
            .withId(1L)
            .withName("test")
            .withPosition(1)
            .withComment("test comment")
            .withDataType(Types.IntegerType.get())
            .withNullable(true)
            .withAutoIncrement(true)
            .withDefaultValue(Literals.integerLiteral(1))
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();

    ColumnEntity columnEntity2 =
        ColumnEntity.builder()
            .withId(2L)
            .withName("test2")
            .withPosition(2)
            .withComment("test comment2")
            .withDataType(Types.StringType.get())
            .withNullable(true)
            .withAutoIncrement(true)
            .withDefaultValue(Literals.stringLiteral("2"))
            .withAuditInfo(
                AuditInfo.builder().withCreator("test2").withCreateTime(Instant.now()).build())
            .build();

    ColumnEntity columnEntity3 =
        ColumnEntity.builder()
            .withId(3L)
            .withName("test3")
            .withPosition(3)
            .withComment("test comment3")
            .withDataType(Types.BooleanType.get())
            .withNullable(true)
            .withAutoIncrement(true)
            .withDefaultValue(Literals.booleanLiteral(true))
            .withAuditInfo(
                AuditInfo.builder().withCreator("test3").withCreateTime(Instant.now()).build())
            .build();

    List<ColumnEntity> columns = Arrays.asList(columnEntity1, columnEntity2, columnEntity3);
    TableEntity tableEntity =
        TableEntity.builder()
            .withId(1L)
            .withName("test")
            .withNamespace(Namespace.of("catalog", "schema"))
            .withColumns(columns)
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();

    Assertions.assertEquals(1L, tableEntity.id());
    Assertions.assertEquals("test", tableEntity.name());
    Assertions.assertEquals(Namespace.of("catalog", "schema"), tableEntity.namespace());
    Assertions.assertEquals(columns, tableEntity.columns());
    Assertions.assertEquals(3, tableEntity.columns().size());
  }
}
