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
package org.apache.gravitino.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.apache.gravitino.connector.BaseColumn;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Test;

final class BaseColumnExtension extends BaseColumn {
  private BaseColumnExtension() {}

  public static class Builder extends BaseColumnBuilder<Builder, BaseColumnExtension> {

    /** Creates a new instance of {@link Builder}. */
    private Builder() {}

    @Override
    protected BaseColumnExtension internalBuild() {
      BaseColumnExtension column = new BaseColumnExtension();
      column.name = name;
      column.comment = comment;
      column.dataType = dataType;
      column.nullable = nullable;
      column.auditInfo = auditInfo;
      return column;
    }
  }

  /**
   * Creates a new instance of {@link Builder}.
   *
   * @return The new instance.
   */
  public static Builder builder() {
    return new Builder();
  }
}

public class TestBaseColumn {

  @Test
  void testColumnFields() {
    BaseColumn column =
        BaseColumnExtension.builder()
            .withName("testColumnName")
            .withComment("testColumnComment")
            .withType(Types.IntegerType.get())
            .build();

    assertEquals("testColumnName", column.name());
    assertEquals("testColumnComment", column.comment());
    assertEquals(Types.IntegerType.get(), column.dataType());
  }

  @Test
  void testEqualsAndHashCode() {
    BaseColumn column1 =
        BaseColumnExtension.builder()
            .withName("testColumnName")
            .withComment("testColumnComment")
            .withType(Types.StringType.get())
            .build();

    BaseColumn column2 =
        BaseColumnExtension.builder()
            .withName("testColumnName")
            .withComment("testColumnComment")
            .withType(Types.StringType.get())
            .build();

    BaseColumn column3 =
        BaseColumnExtension.builder()
            .withName("differentColumnName")
            .withComment("testColumnComment")
            .withType(Types.StringType.get())
            .build();

    BaseColumn column4 =
        BaseColumnExtension.builder()
            .withName("testColumnName")
            .withComment("testColumnComment")
            .withType(Types.IntegerType.get())
            .build();

    assertEquals(column1, column2);
    assertEquals(column1.hashCode(), column2.hashCode());
    assertNotEquals(column1, column3);
    assertNotEquals(column1, column4);
  }
}
