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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.connector.BaseTable;
import org.apache.gravitino.connector.TableOperations;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.junit.jupiter.api.Test;

final class BaseTableExtension extends BaseTable {

  private BaseTableExtension() {}

  @Override
  protected TableOperations newOps() {
    throw new UnsupportedOperationException("BaseTableExtension does not support TableOperations.");
  }

  public static class Builder extends BaseTableBuilder<Builder, BaseTableExtension> {
    /** Creates a new instance of {@link Builder}. */
    private Builder() {}

    @Override
    protected BaseTableExtension internalBuild() {
      BaseTableExtension table = new BaseTableExtension();
      table.name = name;
      table.comment = comment;
      table.properties = properties;
      table.auditInfo = auditInfo;
      table.columns = columns;
      table.partitioning = partitioning;
      return table;
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

public class TestBaseTable {

  @Test
  void testTableFields() {
    Map<String, String> properties = new HashMap<>();
    properties.put("key1", "value1");
    properties.put("key2", "value2");
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("Justin").withCreateTime(Instant.now()).build();

    BaseTable table =
        BaseTableExtension.builder()
            .withName("testTableName")
            .withComment("testTableComment")
            .withColumns(new Column[0])
            .withProperties(properties)
            .withPartitioning(new Transform[0])
            .withAuditInfo(auditInfo)
            .build();

    assertEquals("testTableName", table.name());
    assertEquals("testTableComment", table.comment());
    assertEquals(properties, table.properties());
    assertEquals(auditInfo, table.auditInfo());
    assertArrayEquals(new Column[0], table.columns());
    assertArrayEquals(new Transform[0], table.partitioning());
  }
}
