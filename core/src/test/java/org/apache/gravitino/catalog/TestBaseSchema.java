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

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.connector.BaseSchema;
import org.apache.gravitino.meta.AuditInfo;
import org.junit.jupiter.api.Test;

final class BaseSchemaExtension extends BaseSchema {

  private BaseSchemaExtension() {}

  public static class Builder extends BaseSchemaBuilder<Builder, BaseSchemaExtension> {
    /** Creates a new instance of {@link Builder}. */
    private Builder() {}

    @Override
    protected BaseSchemaExtension internalBuild() {
      BaseSchemaExtension schema = new BaseSchemaExtension();
      schema.name = name;
      schema.comment = comment;
      schema.properties = properties;
      schema.auditInfo = auditInfo;
      return schema;
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

public class TestBaseSchema {

  @Test
  void testSchemaFields() {
    Map<String, String> properties = new HashMap<>();
    properties.put("key1", "value1");
    properties.put("key2", "value2");
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("Justin").withCreateTime(Instant.now()).build();

    BaseSchema schema =
        BaseSchemaExtension.builder()
            .withName("testSchemaName")
            .withComment("testSchemaComment")
            .withProperties(properties)
            .withAuditInfo(auditInfo)
            .build();

    assertEquals("testSchemaName", schema.name());
    assertEquals("testSchemaComment", schema.comment());
    assertEquals(properties, schema.properties());
    assertEquals(auditInfo, schema.auditInfo());
  }
}
