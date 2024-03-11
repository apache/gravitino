/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datastrato.gravitino.connector.BaseSchema;
import com.datastrato.gravitino.meta.AuditInfo;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class BaseSchemaExtension extends BaseSchema {

  private BaseSchemaExtension() {}

  public static class Builder extends BaseSchemaBuilder<Builder, BaseSchemaExtension> {

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
        new BaseSchemaExtension.Builder()
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
