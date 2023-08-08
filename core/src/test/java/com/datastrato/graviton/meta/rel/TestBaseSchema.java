/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.meta.rel;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datastrato.graviton.Entity;
import com.datastrato.graviton.Field;
import com.datastrato.graviton.meta.AuditInfo;
import com.google.common.collect.Maps;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class BaseSchemaTest {

  private final class BaseSchemaExtension extends BaseSchema {
    @Override
    public void validate() {}
  }

  @Test
  void testSchemaFields() {
    BaseSchema schema = new BaseSchemaExtension();
    schema.id = 1L;
    schema.catalogId = 2L;
    schema.name = "testSchemaName";
    schema.comment = "testSchemaComment";
    schema.properties = new HashMap<>();
    schema.properties.put("key1", "value1");
    schema.properties.put("key2", "value2");
    schema.auditInfo =
        new AuditInfo.Builder().withCreator("Justin").withCreateTime(Instant.now()).build();
    Map<Field, Object> expectedFields = Maps.newHashMap();
    expectedFields.put(BaseSchema.ID, 1L);
    expectedFields.put(BaseSchema.CATALOG_ID, 2L);
    expectedFields.put(BaseSchema.NAME, "testSchemaName");
    expectedFields.put(BaseSchema.COMMENT, "testSchemaComment");
    expectedFields.put(BaseSchema.PROPERTIES, schema.properties);
    expectedFields.put(BaseSchema.AUDIT_INFO, schema.auditInfo);

    assertEquals(1L, schema.fields().get(BaseSchema.ID));
    assertEquals(2L, schema.fields().get(BaseSchema.CATALOG_ID));
    assertEquals("testSchemaName", schema.fields().get(BaseSchema.NAME));
    assertEquals("testSchemaComment", schema.fields().get(BaseSchema.COMMENT));
    assertEquals(schema.properties, schema.fields().get(BaseSchema.PROPERTIES));
    assertEquals(schema.auditInfo, schema.fields().get(BaseSchema.AUDIT_INFO));
  }

  @Test
  void testSchemaType() {
    BaseSchema schema = new BaseSchemaExtension();

    assertEquals(Entity.EntityType.SCHEMA, schema.type());
  }

  @Test
  void testEqualsAndHashCode() {
    Instant now = Instant.now();
    BaseSchema schema1 = new BaseSchemaExtension();
    schema1.id = 1L;
    schema1.catalogId = 2L;
    schema1.name = "testSchemaName";
    schema1.comment = "testSchemaComment";
    schema1.properties = new HashMap<>();
    schema1.properties.put("key1", "value1");
    schema1.properties.put("key2", "value2");
    schema1.auditInfo = new AuditInfo.Builder().withCreator("Justin").withCreateTime(now).build();

    BaseSchema schema2 = new BaseSchemaExtension();
    schema2.id = 1L;
    schema2.catalogId = 2L;
    schema2.name = "testSchemaName";
    schema2.comment = "testSchemaComment";
    schema2.properties = new HashMap<>();
    schema2.properties.put("key1", "value1");
    schema2.properties.put("key2", "value2");
    schema2.auditInfo = new AuditInfo.Builder().withCreator("Justin").withCreateTime(now).build();

    assertEquals(schema1, schema2);
    assertEquals(schema1.hashCode(), schema2.hashCode());
  }
}
