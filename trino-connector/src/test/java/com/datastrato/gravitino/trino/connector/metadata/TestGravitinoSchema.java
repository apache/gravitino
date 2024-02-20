/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.metadata;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.datastrato.gravitino.Audit;
import com.datastrato.gravitino.rel.Schema;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

public class TestGravitinoSchema {

  @Test
  public void testGravitinoSchema() {
    Map<String, String> properties = new HashMap<>();
    properties.put("prop1", "test prop1");

    Schema mockSchema = mockSchema("db1", "test schema", properties);
    GravitinoSchema schema = new GravitinoSchema(mockSchema);

    assertEquals(schema.getName(), mockSchema.name());
    assertEquals(schema.getComment(), mockSchema.comment());
    assertEquals(schema.getProperties(), mockSchema.properties());
  }

  public static Schema mockSchema(String name, String comment, Map<String, String> properties) {
    Schema mockSchema = mock(Schema.class);
    when(mockSchema.name()).thenReturn(name);
    when(mockSchema.comment()).thenReturn(comment);
    when(mockSchema.properties()).thenReturn(properties);

    Audit mockAudit = mock(Audit.class);
    when(mockAudit.creator()).thenReturn("gravitino");
    when(mockAudit.createTime()).thenReturn(Instant.now());
    when(mockSchema.auditInfo()).thenReturn(mockAudit);

    return mockSchema;
  }
}
