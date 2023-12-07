/*
 * Copyright 2023 DATASTRATO Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.metadata;

import static org.testng.Assert.assertEquals;

import com.datastrato.gravitino.dto.AuditDTO;
import com.datastrato.gravitino.dto.rel.SchemaDTO;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

public class TestGravitinoSchema {

  @Test
  public void testGravitinoSchema() {
    Map<String, String> properties = new HashMap<>();
    properties.put("prop1", "test prop1");

    SchemaDTO schemaDTO =
        new SchemaDTO.Builder()
            .withName("db1")
            .withComment("test schema")
            .withProperties(properties)
            .withAudit(
                new AuditDTO.Builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();

    GravitinoSchema schema = new GravitinoSchema(schemaDTO);

    assertEquals(schema.getName(), schemaDTO.name());
    assertEquals(schema.getComment(), schemaDTO.comment());
    assertEquals(schema.getProperties(), schemaDTO.properties());
  }
}
