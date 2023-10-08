/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.trino.connector.metadata;

import com.datastrato.graviton.dto.AuditDTO;
import com.datastrato.graviton.dto.rel.SchemaDTO;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestGravitonSchema {

  @Test
  void testGravitonSchema() {
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

    GravitonSchema schema = new GravitonSchema(schemaDTO);

    Assertions.assertEquals(schema.name(), schemaDTO.name());
    Assertions.assertEquals(schema.comment(), schemaDTO.comment());
    Assertions.assertEquals(schema.properties(), schemaDTO.properties());
  }
}
