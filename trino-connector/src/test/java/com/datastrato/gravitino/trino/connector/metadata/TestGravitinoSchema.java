/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.metadata;

import com.datastrato.gravitino.dto.AuditDTO;
import com.datastrato.gravitino.dto.rel.SchemaDTO;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestGravitinoSchema {

  @Test
  void testGravitinoSchema() {
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

    Assertions.assertEquals(schema.getName(), schemaDTO.name());
    Assertions.assertEquals(schema.getComment(), schemaDTO.comment());
    Assertions.assertEquals(schema.getProperties(), schemaDTO.properties());
  }
}
