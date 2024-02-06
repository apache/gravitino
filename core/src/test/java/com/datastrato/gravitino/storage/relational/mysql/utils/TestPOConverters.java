/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.relational.mysql.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datastrato.gravitino.json.JsonUtils;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.BaseMetalake;
import com.datastrato.gravitino.meta.SchemaVersion;
import com.datastrato.gravitino.storage.relational.mysql.po.MetalakePO;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class TestPOConverters {
  private static final LocalDateTime FIX_DATE_TIME = LocalDateTime.of(2024, 2, 6, 0, 0, 0);

  private static final Instant FIX_INSTANT = FIX_DATE_TIME.toInstant(ZoneOffset.UTC);

  @Test
  public void testFromMetalakePO() throws JsonProcessingException {
    MetalakePO metalakePO = createMetalakePO(1L, "test", "this is test");

    BaseMetalake expectedMetalake = createMetalake(1L, "test", "this is test");

    BaseMetalake convertedMetalake = POConverters.fromMetalakePO(metalakePO);

    // Assert
    assertEquals(expectedMetalake.id(), convertedMetalake.id());
    assertEquals(expectedMetalake.name(), convertedMetalake.name());
    assertEquals(expectedMetalake.comment(), convertedMetalake.comment());
    assertEquals(
        expectedMetalake.properties().get("key"), convertedMetalake.properties().get("key"));
    assertEquals(expectedMetalake.auditInfo().creator(), convertedMetalake.auditInfo().creator());
    assertEquals(expectedMetalake.getVersion(), convertedMetalake.getVersion());
  }

  @Test
  public void testToMetalakePO() throws JsonProcessingException {
    BaseMetalake metalake = createMetalake(1L, "test", "this is test");

    MetalakePO expectedMetalakePO = createMetalakePO(1L, "test", "this is test");

    MetalakePO actualMetalakePO = POConverters.toMetalakePO(metalake);

    // Assert
    assertEquals(expectedMetalakePO.getId(), actualMetalakePO.getId());
    assertEquals(expectedMetalakePO.getMetalakeName(), actualMetalakePO.getMetalakeName());
    assertEquals(expectedMetalakePO.getMetalakeComment(), actualMetalakePO.getMetalakeComment());
    assertEquals(expectedMetalakePO.getProperties(), actualMetalakePO.getProperties());
    assertEquals(expectedMetalakePO.getAuditInfo(), actualMetalakePO.getAuditInfo());
    assertEquals(expectedMetalakePO.getSchemaVersion(), actualMetalakePO.getSchemaVersion());
  }

  private static BaseMetalake createMetalake(Long id, String name, String comment) {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(FIX_INSTANT).build();
    Map<String, String> properties = new HashMap<>();
    properties.put("key", "value");
    return new BaseMetalake.Builder()
        .withId(id)
        .withName(name)
        .withComment(comment)
        .withProperties(properties)
        .withAuditInfo(auditInfo)
        .withVersion(SchemaVersion.V_0_1)
        .build();
  }

  private static MetalakePO createMetalakePO(Long id, String name, String comment)
      throws JsonProcessingException {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(FIX_INSTANT).build();
    Map<String, String> properties = new HashMap<>();
    properties.put("key", "value");
    return new MetalakePO.Builder()
        .withId(id)
        .withMetalakeName(name)
        .withMetalakeComment(comment)
        .withProperties(JsonUtils.anyFieldMapper().writeValueAsString(properties))
        .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(auditInfo))
        .withVersion(JsonUtils.anyFieldMapper().writeValueAsString(SchemaVersion.V_0_1))
        .build();
  }
}
