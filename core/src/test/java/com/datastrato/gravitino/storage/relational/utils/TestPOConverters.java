/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.relational.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datastrato.gravitino.json.JsonUtils;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.BaseMetalake;
import com.datastrato.gravitino.meta.SchemaVersion;
import com.datastrato.gravitino.storage.relational.po.MetalakePO;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
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
  public void testFromMetalakePOs() throws JsonProcessingException {
    MetalakePO metalakePO1 = createMetalakePO(1L, "test", "this is test");
    MetalakePO metalakePO2 = createMetalakePO(2L, "test2", "this is test2");
    List<MetalakePO> metalakePOs = new ArrayList<>(Arrays.asList(metalakePO1, metalakePO2));
    List<BaseMetalake> convertedMetalakes = POConverters.fromMetalakePOs(metalakePOs);

    BaseMetalake expectedMetalake1 = createMetalake(1L, "test", "this is test");
    BaseMetalake expectedMetalake2 = createMetalake(2L, "test2", "this is test2");
    List<BaseMetalake> expectedMetalakes =
        new ArrayList<>(Arrays.asList(expectedMetalake1, expectedMetalake2));

    // Assert
    int index = 0;
    for (BaseMetalake metalake : convertedMetalakes) {
      assertEquals(expectedMetalakes.get(index).id(), metalake.id());
      assertEquals(expectedMetalakes.get(index).name(), metalake.name());
      assertEquals(expectedMetalakes.get(index).comment(), metalake.comment());
      assertEquals(
          expectedMetalakes.get(index).properties().get("key"), metalake.properties().get("key"));
      assertEquals(
          expectedMetalakes.get(index).auditInfo().creator(), metalake.auditInfo().creator());
      assertEquals(expectedMetalakes.get(index).getVersion(), metalake.getVersion());
      index++;
    }
  }

  @Test
  public void testInitMetalakePOVersion() throws JsonProcessingException {
    BaseMetalake metalakePO = createMetalake(1L, "test", "this is test");
    MetalakePO initPO = POConverters.initializeMetalakePOWithVersion(metalakePO);
    assertEquals(1, initPO.getCurrentVersion());
    assertEquals(1, initPO.getLastVersion());
    assertEquals(0, initPO.getDeletedAt());
  }

  @Test
  public void testUpdateMetalakePOVersion() throws JsonProcessingException {
    BaseMetalake metalake = createMetalake(1L, "test", "this is test");
    BaseMetalake updatedMetalake = createMetalake(1L, "test", "this is test2");
    MetalakePO initPO = POConverters.initializeMetalakePOWithVersion(metalake);
    MetalakePO updatePO = POConverters.updateMetalakePOWithVersion(initPO, updatedMetalake);
    assertEquals(1, initPO.getCurrentVersion());
    assertEquals(1, initPO.getLastVersion());
    assertEquals(0, initPO.getDeletedAt());
    assertEquals("this is test2", updatePO.getMetalakeComment());
  }

  @Test
  public void testToMetalakePO() throws JsonProcessingException {
    BaseMetalake metalake = createMetalake(1L, "test", "this is test");

    MetalakePO expectedMetalakePO = createMetalakePO(1L, "test", "this is test");

    MetalakePO actualMetalakePO = POConverters.toMetalakePO(metalake);

    // Assert
    assertEquals(expectedMetalakePO.getMetalakeId(), actualMetalakePO.getMetalakeId());
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
        .withMetalakeId(id)
        .withMetalakeName(name)
        .withMetalakeComment(comment)
        .withProperties(JsonUtils.anyFieldMapper().writeValueAsString(properties))
        .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(auditInfo))
        .withSchemaVersion(JsonUtils.anyFieldMapper().writeValueAsString(SchemaVersion.V_0_1))
        .build();
  }
}
