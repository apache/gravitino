/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.relational.mysql.utils;

import com.datastrato.gravitino.json.JsonUtils;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.BaseMetalake;
import com.datastrato.gravitino.meta.SchemaVersion;
import com.datastrato.gravitino.storage.relational.mysql.po.MetalakePO;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.Map;

/** POConverters is a utility class to convert PO to Base and vice versa. */
public class POConverters {
  private POConverters() {}

  /**
   * Convert {@link BaseMetalake} to {@link MetalakePO}
   *
   * @param baseMetalake BaseMetalake object
   * @return MetalakePO object from BaseMetalake object
   * @throws JsonProcessingException
   */
  public static MetalakePO toMetalakePO(BaseMetalake baseMetalake) throws JsonProcessingException {
    return new MetalakePO.Builder()
        .withId(baseMetalake.id())
        .withMetalakeName(baseMetalake.name())
        .withMetalakeComment(baseMetalake.comment())
        .withProperties(JsonUtils.objectMapper().writeValueAsString(baseMetalake.properties()))
        .withAuditInfo(JsonUtils.objectMapper().writeValueAsString(baseMetalake.auditInfo()))
        .withVersion(JsonUtils.objectMapper().writeValueAsString(baseMetalake.getVersion()))
        .build();
  }

  /**
   * Convert {@link MetalakePO} to {@link BaseMetalake}
   *
   * @param metalakePO MetalakePO object
   * @return BaseMetalake object from MetalakePO object
   * @throws JsonProcessingException
   */
  public static BaseMetalake fromMetalakePO(MetalakePO metalakePO) throws JsonProcessingException {
    return new BaseMetalake.Builder()
        .withId(metalakePO.getId())
        .withName(metalakePO.getMetalakeName())
        .withComment(metalakePO.getMetalakeComment())
        .withProperties(JsonUtils.objectMapper().readValue(metalakePO.getProperties(), Map.class))
        .withAuditInfo(
            JsonUtils.objectMapper().readValue(metalakePO.getAuditInfo(), AuditInfo.class))
        .withVersion(
            JsonUtils.objectMapper().readValue(metalakePO.getSchemaVersion(), SchemaVersion.class))
        .build();
  }
}
