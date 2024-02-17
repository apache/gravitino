/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.relational.utils;

import com.datastrato.gravitino.json.JsonUtils;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.BaseMetalake;
import com.datastrato.gravitino.meta.SchemaVersion;
import com.datastrato.gravitino.storage.relational.po.MetalakePO;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** POConverters is a utility class to convert PO to Base and vice versa. */
public class POConverters {

  private POConverters() {}

  /**
   * Convert {@link BaseMetalake} to {@link MetalakePO}
   *
   * @param baseMetalake BaseMetalake object
   * @return MetalakePO object from BaseMetalake object
   */
  public static MetalakePO toMetalakePO(BaseMetalake baseMetalake) {
    try {
      return new MetalakePO.Builder()
          .withId(baseMetalake.id())
          .withMetalakeName(baseMetalake.name())
          .withMetalakeComment(baseMetalake.comment())
          .withProperties(JsonUtils.anyFieldMapper().writeValueAsString(baseMetalake.properties()))
          .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(baseMetalake.auditInfo()))
          .withVersion(JsonUtils.anyFieldMapper().writeValueAsString(baseMetalake.getVersion()))
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize json object:", e);
    }
  }

  /**
   * Convert {@link MetalakePO} to {@link BaseMetalake}
   *
   * @param metalakePO MetalakePO object
   * @return BaseMetalake object from MetalakePO object
   */
  public static BaseMetalake fromMetalakePO(MetalakePO metalakePO) {
    try {
      return new BaseMetalake.Builder()
          .withId(metalakePO.getId())
          .withName(metalakePO.getMetalakeName())
          .withComment(metalakePO.getMetalakeComment())
          .withProperties(
              JsonUtils.anyFieldMapper().readValue(metalakePO.getProperties(), Map.class))
          .withAuditInfo(
              JsonUtils.anyFieldMapper().readValue(metalakePO.getAuditInfo(), AuditInfo.class))
          .withVersion(
              JsonUtils.anyFieldMapper()
                  .readValue(metalakePO.getSchemaVersion(), SchemaVersion.class))
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to deserialize json object:", e);
    }
  }

  /**
   * Convert list of {@link MetalakePO} to list of {@link BaseMetalake}
   *
   * @param metalakePOS list of MetalakePO objects
   * @return list of BaseMetalake objects from list of MetalakePO objects
   */
  public static List<BaseMetalake> fromMetalakePOs(List<MetalakePO> metalakePOS) {
    return metalakePOS.stream().map(POConverters::fromMetalakePO).collect(Collectors.toList());
  }
}
