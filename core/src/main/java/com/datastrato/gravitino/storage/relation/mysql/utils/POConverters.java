/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.relation.mysql.utils;

import com.datastrato.gravitino.json.JsonUtils;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.BaseMetalake;
import com.datastrato.gravitino.meta.SchemaVersion;
import com.datastrato.gravitino.storage.relation.mysql.po.MetalakePO;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.Map;

public class POConverters {
  private POConverters() {}

  public static MetalakePO toMetalakePO(BaseMetalake baseMetalake) throws JsonProcessingException {
    return new MetalakePO.Builder()
        .withId(baseMetalake.id())
        .withMetalakeName(baseMetalake.name())
        .withMetalakeComment(baseMetalake.comment())
        .withProperties(baseMetalake.properties())
        .withAuditInfo((AuditInfo) baseMetalake.auditInfo())
        .withVersion(baseMetalake.getVersion())
        .build();
  }

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
