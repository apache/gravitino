/*
* Copyright 2023 Datastrato.
* This software is licensed under the Apache License version 2.
*/

package com.datastrato.graviton.meta;

import com.datastrato.graviton.Catalog;
import com.datastrato.graviton.Field;
import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestEntity {
  private final Instant now = Instant.now();

  private final SchemaVersion version = SchemaVersion.V_0_1;
  private final AuditInfo auditInfo =
      new AuditInfo.Builder().withCreator("test").withCreateTime(now).build();

  // Metalake test data
  private final Long metalakeId = 1L;
  private final String metalakeName = "testMetalake";
  private final Map<String, String> map = ImmutableMap.of("k1", "v1", "k2", "v2");

  // Catalog test data
  private final Long catalogId = 1L;
  private final String catalogName = "testCatalog";
  private final Catalog.Type type = Catalog.Type.RELATIONAL;

  @Test
  public void testMetalake() {
    BaseMetalake metalake =
        new BaseMetalake.Builder()
            .withId(metalakeId)
            .withName(metalakeName)
            .withAuditInfo(auditInfo)
            .withProperties(map)
            .withVersion(version)
            .build();

    Map<Field, Object> fields = metalake.fields();
    Assertions.assertEquals(metalakeId, fields.get(BaseMetalake.ID));
    Assertions.assertEquals(metalakeName, fields.get(BaseMetalake.NAME));
    Assertions.assertEquals(map, fields.get(BaseMetalake.PROPERTIES));
    Assertions.assertEquals(auditInfo, fields.get(BaseMetalake.AUDIT_INFO));
    Assertions.assertNull(fields.get(BaseMetalake.COMMENT));
    Assertions.assertEquals(version, fields.get(BaseMetalake.SCHEMA_VERSION));
  }

  @Test
  public void testCatalog() {
    String catalogComment = "testComment";
    CatalogEntity testCatalog =
        new CatalogEntity.Builder()
            .withId(catalogId)
            .withMetalakeId(metalakeId)
            .withName(catalogName)
            .withComment(catalogComment)
            .withType(type)
            .withProperties(map)
            .withAuditInfo(auditInfo)
            .build();

    Map<Field, Object> fields = testCatalog.fields();
    Assertions.assertEquals(catalogId, fields.get(CatalogEntity.ID));
    Assertions.assertEquals(metalakeId, fields.get(CatalogEntity.METALAKE_ID));
    Assertions.assertEquals(catalogName, fields.get(CatalogEntity.NAME));
    Assertions.assertEquals(catalogComment, fields.get(CatalogEntity.COMMENT));
    Assertions.assertEquals(type, fields.get(CatalogEntity.TYPE));
    Assertions.assertEquals(map, fields.get(CatalogEntity.PROPERTIES));
    Assertions.assertEquals(auditInfo, fields.get(CatalogEntity.AUDIT_INFO));
  }
}
