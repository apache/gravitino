package com.datastrato.graviton.meta;

import com.datastrato.graviton.Field;
import com.datastrato.graviton.TestCatalog;
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

  // Lakehouse test data
  private final Long lakehouseId = 1L;
  private final String lakehouseName = "testLakehouse";
  private final Map<String, String> map = ImmutableMap.of("k1", "v1", "k2", "v2");

  // Catalog test data
  private final Long catalogId = 1L;
  private final String catalogName = "testCatalog";
  private final BaseCatalog.Type type = BaseCatalog.Type.RELATIONAL;
  private final String catalogComment = "testComment";

  @Test
  public void testLakehouse() {
    BaseLakehouse lakehouse =
        new BaseLakehouse.Builder()
            .withId(lakehouseId)
            .withName(lakehouseName)
            .withAuditInfo(auditInfo)
            .withProperties(map)
            .withVersion(version)
            .build();

    Map<Field, Object> fields = lakehouse.fields();
    Assertions.assertEquals(lakehouseId, fields.get(BaseLakehouse.ID));
    Assertions.assertEquals(lakehouseName, fields.get(BaseLakehouse.NAME));
    Assertions.assertEquals(map, fields.get(BaseLakehouse.PROPERTIES));
    Assertions.assertEquals(auditInfo, fields.get(BaseLakehouse.AUDIT_INFO));
    Assertions.assertNull(fields.get(BaseLakehouse.COMMENT));
    Assertions.assertEquals(version, fields.get(BaseLakehouse.SCHEMA_VERSION));
  }

  @Test
  public void testCatalog() {
    TestCatalog testCatalog =
        new TestCatalog.Builder()
            .withId(catalogId)
            .withLakehouseId(lakehouseId)
            .withName(catalogName)
            .withComment(catalogComment)
            .withType(type)
            .withProperties(map)
            .withAuditInfo(auditInfo)
            .build();

    Map<Field, Object> fields = testCatalog.fields();
    Assertions.assertEquals(catalogId, fields.get(TestCatalog.ID));
    Assertions.assertEquals(lakehouseId, fields.get(TestCatalog.LAKEHOUSE_ID));
    Assertions.assertEquals(catalogName, fields.get(TestCatalog.NAME));
    Assertions.assertEquals(catalogComment, fields.get(TestCatalog.COMMENT));
    Assertions.assertEquals(type, fields.get(TestCatalog.TYPE));
    Assertions.assertEquals(map, fields.get(TestCatalog.PROPERTIES));
    Assertions.assertEquals(auditInfo, fields.get(TestCatalog.AUDIT_INFO));
  }
}
