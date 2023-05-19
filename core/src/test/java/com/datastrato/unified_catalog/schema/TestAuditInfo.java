package com.datastrato.unified_catalog.schema;

import java.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestAuditInfo {

  @Test
  public void testAuditInfoField() {
    Instant now = Instant.now();
    String creator = "test";
    String lastModifier = "test1";
    String lastAccessUser = "test2";

    AuditInfo auditInfo =
        new AuditInfo.Builder()
            .withCreator(creator)
            .withCreateTime(now)
            .withLastModifier(lastModifier)
            .withLastModifiedTime(now)
            .withLastAccessUser(lastAccessUser)
            .withLastAccessTime(now)
            .build();

    auditInfo.validate();

    Assertions.assertEquals(creator, auditInfo.getCreator());
    Assertions.assertEquals(now, auditInfo.getCreateTime());
    Assertions.assertEquals(lastModifier, auditInfo.getLastModifier());
    Assertions.assertEquals(now, auditInfo.getLastModifiedTime());
    Assertions.assertEquals(lastAccessUser, auditInfo.getLastAccessUser());
    Assertions.assertEquals(now, auditInfo.getLastAccessTime());
  }

  @Test
  public void testAuditInfoValidate() {
    Instant now = Instant.now();
    String creator = "test";
    String lastModifier = "test1";
    String lastAccessUser = "test2";

    AuditInfo auditInfo = new AuditInfo.Builder().withCreator(creator).withCreateTime(now).build();

    auditInfo.validate();
    Assertions.assertNull(auditInfo.getLastModifier());
    Assertions.assertNull(auditInfo.getLastModifiedTime());
    Assertions.assertNull(auditInfo.getLastAccessUser());
    Assertions.assertNull(auditInfo.getLastAccessTime());

    Throwable exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new AuditInfo.Builder().withCreator(creator).build());
    Assertions.assertEquals("Field create_time is required", exception.getMessage());

    Throwable exception1 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> {
              new AuditInfo.Builder()
                  .withCreator(creator)
                  .withCreateTime(now)
                  .withLastModifier(lastModifier)
                  .build();
            });
    Assertions.assertEquals(
        "last_modifier and last_modified_time must be both set or both not set",
        exception1.getMessage());

    Throwable exception2 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> {
              new AuditInfo.Builder()
                  .withCreator(creator)
                  .withCreateTime(now)
                  .withLastAccessUser(lastAccessUser)
                  .build();
            });
    Assertions.assertEquals(
        "last_access_user and last_access_time must be both set or both not set",
        exception2.getMessage());
  }
}
