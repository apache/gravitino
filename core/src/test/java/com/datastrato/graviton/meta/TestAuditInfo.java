/*
* Copyright 2023 Datastrato.
* This software is licensed under the Apache License version 2.
*/

package com.datastrato.graviton.meta;

import java.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestAuditInfo {

  @Test
  public void testAuditInfoField() {
    Instant now = Instant.now();
    String creator = "test";
    String lastModifier = "test1";

    AuditInfo auditInfo =
        new AuditInfo.Builder()
            .withCreator(creator)
            .withCreateTime(now)
            .withLastModifier(lastModifier)
            .withLastModifiedTime(now)
            .build();

    auditInfo.validate();

    Assertions.assertEquals(creator, auditInfo.creator());
    Assertions.assertEquals(now, auditInfo.createTime());
    Assertions.assertEquals(lastModifier, auditInfo.lastModifier());
    Assertions.assertEquals(now, auditInfo.lastModifiedTime());
  }

  @Test
  public void testAuditInfoValidate() {
    Instant now = Instant.now();
    String creator = "test";
    String lastModifier = "test1";

    AuditInfo auditInfo = new AuditInfo.Builder().withCreator(creator).withCreateTime(now).build();

    auditInfo.validate();
    Assertions.assertNull(auditInfo.lastModifier());
    Assertions.assertNull(auditInfo.lastModifiedTime());

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
  }
}
