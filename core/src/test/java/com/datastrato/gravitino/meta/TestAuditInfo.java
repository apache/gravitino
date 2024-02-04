/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.meta;

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
        AuditInfo.builder()
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

    AuditInfo auditInfo = AuditInfo.builder().withCreator(creator).withCreateTime(now).build();

    auditInfo.validate();
    Assertions.assertNull(auditInfo.lastModifier());
    Assertions.assertNull(auditInfo.lastModifiedTime());

    AuditInfo auditInfo1 =
        AuditInfo.builder().withLastModifier(lastModifier).withLastModifiedTime(now).build();
    auditInfo1.validate();
    Assertions.assertNull(auditInfo1.creator());
    Assertions.assertNull(auditInfo1.createTime());

    AuditInfo auditInfo2 = AuditInfo.builder().build();
    auditInfo2.validate();
    Assertions.assertNull(auditInfo2.creator());
    Assertions.assertNull(auditInfo2.createTime());
    Assertions.assertNull(auditInfo2.lastModifier());
    Assertions.assertNull(auditInfo2.lastModifiedTime());
  }
}
