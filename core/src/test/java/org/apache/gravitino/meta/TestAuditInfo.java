/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.meta;

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

  @Test
  public void testBuilderReuseDoesNotAffectBuiltInstance() {
    AuditInfo.Builder builder = AuditInfo.builder().withCreator("first");
    AuditInfo first = builder.build();
    builder.withCreator("second");
    AuditInfo second = builder.build();

    Assertions.assertEquals("first", first.creator());
    Assertions.assertEquals("second", second.creator());
  }
}
