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
package org.apache.gravitino.storage.relational.po;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.time.Instant;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDomainRelPO {

  private static final Instant CREATE_TIME = Instant.ofEpochMilli(1000L);

  private static final AuditInfo AUDIT_INFO =
      AuditInfo.builder().withCreator("creator").withCreateTime(CREATE_TIME).build();

  @Test
  public void testInitializeDomainRelPO() throws JsonProcessingException {
    DomainRelPO domainRelPO = DomainRelPO.initializeDomainRelPO(1L, 10L, 11L, AUDIT_INFO);

    Assertions.assertEquals(1L, domainRelPO.metalakeId());
    Assertions.assertEquals(10L, domainRelPO.parentId());
    Assertions.assertEquals(11L, domainRelPO.childId());
    Assertions.assertNull(domainRelPO.parentName());
    Assertions.assertEquals(1L, domainRelPO.currentVersion());
    Assertions.assertEquals(1L, domainRelPO.lastVersion());
    Assertions.assertEquals(0L, domainRelPO.deletedAt());
    Assertions.assertEquals(AUDIT_INFO, readAuditInfo(domainRelPO));
  }

  @Test
  public void testUpdateDomainRelPOKeepsCreator() throws JsonProcessingException {
    DomainRelPO oldDomainRelPO = DomainRelPO.initializeDomainRelPO(1L, 10L, 11L, AUDIT_INFO);
    Instant modifiedTime = Instant.ofEpochMilli(2000L);
    AuditInfo modifiedAuditInfo =
        AuditInfo.builder()
            .withCreator("ignored")
            .withCreateTime(Instant.ofEpochMilli(3000L))
            .withLastModifier("modifier")
            .withLastModifiedTime(modifiedTime)
            .build();

    DomainRelPO newDomainRelPO =
        DomainRelPO.updateDomainRelPO(oldDomainRelPO, 12L, modifiedAuditInfo);
    Assertions.assertEquals(1L, newDomainRelPO.metalakeId());
    Assertions.assertEquals(12L, newDomainRelPO.parentId());
    Assertions.assertEquals(11L, newDomainRelPO.childId());
    Assertions.assertEquals(2L, newDomainRelPO.currentVersion());
    Assertions.assertEquals(2L, newDomainRelPO.lastVersion());
    Assertions.assertEquals(0L, newDomainRelPO.deletedAt());

    AuditInfo storedAuditInfo = readAuditInfo(newDomainRelPO);
    Assertions.assertEquals("creator", storedAuditInfo.creator());
    Assertions.assertEquals(CREATE_TIME, storedAuditInfo.createTime());
    Assertions.assertEquals("modifier", storedAuditInfo.lastModifier());
    Assertions.assertEquals(modifiedTime, storedAuditInfo.lastModifiedTime());
  }

  @Test
  public void testDomainRelPOValidation() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> DomainRelPO.initializeDomainRelPO(1L, 10L, 10L, AUDIT_INFO));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> DomainRelPO.initializeDomainRelPO(null, 10L, 11L, AUDIT_INFO));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> DomainRelPO.initializeDomainRelPO(1L, null, 11L, AUDIT_INFO));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> DomainRelPO.initializeDomainRelPO(1L, 10L, null, AUDIT_INFO));

    DomainRelPO domainRelPO = DomainRelPO.initializeDomainRelPO(1L, 10L, 11L, AUDIT_INFO);
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> DomainRelPO.updateDomainRelPO(domainRelPO, 11L, AUDIT_INFO));
  }

  private AuditInfo readAuditInfo(DomainRelPO domainRelPO) throws JsonProcessingException {
    return JsonUtils.anyFieldMapper().readValue(domainRelPO.auditInfo(), AuditInfo.class);
  }
}
