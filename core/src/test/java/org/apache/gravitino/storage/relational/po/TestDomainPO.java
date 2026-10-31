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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.time.Instant;
import java.util.List;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.DomainEntity;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDomainPO {

  private static final Namespace NAMESPACE = NamespaceUtil.ofDomain("metalake");

  private static final AuditInfo AUDIT_INFO =
      AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

  @Test
  public void testInitializeAndConvertDomainPO() {
    DomainEntity domain = newDomain(10L, "Finance/Trading", "comment", ImmutableMap.of("k", "v"));

    DomainPO domainPO = DomainPO.initializeDomainPO(domain, 1L);
    Assertions.assertEquals(10L, domainPO.domainId());
    Assertions.assertEquals("Finance/Trading", domainPO.domainName());
    Assertions.assertEquals(1L, domainPO.metalakeId());
    Assertions.assertEquals("comment", domainPO.comment());
    Assertions.assertEquals("DRAFT", domainPO.status());
    Assertions.assertEquals(1L, domainPO.currentVersion());
    Assertions.assertEquals(1L, domainPO.lastVersion());
    Assertions.assertEquals(0L, domainPO.deletedAt());

    Assertions.assertEquals(domain, DomainPO.fromDomainPO(domainPO, NAMESPACE));
  }

  @Test
  public void testConvertDomainWithoutCommentAndProperties() {
    DomainEntity domain = newDomain(10L, "Finance", null, null);

    DomainEntity converted =
        DomainPO.fromDomainPO(DomainPO.initializeDomainPO(domain, 1L), NAMESPACE);
    Assertions.assertNull(converted.comment());
    Assertions.assertNull(converted.properties());
    Assertions.assertEquals(domain, converted);
  }

  @Test
  public void testConvertedDomainCarriesNoDerivedFields() {
    DomainEntity domain =
        newDomain(10L, "Trading", null, null).toBuilder()
            .withParent("Finance")
            .withBackingTagId(100L)
            .withBackingTagName("Finance/Trading")
            .withBackingTagValue("trading")
            .build();

    DomainEntity converted =
        DomainPO.fromDomainPO(DomainPO.initializeDomainPO(domain, 1L), NAMESPACE);
    Assertions.assertFalse(converted.parent().isPresent());
    // The binding is stored by tag ID; the tag name comes from a join with the live tag.
    Assertions.assertFalse(converted.backingTag().isPresent());
    Assertions.assertEquals(100L, converted.backingTagId());
    Assertions.assertEquals("trading", converted.backingTagValue());
    Assertions.assertEquals(
        domain.toBuilder().withParent(null).withBackingTagName(null).build(), converted);
  }

  @Test
  public void testConvertDomainPOWithParent() {
    DomainEntity domain = newDomain(10L, "Trading", null, null);
    DomainEntity converted =
        DomainPO.fromDomainPO(DomainPO.initializeDomainPO(domain, 1L), NAMESPACE, "Finance");
    Assertions.assertEquals("Finance", converted.parent().orElse(null));
    Assertions.assertEquals(domain.toBuilder().withParent("Finance").build(), converted);
  }

  @Test
  public void testUpdateDomainPO() {
    DomainPO oldDomainPO =
        DomainPO.initializeDomainPO(newDomain(10L, "Finance", "comment", null), 1L);

    DomainEntity updated =
        newDomain(10L, "Finance/Tax", "new comment", ImmutableMap.of("k", "v")).toBuilder()
            .withStatus(Domain.Status.PUBLISHED)
            .build();
    DomainPO newDomainPO = DomainPO.updateDomainPO(oldDomainPO, updated);

    Assertions.assertEquals(10L, newDomainPO.domainId());
    Assertions.assertEquals(1L, newDomainPO.metalakeId());
    Assertions.assertEquals("Finance/Tax", newDomainPO.domainName());
    Assertions.assertEquals("new comment", newDomainPO.comment());
    Assertions.assertEquals("PUBLISHED", newDomainPO.status());
    Assertions.assertEquals(2L, newDomainPO.currentVersion());
    Assertions.assertEquals(2L, newDomainPO.lastVersion());
    Assertions.assertEquals(0L, newDomainPO.deletedAt());
    Assertions.assertEquals(updated, DomainPO.fromDomainPO(newDomainPO, NAMESPACE));
  }

  @Test
  public void testFromDomainPOs() {
    DomainEntity finance = newDomain(10L, "Finance", null, null);
    DomainEntity markets = newDomain(11L, "Markets", null, null);
    List<DomainPO> domainPOs =
        Lists.newArrayList(
            DomainPO.initializeDomainPO(finance, 1L), DomainPO.initializeDomainPO(markets, 1L));

    // Only sub-domains are in the parent map; the others are top-level.
    Assertions.assertEquals(
        Lists.newArrayList(finance, markets.toBuilder().withParent("Finance").build()),
        DomainPO.fromDomainPOs(domainPOs, NAMESPACE, ImmutableMap.of(11L, "Finance")));
  }

  @Test
  public void testDomainPOBuilderValidation() {
    Assertions.assertDoesNotThrow(() -> validBuilder().build());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> validBuilder().withDomainId(null).build());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> validBuilder().withDomainName(" ").build());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> validBuilder().withMetalakeId(null).build());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> validBuilder().withStatus(null).build());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> validBuilder().withAuditInfo(null).build());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> validBuilder().withCurrentVersion(null).build());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> validBuilder().withLastVersion(null).build());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> validBuilder().withDeletedAt(null).build());
    Assertions.assertDoesNotThrow(
        () -> validBuilder().withBackingTagId(100L).withBackingTagValue("trading").build());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> validBuilder().withBackingTagValue("trading").build());
  }

  private DomainPO.DomainPOBuilder validBuilder() {
    return DomainPO.builder()
        .withDomainId(10L)
        .withDomainName("Finance")
        .withMetalakeId(1L)
        .withStatus("DRAFT")
        .withAuditInfo("{}")
        .withCurrentVersion(1L)
        .withLastVersion(1L)
        .withDeletedAt(0L);
  }

  private DomainEntity newDomain(
      Long id, String name, String comment, ImmutableMap<String, String> properties) {
    return DomainEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(NAMESPACE)
        .withComment(comment)
        .withProperties(properties)
        .withStatus(Domain.Status.DRAFT)
        .withAuditInfo(AUDIT_INFO)
        .build();
  }
}
