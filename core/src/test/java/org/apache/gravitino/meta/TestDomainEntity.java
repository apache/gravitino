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

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityFieldLimits;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.tag.TagValue;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDomainEntity {

  @Test
  public void testDomainEntity() {
    DomainEntity domain =
        DomainEntity.builder()
            .withId(1L)
            .withName("Finance/Trading")
            .withNamespace(NamespaceUtil.ofDomain("metalake"))
            .withComment("comment")
            .withProperties(ImmutableMap.of("k", "v"))
            .withStatus(Domain.Status.DRAFT)
            .withParent("Finance")
            .withBackingTagId(100L)
            .withBackingTagName("dept")
            .withBackingTagValue("trading")
            .withAuditInfo(AuditInfo.EMPTY)
            .build();

    Assertions.assertEquals(Entity.EntityType.DOMAIN, domain.type());
    Assertions.assertEquals(1L, domain.id());
    Assertions.assertEquals("Finance/Trading", domain.name());
    Assertions.assertEquals("comment", domain.comment());
    Assertions.assertEquals(ImmutableMap.of("k", "v"), domain.properties());
    Assertions.assertEquals(Domain.Status.DRAFT, domain.status());
    Assertions.assertEquals("Finance", domain.parent().orElse(null));
    Assertions.assertEquals(TagValue.of("dept", "trading"), domain.backingTag().orElse(null));
    Assertions.assertEquals(100L, domain.backingTagId());
    Assertions.assertEquals("trading", domain.backingTagValue());
    Assertions.assertEquals(AuditInfo.EMPTY, domain.auditInfo());

    // The parent and the backing tag name are not stored with the domain.
    Assertions.assertEquals(
        ImmutableMap.builder()
            .put(DomainEntity.ID, 1L)
            .put(DomainEntity.NAME, "Finance/Trading")
            .put(DomainEntity.COMMENT, "comment")
            .put(DomainEntity.PROPERTIES, ImmutableMap.of("k", "v"))
            .put(DomainEntity.STATUS, Domain.Status.DRAFT)
            .put(DomainEntity.BACKING_TAG_ID, 100L)
            .put(DomainEntity.BACKING_TAG_VALUE, "trading")
            .put(DomainEntity.AUDIT_INFO, AuditInfo.EMPTY)
            .build(),
        domain.fields());
  }

  @Test
  public void testDomainEntityBackingTag() {
    DomainEntity domain = newDomain();
    Assertions.assertFalse(domain.backingTag().isPresent());
    Assertions.assertNull(domain.backingTagId());

    DomainEntity valueless =
        domain.toBuilder().withBackingTagId(100L).withBackingTagName("Finance").build();
    Assertions.assertEquals(TagValue.noValue("Finance"), valueless.backingTag().orElse(null));
    Assertions.assertNull(valueless.backingTagValue());

    // A deleted tag keeps its ID on the domain but has no name, so there is no backing tag.
    DomainEntity deletedTag =
        domain.toBuilder().withBackingTagId(100L).withBackingTagValue("trading").build();
    Assertions.assertFalse(deletedTag.backingTag().isPresent());
    Assertions.assertEquals(100L, deletedTag.backingTagId());
    Assertions.assertEquals("trading", deletedTag.backingTagValue());

    // Clearing the tag ID unbinds the tag, without clearing the name read from the tag.
    DomainEntity unbound = valueless.toBuilder().withBackingTagId(null).build();
    Assertions.assertFalse(unbound.backingTag().isPresent());
    Assertions.assertEquals(domain, unbound);
    Assertions.assertFalse(
        domain.toBuilder().withBackingTagName("Finance").build().backingTag().isPresent());

    // A tag value needs the tag ID.
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> domain.toBuilder().withBackingTagValue("trading").build());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            domain.toBuilder()
                .withBackingTagId(100L)
                .withBackingTagValue(
                    StringUtils.repeat("a", EntityFieldLimits.MAX_TAG_VALUE_LENGTH + 1))
                .build());

    // A tag bound without a value is stored as NULL, never as a blank value.
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> domain.toBuilder().withBackingTagId(100L).withBackingTagValue("").build());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> domain.toBuilder().withBackingTagId(100L).withBackingTagValue("  ").build());
  }

  @Test
  public void testDomainEntityValidation() {
    Assertions.assertDoesNotThrow(() -> newDomain().toBuilder().withComment(null).build());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> newDomain().toBuilder().withId(null).build());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> newDomain().toBuilder().withName(null).build());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> newDomain().toBuilder().withStatus(null).build());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> newDomain().toBuilder().withAuditInfo(null).build());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            newDomain().toBuilder()
                .withName(StringUtils.repeat("a", EntityFieldLimits.MAX_NAME_LENGTH + 1))
                .build());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            newDomain().toBuilder()
                .withComment(StringUtils.repeat("a", EntityFieldLimits.MAX_COMMENT_LENGTH + 1))
                .build());
  }

  @Test
  public void testDomainEntityCopyAndEquality() {
    DomainEntity domain = newDomain();
    DomainEntity copy = domain.toBuilder().build();
    Assertions.assertEquals(domain, copy);
    Assertions.assertEquals(domain.hashCode(), copy.hashCode());

    Assertions.assertNotEquals(
        domain, domain.toBuilder().withStatus(Domain.Status.PUBLISHED).build());
    Assertions.assertNotEquals(domain, domain.toBuilder().withParent("Markets").build());
    Assertions.assertNotEquals(domain, domain.toBuilder().withBackingTagId(100L).build());
  }

  private DomainEntity newDomain() {
    return DomainEntity.builder()
        .withId(1L)
        .withName("Trading")
        .withNamespace(NamespaceUtil.ofDomain("metalake"))
        .withStatus(Domain.Status.DRAFT)
        .withParent("Finance")
        .withAuditInfo(AuditInfo.EMPTY)
        .build();
  }
}
