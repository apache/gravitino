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
package org.apache.gravitino.policy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Collections;
import java.util.function.Function;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.tag.TagChange;
import org.apache.gravitino.tag.TagManager;
import org.apache.gravitino.tag.TagValueConstraint;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Test;

/** Tests hidden tag models and reserved namespace protection. */
public class TestHiddenPolicyTag {

  /** Rejects alternate casing and noncanonical identity encodings. */
  @Test
  public void testReservedNames() {
    assertEquals(123, HiddenPolicyTag.policyId(HiddenPolicyTag.name(123)));
    assertTrue(HiddenPolicyTag.isHidden("RESERVED_123"));
    assertFalse(HiddenPolicyTag.isHidden(null));
    assertFalse(HiddenPolicyTag.isHidden("data_domain"));
    assertThrows(
        IllegalArgumentException.class, () -> HiddenPolicyTag.checkVisible("RESERVED_123"));
    assertThrows(IllegalArgumentException.class, () -> HiddenPolicyTag.policyId("reserved_0123"));
    assertThrows(IllegalArgumentException.class, () -> HiddenPolicyTag.name(-1));
    assertThrows(
        IllegalArgumentException.class,
        () -> HiddenPolicyTag.checkVisible("reserved_business_tag"));
    assertEquals("reserved_123", HiddenPolicyTag.name(123));
  }

  /** New policies use the tag model, with matching ownership and audit metadata. */
  @Test
  public void testCreateHiddenTagModel() throws Exception {
    AuditInfo audit = AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();
    PolicyEntity policy =
        PolicyEntity.builder()
            .withId(12L)
            .withName("policy")
            .withNamespace(NamespaceUtil.ofPolicy("m"))
            .withPolicyType(Policy.BuiltInType.CUSTOM)
            .withEnabled(false)
            .withContent(
                PolicyContents.custom(
                    Collections.singletonMap("rule", 1),
                    Collections.singleton(MetadataObject.Type.TABLE),
                    null))
            .withAuditInfo(audit)
            .build();
    TagEntity tag = HiddenPolicyTag.create(policy, 34L);
    assertEquals(34L, tag.id());
    assertTrue(tag.hidden());
    assertEquals(HiddenPolicyTag.name(policy.id()), tag.name());
    assertEquals(NamespaceUtil.ofTag("m"), tag.namespace());
    assertEquals(audit, tag.auditInfo());
    assertEquals(TagValueConstraint.noValue(), tag.valueConstraint());
    assertFalse(tag.assignment().isPresent());
    assertEquals(
        HiddenPolicyTag.properties(policy.id()),
        JsonUtils.anyFieldMapper().writeValueAsString(tag.properties()));
  }

  /** A database alias must not bypass checks against the actual stored tag name. */
  @Test
  public void testResolvedHiddenTagIsProtected() throws Exception {
    String alias = "database-alias";
    EntityStore store = mock(EntityStore.class);
    AuditInfo audit = AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();
    BaseMetalake metalake =
        BaseMetalake.builder()
            .withId(1L)
            .withName("m")
            .withVersion(SchemaVersion.V_0_1)
            .withAuditInfo(audit)
            .build();
    when(store.get(NameIdentifier.of("m"), Entity.EntityType.METALAKE, BaseMetalake.class))
        .thenReturn(metalake);
    TagEntity hidden =
        TagEntity.builder()
            .withId(2L)
            .withName("internal_tag_without_reserved_prefix")
            .withHidden(true)
            .withNamespace(NamespaceUtil.ofTag("m"))
            .withAuditInfo(audit)
            .build();
    when(store.get(NameIdentifierUtil.ofTag("m", alias), Entity.EntityType.TAG, TagEntity.class))
        .thenReturn(hidden);
    when(store.update(
            any(NameIdentifier.class), eq(TagEntity.class), eq(Entity.EntityType.TAG), any()))
        .thenAnswer(
            invocation -> {
              Function<TagEntity, TagEntity> update = invocation.getArgument(3);
              return update.apply(hidden);
            });
    TagManager tags = new TagManager(RandomIdGenerator.INSTANCE, store);
    assertThrows(IllegalArgumentException.class, () -> tags.getTag("m", alias));
    assertThrows(IllegalArgumentException.class, () -> tags.deleteTag("m", alias));
    assertThrows(
        IllegalArgumentException.class,
        () -> tags.alterTag("m", alias, TagChange.updateComment("changed")));
    assertThrows(IllegalArgumentException.class, () -> tags.listMetadataObjectsForTag("m", alias));
    assertThrows(
        IllegalArgumentException.class, () -> tags.listPolicyAssociationsForTag("m", alias));
    assertThrows(
        IllegalArgumentException.class,
        () -> tags.addPolicyForTag("m", alias, "p", AllValuesSelector.get()));
    assertThrows(IllegalArgumentException.class, () -> tags.removePolicyFromTag("m", alias, "p"));
    MetadataObject object = MetadataObjects.parse("catalog", MetadataObject.Type.CATALOG);
    assertThrows(
        IllegalArgumentException.class,
        () -> tags.associateTagsForMetadataObject("m", object, new String[] {alias}, null));
    assertThrows(
        IllegalArgumentException.class,
        () -> tags.associateTagsForMetadataObject("m", object, null, new String[] {alias}));
    verify(store, never()).delete(any(NameIdentifier.class), eq(Entity.EntityType.TAG));
  }
}
