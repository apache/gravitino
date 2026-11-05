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
package org.apache.gravitino.authorization;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests that hidden tags cannot receive ownership or user privileges. */
public class TestHiddenTagAuthorization {

  /** Both canonical and ordinary names are rejected before any authorization write. */
  @ParameterizedTest
  @ValueSource(strings = {"reserved_12", "ordinary_hidden_tag"})
  public void testRejectsHiddenTagWrites(String name) throws IOException {
    EntityStore store =
        mock(EntityStore.class, withSettings().extraInterfaces(SupportsRelationOperations.class));
    TagEntity tag = tag(name, true);
    when(store.get(tag.nameIdentifier(), Entity.EntityType.TAG, TagEntity.class)).thenReturn(tag);
    MetadataObject object = MetadataObjects.parse(name, MetadataObject.Type.TAG);
    OwnerManager owners = new OwnerManager(store);
    RoleManager roles = new RoleManager(store, RandomIdGenerator.INSTANCE);
    PermissionManager permissions = new PermissionManager(store, roles);

    assertThrows(
        NoSuchTagException.class, () -> owners.setOwner("m", object, "user", Owner.Type.USER));
    assertThrows(
        NoSuchTagException.class,
        () -> owners.setOwners("m", Collections.singletonList(object), "group", Owner.Type.GROUP));
    assertThrows(
        NoSuchTagException.class,
        () ->
            permissions.grantPrivilegesToRole(
                "m", "role", object, Collections.singleton(Privileges.ApplyTag.allow())));
    assertThrows(
        NoSuchTagException.class,
        () ->
            roles.createRole(
                "m",
                "role",
                Collections.emptyMap(),
                Collections.singletonList(
                    SecurableObjects.ofTag(
                        name, Collections.singletonList(Privileges.ApplyTag.allow())))));
    verify(store, times(4)).get(tag.nameIdentifier(), Entity.EntityType.TAG, TagEntity.class);
    verifyNoMoreInteractions(store);
    AuthorizationUtils.checkVisibleTag(
        store, "m", MetadataObjects.parse("catalog", MetadataObject.Type.CATALOG));
    verifyNoMoreInteractions(store);
  }

  /** Legacy visible reserved names remain valid, while missing tags return not-found. */
  @ParameterizedTest
  @ValueSource(strings = {"reserved_legacy", "Reserved_legacy", "business_tag"})
  public void testVisibleAndMissingTags(String name) throws IOException {
    EntityStore store = mock(EntityStore.class);
    TagEntity tag = tag(name, false);
    when(store.get(tag.nameIdentifier(), Entity.EntityType.TAG, TagEntity.class)).thenReturn(tag);
    MetadataObject object = MetadataObjects.parse(name, MetadataObject.Type.TAG);
    assertDoesNotThrow(() -> AuthorizationUtils.checkVisibleTag(store, "m", object));
    verify(store).get(tag.nameIdentifier(), Entity.EntityType.TAG, TagEntity.class);
    when(store.get(
            NameIdentifierUtil.ofTag("m", "missing"), Entity.EntityType.TAG, TagEntity.class))
        .thenThrow(new NoSuchEntityException("missing"));
    assertThrows(
        NoSuchTagException.class,
        () ->
            AuthorizationUtils.checkVisibleTag(
                store, "m", MetadataObjects.parse("missing", MetadataObject.Type.TAG)));
  }

  private TagEntity tag(String name, boolean hidden) {
    return TagEntity.builder()
        .withId(1L)
        .withName(name)
        .withNamespace(NamespaceUtil.ofTag("m"))
        .withHidden(hidden)
        .withAuditInfo(
            AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
        .build();
  }
}
