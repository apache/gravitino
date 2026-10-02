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

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.authorization.AuthorizationPlugin;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.meta.UserEntity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

public class TestFutureGrantManager {
  private static EntityStore entityStore = mock(EntityStore.class);
  private static OwnerManager ownerManager = mock(OwnerManager.class);
  private static String METALAKE = "metalake";
  private static AuthorizationPlugin authorizationPlugin;
  private static BaseMetalake metalakeEntity =
      BaseMetalake.builder()
          .withId(1L)
          .withName(METALAKE)
          .withAuditInfo(
              AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
          .withVersion(SchemaVersion.V_0_1)
          .build();
  private static BaseCatalog catalog;

  @BeforeAll
  public static void setUp() throws Exception {
    entityStore.put(metalakeEntity, true);

    catalog = mock(BaseCatalog.class);
    authorizationPlugin = mock(AuthorizationPlugin.class);
    when(catalog.getAuthorizationPlugin()).thenReturn(authorizationPlugin);
  }

  @Test
  void testGrantNormally() throws IOException {
    FutureGrantManager manager = new FutureGrantManager(entityStore, ownerManager);

    SupportsRelationOperations relationOperations = mock(SupportsRelationOperations.class);
    when(entityStore.relationOperations()).thenReturn(relationOperations);
    when(ownerManager.getOwner(any(), any())).thenReturn(Optional.empty());

    // test no securable objects
    RoleEntity roleEntity = mock(RoleEntity.class);
    when(roleEntity.id()).thenReturn(2L);
    when(roleEntity.name()).thenReturn("role1");
    when(roleEntity.namespace()).thenReturn(Namespace.of(METALAKE));
    when(roleEntity.auditInfo()).thenReturn(metalakeEntity.auditInfo());
    when(roleEntity.properties()).thenReturn(Collections.emptyMap());
    when(relationOperations.listEntitiesByRelation(
            SupportsRelationOperations.Type.METADATA_OBJECT_ROLE_REL,
            NameIdentifier.of(METALAKE),
            Entity.EntityType.METALAKE))
        .thenReturn(Lists.newArrayList(roleEntity));
    UserEntity userEntity = mock(UserEntity.class);
    when(relationOperations.listEntitiesByRelation(
            SupportsRelationOperations.Type.ROLE_USER_REL,
            AuthorizationUtils.ofRole(METALAKE, "role1"),
            Entity.EntityType.ROLE))
        .thenReturn(Lists.newArrayList(userEntity));
    when(relationOperations.listEntitiesByRelation(
            SupportsRelationOperations.Type.ROLE_GROUP_REL,
            AuthorizationUtils.ofRole(METALAKE, "role1"),
            Entity.EntityType.ROLE))
        .thenReturn(Collections.emptyList());
    when(roleEntity.securableObjects()).thenReturn(Collections.emptyList());

    manager.grantNewlyCreatedCatalog(METALAKE, catalog);
    verify(authorizationPlugin, never()).onGrantedRolesToUser(any(), any());
    verify(authorizationPlugin, never()).onGrantedRolesToGroup(any(), any());
    verify(authorizationPlugin, never()).onOwnerSet(any(), any(), any());

    // test only grant users
    reset(authorizationPlugin);
    when(ownerManager.getOwner(any(), any()))
        .thenReturn(
            Optional.of(
                new Owner() {
                  @Override
                  public String name() {
                    return "test";
                  }

                  @Override
                  public Type type() {
                    return Type.USER;
                  }
                }));

    SecurableObject securableObject = mock(SecurableObject.class);
    when(securableObject.type()).thenReturn(MetadataObject.Type.METALAKE);
    when(securableObject.fullName()).thenReturn(METALAKE);
    when(securableObject.privileges())
        .thenReturn(Lists.newArrayList(Privileges.CreateTable.allow()));
    when(roleEntity.securableObjects()).thenReturn(Lists.newArrayList(securableObject));
    when(roleEntity.nameIdentifier()).thenReturn(AuthorizationUtils.ofRole(METALAKE, "role1"));

    manager.grantNewlyCreatedCatalog(METALAKE, catalog);
    verify(authorizationPlugin).onOwnerSet(any(), any(), any());
    verify(authorizationPlugin).onGrantedRolesToUser(any(), any());
    verify(authorizationPlugin, never()).onGrantedRolesToGroup(any(), any());

    // test only grant groups
    reset(authorizationPlugin);
    GroupEntity groupEntity = mock(GroupEntity.class);
    when(relationOperations.listEntitiesByRelation(
            SupportsRelationOperations.Type.ROLE_USER_REL,
            AuthorizationUtils.ofRole(METALAKE, "role1"),
            Entity.EntityType.ROLE))
        .thenReturn(Collections.emptyList());
    when(relationOperations.listEntitiesByRelation(
            SupportsRelationOperations.Type.ROLE_GROUP_REL,
            AuthorizationUtils.ofRole(METALAKE, "role1"),
            Entity.EntityType.ROLE))
        .thenReturn(Lists.newArrayList(groupEntity));
    manager.grantNewlyCreatedCatalog(METALAKE, catalog);
    verify(authorizationPlugin).onOwnerSet(any(), any(), any());
    verify(authorizationPlugin, never()).onGrantedRolesToUser(any(), any());
    verify(authorizationPlugin).onGrantedRolesToGroup(any(), any());

    // test users and groups
    reset(authorizationPlugin);
    when(relationOperations.listEntitiesByRelation(
            SupportsRelationOperations.Type.ROLE_USER_REL,
            AuthorizationUtils.ofRole(METALAKE, "role1"),
            Entity.EntityType.ROLE))
        .thenReturn(Lists.newArrayList(userEntity));
    when(relationOperations.listEntitiesByRelation(
            SupportsRelationOperations.Type.ROLE_GROUP_REL,
            AuthorizationUtils.ofRole(METALAKE, "role1"),
            Entity.EntityType.ROLE))
        .thenReturn(Lists.newArrayList(groupEntity));
    manager.grantNewlyCreatedCatalog(METALAKE, catalog);
    verify(authorizationPlugin).onOwnerSet(any(), any(), any());
    verify(authorizationPlugin).onGrantedRolesToUser(any(), any());
    verify(authorizationPlugin).onGrantedRolesToGroup(any(), any());

    // test to skip unnecessary roles
    reset(authorizationPlugin);
    when(securableObject.privileges())
        .thenReturn(Lists.newArrayList(Privileges.CreateCatalog.allow()));
    manager.grantNewlyCreatedCatalog(METALAKE, catalog);
    verify(authorizationPlugin).onOwnerSet(any(), any(), any());
    verify(authorizationPlugin, never()).onGrantedRolesToUser(any(), any());
    verify(authorizationPlugin, never()).onGrantedRolesToGroup(any(), any());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testMixedRoleFutureGrants(boolean grantToUser) throws IOException {
    EntityStore store = mock(EntityStore.class);
    OwnerManager owners = mock(OwnerManager.class);
    SupportsRelationOperations relations = mock(SupportsRelationOperations.class);
    when(store.relationOperations()).thenReturn(relations);
    when(owners.getOwner(any(), any())).thenReturn(Optional.empty());
    BaseCatalog newCatalog = mock(BaseCatalog.class);
    when(newCatalog.name()).thenReturn("new_catalog");
    AuthorizationPlugin plugin = mock(AuthorizationPlugin.class);
    when(newCatalog.getAuthorizationPlugin()).thenReturn(plugin);
    SecurableObject inherited =
        SecurableObjects.ofMetalake(
            METALAKE,
            Lists.newArrayList(
                Privileges.SelectTable.allow(), Privileges.SelectSemanticModel.allow()));
    SecurableObject semanticModel =
        SecurableObjects.parse(
            "old_catalog.schema.model",
            MetadataObject.Type.SEMANTIC_MODEL,
            Lists.newArrayList(Privileges.SelectSemanticModel.allow()));
    SecurableObject otherTable =
        SecurableObjects.parse(
            "old_catalog.schema.table",
            MetadataObject.Type.TABLE,
            Lists.newArrayList(Privileges.SelectTable.allow()));
    RoleEntity role =
        RoleEntity.builder()
            .withId(2L)
            .withName("mixed_role")
            .withNamespace(Namespace.of(METALAKE))
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .withProperties(Collections.emptyMap())
            .withSecurableObjects(Lists.newArrayList(inherited, semanticModel, otherTable))
            .build();
    when(relations.listEntitiesByRelation(
            SupportsRelationOperations.Type.METADATA_OBJECT_ROLE_REL,
            NameIdentifier.of(METALAKE),
            Entity.EntityType.METALAKE))
        .thenReturn(Lists.newArrayList(role));
    UserEntity user = mock(UserEntity.class);
    GroupEntity group = mock(GroupEntity.class);
    when(relations.listEntitiesByRelation(
            SupportsRelationOperations.Type.ROLE_USER_REL,
            role.nameIdentifier(),
            Entity.EntityType.ROLE))
        .thenReturn(grantToUser ? Lists.newArrayList(user) : Collections.emptyList());
    when(relations.listEntitiesByRelation(
            SupportsRelationOperations.Type.ROLE_GROUP_REL,
            role.nameIdentifier(),
            Entity.EntityType.ROLE))
        .thenReturn(grantToUser ? Collections.emptyList() : Lists.newArrayList(group));

    new FutureGrantManager(store, owners).grantNewlyCreatedCatalog(METALAKE, newCatalog);

    ArgumentCaptor<List<Role>> roles = ArgumentCaptor.forClass(List.class);
    if (grantToUser) {
      verify(plugin).onGrantedRolesToUser(roles.capture(), eq(user));
      verify(plugin, never()).onGrantedRolesToGroup(any(), any());
    } else {
      verify(plugin).onGrantedRolesToGroup(roles.capture(), eq(group));
      verify(plugin, never()).onGrantedRolesToUser(any(), any());
    }
    Assertions.assertEquals(1, roles.getValue().size());
    Role filtered = roles.getValue().get(0);
    Assertions.assertEquals(role.name(), filtered.name());
    Assertions.assertEquals(1, filtered.securableObjects().size());
    Assertions.assertEquals(
        MetadataObject.Type.METALAKE, filtered.securableObjects().get(0).type());
    Assertions.assertEquals(
        Lists.newArrayList(Privileges.SelectTable.allow()),
        filtered.securableObjects().get(0).privileges());
    Assertions.assertEquals(3, role.securableObjects().size());
    Assertions.assertEquals(2, role.securableObjects().get(0).privileges().size());
  }

  @Test
  void testGrantWithException() throws IOException {
    FutureGrantManager manager = new FutureGrantManager(entityStore, ownerManager);
    SupportsRelationOperations relationOperations = mock(SupportsRelationOperations.class);
    when(entityStore.relationOperations()).thenReturn(relationOperations);
    doThrow(new IOException("mock error"))
        .when(relationOperations)
        .listEntitiesByRelation(any(), any(), any());
    Assertions.assertThrows(
        RuntimeException.class, () -> manager.grantNewlyCreatedCatalog(METALAKE, catalog));
  }
}
