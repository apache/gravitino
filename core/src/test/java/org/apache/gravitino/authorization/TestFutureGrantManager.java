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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.Optional;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
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
