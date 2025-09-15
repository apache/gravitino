/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.server.authorization.jcasbin;

import static org.apache.gravitino.authorization.Privilege.Name.USE_CATALOG;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.lang.reflect.Field;
import java.security.Principal;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.server.ServerConfig;
import org.apache.gravitino.server.authorization.MetadataIdConverter;
import org.apache.gravitino.storage.relational.po.SecurableObjectPO;
import org.apache.gravitino.storage.relational.service.OwnerMetaService;
import org.apache.gravitino.storage.relational.utils.POConverters;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/** Test of {@link JcasbinAuthorizer} */
public class TestJcasbinAuthorizer {

  private static final Long USER_METALAKE_ID = 1L;

  private static final Long USER_ID = 2L;

  private static final Long ALLOW_ROLE_ID = 3L;

  private static final Long DENY_ROLE_ID = 5L;

  private static final Long CATALOG_ID = 4L;

  private static final String USERNAME = "tester";

  private static final String METALAKE = "testMetalake";

  private static EntityStore entityStore = mock(EntityStore.class);

  private static GravitinoEnv gravitinoEnv = mock(GravitinoEnv.class);

  private static SupportsRelationOperations supportsRelationOperations =
      mock(SupportsRelationOperations.class);

  private static MockedStatic<PrincipalUtils> principalUtilsMockedStatic;

  private static MockedStatic<GravitinoEnv> gravitinoEnvMockedStatic;

  private static MockedStatic<MetadataIdConverter> metadataIdConverterMockedStatic;

  private static MockedStatic<OwnerMetaService> ownerMetaServiceMockedStatic;

  private static JcasbinAuthorizer jcasbinAuthorizer;

  private static ObjectMapper objectMapper = new ObjectMapper();

  @BeforeAll
  public static void setup() throws IOException {
    OwnerMetaService ownerMetaService = mock(OwnerMetaService.class);
    ownerMetaServiceMockedStatic = mockStatic(OwnerMetaService.class);
    ownerMetaServiceMockedStatic.when(OwnerMetaService::getInstance).thenReturn(ownerMetaService);
    gravitinoEnvMockedStatic = mockStatic(GravitinoEnv.class);
    gravitinoEnvMockedStatic.when(GravitinoEnv::getInstance).thenReturn(gravitinoEnv);
    when(gravitinoEnv.config()).thenReturn(new ServerConfig());
    principalUtilsMockedStatic = mockStatic(PrincipalUtils.class);
    metadataIdConverterMockedStatic = mockStatic(MetadataIdConverter.class);
    principalUtilsMockedStatic
        .when(PrincipalUtils::getCurrentPrincipal)
        .thenReturn(new UserPrincipal(USERNAME));
    principalUtilsMockedStatic.when(() -> PrincipalUtils.doAs(any(), any())).thenCallRealMethod();
    metadataIdConverterMockedStatic
        .when(() -> MetadataIdConverter.getID(any(), eq(METALAKE)))
        .thenReturn(CATALOG_ID);
    when(gravitinoEnv.entityStore()).thenReturn(entityStore);
    when(entityStore.relationOperations()).thenReturn(supportsRelationOperations);
    when(entityStore.get(
            eq(NameIdentifierUtil.ofUser(METALAKE, USERNAME)),
            eq(Entity.EntityType.USER),
            eq(UserEntity.class)))
        .thenReturn(getUserEntity());
    jcasbinAuthorizer = new JcasbinAuthorizer();
    jcasbinAuthorizer.initialize();
    BaseMetalake baseMetalake =
        BaseMetalake.builder()
            .withId(USER_METALAKE_ID)
            .withVersion(SchemaVersion.V_0_1)
            .withAuditInfo(AuditInfo.EMPTY)
            .withName(METALAKE)
            .build();
    when(entityStore.get(
            eq(NameIdentifierUtil.ofMetalake(METALAKE)),
            eq(Entity.EntityType.METALAKE),
            eq(BaseMetalake.class)))
        .thenReturn(baseMetalake);
  }

  @AfterAll
  public static void stop() {
    if (principalUtilsMockedStatic != null) {
      principalUtilsMockedStatic.close();
    }
    if (metadataIdConverterMockedStatic != null) {
      metadataIdConverterMockedStatic.close();
    }
    if (ownerMetaServiceMockedStatic != null) {
      ownerMetaServiceMockedStatic.close();
    }
    if (gravitinoEnvMockedStatic != null) {
      gravitinoEnvMockedStatic.close();
    }
  }

  @Test
  public void testAuthorize() throws IOException {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    assertFalse(doAuthorize(currentPrincipal));
    RoleEntity allowRole =
        getRoleEntity(ALLOW_ROLE_ID, "allowRole", ImmutableList.of(getAllowSecurableObject()));
    when(entityStore.get(
            eq(NameIdentifierUtil.ofRole(METALAKE, allowRole.name())),
            eq(Entity.EntityType.ROLE),
            eq(RoleEntity.class)))
        .thenReturn(allowRole);
    NameIdentifier userNameIdentifier = NameIdentifierUtil.ofUser(METALAKE, USERNAME);
    // Mock adds roles to users.
    when(supportsRelationOperations.listEntitiesByRelation(
            eq(SupportsRelationOperations.Type.ROLE_USER_REL),
            eq(userNameIdentifier),
            eq(Entity.EntityType.USER)))
        .thenReturn(ImmutableList.of(allowRole));
    assertTrue(doAuthorize(currentPrincipal));
    // Test role cache.
    // When permissions are changed but handleRolePrivilegeChange is not executed, the system will
    // use the cached permissions in JCasbin, so authorize can succeed.
    Long newRoleId = -1L;
    RoleEntity tempNewRole = getRoleEntity(newRoleId, "tempNewRole", ImmutableList.of());
    when(entityStore.get(
            eq(NameIdentifierUtil.ofRole(METALAKE, tempNewRole.name())),
            eq(Entity.EntityType.ROLE),
            eq(RoleEntity.class)))
        .thenReturn(tempNewRole);
    when(supportsRelationOperations.listEntitiesByRelation(
            eq(SupportsRelationOperations.Type.ROLE_USER_REL),
            eq(userNameIdentifier),
            eq(Entity.EntityType.USER)))
        .thenReturn(ImmutableList.of(tempNewRole));
    assertTrue(doAuthorize(currentPrincipal));
    // After clearing the cache, authorize will fail
    jcasbinAuthorizer.handleRolePrivilegeChange(ALLOW_ROLE_ID);
    //    assertFalse(doAuthorize(currentPrincipal));
    // When the user is re-assigned the correct role, the authorization will succeed.
    when(supportsRelationOperations.listEntitiesByRelation(
            eq(SupportsRelationOperations.Type.ROLE_USER_REL),
            eq(userNameIdentifier),
            eq(Entity.EntityType.USER)))
        .thenReturn(ImmutableList.of(allowRole));
    when(entityStore.get(
            eq(NameIdentifierUtil.ofRole(METALAKE, allowRole.name())),
            eq(Entity.EntityType.ROLE),
            eq(RoleEntity.class)))
        .thenReturn(allowRole);
    assertTrue(doAuthorize(currentPrincipal));
    // Test deny
    RoleEntity denyRole =
        getRoleEntity(DENY_ROLE_ID, "denyRole", ImmutableList.of(getDenySecurableObject()));
    when(entityStore.get(
            eq(NameIdentifierUtil.ofRole(METALAKE, denyRole.name())),
            eq(Entity.EntityType.ROLE),
            eq(RoleEntity.class)))
        .thenReturn(denyRole);
    when(supportsRelationOperations.listEntitiesByRelation(
            eq(SupportsRelationOperations.Type.ROLE_USER_REL),
            eq(userNameIdentifier),
            eq(Entity.EntityType.USER)))
        .thenReturn(ImmutableList.of(allowRole, denyRole));

    assertFalse(doAuthorize(currentPrincipal));
  }

  @Test
  public void testAuthorizeByOwner() throws IOException {
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    assertFalse(doAuthorizeOwner(currentPrincipal));
    NameIdentifier catalogIdent = NameIdentifierUtil.ofCatalog(METALAKE, "testCatalog");
    when(supportsRelationOperations.listEntitiesByRelation(
            eq(SupportsRelationOperations.Type.OWNER_REL),
            eq(catalogIdent),
            eq(Entity.EntityType.CATALOG)))
        .thenReturn(ImmutableList.of(getUserEntity()));
    assertTrue(doAuthorizeOwner(currentPrincipal));
    when(supportsRelationOperations.listEntitiesByRelation(
            eq(SupportsRelationOperations.Type.OWNER_REL),
            eq(catalogIdent),
            eq(Entity.EntityType.CATALOG)))
        .thenReturn(new ArrayList<>());
    jcasbinAuthorizer.handleMetadataOwnerChange(
        METALAKE, USER_ID, catalogIdent, Entity.EntityType.CATALOG);
    assertFalse(doAuthorizeOwner(currentPrincipal));
  }

  private Boolean doAuthorize(Principal currentPrincipal) {
    return jcasbinAuthorizer.authorize(
        currentPrincipal,
        "testMetalake",
        MetadataObjects.of(null, "testCatalog", MetadataObject.Type.CATALOG),
        USE_CATALOG,
        new AuthorizationRequestContext());
  }

  private Boolean doAuthorizeOwner(Principal currentPrincipal) {
    return jcasbinAuthorizer.isOwner(
        currentPrincipal,
        "testMetalake",
        MetadataObjects.of(null, "testCatalog", MetadataObject.Type.CATALOG));
  }

  private static UserEntity getUserEntity() {
    return UserEntity.builder()
        .withId(USER_ID)
        .withName(USERNAME)
        .withAuditInfo(AuditInfo.EMPTY)
        .build();
  }

  private static RoleEntity getRoleEntity(
      Long roleId, String roleName, List<SecurableObject> securableObjects) {
    Namespace namespace = NamespaceUtil.ofRole(METALAKE);
    return RoleEntity.builder()
        .withNamespace(namespace)
        .withId(roleId)
        .withName(roleName)
        .withAuditInfo(AuditInfo.EMPTY)
        .withSecurableObjects(securableObjects)
        .build();
  }

  private static SecurableObjectPO getAllowSecurableObjectPO() {
    ImmutableList<Privilege.Name> privileges = ImmutableList.of(USE_CATALOG);
    List<String> privilegeNames = privileges.stream().map(Enum::name).collect(Collectors.toList());
    ImmutableList<String> conditions = ImmutableList.of("ALLOW");

    try {
      return SecurableObjectPO.builder()
          .withType(String.valueOf(MetadataObject.Type.CATALOG))
          .withMetadataObjectId(CATALOG_ID)
          .withRoleId(ALLOW_ROLE_ID)
          .withPrivilegeNames(objectMapper.writeValueAsString(privilegeNames))
          .withPrivilegeConditions(objectMapper.writeValueAsString(conditions))
          .withDeletedAt(0L)
          .withCurrentVersion(1L)
          .withLastVersion(1L)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private static SecurableObject getAllowSecurableObject() {
    return POConverters.fromSecurableObjectPO(
        "testCatalog", getAllowSecurableObjectPO(), MetadataObject.Type.CATALOG);
  }

  private static SecurableObjectPO getDenySecurableObjectPO() {
    ImmutableList<Privilege.Name> privileges = ImmutableList.of(USE_CATALOG);
    ImmutableList<String> conditions = ImmutableList.of("DENY");
    try {
      return SecurableObjectPO.builder()
          .withType(String.valueOf(MetadataObject.Type.CATALOG))
          .withMetadataObjectId(CATALOG_ID)
          .withRoleId(DENY_ROLE_ID)
          .withPrivilegeNames(objectMapper.writeValueAsString(privileges))
          .withPrivilegeConditions(objectMapper.writeValueAsString(conditions))
          .withDeletedAt(0L)
          .withCurrentVersion(1L)
          .withLastVersion(1L)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private static SecurableObject getDenySecurableObject() {
    return POConverters.fromSecurableObjectPO(
        "testCatalog2", getDenySecurableObjectPO(), MetadataObject.Type.CATALOG);
  }

  private static void makeCompletableFutureUseCurrentThread(JcasbinAuthorizer jcasbinAuthorizer) {
    try {
      Executor currentThread = Runnable::run;
      Class<JcasbinAuthorizer> jcasbinAuthorizerClass = JcasbinAuthorizer.class;
      Field field = jcasbinAuthorizerClass.getDeclaredField("executor");
      field.setAccessible(true);
      FieldUtils.writeField(field, jcasbinAuthorizer, currentThread);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
