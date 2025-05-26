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
import java.security.Principal;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.server.authorization.MetadataIdConverter;
import org.apache.gravitino.server.web.ObjectMapperProvider;
import org.apache.gravitino.storage.relational.po.SecurableObjectPO;
import org.apache.gravitino.storage.relational.service.MetalakeMetaService;
import org.apache.gravitino.storage.relational.service.RoleMetaService;
import org.apache.gravitino.storage.relational.service.UserMetaService;
import org.apache.gravitino.utils.NameIdentifierUtil;
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

  private static UserMetaService mockUserMetaService = mock(UserMetaService.class);

  private static RoleMetaService roleMetaService = mock(RoleMetaService.class);

  private static EntityStore entityStore = mock(EntityStore.class);

  private static GravitinoEnv gravitinoEnv = mock(GravitinoEnv.class);

  private static SupportsRelationOperations supportsRelationOperations =
      mock(SupportsRelationOperations.class);

  private static MockedStatic<PrincipalUtils> principalUtilsMockedStatic;

  private static MockedStatic<UserMetaService> userMetaServiceMockedStatic;

  private static MockedStatic<GravitinoEnv> gravitinoEnvMockedStatic;

  private static MockedStatic<MetalakeMetaService> metalakeMetaServiceMockedStatic;

  private static MockedStatic<MetadataIdConverter> metadataIdConverterMockedStatic;

  private static MockedStatic<RoleMetaService> roleMetaServiceMockedStatic;

  private static JcasbinAuthorizer jcasbinAuthorizer;

  @BeforeAll
  public static void setup() throws IOException {
    jcasbinAuthorizer = new JcasbinAuthorizer();
    jcasbinAuthorizer.initialize();
    when(mockUserMetaService.getUserIdByMetalakeIdAndName(USER_METALAKE_ID, USERNAME))
        .thenReturn(USER_ID);
    principalUtilsMockedStatic = mockStatic(PrincipalUtils.class);
    userMetaServiceMockedStatic = mockStatic(UserMetaService.class);
    metalakeMetaServiceMockedStatic = mockStatic(MetalakeMetaService.class);
    roleMetaServiceMockedStatic = mockStatic(RoleMetaService.class);
    metadataIdConverterMockedStatic = mockStatic(MetadataIdConverter.class);
    gravitinoEnvMockedStatic = mockStatic(GravitinoEnv.class);
    gravitinoEnvMockedStatic.when(GravitinoEnv::getInstance).thenReturn(gravitinoEnv);
    roleMetaServiceMockedStatic.when(RoleMetaService::getInstance).thenReturn(roleMetaService);
    userMetaServiceMockedStatic.when(UserMetaService::getInstance).thenReturn(mockUserMetaService);
    principalUtilsMockedStatic
        .when(PrincipalUtils::getCurrentPrincipal)
        .thenReturn(new UserPrincipal(USERNAME));
    metadataIdConverterMockedStatic
        .when(() -> MetadataIdConverter.doConvert(any()))
        .thenReturn(CATALOG_ID);
    roleMetaServiceMockedStatic
        .when(() -> RoleMetaService.listSecurableObjectsByRoleId(eq(ALLOW_ROLE_ID)))
        .thenReturn(ImmutableList.of(getAllowSecurableObjectPO()));
    roleMetaServiceMockedStatic
        .when(() -> RoleMetaService.listSecurableObjectsByRoleId(eq(DENY_ROLE_ID)))
        .thenReturn(ImmutableList.of(getDenySecurableObjectPO()));
    when(gravitinoEnv.entityStore()).thenReturn(entityStore);
    when(entityStore.relationOperations()).thenReturn(supportsRelationOperations);
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
    if (userMetaServiceMockedStatic != null) {
      userMetaServiceMockedStatic.close();
    }
    if (metalakeMetaServiceMockedStatic != null) {
      metalakeMetaServiceMockedStatic.close();
    }
  }

  @Test
  public void testAuthorize() throws IOException {
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    assertFalse(doAuthorize(currentPrincipal));
    RoleEntity allowRole = getRoleEntity(ALLOW_ROLE_ID);
    NameIdentifier userNameIdentifier = NameIdentifierUtil.ofUser(METALAKE, USERNAME);
    // Mock adds roles to users.
    when(supportsRelationOperations.listEntitiesByRelation(
            eq(SupportsRelationOperations.Type.ROLE_USER_REL),
            eq(userNameIdentifier),
            eq(Entity.EntityType.ROLE)))
        .thenReturn(ImmutableList.of(allowRole));
    assertTrue(doAuthorize(currentPrincipal));
    // Test role cache.
    // When permissions are changed but handleRolePrivilegeChange is not executed, the system will
    // use the cached permissions in JCasbin, so authorize can succeed.
    Long newRoleId = -1L;
    RoleEntity tempNewRole = getRoleEntity(newRoleId);
    when(supportsRelationOperations.listEntitiesByRelation(
            eq(SupportsRelationOperations.Type.ROLE_USER_REL),
            eq(userNameIdentifier),
            eq(Entity.EntityType.ROLE)))
        .thenReturn(ImmutableList.of(tempNewRole));
    assertTrue(doAuthorize(currentPrincipal));
    // After clearing the cache, authorize will fail
    jcasbinAuthorizer.handleRolePrivilegeChange(ALLOW_ROLE_ID);
    assertFalse(doAuthorize(currentPrincipal));
    // When the user is re-assigned the correct role, the authorization will succeed.
    when(supportsRelationOperations.listEntitiesByRelation(
            eq(SupportsRelationOperations.Type.ROLE_USER_REL),
            eq(userNameIdentifier),
            eq(Entity.EntityType.ROLE)))
        .thenReturn(ImmutableList.of(allowRole));
    assertTrue(doAuthorize(currentPrincipal));
    // Test deny
    RoleEntity denyRole = getRoleEntity(DENY_ROLE_ID);
    when(supportsRelationOperations.listEntitiesByRelation(
            eq(SupportsRelationOperations.Type.ROLE_USER_REL),
            eq(userNameIdentifier),
            eq(Entity.EntityType.ROLE)))
        .thenReturn(ImmutableList.of(allowRole, denyRole));
    assertFalse(doAuthorize(currentPrincipal));
  }

  private boolean doAuthorize(Principal currentPrincipal) {
    return jcasbinAuthorizer.authorize(
        currentPrincipal,
        "testMetalake",
        MetadataObjects.of(null, "testCatalog", MetadataObject.Type.CATALOG),
        USE_CATALOG);
  }

  private static RoleEntity getRoleEntity(Long roleId) {
    return RoleEntity.builder()
        .withId(roleId)
        .withName("roleName")
        .withAuditInfo(AuditInfo.EMPTY)
        .build();
  }

  private static SecurableObjectPO getAllowSecurableObjectPO() {
    ImmutableList<Privilege.Name> privileges = ImmutableList.of(USE_CATALOG);
    ImmutableList<String> conditions = ImmutableList.of("ALLOW");
    ObjectMapper objectMapper = ObjectMapperProvider.objectMapper();
    try {
      return SecurableObjectPO.builder()
          .withType(String.valueOf(MetadataObject.Type.CATALOG))
          .withMetadataObjectId(CATALOG_ID)
          .withRoleId(ALLOW_ROLE_ID)
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

  private static SecurableObjectPO getDenySecurableObjectPO() {
    ImmutableList<Privilege.Name> privileges = ImmutableList.of(USE_CATALOG);
    ImmutableList<String> conditions = ImmutableList.of("DENY");
    ObjectMapper objectMapper = ObjectMapperProvider.objectMapper();
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
}
