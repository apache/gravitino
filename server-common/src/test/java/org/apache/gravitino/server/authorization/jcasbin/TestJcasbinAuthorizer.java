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

import static org.apache.gravitino.authorization.Privilege.Name.RUN_JOB;
import static org.apache.gravitino.authorization.Privilege.Name.SELECT_TABLE;
import static org.apache.gravitino.authorization.Privilege.Name.USE_CATALOG;
import static org.apache.gravitino.authorization.Privilege.Name.USE_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.lang.reflect.Field;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.casbin.jcasbin.main.Enforcer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
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

  @BeforeEach
  public void resetUserRoleStubBetweenTests() throws IOException {
    // Restore the default empty role assignment for "tester" so that one test's stubbed roles
    // don't leak into the next test. Each test re-stubs its own role list before asserting.
    mockUserRoles(NameIdentifierUtil.ofUser(METALAKE, USERNAME));
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
  public void testAuthorize() throws Exception {
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
    // After re-assigning the user from allowRole to a role with no privileges, authorize must
    // return false even though allowRole's policies are still cached in the enforcer. Each
    // request iterates the user's fresh role list, so removed role assignments take effect
    // immediately without waiting for a handleRolePrivilegeChange call.
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
    assertFalse(doAuthorize(currentPrincipal));
    // Invalidating the role cache is still a no-op in this scenario; we left it in to exercise
    // the invalidation path.
    jcasbinAuthorizer.handleRolePrivilegeChange(ALLOW_ROLE_ID);
    assertFalse(doAuthorize(currentPrincipal));
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
  public void testAuthorizeByOwner() throws Exception {
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    assertFalse(doAuthorizeOwner(currentPrincipal));
    NameIdentifier catalogIdent = NameIdentifierUtil.ofCatalog(METALAKE, "testCatalog");
    List<UserEntity> owners = ImmutableList.of(getUserEntity());
    doReturn(owners)
        .when(supportsRelationOperations)
        .listEntitiesByRelation(
            eq(SupportsRelationOperations.Type.OWNER_REL),
            eq(catalogIdent),
            eq(Entity.EntityType.CATALOG));
    getOwnerRelCache(jcasbinAuthorizer).invalidateAll();
    assertTrue(doAuthorizeOwner(currentPrincipal));
    doReturn(new ArrayList<>())
        .when(supportsRelationOperations)
        .listEntitiesByRelation(
            eq(SupportsRelationOperations.Type.OWNER_REL),
            eq(catalogIdent),
            eq(Entity.EntityType.CATALOG));
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
    AuthorizationRequestContext authorizationRequestContext = new AuthorizationRequestContext();
    return jcasbinAuthorizer.isOwner(
        currentPrincipal,
        "testMetalake",
        MetadataObjects.of(null, "testCatalog", MetadataObject.Type.CATALOG),
        authorizationRequestContext);
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

  @Test
  public void testRoleCacheInvalidation() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);

    // Get the loadedRoles cache via reflection
    Cache<Long, Map<JcasbinAuthorizer.PolicyKey, JcasbinAuthorizer.Effect>> loadedRoles =
        getLoadedRolesCache(jcasbinAuthorizer);

    // Manually add a role to the cache
    Long testRoleId = 100L;
    loadedRoles.put(testRoleId, Collections.emptyMap());

    // Verify it's in the cache
    assertNotNull(loadedRoles.getIfPresent(testRoleId));

    // Call handleRolePrivilegeChange which should invalidate the cache entry
    jcasbinAuthorizer.handleRolePrivilegeChange(testRoleId);

    // Verify it's removed from the cache
    assertNull(loadedRoles.getIfPresent(testRoleId));
  }

  @Test
  public void testOwnerCacheInvalidation() throws Exception {
    // Get the ownerRel cache via reflection
    Cache<Long, Optional<Long>> ownerRel = getOwnerRelCache(jcasbinAuthorizer);

    // Manually add an owner relation to the cache
    ownerRel.put(CATALOG_ID, Optional.of(USER_ID));

    // Verify it's in the cache
    assertNotNull(ownerRel.getIfPresent(CATALOG_ID));

    // Create a mock NameIdentifier for the metadata object
    NameIdentifier catalogIdent = NameIdentifierUtil.ofCatalog(METALAKE, "testCatalog");

    // Call handleMetadataOwnerChange which should invalidate the cache entry
    jcasbinAuthorizer.handleMetadataOwnerChange(
        METALAKE, USER_ID, catalogIdent, Entity.EntityType.CATALOG);

    // Verify it's removed from the cache
    assertNull(ownerRel.getIfPresent(CATALOG_ID));
  }

  @Test
  public void testRoleCacheSynchronousRemovalListenerDeletesPolicy() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);

    // Get the enforcers via reflection
    Enforcer allowEnforcer = getAllowEnforcer(jcasbinAuthorizer);
    Enforcer denyEnforcer = getDenyEnforcer(jcasbinAuthorizer);

    // Get the loadedRoles cache
    Cache<Long, Map<JcasbinAuthorizer.PolicyKey, JcasbinAuthorizer.Effect>> loadedRoles =
        getLoadedRolesCache(jcasbinAuthorizer);

    // Add a role and its policy to the enforcer
    Long testRoleId = 300L;
    String roleIdStr = String.valueOf(testRoleId);

    // Add a policy for this role
    allowEnforcer.addPolicy(roleIdStr, "CATALOG", "999", "USE_CATALOG", "allow");
    denyEnforcer.addPolicy(roleIdStr, "CATALOG", "999", "USE_CATALOG", "allow");

    // Add role to cache
    loadedRoles.put(testRoleId, Collections.emptyMap());

    // Verify role exists in enforcer (has policy)
    assertTrue(allowEnforcer.hasPolicy(roleIdStr, "CATALOG", "999", "USE_CATALOG", "allow"));
    assertTrue(denyEnforcer.hasPolicy(roleIdStr, "CATALOG", "999", "USE_CATALOG", "allow"));

    // Invalidate the cache entry - this triggers the synchronous removal listener
    // (using executor(Runnable::run) to ensure synchronous execution)
    loadedRoles.invalidate(testRoleId);

    // Verify the role's policies have been deleted from enforcers (synchronous, no need to wait)
    assertFalse(allowEnforcer.hasPolicy(roleIdStr, "CATALOG", "999", "USE_CATALOG", "allow"));
    assertFalse(denyEnforcer.hasPolicy(roleIdStr, "CATALOG", "999", "USE_CATALOG", "allow"));
  }

  @Test
  public void testAuthorizeAndDenyReturnFalseForUserWithNoRoles() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    NameIdentifier userIdent = NameIdentifierUtil.ofUser(METALAKE, USERNAME);
    mockUserRoles(userIdent);
    AuthorizationRequestContext ctx = new AuthorizationRequestContext();
    MetadataObject catalog = MetadataObjects.of(null, "testCatalog", MetadataObject.Type.CATALOG);
    assertFalse(jcasbinAuthorizer.authorize(currentPrincipal, METALAKE, catalog, USE_CATALOG, ctx));
    assertFalse(jcasbinAuthorizer.deny(currentPrincipal, METALAKE, catalog, USE_CATALOG, ctx));
  }

  @Test
  public void testDenyEndpointReturnsTrueForExplicitDenyRole() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    NameIdentifier userIdent = NameIdentifierUtil.ofUser(METALAKE, USERNAME);
    Long roleId = 1001L;
    RoleEntity denyRole =
        getRoleEntity(
            roleId,
            "denyOnlyRole" + roleId,
            ImmutableList.of(
                makeSecurableObject(
                    "testCatalog", MetadataObject.Type.CATALOG, roleId, USE_CATALOG, "DENY")));
    mockRoleEntity(denyRole);
    mockUserRoles(userIdent, denyRole);
    MetadataObject catalog = MetadataObjects.of(null, "testCatalog", MetadataObject.Type.CATALOG);
    AuthorizationRequestContext ctx = new AuthorizationRequestContext();
    assertTrue(jcasbinAuthorizer.deny(currentPrincipal, METALAKE, catalog, USE_CATALOG, ctx));
    assertFalse(jcasbinAuthorizer.authorize(currentPrincipal, METALAKE, catalog, USE_CATALOG, ctx));
  }

  @Test
  public void testDenyEndpointReturnsFalseForAllowOnlyRole() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    NameIdentifier userIdent = NameIdentifierUtil.ofUser(METALAKE, USERNAME);
    Long roleId = 1002L;
    RoleEntity allowRole =
        getRoleEntity(
            roleId,
            "allowOnlyRole" + roleId,
            ImmutableList.of(
                makeSecurableObject(
                    "testCatalog", MetadataObject.Type.CATALOG, roleId, USE_CATALOG, "ALLOW")));
    mockRoleEntity(allowRole);
    mockUserRoles(userIdent, allowRole);
    MetadataObject catalog = MetadataObjects.of(null, "testCatalog", MetadataObject.Type.CATALOG);
    AuthorizationRequestContext ctx = new AuthorizationRequestContext();
    assertTrue(jcasbinAuthorizer.authorize(currentPrincipal, METALAKE, catalog, USE_CATALOG, ctx));
    assertFalse(jcasbinAuthorizer.deny(currentPrincipal, METALAKE, catalog, USE_CATALOG, ctx));
  }

  @Test
  public void testLoadPolicyByRoleEntityAddsAllowOnlyToAllowEnforcer() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    NameIdentifier userIdent = NameIdentifierUtil.ofUser(METALAKE, USERNAME);
    Long roleId = 2010L;
    RoleEntity allowRole =
        getRoleEntity(
            roleId,
            "allowLoadRole" + roleId,
            ImmutableList.of(
                makeSecurableObject(
                    "testCatalog", MetadataObject.Type.CATALOG, roleId, USE_CATALOG, "ALLOW")));
    mockRoleEntity(allowRole);
    mockUserRoles(userIdent, allowRole);

    MetadataObject catalog = MetadataObjects.of(null, "testCatalog", MetadataObject.Type.CATALOG);
    assertTrue(
        jcasbinAuthorizer.authorize(
            currentPrincipal, METALAKE, catalog, USE_CATALOG, new AuthorizationRequestContext()));

    String roleIdStr = String.valueOf(roleId);
    String metadataIdStr = String.valueOf(CATALOG_ID);
    Enforcer allowEnforcer = getAllowEnforcer(jcasbinAuthorizer);
    Enforcer denyEnforcer = getDenyEnforcer(jcasbinAuthorizer);
    assertTrue(
        allowEnforcer.hasPolicy(roleIdStr, "CATALOG", metadataIdStr, "USE_CATALOG", "allow"));
    assertFalse(
        denyEnforcer.hasPolicy(roleIdStr, "CATALOG", metadataIdStr, "USE_CATALOG", "allow"));

    Cache<Long, Map<JcasbinAuthorizer.PolicyKey, JcasbinAuthorizer.Effect>> loadedRoles =
        getLoadedRolesCache(jcasbinAuthorizer);
    assertEquals(
        JcasbinAuthorizer.Effect.ALLOW,
        loadedRoles
            .getIfPresent(roleId)
            .get(new JcasbinAuthorizer.PolicyKey("CATALOG", CATALOG_ID, "USE_CATALOG")));
  }

  @Test
  public void testLoadPolicyByRoleEntityAddsDenyToBothEnforcersWithExpectedEffects()
      throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    NameIdentifier userIdent = NameIdentifierUtil.ofUser(METALAKE, USERNAME);
    Long roleId = 2011L;
    RoleEntity denyRole =
        getRoleEntity(
            roleId,
            "denyLoadRole" + roleId,
            ImmutableList.of(
                makeSecurableObject(
                    "testCatalog", MetadataObject.Type.CATALOG, roleId, USE_CATALOG, "DENY")));
    mockRoleEntity(denyRole);
    mockUserRoles(userIdent, denyRole);

    MetadataObject catalog = MetadataObjects.of(null, "testCatalog", MetadataObject.Type.CATALOG);
    assertFalse(
        jcasbinAuthorizer.authorize(
            currentPrincipal, METALAKE, catalog, USE_CATALOG, new AuthorizationRequestContext()));
    assertTrue(
        jcasbinAuthorizer.deny(
            currentPrincipal, METALAKE, catalog, USE_CATALOG, new AuthorizationRequestContext()));

    String roleIdStr = String.valueOf(roleId);
    String metadataIdStr = String.valueOf(CATALOG_ID);
    Enforcer allowEnforcer = getAllowEnforcer(jcasbinAuthorizer);
    Enforcer denyEnforcer = getDenyEnforcer(jcasbinAuthorizer);
    assertTrue(allowEnforcer.hasPolicy(roleIdStr, "CATALOG", metadataIdStr, "USE_CATALOG", "deny"));
    assertTrue(denyEnforcer.hasPolicy(roleIdStr, "CATALOG", metadataIdStr, "USE_CATALOG", "allow"));

    Cache<Long, Map<JcasbinAuthorizer.PolicyKey, JcasbinAuthorizer.Effect>> loadedRoles =
        getLoadedRolesCache(jcasbinAuthorizer);
    assertEquals(
        JcasbinAuthorizer.Effect.DENY,
        loadedRoles
            .getIfPresent(roleId)
            .get(new JcasbinAuthorizer.PolicyKey("CATALOG", CATALOG_ID, "USE_CATALOG")));
  }

  @Test
  public void testCrossRoleDenyBeatsAllowOnBothEndpoints() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    NameIdentifier userIdent = NameIdentifierUtil.ofUser(METALAKE, USERNAME);
    Long allowRoleId = 1007L;
    Long denyRoleId = 1008L;
    RoleEntity allowRole =
        getRoleEntity(
            allowRoleId,
            "mixedAllow" + allowRoleId,
            ImmutableList.of(
                makeSecurableObject(
                    "testCatalog",
                    MetadataObject.Type.CATALOG,
                    allowRoleId,
                    USE_CATALOG,
                    "ALLOW")));
    RoleEntity denyRole =
        getRoleEntity(
            denyRoleId,
            "mixedDeny" + denyRoleId,
            ImmutableList.of(
                makeSecurableObject(
                    "testCatalog", MetadataObject.Type.CATALOG, denyRoleId, USE_CATALOG, "DENY")));
    mockRoleEntity(allowRole);
    mockRoleEntity(denyRole);
    mockUserRoles(userIdent, allowRole, denyRole);
    MetadataObject catalog = MetadataObjects.of(null, "testCatalog", MetadataObject.Type.CATALOG);
    AuthorizationRequestContext ctx = new AuthorizationRequestContext();
    assertFalse(jcasbinAuthorizer.authorize(currentPrincipal, METALAKE, catalog, USE_CATALOG, ctx));
    assertTrue(jcasbinAuthorizer.deny(currentPrincipal, METALAKE, catalog, USE_CATALOG, ctx));
  }

  @Test
  public void testRoleAssignmentChangeImmediatelyVisibleToDenyEndpoint() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    NameIdentifier userIdent = NameIdentifierUtil.ofUser(METALAKE, USERNAME);
    Long denyRoleId = 1009L;
    RoleEntity denyRole =
        getRoleEntity(
            denyRoleId,
            "denyAssignRole" + denyRoleId,
            ImmutableList.of(
                makeSecurableObject(
                    "testCatalog", MetadataObject.Type.CATALOG, denyRoleId, USE_CATALOG, "DENY")));
    mockRoleEntity(denyRole);
    mockUserRoles(userIdent, denyRole);
    MetadataObject catalog = MetadataObjects.of(null, "testCatalog", MetadataObject.Type.CATALOG);
    assertTrue(
        jcasbinAuthorizer.deny(
            currentPrincipal, METALAKE, catalog, USE_CATALOG, new AuthorizationRequestContext()));
    // Revoke the deny role assignment from the user. Even though the enforcer still has the
    // user→role edge from the previous request and the role's policy is still cached, the new
    // request reads a fresh userRoleIds and therefore must no longer see the deny.
    mockUserRoles(userIdent);
    assertFalse(
        jcasbinAuthorizer.deny(
            currentPrincipal, METALAKE, catalog, USE_CATALOG, new AuthorizationRequestContext()));
  }

  @Test
  public void testHandleRolePrivilegeChangeRemovesRoleFromIndex() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    NameIdentifier userIdent = NameIdentifierUtil.ofUser(METALAKE, USERNAME);
    Long roleId = 1003L;
    RoleEntity role =
        getRoleEntity(
            roleId,
            "indexRole" + roleId,
            ImmutableList.of(
                makeSecurableObject(
                    "testCatalog", MetadataObject.Type.CATALOG, roleId, USE_CATALOG, "ALLOW")));
    mockRoleEntity(role);
    mockUserRoles(userIdent, role);
    assertTrue(
        jcasbinAuthorizer.authorize(
            currentPrincipal,
            METALAKE,
            MetadataObjects.of(null, "testCatalog", MetadataObject.Type.CATALOG),
            USE_CATALOG,
            new AuthorizationRequestContext()));
    Cache<Long, Map<JcasbinAuthorizer.PolicyKey, JcasbinAuthorizer.Effect>> loadedRoles =
        getLoadedRolesCache(jcasbinAuthorizer);
    assertNotNull(loadedRoles.getIfPresent(roleId));
    jcasbinAuthorizer.handleRolePrivilegeChange(roleId);
    assertNull(loadedRoles.getIfPresent(roleId));
  }

  @Test
  public void testRoleIndexReloadAfterPrivilegeChange() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    NameIdentifier userIdent = NameIdentifierUtil.ofUser(METALAKE, USERNAME);
    Long roleId = 1004L;
    RoleEntity withPrivilege =
        getRoleEntity(
            roleId,
            "reloadRole" + roleId,
            ImmutableList.of(
                makeSecurableObject(
                    "testCatalog", MetadataObject.Type.CATALOG, roleId, USE_CATALOG, "ALLOW")));
    mockRoleEntity(withPrivilege);
    mockUserRoles(userIdent, withPrivilege);
    MetadataObject catalog = MetadataObjects.of(null, "testCatalog", MetadataObject.Type.CATALOG);
    assertTrue(
        jcasbinAuthorizer.authorize(
            currentPrincipal, METALAKE, catalog, USE_CATALOG, new AuthorizationRequestContext()));
    // Revoke the privilege from the role and invalidate. The index must be repopulated
    // from the new (empty) policy set, not stay stuck on the old ALLOW entry.
    RoleEntity withoutPrivilege = getRoleEntity(roleId, "reloadRole" + roleId, ImmutableList.of());
    mockRoleEntity(withoutPrivilege);
    jcasbinAuthorizer.handleRolePrivilegeChange(roleId);
    assertFalse(
        jcasbinAuthorizer.authorize(
            currentPrincipal, METALAKE, catalog, USE_CATALOG, new AuthorizationRequestContext()));
  }

  @Test
  public void testConcurrentRoleLoadsShareOnePolicyIndex() throws Exception {
    Long roleId = 1010L;
    JcasbinAuthorizer.PolicyKey policyKey =
        new JcasbinAuthorizer.PolicyKey("CATALOG", CATALOG_ID, "USE_CATALOG");
    Map<JcasbinAuthorizer.PolicyKey, JcasbinAuthorizer.Effect> expectedIndex =
        Collections.singletonMap(policyKey, JcasbinAuthorizer.Effect.ALLOW);
    Cache<Long, Map<JcasbinAuthorizer.PolicyKey, JcasbinAuthorizer.Effect>> loadedRoles =
        getLoadedRolesCache(jcasbinAuthorizer);
    AtomicInteger roleLoadCount = new AtomicInteger();
    CountDownLatch ready = new CountDownLatch(2);
    CountDownLatch start = new CountDownLatch(1);
    ExecutorService requestExecutor = Executors.newFixedThreadPool(2);
    try {
      Future<Map<JcasbinAuthorizer.PolicyKey, JcasbinAuthorizer.Effect>> first =
          requestExecutor.submit(
              () -> {
                ready.countDown();
                start.await(5, TimeUnit.SECONDS);
                return loadedRoles.get(
                    roleId,
                    unused -> {
                      roleLoadCount.incrementAndGet();
                      try {
                        Thread.sleep(100L);
                      } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                      }
                      return expectedIndex;
                    });
              });
      Future<Map<JcasbinAuthorizer.PolicyKey, JcasbinAuthorizer.Effect>> second =
          requestExecutor.submit(
              () -> {
                ready.countDown();
                start.await(5, TimeUnit.SECONDS);
                return loadedRoles.get(
                    roleId,
                    unused -> {
                      roleLoadCount.incrementAndGet();
                      try {
                        Thread.sleep(100L);
                      } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                      }
                      return expectedIndex;
                    });
              });
      assertTrue(ready.await(5, TimeUnit.SECONDS));
      start.countDown();
      assertEquals(expectedIndex, first.get(5, TimeUnit.SECONDS));
      assertEquals(expectedIndex, second.get(5, TimeUnit.SECONDS));
    } finally {
      requestExecutor.shutdownNow();
    }
    assertEquals(1, roleLoadCount.get());
    assertEquals(expectedIndex, loadedRoles.getIfPresent(roleId));
  }

  @Test
  public void testLoadedRoleCacheStoresPolicyIndexValue() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    NameIdentifier userIdent = NameIdentifierUtil.ofUser(METALAKE, USERNAME);
    Long roleId = 1011L;
    RoleEntity role =
        getRoleEntity(
            roleId,
            "cachedIndexRole" + roleId,
            ImmutableList.of(
                makeSecurableObject(
                    "testCatalog", MetadataObject.Type.CATALOG, roleId, USE_CATALOG, "ALLOW")));
    mockRoleEntity(role);
    mockUserRoles(userIdent, role);
    MetadataObject catalog = MetadataObjects.of(null, "testCatalog", MetadataObject.Type.CATALOG);
    assertTrue(
        jcasbinAuthorizer.authorize(
            currentPrincipal, METALAKE, catalog, USE_CATALOG, new AuthorizationRequestContext()));
    Map<JcasbinAuthorizer.PolicyKey, JcasbinAuthorizer.Effect> policyIndex =
        getLoadedRolesCache(jcasbinAuthorizer).getIfPresent(roleId);
    assertNotNull(policyIndex);
    assertEquals(
        JcasbinAuthorizer.Effect.ALLOW,
        policyIndex.get(new JcasbinAuthorizer.PolicyKey("CATALOG", CATALOG_ID, "USE_CATALOG")));
  }

  @Test
  public void testCachedRolePolicyIndexSkipsRoleEntityLoad() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    NameIdentifier userIdent = NameIdentifierUtil.ofUser(METALAKE, USERNAME);
    Long roleId = 1012L;
    RoleEntity role = getRoleEntity(roleId, "cachedRole" + roleId, ImmutableList.of());
    mockUserRoles(userIdent, role);
    getLoadedRolesCache(jcasbinAuthorizer)
        .put(
            roleId,
            Collections.singletonMap(
                new JcasbinAuthorizer.PolicyKey("CATALOG", CATALOG_ID, "USE_CATALOG"),
                JcasbinAuthorizer.Effect.ALLOW));
    when(entityStore.get(
            eq(NameIdentifierUtil.ofRole(METALAKE, role.name())),
            eq(Entity.EntityType.ROLE),
            eq(RoleEntity.class)))
        .thenThrow(new AssertionError("Cached roles should not be loaded again."));
    assertTrue(
        jcasbinAuthorizer.authorize(
            currentPrincipal,
            METALAKE,
            MetadataObjects.of(null, "testCatalog", MetadataObject.Type.CATALOG),
            USE_CATALOG,
            new AuthorizationRequestContext()));
  }

  @Test
  public void testPolicyKeyEqualityAndHash() {
    JcasbinAuthorizer.PolicyKey base =
        new JcasbinAuthorizer.PolicyKey("CATALOG", 1L, "USE_CATALOG");
    JcasbinAuthorizer.PolicyKey same =
        new JcasbinAuthorizer.PolicyKey("CATALOG", 1L, "USE_CATALOG");
    JcasbinAuthorizer.PolicyKey differentType =
        new JcasbinAuthorizer.PolicyKey("SCHEMA", 1L, "USE_CATALOG");
    JcasbinAuthorizer.PolicyKey differentId =
        new JcasbinAuthorizer.PolicyKey("CATALOG", 2L, "USE_CATALOG");
    JcasbinAuthorizer.PolicyKey differentPrivilege =
        new JcasbinAuthorizer.PolicyKey("CATALOG", 1L, "SELECT_TABLE");
    assertEquals(base, same);
    assertEquals(base.hashCode(), same.hashCode());
    assertNotEquals(base, differentType);
    assertNotEquals(base, differentId);
    assertNotEquals(base, differentPrivilege);
    assertNotEquals(base, null);
    assertNotEquals(base, "not a policy key");
  }

  @Test
  public void testIcebergTableLevelAuthorizeMatchesByEntityType() throws Exception {
    // Iceberg API auth resolves to a TABLE-typed MetadataObject and probes SELECT_TABLE on it.
    // The per-role index must key on entity type, so a TABLE-level grant must NOT satisfy a
    // CATALOG-level probe (different type → different PolicyKey).
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    NameIdentifier userIdent = NameIdentifierUtil.ofUser(METALAKE, USERNAME);
    Long roleId = 1005L;
    RoleEntity tableRole =
        getRoleEntity(
            roleId,
            "icebergTableRole" + roleId,
            ImmutableList.of(
                makeSecurableObject(
                    "icebergCat.icebergSchema.icebergTable",
                    MetadataObject.Type.TABLE,
                    roleId,
                    SELECT_TABLE,
                    "ALLOW")));
    mockRoleEntity(tableRole);
    mockUserRoles(userIdent, tableRole);

    assertTrue(
        jcasbinAuthorizer.authorize(
            currentPrincipal,
            METALAKE,
            MetadataObjects.of(
                ImmutableList.of("icebergCat", "icebergSchema", "icebergTable"),
                MetadataObject.Type.TABLE),
            SELECT_TABLE,
            new AuthorizationRequestContext()));

    // Different privilege on the same TABLE: no policy → false.
    assertFalse(
        jcasbinAuthorizer.authorize(
            currentPrincipal,
            METALAKE,
            MetadataObjects.of(
                ImmutableList.of("icebergCat", "icebergSchema", "icebergTable"),
                MetadataObject.Type.TABLE),
            USE_CATALOG,
            new AuthorizationRequestContext()));

    // Different entity type, same id: PolicyKey differs by `type`, so this must be false.
    assertFalse(
        jcasbinAuthorizer.authorize(
            currentPrincipal,
            METALAKE,
            MetadataObjects.of(null, "icebergCat", MetadataObject.Type.CATALOG),
            SELECT_TABLE,
            new AuthorizationRequestContext()));
  }

  @Test
  public void testGravitinoApiSchemaLevelAuthorize() throws Exception {
    // Gravitino API auth flows through AuthorizationExpressionEvaluator → JcasbinAuthorizer with
    // SCHEMA-typed objects for schema-scoped operations. Verify that a SCHEMA-level USE_SCHEMA
    // grant via a role authorizes the schema and only the schema.
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    NameIdentifier userIdent = NameIdentifierUtil.ofUser(METALAKE, USERNAME);
    Long roleId = 1006L;
    RoleEntity schemaRole =
        getRoleEntity(
            roleId,
            "schemaRole" + roleId,
            ImmutableList.of(
                makeSecurableObject(
                    "cat1.schema1", MetadataObject.Type.SCHEMA, roleId, USE_SCHEMA, "ALLOW")));
    mockRoleEntity(schemaRole);
    mockUserRoles(userIdent, schemaRole);

    assertTrue(
        jcasbinAuthorizer.authorize(
            currentPrincipal,
            METALAKE,
            MetadataObjects.of(ImmutableList.of("cat1", "schema1"), MetadataObject.Type.SCHEMA),
            USE_SCHEMA,
            new AuthorizationRequestContext()));

    // USE_CATALOG on the parent catalog has no matching policy.
    assertFalse(
        jcasbinAuthorizer.authorize(
            currentPrincipal,
            METALAKE,
            MetadataObjects.of(null, "cat1", MetadataObject.Type.CATALOG),
            USE_CATALOG,
            new AuthorizationRequestContext()));
  }

  @Test
  public void testIntraRoleDenyBeatsAllowForSameKey() throws Exception {
    // Within a single role, granting both ALLOW and DENY for the same (type, metadataId,
    // privilege) must resolve to DENY. The unified resolveEffect relies on
    // loadPolicyByRoleEntity's merge function (existing == DENY ? existing : incoming) to enforce
    // intra-role priority, so authorize/deny see DENY regardless of insertion order.
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    NameIdentifier userIdent = NameIdentifierUtil.ofUser(METALAKE, USERNAME);
    Long roleId = 2020L;
    SecurableObject allow =
        makeSecurableObject(
            "testCatalog", MetadataObject.Type.CATALOG, roleId, USE_CATALOG, "ALLOW");
    SecurableObject deny =
        makeSecurableObject(
            "testCatalog", MetadataObject.Type.CATALOG, roleId, USE_CATALOG, "DENY");
    RoleEntity mixedRole =
        getRoleEntity(roleId, "mixedSameKey" + roleId, ImmutableList.of(allow, deny));
    mockRoleEntity(mixedRole);
    mockUserRoles(userIdent, mixedRole);

    MetadataObject catalog = MetadataObjects.of(null, "testCatalog", MetadataObject.Type.CATALOG);
    AuthorizationRequestContext ctx = new AuthorizationRequestContext();
    assertFalse(jcasbinAuthorizer.authorize(currentPrincipal, METALAKE, catalog, USE_CATALOG, ctx));
    assertTrue(jcasbinAuthorizer.deny(currentPrincipal, METALAKE, catalog, USE_CATALOG, ctx));

    Cache<Long, Map<JcasbinAuthorizer.PolicyKey, JcasbinAuthorizer.Effect>> loadedRoles =
        getLoadedRolesCache(jcasbinAuthorizer);
    assertEquals(
        JcasbinAuthorizer.Effect.DENY,
        loadedRoles
            .getIfPresent(roleId)
            .get(new JcasbinAuthorizer.PolicyKey("CATALOG", CATALOG_ID, "USE_CATALOG")));
  }

  @Test
  public void testAuthorizeAndDenyBothFalseWhenRoleHasNoRuleForKey() throws Exception {
    // The role grants USE_CATALOG on the catalog but the request probes SELECT_TABLE. The shared
    // resolver returns null (no matching rule), so both endpoints must return false. This guards
    // against the unified resolveEffect collapsing "no rule" into either ALLOW or DENY.
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    NameIdentifier userIdent = NameIdentifierUtil.ofUser(METALAKE, USERNAME);
    Long roleId = 2021L;
    RoleEntity role =
        getRoleEntity(
            roleId,
            "noRuleRole" + roleId,
            ImmutableList.of(
                makeSecurableObject(
                    "testCatalog", MetadataObject.Type.CATALOG, roleId, USE_CATALOG, "ALLOW")));
    mockRoleEntity(role);
    mockUserRoles(userIdent, role);

    MetadataObject catalog = MetadataObjects.of(null, "testCatalog", MetadataObject.Type.CATALOG);
    AuthorizationRequestContext ctx = new AuthorizationRequestContext();
    assertFalse(
        jcasbinAuthorizer.authorize(currentPrincipal, METALAKE, catalog, SELECT_TABLE, ctx));
    assertFalse(jcasbinAuthorizer.deny(currentPrincipal, METALAKE, catalog, SELECT_TABLE, ctx));
  }

  @Test
  public void testAuthorizeAndDenyShareSameResolveAcrossPrivileges() throws Exception {
    // A single role grants ALLOW on USE_CATALOG and DENY on USE_SCHEMA for the same metadata
    // object. The unified resolveEffect must return the corresponding Effect for each PolicyKey
    // independently, so authorize/deny disagree per privilege rather than collapsing to one
    // verdict for the whole role. This is the core property roryqi asked for: one resolver, two
    // endpoints differing only in how they compare the result.
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    NameIdentifier userIdent = NameIdentifierUtil.ofUser(METALAKE, USERNAME);
    Long roleId = 2022L;
    SecurableObject allowUseCatalog =
        makeSecurableObject(
            "testCatalog", MetadataObject.Type.CATALOG, roleId, USE_CATALOG, "ALLOW");
    SecurableObject denyUseSchema =
        makeSecurableObject("testCatalog", MetadataObject.Type.CATALOG, roleId, USE_SCHEMA, "DENY");
    RoleEntity role =
        getRoleEntity(
            roleId, "mixedPrivRole" + roleId, ImmutableList.of(allowUseCatalog, denyUseSchema));
    mockRoleEntity(role);
    mockUserRoles(userIdent, role);

    MetadataObject catalog = MetadataObjects.of(null, "testCatalog", MetadataObject.Type.CATALOG);
    AuthorizationRequestContext ctx = new AuthorizationRequestContext();
    assertTrue(jcasbinAuthorizer.authorize(currentPrincipal, METALAKE, catalog, USE_CATALOG, ctx));
    assertFalse(jcasbinAuthorizer.deny(currentPrincipal, METALAKE, catalog, USE_CATALOG, ctx));
    assertFalse(jcasbinAuthorizer.authorize(currentPrincipal, METALAKE, catalog, USE_SCHEMA, ctx));
    assertTrue(jcasbinAuthorizer.deny(currentPrincipal, METALAKE, catalog, USE_SCHEMA, ctx));
  }

  @Test
  public void testAuthorizeReturnsFalseForDirectPrivilegeDenyOnly() throws Exception {
    // Reproduces the case roryqi flagged: an OGNL expression like `METALAKE::RUN_JOB` is rewritten
    // by AuthorizationExpressionConverter to a *single* `authorizer.authorize(...)` call — there is
    // no companion `authorizer.deny(...)` call (which only ANY_xxx aliases generate). So the
    // authorize endpoint must independently respect explicit DENY policies; a role that grants
    // only DENY RUN_JOB on the metalake must cause authorize() to return false.
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    NameIdentifier userIdent = NameIdentifierUtil.ofUser(METALAKE, USERNAME);
    Long roleId = 3001L;
    RoleEntity denyRole =
        getRoleEntity(
            roleId,
            "denyRunJobRole" + roleId,
            ImmutableList.of(
                makeSecurableObject(
                    "testMetalake", MetadataObject.Type.METALAKE, roleId, RUN_JOB, "DENY")));
    mockRoleEntity(denyRole);
    mockUserRoles(userIdent, denyRole);

    MetadataObject metalake = MetadataObjects.of(null, METALAKE, MetadataObject.Type.METALAKE);
    AuthorizationRequestContext ctx = new AuthorizationRequestContext();
    assertFalse(jcasbinAuthorizer.authorize(currentPrincipal, METALAKE, metalake, RUN_JOB, ctx));
    assertTrue(jcasbinAuthorizer.deny(currentPrincipal, METALAKE, metalake, RUN_JOB, ctx));
  }

  @Test
  public void testAuthorizeReturnsFalseForDirectPrivilegeDenyBeatsAllowRole() throws Exception {
    // Companion to testAuthorizeReturnsFalseForDirectPrivilegeDenyOnly per roryqi's review: also
    // assign the user a sibling role that ALLOWs RUN_JOB on the same metalake, so the test pins
    // down DENY-beats-ALLOW on the authorize endpoint when the OGNL expression `METALAKE::RUN_JOB`
    // emits only an authorize() call (no deny() probe). Distinct role IDs from any other test to
    // avoid cache-pollution interactions.
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    NameIdentifier userIdent = NameIdentifierUtil.ofUser(METALAKE, USERNAME);
    Long denyRoleId = 3010L;
    Long allowRoleId = 3011L;
    RoleEntity denyRole =
        getRoleEntity(
            denyRoleId,
            "denyRunJobRole" + denyRoleId,
            ImmutableList.of(
                makeSecurableObject(
                    "testMetalake", MetadataObject.Type.METALAKE, denyRoleId, RUN_JOB, "DENY")));
    RoleEntity allowRole =
        getRoleEntity(
            allowRoleId,
            "allowRunJobRole" + allowRoleId,
            ImmutableList.of(
                makeSecurableObject(
                    "testMetalake", MetadataObject.Type.METALAKE, allowRoleId, RUN_JOB, "ALLOW")));
    mockRoleEntity(denyRole);
    mockRoleEntity(allowRole);
    mockUserRoles(userIdent, denyRole, allowRole);

    MetadataObject metalake = MetadataObjects.of(null, METALAKE, MetadataObject.Type.METALAKE);
    AuthorizationRequestContext ctx = new AuthorizationRequestContext();
    assertFalse(jcasbinAuthorizer.authorize(currentPrincipal, METALAKE, metalake, RUN_JOB, ctx));
    assertTrue(jcasbinAuthorizer.deny(currentPrincipal, METALAKE, metalake, RUN_JOB, ctx));
  }

  @Test
  public void testAuthorizeReturnsFalseForDirectPrivilegeIntraRoleDeny() throws Exception {
    // Same role grants both ALLOW and DENY for METALAKE::RUN_JOB. Even though the expression
    // converter only emits a single authorize() call (no deny() probe), DENY must still beat
    // ALLOW inside the role and propagate through resolveEffect to the authorize endpoint.
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    NameIdentifier userIdent = NameIdentifierUtil.ofUser(METALAKE, USERNAME);
    Long roleId = 3002L;
    SecurableObject allow =
        makeSecurableObject("testMetalake", MetadataObject.Type.METALAKE, roleId, RUN_JOB, "ALLOW");
    SecurableObject deny =
        makeSecurableObject("testMetalake", MetadataObject.Type.METALAKE, roleId, RUN_JOB, "DENY");
    RoleEntity mixedRole =
        getRoleEntity(roleId, "mixedRunJobRole" + roleId, ImmutableList.of(allow, deny));
    mockRoleEntity(mixedRole);
    mockUserRoles(userIdent, mixedRole);

    MetadataObject metalake = MetadataObjects.of(null, METALAKE, MetadataObject.Type.METALAKE);
    AuthorizationRequestContext ctx = new AuthorizationRequestContext();
    assertFalse(jcasbinAuthorizer.authorize(currentPrincipal, METALAKE, metalake, RUN_JOB, ctx));
    assertTrue(jcasbinAuthorizer.deny(currentPrincipal, METALAKE, metalake, RUN_JOB, ctx));
  }

  @Test
  public void testAuthorizeReturnsFalseForDirectPrivilegeCrossRoleDeny() throws Exception {
    // One role grants ALLOW RUN_JOB on metalake, another role grants DENY RUN_JOB on the same
    // metalake. The direct-privilege expression `METALAKE::RUN_JOB` only triggers authorize();
    // cross-role DENY priority must still take effect from the authorize side without relying on
    // a paired deny() call from an ANY_xxx expansion.
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    NameIdentifier userIdent = NameIdentifierUtil.ofUser(METALAKE, USERNAME);
    Long allowRoleId = 3003L;
    Long denyRoleId = 3004L;
    RoleEntity allowRole =
        getRoleEntity(
            allowRoleId,
            "allowRunJob" + allowRoleId,
            ImmutableList.of(
                makeSecurableObject(
                    "testMetalake", MetadataObject.Type.METALAKE, allowRoleId, RUN_JOB, "ALLOW")));
    RoleEntity denyRole =
        getRoleEntity(
            denyRoleId,
            "denyRunJob" + denyRoleId,
            ImmutableList.of(
                makeSecurableObject(
                    "testMetalake", MetadataObject.Type.METALAKE, denyRoleId, RUN_JOB, "DENY")));
    mockRoleEntity(allowRole);
    mockRoleEntity(denyRole);
    mockUserRoles(userIdent, allowRole, denyRole);

    MetadataObject metalake = MetadataObjects.of(null, METALAKE, MetadataObject.Type.METALAKE);
    AuthorizationRequestContext ctx = new AuthorizationRequestContext();
    assertFalse(jcasbinAuthorizer.authorize(currentPrincipal, METALAKE, metalake, RUN_JOB, ctx));
    assertTrue(jcasbinAuthorizer.deny(currentPrincipal, METALAKE, metalake, RUN_JOB, ctx));
  }

  @Test
  public void testIsOwnerReturnsFalseWhenAnotherUserIsOwner() throws Exception {
    // Existing testAuthorizeByOwner only covers "user is owner" and "no owner cached". This case
    // pins the third branch: ownerRel resolves to a *different* user, so isOwner must return
    // false even though the cache entry exists.
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    Long otherUserId = USER_ID + 1000L;
    Cache<Long, Optional<Long>> ownerRel = getOwnerRelCache(jcasbinAuthorizer);
    ownerRel.invalidateAll();
    ownerRel.put(CATALOG_ID, Optional.of(otherUserId));

    NameIdentifier catalogIdent = NameIdentifierUtil.ofCatalog(METALAKE, "testCatalog");
    // Stub the relation lookup so loadOwnerPolicy doesn't overwrite our manually-injected entry
    // when isOwner walks through. We pre-seed ownerRel above, so loadOwnerPolicy's
    // `getIfPresent != null` short-circuit fires and the relation lookup is never reached — but
    // stub it defensively so a future refactor that drops the short-circuit doesn't silently
    // change this test's semantics.
    doReturn(ImmutableList.of())
        .when(supportsRelationOperations)
        .listEntitiesByRelation(
            eq(SupportsRelationOperations.Type.OWNER_REL),
            eq(catalogIdent),
            eq(Entity.EntityType.CATALOG));

    assertFalse(doAuthorizeOwner(currentPrincipal));
  }

  @Test
  public void testAuthorizeReturnsFalseWhenMetadataIdLookupFails() throws Exception {
    // loadAndResolveEffect swallows exceptions from the user/metadata id lookup and returns null
    // (treated as "no rule"). This guards against an unauthenticated user or a transient
    // catalog-resolution error inadvertently leaking through as an ALLOW or DENY verdict — both
    // endpoints must return false.
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    MetadataObject unknown =
        MetadataObjects.of(null, "missingCatalog", MetadataObject.Type.CATALOG);
    metadataIdConverterMockedStatic
        .when(() -> MetadataIdConverter.getID(eq(unknown), eq(METALAKE)))
        .thenThrow(new RuntimeException("simulated id lookup failure"));
    try {
      AuthorizationRequestContext ctx = new AuthorizationRequestContext();
      assertFalse(
          jcasbinAuthorizer.authorize(currentPrincipal, METALAKE, unknown, USE_CATALOG, ctx));
      assertFalse(jcasbinAuthorizer.deny(currentPrincipal, METALAKE, unknown, USE_CATALOG, ctx));
    } finally {
      // Restore the default stub so subsequent tests still resolve to CATALOG_ID.
      metadataIdConverterMockedStatic
          .when(() -> MetadataIdConverter.getID(eq(unknown), eq(METALAKE)))
          .thenReturn(CATALOG_ID);
    }
  }

  @Test
  public void testCacheInitialization() throws Exception {
    // Verify that caches are initialized
    Cache<Long, Map<JcasbinAuthorizer.PolicyKey, JcasbinAuthorizer.Effect>> loadedRoles =
        getLoadedRolesCache(jcasbinAuthorizer);
    Cache<Long, Optional<Long>> ownerRel = getOwnerRelCache(jcasbinAuthorizer);

    assertNotNull(loadedRoles, "loadedRoles cache should be initialized");
    assertNotNull(ownerRel, "ownerRel cache should be initialized");
  }

  @SuppressWarnings("unchecked")
  private static Cache<Long, Map<JcasbinAuthorizer.PolicyKey, JcasbinAuthorizer.Effect>>
      getLoadedRolesCache(JcasbinAuthorizer authorizer) throws Exception {
    Field field = JcasbinAuthorizer.class.getDeclaredField("loadedRoles");
    field.setAccessible(true);
    return (Cache<Long, Map<JcasbinAuthorizer.PolicyKey, JcasbinAuthorizer.Effect>>)
        field.get(authorizer);
  }

  @SuppressWarnings("unchecked")
  private static Cache<Long, Optional<Long>> getOwnerRelCache(JcasbinAuthorizer authorizer)
      throws Exception {
    Field field = JcasbinAuthorizer.class.getDeclaredField("ownerRel");
    field.setAccessible(true);
    return (Cache<Long, Optional<Long>>) field.get(authorizer);
  }

  private static Enforcer getAllowEnforcer(JcasbinAuthorizer authorizer) throws Exception {
    Field field = JcasbinAuthorizer.class.getDeclaredField("allowEnforcer");
    field.setAccessible(true);
    return (Enforcer) field.get(authorizer);
  }

  private static Enforcer getDenyEnforcer(JcasbinAuthorizer authorizer) throws Exception {
    Field field = JcasbinAuthorizer.class.getDeclaredField("denyEnforcer");
    field.setAccessible(true);
    return (Enforcer) field.get(authorizer);
  }

  private static SecurableObject makeSecurableObject(
      String name,
      MetadataObject.Type type,
      Long roleId,
      Privilege.Name privilege,
      String condition) {
    try {
      SecurableObjectPO po =
          SecurableObjectPO.builder()
              .withType(String.valueOf(type))
              .withMetadataObjectId(CATALOG_ID)
              .withRoleId(roleId)
              .withPrivilegeNames(
                  objectMapper.writeValueAsString(ImmutableList.of(privilege.name())))
              .withPrivilegeConditions(objectMapper.writeValueAsString(ImmutableList.of(condition)))
              .withDeletedAt(0L)
              .withCurrentVersion(1L)
              .withLastVersion(1L)
              .build();
      return POConverters.fromSecurableObjectPO(name, po, type);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private static void mockUserRoles(NameIdentifier userIdent, RoleEntity... roles)
      throws IOException {
    when(supportsRelationOperations.listEntitiesByRelation(
            eq(SupportsRelationOperations.Type.ROLE_USER_REL),
            eq(userIdent),
            eq(Entity.EntityType.USER)))
        .thenReturn(ImmutableList.copyOf(roles));
  }

  private static void mockRoleEntity(RoleEntity role) throws IOException {
    when(entityStore.get(
            eq(NameIdentifierUtil.ofRole(METALAKE, role.name())),
            eq(Entity.EntityType.ROLE),
            eq(RoleEntity.class)))
        .thenReturn(role);
  }
}
