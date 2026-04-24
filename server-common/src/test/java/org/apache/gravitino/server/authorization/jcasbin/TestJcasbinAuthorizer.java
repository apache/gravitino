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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
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
import org.apache.gravitino.cache.GravitinoCache;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.server.ServerConfig;
import org.apache.gravitino.server.authorization.MetadataIdConverter;
import org.apache.gravitino.storage.relational.mapper.OwnerMetaMapper;
import org.apache.gravitino.storage.relational.mapper.RoleMetaMapper;
import org.apache.gravitino.storage.relational.mapper.UserMetaMapper;
import org.apache.gravitino.storage.relational.po.RolePO;
import org.apache.gravitino.storage.relational.po.SecurableObjectPO;
import org.apache.gravitino.storage.relational.po.auth.RoleUpdatedAt;
import org.apache.gravitino.storage.relational.po.auth.UserAuthInfo;
import org.apache.gravitino.storage.relational.service.OwnerMetaService;
import org.apache.gravitino.storage.relational.utils.POConverters;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.casbin.jcasbin.main.Enforcer;
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

  private static MockedStatic<SessionUtils> sessionUtilsMockedStatic;

  private static UserMetaMapper userMetaMapper = mock(UserMetaMapper.class);

  private static RoleMetaMapper roleMetaMapper = mock(RoleMetaMapper.class);

  private static OwnerMetaMapper ownerMetaMapper = mock(OwnerMetaMapper.class);

  private static JcasbinAuthorizer jcasbinAuthorizer;

  private static ObjectMapper objectMapper = new ObjectMapper();

  @BeforeAll
  public static void setup() throws IOException {
    OwnerMetaService ownerMetaService = mock(OwnerMetaService.class);
    ownerMetaServiceMockedStatic = mockStatic(OwnerMetaService.class);
    ownerMetaServiceMockedStatic.when(OwnerMetaService::getInstance).thenReturn(ownerMetaService);

    // Mock SessionUtils.getWithoutCommit to delegate to our mock mappers
    sessionUtilsMockedStatic = mockStatic(SessionUtils.class);
    sessionUtilsMockedStatic
        .when(() -> SessionUtils.getWithoutCommit(any(), any()))
        .thenAnswer(
            invocation -> {
              Class<?> mapperClass = invocation.getArgument(0);
              java.util.function.Function<Object, Object> func = invocation.getArgument(1);
              if (mapperClass == UserMetaMapper.class) {
                return func.apply(userMetaMapper);
              } else if (mapperClass == RoleMetaMapper.class) {
                return func.apply(roleMetaMapper);
              } else if (mapperClass == OwnerMetaMapper.class) {
                return func.apply(ownerMetaMapper);
              }
              return null;
            });

    // Default mock: getUserInfo returns a valid user
    when(userMetaMapper.getUserInfo(eq(METALAKE), eq(USERNAME)))
        .thenReturn(new UserAuthInfo(USER_ID, 1000L));

    // Default: no roles assigned initially
    when(roleMetaMapper.listRolesByUserId(eq(USER_ID))).thenReturn(ImmutableList.of());
    when(roleMetaMapper.batchGetUpdatedAt(any())).thenReturn(ImmutableList.of());

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
    if (sessionUtilsMockedStatic != null) {
      sessionUtilsMockedStatic.close();
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
    // No roles assigned — should fail
    assertFalse(doAuthorize(currentPrincipal));

    // Set up allowRole
    RoleEntity allowRole =
        getRoleEntity(ALLOW_ROLE_ID, "allowRole", ImmutableList.of(getAllowSecurableObject()));
    when(entityStore.get(
            eq(NameIdentifierUtil.ofRole(METALAKE, allowRole.name())),
            eq(Entity.EntityType.ROLE),
            eq(RoleEntity.class)))
        .thenReturn(allowRole);

    // Mock mapper: user has allowRole
    long now = System.currentTimeMillis();
    RolePO allowRolePO = buildRolePO(ALLOW_ROLE_ID, "allowRole");
    when(roleMetaMapper.listRolesByUserId(eq(USER_ID))).thenReturn(ImmutableList.of(allowRolePO));
    when(roleMetaMapper.batchGetUpdatedAt(any()))
        .thenReturn(ImmutableList.of(new RoleUpdatedAt(ALLOW_ROLE_ID, "allowRole", now)));
    // Bump user version to invalidate userRoleCache
    when(userMetaMapper.getUserInfo(eq(METALAKE), eq(USERNAME)))
        .thenReturn(new UserAuthInfo(USER_ID, now));

    assertTrue(doAuthorize(currentPrincipal));

    // Test role cache: policies stay in JCasbin even if user's role changes,
    // as long as handleRolePrivilegeChange is not called.
    Long newRoleId = -1L;
    RoleEntity tempNewRole = getRoleEntity(newRoleId, "tempNewRole", ImmutableList.of());
    when(entityStore.get(
            eq(NameIdentifierUtil.ofRole(METALAKE, tempNewRole.name())),
            eq(Entity.EntityType.ROLE),
            eq(RoleEntity.class)))
        .thenReturn(tempNewRole);
    RolePO tempNewRolePO = buildRolePO(newRoleId, "tempNewRole");
    when(roleMetaMapper.listRolesByUserId(eq(USER_ID))).thenReturn(ImmutableList.of(tempNewRolePO));
    long now2 = now + 1;
    when(roleMetaMapper.batchGetUpdatedAt(any()))
        .thenReturn(ImmutableList.of(new RoleUpdatedAt(newRoleId, "tempNewRole", now2)));
    when(userMetaMapper.getUserInfo(eq(METALAKE), eq(USERNAME)))
        .thenReturn(new UserAuthInfo(USER_ID, now2));
    assertTrue(doAuthorize(currentPrincipal));

    // After clearing the cache, authorize will fail
    jcasbinAuthorizer.handleRolePrivilegeChange(ALLOW_ROLE_ID);

    // Re-assign allowRole, the authorization will succeed
    when(roleMetaMapper.listRolesByUserId(eq(USER_ID))).thenReturn(ImmutableList.of(allowRolePO));
    long now3 = now2 + 1;
    when(roleMetaMapper.batchGetUpdatedAt(any()))
        .thenReturn(ImmutableList.of(new RoleUpdatedAt(ALLOW_ROLE_ID, "allowRole", now3)));
    when(userMetaMapper.getUserInfo(eq(METALAKE), eq(USERNAME)))
        .thenReturn(new UserAuthInfo(USER_ID, now3));
    assertTrue(doAuthorize(currentPrincipal));

    // Test deny
    RoleEntity denyRole =
        getRoleEntity(DENY_ROLE_ID, "denyRole", ImmutableList.of(getDenySecurableObject()));
    when(entityStore.get(
            eq(NameIdentifierUtil.ofRole(METALAKE, denyRole.name())),
            eq(Entity.EntityType.ROLE),
            eq(RoleEntity.class)))
        .thenReturn(denyRole);
    RolePO denyRolePO = buildRolePO(DENY_ROLE_ID, "denyRole");
    when(roleMetaMapper.listRolesByUserId(eq(USER_ID)))
        .thenReturn(ImmutableList.of(allowRolePO, denyRolePO));
    long now4 = now3 + 1;
    when(roleMetaMapper.batchGetUpdatedAt(any()))
        .thenReturn(
            ImmutableList.of(
                new RoleUpdatedAt(ALLOW_ROLE_ID, "allowRole", now4),
                new RoleUpdatedAt(DENY_ROLE_ID, "denyRole", now4)));
    when(userMetaMapper.getUserInfo(eq(METALAKE), eq(USERNAME)))
        .thenReturn(new UserAuthInfo(USER_ID, now4));
    assertFalse(doAuthorize(currentPrincipal));
  }

  @Test
  public void testAuthorizeByOwner() throws Exception {
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    // No owner set — should fail
    when(ownerMetaMapper.selectOwnerByMetadataObjectId(eq(CATALOG_ID))).thenReturn(null);
    getOwnerRelCache(jcasbinAuthorizer).invalidateAll();
    assertFalse(doAuthorizeOwner(currentPrincipal));

    // Set owner to current user
    org.apache.gravitino.storage.relational.po.auth.OwnerInfo ownerInfo =
        new org.apache.gravitino.storage.relational.po.auth.OwnerInfo(USER_ID, "USER");
    when(ownerMetaMapper.selectOwnerByMetadataObjectId(eq(CATALOG_ID))).thenReturn(ownerInfo);
    getOwnerRelCache(jcasbinAuthorizer).invalidateAll();
    assertTrue(doAuthorizeOwner(currentPrincipal));

    // Remove owner
    when(ownerMetaMapper.selectOwnerByMetadataObjectId(eq(CATALOG_ID))).thenReturn(null);
    NameIdentifier catalogIdent = NameIdentifierUtil.ofCatalog(METALAKE, "testCatalog");
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

  @SuppressWarnings("UnusedVariable")
  private static void makeCompletableFutureUseCurrentThread(
      @SuppressWarnings("unused") JcasbinAuthorizer jcasbinAuthorizer) {
    // No-op: the executor field was removed during cache refactoring.
    // Role loading is now synchronous via requestContext.loadRole().
  }

  @Test
  public void testRoleCacheInvalidation() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);

    // Get the loadedRoles cache via reflection
    GravitinoCache<Long, Long> loadedRoles = getLoadedRolesCache(jcasbinAuthorizer);

    // Manually add a role to the cache
    Long testRoleId = 100L;
    loadedRoles.put(testRoleId, System.currentTimeMillis());

    // Verify it's in the cache
    assertTrue(loadedRoles.getIfPresent(testRoleId).isPresent());

    // Call handleRolePrivilegeChange which should invalidate the cache entry
    jcasbinAuthorizer.handleRolePrivilegeChange(testRoleId);

    // Verify it's removed from the cache
    assertFalse(loadedRoles.getIfPresent(testRoleId).isPresent());
  }

  @Test
  public void testOwnerCacheInvalidation() throws Exception {
    // Get the ownerRel cache via reflection
    GravitinoCache<Long, Optional<Long>> ownerRel = getOwnerRelCache(jcasbinAuthorizer);

    // Manually add an owner relation to the cache
    ownerRel.put(CATALOG_ID, Optional.of(USER_ID));

    // Verify it's in the cache
    assertTrue(ownerRel.getIfPresent(CATALOG_ID).isPresent());

    // Create a mock NameIdentifier for the metadata object
    NameIdentifier catalogIdent = NameIdentifierUtil.ofCatalog(METALAKE, "testCatalog");

    // Call handleMetadataOwnerChange which should invalidate the cache entry
    jcasbinAuthorizer.handleMetadataOwnerChange(
        METALAKE, USER_ID, catalogIdent, Entity.EntityType.CATALOG);

    // Verify it's removed from the cache
    assertFalse(ownerRel.getIfPresent(CATALOG_ID).isPresent());
  }

  @Test
  public void testRoleCacheSynchronousRemovalListenerDeletesPolicy() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);

    // Get the enforcers via reflection
    Enforcer allowEnforcer = getAllowEnforcer(jcasbinAuthorizer);
    Enforcer denyEnforcer = getDenyEnforcer(jcasbinAuthorizer);

    // Get the loadedRoles cache
    GravitinoCache<Long, Long> loadedRoles = getLoadedRolesCache(jcasbinAuthorizer);

    // Add a role and its policy to the enforcer
    Long testRoleId = 300L;
    String roleIdStr = String.valueOf(testRoleId);

    // Add a policy for this role
    allowEnforcer.addPolicy(roleIdStr, "CATALOG", "999", "USE_CATALOG", "allow");
    denyEnforcer.addPolicy(roleIdStr, "CATALOG", "999", "USE_CATALOG", "allow");

    // Add role to cache
    loadedRoles.put(testRoleId, System.currentTimeMillis());

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
  public void testCacheInitialization() throws Exception {
    // Verify that caches are initialized
    GravitinoCache<Long, Long> loadedRolesCache = getLoadedRolesCache(jcasbinAuthorizer);
    GravitinoCache<Long, Optional<Long>> ownerRelCache = getOwnerRelCache(jcasbinAuthorizer);

    assertNotNull(loadedRolesCache, "loadedRoles cache should be initialized");
    assertNotNull(ownerRelCache, "ownerRel cache should be initialized");
  }

  /** Tests {@link JcasbinAuthorizer#hasMetadataPrivilegePermission} hierarchy walk */
  @Test
  public void testHasMetadataPrivilegePermission() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);

    // --- Case 1: no MANAGE_GRANTS anywhere → false ---
    mockUserRoles();
    assertFalse(
        jcasbinAuthorizer.hasMetadataPrivilegePermission(
            METALAKE,
            "TABLE",
            "testCatalog.testSchema.testTable",
            new AuthorizationRequestContext()),
        "No MANAGE_GRANTS grants should return false");

    // --- Case 2: METALAKE-level MANAGE_GRANTS covers a TABLE ---
    Long metalakeGrantRoleId = 201L;
    RoleEntity metalakeGrantRole =
        getRoleEntity(
            metalakeGrantRoleId,
            "metalakeGrantRole",
            ImmutableList.of(
                buildManageGrantsSecurableObject(
                    metalakeGrantRoleId, MetadataObject.Type.METALAKE, METALAKE)));
    when(entityStore.get(
            eq(NameIdentifierUtil.ofRole(METALAKE, metalakeGrantRole.name())),
            eq(Entity.EntityType.ROLE),
            eq(RoleEntity.class)))
        .thenReturn(metalakeGrantRole);
    mockUserRoles(metalakeGrantRoleId, "metalakeGrantRole");
    assertTrue(
        jcasbinAuthorizer.hasMetadataPrivilegePermission(
            METALAKE,
            "TABLE",
            "testCatalog.testSchema.testTable",
            new AuthorizationRequestContext()),
        "METALAKE-level MANAGE_GRANTS should cover TABLE within it");

    // --- Case 3: CATALOG-level MANAGE_GRANTS covers TABLE/SCHEMA ---
    Long catalogGrantRoleId = 200L;
    RoleEntity catalogGrantRole =
        getRoleEntity(
            catalogGrantRoleId,
            "catalogGrantRole",
            ImmutableList.of(
                buildManageGrantsSecurableObject(
                    catalogGrantRoleId, MetadataObject.Type.CATALOG, "testCatalog")));
    when(entityStore.get(
            eq(NameIdentifierUtil.ofRole(METALAKE, catalogGrantRole.name())),
            eq(Entity.EntityType.ROLE),
            eq(RoleEntity.class)))
        .thenReturn(catalogGrantRole);
    mockUserRoles(catalogGrantRoleId, "catalogGrantRole");
    assertTrue(
        jcasbinAuthorizer.hasMetadataPrivilegePermission(
            METALAKE,
            "TABLE",
            "testCatalog.testSchema.testTable",
            new AuthorizationRequestContext()),
        "CATALOG-level MANAGE_GRANTS should cover TABLE within it");
    assertTrue(
        jcasbinAuthorizer.hasMetadataPrivilegePermission(
            METALAKE, "SCHEMA", "testCatalog.testSchema", new AuthorizationRequestContext()),
        "CATALOG-level MANAGE_GRANTS should cover SCHEMA within it");

    // --- Case 4: TABLE-level MANAGE_GRANTS covers the table itself ---
    Long tableGrantRoleId = 202L;
    RoleEntity tableGrantRole =
        getRoleEntity(
            tableGrantRoleId,
            "tableGrantRole",
            ImmutableList.of(
                buildManageGrantsSecurableObject(
                    tableGrantRoleId,
                    MetadataObject.Type.TABLE,
                    "testCatalog.testSchema.testTable")));
    when(entityStore.get(
            eq(NameIdentifierUtil.ofRole(METALAKE, tableGrantRole.name())),
            eq(Entity.EntityType.ROLE),
            eq(RoleEntity.class)))
        .thenReturn(tableGrantRole);
    mockUserRoles(tableGrantRoleId, "tableGrantRole");
    assertTrue(
        jcasbinAuthorizer.hasMetadataPrivilegePermission(
            METALAKE,
            "TABLE",
            "testCatalog.testSchema.testTable",
            new AuthorizationRequestContext()),
        "TABLE-level MANAGE_GRANTS should cover itself");

    // --- Case 5: invalid type string → IllegalArgumentException ---
    assertThrows(
        IllegalArgumentException.class,
        () ->
            jcasbinAuthorizer.hasMetadataPrivilegePermission(
                METALAKE, "INVALID_TYPE", "testCatalog", new AuthorizationRequestContext()));
  }

  /**
   * Builds a {@link SecurableObject} carrying an ALLOW {@code MANAGE_GRANTS} privilege bound to
   * {@code type} with the shared test metadata ID ({@link #CATALOG_ID}).
   */
  private static SecurableObject buildManageGrantsSecurableObject(
      Long roleId, MetadataObject.Type type, String objectName) {
    try {
      ImmutableList<String> privilegeNames = ImmutableList.of("MANAGE_GRANTS");
      ImmutableList<String> conditions = ImmutableList.of("ALLOW");
      SecurableObjectPO po =
          SecurableObjectPO.builder()
              .withType(String.valueOf(type))
              .withMetadataObjectId(CATALOG_ID)
              .withRoleId(roleId)
              .withPrivilegeNames(objectMapper.writeValueAsString(privilegeNames))
              .withPrivilegeConditions(objectMapper.writeValueAsString(conditions))
              .withDeletedAt(0L)
              .withCurrentVersion(1L)
              .withLastVersion(1L)
              .build();
      return POConverters.fromSecurableObjectPO(objectName, po, type);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("unchecked")
  private static GravitinoCache<Long, Long> getLoadedRolesCache(JcasbinAuthorizer authorizer)
      throws Exception {
    Field field = JcasbinAuthorizer.class.getDeclaredField("loadedRoles");
    field.setAccessible(true);
    return (GravitinoCache<Long, Long>) field.get(authorizer);
  }

  @SuppressWarnings("unchecked")
  private static GravitinoCache<Long, Optional<Long>> getOwnerRelCache(JcasbinAuthorizer authorizer)
      throws Exception {
    Field field = JcasbinAuthorizer.class.getDeclaredField("ownerRelCache");
    field.setAccessible(true);
    return (GravitinoCache<Long, Optional<Long>>) field.get(authorizer);
  }

  /** Mock mapper to assign zero roles. Bumps user version to invalidate cache. */
  private static void mockUserRoles() {
    long now = System.currentTimeMillis();
    when(roleMetaMapper.listRolesByUserId(eq(USER_ID))).thenReturn(ImmutableList.of());
    when(roleMetaMapper.batchGetUpdatedAt(any())).thenReturn(ImmutableList.of());
    when(userMetaMapper.getUserInfo(eq(METALAKE), eq(USERNAME)))
        .thenReturn(new UserAuthInfo(USER_ID, now));
  }

  /** Mock mapper to assign a single role. Bumps user version to invalidate cache. */
  private static void mockUserRoles(Long roleId, String roleName) {
    long now = System.currentTimeMillis();
    when(roleMetaMapper.listRolesByUserId(eq(USER_ID)))
        .thenReturn(ImmutableList.of(buildRolePO(roleId, roleName)));
    when(roleMetaMapper.batchGetUpdatedAt(any()))
        .thenReturn(ImmutableList.of(new RoleUpdatedAt(roleId, roleName, now)));
    when(userMetaMapper.getUserInfo(eq(METALAKE), eq(USERNAME)))
        .thenReturn(new UserAuthInfo(USER_ID, now));
  }

  private static RolePO buildRolePO(Long roleId, String roleName) {
    return RolePO.builder()
        .withRoleId(roleId)
        .withRoleName(roleName)
        .withMetalakeId(USER_METALAKE_ID)
        .withProperties("{}")
        .withAuditInfo("{}")
        .withCurrentVersion(1L)
        .withLastVersion(1L)
        .withDeletedAt(0L)
        .build();
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
}
