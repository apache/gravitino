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
import static org.mockito.ArgumentMatchers.anyLong;
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
import java.util.function.Function;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
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
import org.apache.gravitino.storage.relational.po.OwnerRelInfoPO;
import org.apache.gravitino.storage.relational.po.RolePO;
import org.apache.gravitino.storage.relational.po.RoleVersionInfoPO;
import org.apache.gravitino.storage.relational.po.SecurableObjectPO;
import org.apache.gravitino.storage.relational.po.UserVersionInfoPO;
import org.apache.gravitino.storage.relational.service.RoleMetaService;
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

  private static final Integer ROLE_VERSION_1 = 1;

  private static final String USERNAME = "tester";

  private static final String METALAKE = "testMetalake";

  private static EntityStore entityStore = mock(EntityStore.class);

  private static GravitinoEnv gravitinoEnv = mock(GravitinoEnv.class);

  private static SupportsRelationOperations supportsRelationOperations =
      mock(SupportsRelationOperations.class);

  private static MockedStatic<PrincipalUtils> principalUtilsMockedStatic;

  private static MockedStatic<GravitinoEnv> gravitinoEnvMockedStatic;

  private static MockedStatic<MetadataIdConverter> metadataIdConverterMockedStatic;

  private static MockedStatic<SessionUtils> sessionUtilsMockedStatic;

  private static MockedStatic<RoleMetaService> roleMetaServiceMockedStatic;

  private static JcasbinAuthorizer jcasbinAuthorizer;

  private static ObjectMapper objectMapper = new ObjectMapper();

  /** Default role PO list returned by the DB mock. */
  private static List<RolePO> defaultRolePOs;

  /** Default role version list returned by the DB mock. */
  private static List<RoleVersionInfoPO> defaultRoleVersions;

  @BeforeAll
  public static void setup() throws IOException {
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

    // Set up default DB mock data
    RolePO allowRolePO = mock(RolePO.class);
    when(allowRolePO.getRoleId()).thenReturn(ALLOW_ROLE_ID);
    defaultRolePOs = ImmutableList.of(allowRolePO);

    defaultRoleVersions = ImmutableList.of(makeRoleVersionInfo(ALLOW_ROLE_ID, ROLE_VERSION_1));

    // Mock SessionUtils to dispatch to mock mappers
    sessionUtilsMockedStatic = mockStatic(SessionUtils.class);
    setupDefaultSessionMocks();

    // Mock RoleMetaService.listSecurableObjectsByRoleId
    roleMetaServiceMockedStatic = mockStatic(RoleMetaService.class);
    roleMetaServiceMockedStatic
        .when(() -> RoleMetaService.listSecurableObjectsByRoleId(eq(ALLOW_ROLE_ID)))
        .thenReturn(ImmutableList.of(getAllowSecurableObjectPO()));
    roleMetaServiceMockedStatic
        .when(() -> RoleMetaService.listSecurableObjectsByRoleId(eq(DENY_ROLE_ID)))
        .thenReturn(ImmutableList.of(getDenySecurableObjectPO()));

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

  /** Configures SessionUtils mock to return default user version info and empty role lists. */
  private static void setupDefaultSessionMocks() {
    // By default, return no roles (no DB user)
    sessionUtilsMockedStatic
        .when(() -> SessionUtils.getWithoutCommit(eq(UserMetaMapper.class), any()))
        .thenReturn(null);
    sessionUtilsMockedStatic
        .when(() -> SessionUtils.getWithoutCommit(eq(RoleMetaMapper.class), any()))
        .thenReturn(ImmutableList.of());
    sessionUtilsMockedStatic
        .when(() -> SessionUtils.getWithoutCommit(eq(OwnerMetaMapper.class), any()))
        .thenReturn(null);
  }

  /**
   * Configures UserMetaMapper mock to return a user with the given userId and roleGrantsVersion.
   */
  private static void mockUserVersionInfo(long userId, int roleGrantsVersion) {
    UserVersionInfoPO info = makeUserVersionInfo(userId, roleGrantsVersion);
    sessionUtilsMockedStatic
        .when(() -> SessionUtils.getWithoutCommit(eq(UserMetaMapper.class), any()))
        .thenAnswer(
            invocation -> {
              Function<UserMetaMapper, ?> fn = invocation.getArgument(1);
              UserMetaMapper mockMapper = mock(UserMetaMapper.class);
              when(mockMapper.getUserVersionInfo(any(), any())).thenReturn(info);
              return fn.apply(mockMapper);
            });
  }

  /** Configures RoleMetaMapper mock to return the given rolePOs and roleVersions. */
  private static void mockRoleData(List<RolePO> rolePOs, List<RoleVersionInfoPO> roleVersions) {
    sessionUtilsMockedStatic
        .when(() -> SessionUtils.getWithoutCommit(eq(RoleMetaMapper.class), any()))
        .thenAnswer(
            invocation -> {
              Function<RoleMetaMapper, ?> fn = invocation.getArgument(1);
              RoleMetaMapper mockMapper = mock(RoleMetaMapper.class);
              when(mockMapper.listRolesByUserId(anyLong())).thenReturn(rolePOs);
              when(mockMapper.batchGetSecurableObjectsVersions(any())).thenReturn(roleVersions);
              return fn.apply(mockMapper);
            });
  }

  /** Configures OwnerMetaMapper mock to return the given owner info. */
  private static void mockOwnerData(OwnerRelInfoPO ownerInfo) {
    sessionUtilsMockedStatic
        .when(() -> SessionUtils.getWithoutCommit(eq(OwnerMetaMapper.class), any()))
        .thenAnswer(
            invocation -> {
              Function<OwnerMetaMapper, ?> fn = invocation.getArgument(1);
              OwnerMetaMapper mockMapper = mock(OwnerMetaMapper.class);
              when(mockMapper.selectOwnerByMetadataObjectId(anyLong())).thenReturn(ownerInfo);
              return fn.apply(mockMapper);
            });
  }

  @AfterAll
  public static void stop() throws IOException {
    if (principalUtilsMockedStatic != null) {
      principalUtilsMockedStatic.close();
    }
    if (metadataIdConverterMockedStatic != null) {
      metadataIdConverterMockedStatic.close();
    }
    if (sessionUtilsMockedStatic != null) {
      sessionUtilsMockedStatic.close();
    }
    if (roleMetaServiceMockedStatic != null) {
      roleMetaServiceMockedStatic.close();
    }
    if (gravitinoEnvMockedStatic != null) {
      gravitinoEnvMockedStatic.close();
    }
    if (jcasbinAuthorizer != null) {
      jcasbinAuthorizer.close();
    }
  }

  @Test
  public void testAuthorize() throws Exception {
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();

    // No user in DB → authorize fails
    setupDefaultSessionMocks();
    assertFalse(doAuthorize(currentPrincipal));

    // User exists with ALLOW role
    mockUserVersionInfo(USER_ID, 1);
    mockRoleData(defaultRolePOs, defaultRoleVersions);
    assertTrue(doAuthorize(currentPrincipal));

    // Test deny: add a deny role alongside the allow role
    RolePO denyRolePO = mock(RolePO.class);
    when(denyRolePO.getRoleId()).thenReturn(DENY_ROLE_ID);
    List<RolePO> bothRoles = ImmutableList.of(defaultRolePOs.get(0), denyRolePO);
    List<RoleVersionInfoPO> bothVersions =
        ImmutableList.of(
            defaultRoleVersions.get(0), makeRoleVersionInfo(DENY_ROLE_ID, ROLE_VERSION_1));

    // Bump the user's roleGrantsVersion so the cache is invalidated and roles are reloaded
    mockUserVersionInfo(USER_ID, 2);
    mockRoleData(bothRoles, bothVersions);
    assertFalse(doAuthorize(currentPrincipal));
  }

  @Test
  public void testAuthorizeByOwner() throws Exception {
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();

    // Owner is the current user → isOwner = true
    mockUserVersionInfo(USER_ID, 1);
    mockOwnerData(makeOwnerInfo(USER_ID, "USER"));
    assertTrue(doAuthorizeOwner(currentPrincipal));

    // Different owner → isOwner = false
    mockOwnerData(makeOwnerInfo(999L, "USER"));
    assertFalse(doAuthorizeOwner(currentPrincipal));

    // No owner row → isOwner = false
    mockOwnerData(null);
    assertFalse(doAuthorizeOwner(currentPrincipal));
  }

  @Test
  public void testRoleCacheInvalidation() throws Exception {
    GravitinoCache<Long, Integer> loadedRoles = getLoadedRolesCache(jcasbinAuthorizer);

    // Manually add a role version to the cache
    Long testRoleId = 100L;
    loadedRoles.put(testRoleId, 1);

    // Verify it's in the cache
    assertTrue(loadedRoles.getIfPresent(testRoleId).isPresent());

    // Call handleRolePrivilegeChange which should invalidate the cache entry
    jcasbinAuthorizer.handleRolePrivilegeChange(testRoleId);

    // Verify it's removed from the cache
    assertFalse(loadedRoles.getIfPresent(testRoleId).isPresent());
  }

  @Test
  public void testRoleCacheSynchronousRemovalListenerDeletesPolicy() throws Exception {
    // Get the enforcers via reflection
    Enforcer allowEnforcer = getAllowEnforcer(jcasbinAuthorizer);
    Enforcer denyEnforcer = getDenyEnforcer(jcasbinAuthorizer);

    // Get the loadedRoles cache
    GravitinoCache<Long, Integer> loadedRoles = getLoadedRolesCache(jcasbinAuthorizer);

    // Add a role and its policy to the enforcer
    Long testRoleId = 300L;
    String roleIdStr = String.valueOf(testRoleId);

    // Add a policy for this role
    allowEnforcer.addPolicy(roleIdStr, "CATALOG", "999", "USE_CATALOG", "allow");
    denyEnforcer.addPolicy(roleIdStr, "CATALOG", "999", "USE_CATALOG", "allow");

    // Add role version to cache
    loadedRoles.put(testRoleId, 1);

    // Verify policy exists in enforcer
    assertTrue(allowEnforcer.hasPolicy(roleIdStr, "CATALOG", "999", "USE_CATALOG", "allow"));
    assertTrue(denyEnforcer.hasPolicy(roleIdStr, "CATALOG", "999", "USE_CATALOG", "allow"));

    // Invalidate the cache entry — this triggers the synchronous removal listener
    loadedRoles.invalidate(testRoleId);

    // Verify the role's policies have been deleted from enforcers
    assertFalse(allowEnforcer.hasPolicy(roleIdStr, "CATALOG", "999", "USE_CATALOG", "allow"));
    assertFalse(denyEnforcer.hasPolicy(roleIdStr, "CATALOG", "999", "USE_CATALOG", "allow"));
  }

  @Test
  public void testCacheInitialization() throws Exception {
    // Verify that both caches are initialized after initialize()
    GravitinoCache<Long, Integer> loadedRoles = getLoadedRolesCache(jcasbinAuthorizer);
    GravitinoCache<String, ?> userRoleCache = getUserRoleCache(jcasbinAuthorizer);

    assertNotNull(loadedRoles, "loadedRoles cache should be initialized");
    assertNotNull(userRoleCache, "userRoleCache should be initialized");
  }

  @Test
  public void testVersionValidation() throws Exception {
    // Set up user with role at version 1
    mockUserVersionInfo(USER_ID, 1);
    mockRoleData(defaultRolePOs, defaultRoleVersions);

    // First call — loads and caches
    assertTrue(doAuthorize(PrincipalUtils.getCurrentPrincipal()));

    GravitinoCache<Long, Integer> loadedRoles = getLoadedRolesCache(jcasbinAuthorizer);

    // Verify role is in the version cache
    assertTrue(loadedRoles.getIfPresent(ALLOW_ROLE_ID).isPresent());

    // Simulate a role privilege change via handleRolePrivilegeChange
    jcasbinAuthorizer.handleRolePrivilegeChange(ALLOW_ROLE_ID);

    // Role should no longer be in version cache
    assertFalse(loadedRoles.getIfPresent(ALLOW_ROLE_ID).isPresent());

    // Next call reloads the role (DB version returned as 2)
    mockRoleData(defaultRolePOs, ImmutableList.of(makeRoleVersionInfo(ALLOW_ROLE_ID, 2)));
    assertTrue(doAuthorize(PrincipalUtils.getCurrentPrincipal()));

    // Now the cache should hold version 2
    assertTrue(loadedRoles.getIfPresent(ALLOW_ROLE_ID).isPresent());
  }

  /** Tests {@link JcasbinAuthorizer#hasMetadataPrivilegePermission} hierarchy walk */
  @Test
  public void testHasMetadataPrivilegePermission() throws Exception {
    // --- Case 1: no MANAGE_GRANTS anywhere → false ---
    setupDefaultSessionMocks(); // no user
    assertFalse(
        jcasbinAuthorizer.hasMetadataPrivilegePermission(
            METALAKE,
            "TABLE",
            "testCatalog.testSchema.testTable",
            new AuthorizationRequestContext()),
        "No MANAGE_GRANTS grants should return false");

    // --- Case 2: METALAKE-level MANAGE_GRANTS covers a TABLE ---
    Long metalakeGrantRoleId = 201L;
    RolePO metalakeGrantRolePO = mock(RolePO.class);
    when(metalakeGrantRolePO.getRoleId()).thenReturn(metalakeGrantRoleId);

    roleMetaServiceMockedStatic
        .when(() -> RoleMetaService.listSecurableObjectsByRoleId(eq(metalakeGrantRoleId)))
        .thenReturn(
            ImmutableList.of(
                buildManageGrantsSecurableObjectPO(
                    metalakeGrantRoleId, MetadataObject.Type.METALAKE)));

    mockUserVersionInfo(USER_ID, 10);
    mockRoleData(
        ImmutableList.of(metalakeGrantRolePO),
        ImmutableList.of(makeRoleVersionInfo(metalakeGrantRoleId, 1)));
    assertTrue(
        jcasbinAuthorizer.hasMetadataPrivilegePermission(
            METALAKE,
            "TABLE",
            "testCatalog.testSchema.testTable",
            new AuthorizationRequestContext()),
        "METALAKE-level MANAGE_GRANTS should cover TABLE within it");

    // --- Case 3: CATALOG-level MANAGE_GRANTS covers TABLE/SCHEMA ---
    Long catalogGrantRoleId = 200L;
    RolePO catalogGrantRolePO = mock(RolePO.class);
    when(catalogGrantRolePO.getRoleId()).thenReturn(catalogGrantRoleId);

    roleMetaServiceMockedStatic
        .when(() -> RoleMetaService.listSecurableObjectsByRoleId(eq(catalogGrantRoleId)))
        .thenReturn(
            ImmutableList.of(
                buildManageGrantsSecurableObjectPO(
                    catalogGrantRoleId, MetadataObject.Type.CATALOG)));

    mockUserVersionInfo(USER_ID, 11);
    mockRoleData(
        ImmutableList.of(catalogGrantRolePO),
        ImmutableList.of(makeRoleVersionInfo(catalogGrantRoleId, 1)));
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
    RolePO tableGrantRolePO = mock(RolePO.class);
    when(tableGrantRolePO.getRoleId()).thenReturn(tableGrantRoleId);

    roleMetaServiceMockedStatic
        .when(() -> RoleMetaService.listSecurableObjectsByRoleId(eq(tableGrantRoleId)))
        .thenReturn(
            ImmutableList.of(
                buildManageGrantsSecurableObjectPO(tableGrantRoleId, MetadataObject.Type.TABLE)));

    mockUserVersionInfo(USER_ID, 12);
    mockRoleData(
        ImmutableList.of(tableGrantRolePO),
        ImmutableList.of(makeRoleVersionInfo(tableGrantRoleId, 1)));
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

  private Boolean doAuthorize(Principal currentPrincipal) {
    return jcasbinAuthorizer.authorize(
        currentPrincipal,
        METALAKE,
        MetadataObjects.of(null, "testCatalog", MetadataObject.Type.CATALOG),
        USE_CATALOG,
        new AuthorizationRequestContext());
  }

  private Boolean doAuthorizeOwner(Principal currentPrincipal) {
    return jcasbinAuthorizer.isOwner(
        currentPrincipal,
        METALAKE,
        MetadataObjects.of(null, "testCatalog", MetadataObject.Type.CATALOG),
        new AuthorizationRequestContext());
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
    return RoleEntity.builder()
        .withNamespace(NamespaceUtil.ofRole(METALAKE))
        .withId(roleId)
        .withName(roleName)
        .withAuditInfo(AuditInfo.EMPTY)
        .withSecurableObjects(securableObjects)
        .build();
  }

  private static SecurableObjectPO getAllowSecurableObjectPO() {
    ImmutableList<String> privilegeNames = ImmutableList.of(USE_CATALOG.name());
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

  private static SecurableObjectPO getDenySecurableObjectPO() {
    ImmutableList<String> privilegeNames = ImmutableList.of(USE_CATALOG.name());
    ImmutableList<String> conditions = ImmutableList.of("DENY");
    try {
      return SecurableObjectPO.builder()
          .withType(String.valueOf(MetadataObject.Type.CATALOG))
          .withMetadataObjectId(CATALOG_ID)
          .withRoleId(DENY_ROLE_ID)
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

  /**
   * Builds a {@link SecurableObjectPO} carrying an ALLOW {@code MANAGE_GRANTS} privilege bound to
   * the given type with the shared test metadata ID ({@link #CATALOG_ID}).
   */
  private static SecurableObjectPO buildManageGrantsSecurableObjectPO(
      Long roleId, MetadataObject.Type type) {
    try {
      ImmutableList<String> privilegeNames = ImmutableList.of("MANAGE_GRANTS");
      ImmutableList<String> conditions = ImmutableList.of("ALLOW");
      return SecurableObjectPO.builder()
          .withType(String.valueOf(type))
          .withMetadataObjectId(CATALOG_ID)
          .withRoleId(roleId)
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

  @SuppressWarnings("unchecked")
  private static GravitinoCache<Long, Integer> getLoadedRolesCache(JcasbinAuthorizer authorizer)
      throws Exception {
    Field field = JcasbinAuthorizer.class.getDeclaredField("loadedRoles");
    field.setAccessible(true);
    return (GravitinoCache<Long, Integer>) field.get(authorizer);
  }

  @SuppressWarnings("unchecked")
  private static GravitinoCache<String, ?> getUserRoleCache(JcasbinAuthorizer authorizer)
      throws Exception {
    Field field = JcasbinAuthorizer.class.getDeclaredField("userRoleCache");
    field.setAccessible(true);
    return (GravitinoCache<String, ?>) field.get(authorizer);
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

  private static UserVersionInfoPO makeUserVersionInfo(long userId, int roleGrantsVersion) {
    UserVersionInfoPO po = new UserVersionInfoPO();
    po.setUserId(userId);
    po.setRoleGrantsVersion(roleGrantsVersion);
    return po;
  }

  private static RoleVersionInfoPO makeRoleVersionInfo(long roleId, int securableObjectsVersion) {
    RoleVersionInfoPO po = new RoleVersionInfoPO();
    po.setRoleId(roleId);
    po.setSecurableObjectsVersion(securableObjectsVersion);
    return po;
  }

  private static OwnerRelInfoPO makeOwnerInfo(long ownerId, String ownerType) {
    OwnerRelInfoPO po = new OwnerRelInfoPO();
    po.setOwnerId(ownerId);
    po.setOwnerType(ownerType);
    return po;
  }
}
