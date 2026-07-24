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

import static org.apache.gravitino.authorization.Privilege.Name.SELECT_TABLE;
import static org.apache.gravitino.authorization.Privilege.Name.USE_CATALOG;
import static org.apache.gravitino.authorization.Privilege.Name.USE_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.UserGroup;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.auth.ActiveRoles;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.cache.GravitinoCache;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.server.ServerConfig;
import org.apache.gravitino.server.authorization.MetadataIdConverter;
import org.apache.gravitino.storage.relational.mapper.EntityChangeLogMapper;
import org.apache.gravitino.storage.relational.mapper.GroupMetaMapper;
import org.apache.gravitino.storage.relational.mapper.OwnerMetaMapper;
import org.apache.gravitino.storage.relational.mapper.RoleMetaMapper;
import org.apache.gravitino.storage.relational.mapper.UserMetaMapper;
import org.apache.gravitino.storage.relational.po.RolePO;
import org.apache.gravitino.storage.relational.po.SecurableObjectPO;
import org.apache.gravitino.storage.relational.po.auth.AuthPrefetchRow;
import org.apache.gravitino.storage.relational.po.auth.GroupUpdatedAt;
import org.apache.gravitino.storage.relational.po.auth.OwnerInfo;
import org.apache.gravitino.storage.relational.po.auth.RoleUpdatedAt;
import org.apache.gravitino.storage.relational.po.auth.UserUpdatedAt;
import org.apache.gravitino.storage.relational.service.OwnerMetaService;
import org.apache.gravitino.storage.relational.utils.POConverters;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.casbin.jcasbin.main.Enforcer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/** Test of {@link JcasbinAuthorizer} */
public class TestJcasbinAuthorizer {

  private static final Long USER_METALAKE_ID = 1L;

  private static final Long USER_ID = 2L;

  private static final Long ALLOW_ROLE_ID = 3L;

  private static final Long DENY_ROLE_ID = 5L;

  private static final Long CATALOG_ID = 4L;

  private static final String USERNAME = "tester";

  private static final String METALAKE = "testMetalake";

  private static final Long GROUP_ID = 6L;

  private static final String GROUP_NAME = "testGroup";

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

  private static GroupMetaMapper groupMetaMapper = mock(GroupMetaMapper.class);

  private static RoleMetaMapper roleMetaMapper = mock(RoleMetaMapper.class);

  private static OwnerMetaMapper ownerMetaMapper = mock(OwnerMetaMapper.class);

  private static EntityChangeLogMapper entityChangeLogMapper = mock(EntityChangeLogMapper.class);

  /**
   * Tracks roles registered via {@link #mockRoleInStore} so {@code
   * roleMetaMapper.batchGetRoleUpdatedAt} can return their versions on demand.
   */
  private static final Map<Long, RoleUpdatedAt> mockedRoleVersions = new HashMap<>();

  /**
   * Monotonic counter for {@code group_meta.updated_at} mocks so that successive {@link
   * #mockGroupWithRoles} calls always advance the version, forcing the groupRoleCache to miss even
   * when the wall clock hasn't advanced.
   */
  private static final AtomicLong groupVersionCounter = new AtomicLong(1L);

  private static final AtomicLong roleVersionCounter = new AtomicLong(1L);

  private static final AtomicLong userVersionCounter = new AtomicLong(1L);

  /**
   * Recreated per test in {@link #createAuthorizer()} so each case starts with empty enforcer state
   * and a fresh cache; the previous static instance leaked g-rows and cache entries across cases.
   */
  private JcasbinAuthorizer jcasbinAuthorizer;

  private static ObjectMapper objectMapper = new ObjectMapper();

  @BeforeAll
  public static void setup() throws IOException {
    OwnerMetaService ownerMetaService = mock(OwnerMetaService.class);
    ownerMetaServiceMockedStatic = mockStatic(OwnerMetaService.class);
    ownerMetaServiceMockedStatic.when(OwnerMetaService::getInstance).thenReturn(ownerMetaService);
    when(ownerMetaMapper.selectMaxChangedOwner()).thenReturn(null);
    when(ownerMetaMapper.selectChangedOwners(anyLong(), anyLong()))
        .thenReturn(Collections.emptyList());
    when(entityChangeLogMapper.selectMaxChangeId()).thenReturn(0L);
    when(entityChangeLogMapper.selectEntityChanges(anyLong(), anyInt()))
        .thenReturn(Collections.emptyList());

    // The change poller probes entity_change_log + owner_meta on startup and owner lookups go via
    // SessionUtils; mock SessionUtils to delegate to mapper mocks so tests can stub owner state
    // without opening a real MyBatis session. Poller-only mapper calls return safe empty defaults.
    sessionUtilsMockedStatic = mockStatic(SessionUtils.class);
    sessionUtilsMockedStatic
        .when(() -> SessionUtils.getWithoutCommit(any(), any()))
        .thenAnswer(
            invocation -> {
              Class<?> mapperClass = invocation.getArgument(0);
              Function<Object, Object> func = invocation.getArgument(1);
              if (mapperClass == UserMetaMapper.class) {
                return func.apply(userMetaMapper);
              } else if (mapperClass == GroupMetaMapper.class) {
                return func.apply(groupMetaMapper);
              } else if (mapperClass == RoleMetaMapper.class) {
                return func.apply(roleMetaMapper);
              } else if (mapperClass == OwnerMetaMapper.class) {
                return func.apply(ownerMetaMapper);
              }
              if (mapperClass == EntityChangeLogMapper.class) {
                return func.apply(entityChangeLogMapper);
              }
              return null;
            });

    // Default mock: getUserInfo returns a valid user
    when(userMetaMapper.getUserUpdatedAt(eq(METALAKE), eq(USERNAME)))
        .thenReturn(new UserUpdatedAt(USER_ID, 1000L));

    // Fat-JOIN variant used by the cache-warm path: assemble user + groups + direct user roles
    // + group-inherited roles + role versions from the existing per-subject mocks. Lets tests
    // continue stubbing at the per-subject granularity.
    when(userMetaMapper.batchGetAuthSubjectsForUser(anyString(), anyString(), anyList()))
        .thenAnswer(
            invocation -> {
              String mlk = invocation.getArgument(0);
              String uname = invocation.getArgument(1);
              List<String> gNames = invocation.getArgument(2);
              List<AuthPrefetchRow> rows = new ArrayList<>();
              UserUpdatedAt u = userMetaMapper.getUserUpdatedAt(mlk, uname);
              if (u != null) {
                rows.add(AuthPrefetchRow.forUser(u.getUserId(), uname, u.getUpdatedAt()));
                List<RolePO> directRoles = roleMetaMapper.listRolesByUserId(u.getUserId());
                if (directRoles != null) {
                  for (RolePO rp : directRoles) {
                    RoleUpdatedAt rv = mockedRoleVersions.get(rp.getRoleId());
                    long roleUpdatedAt = rv != null ? rv.getUpdatedAt() : 0L;
                    rows.add(
                        AuthPrefetchRow.forUserRole(
                            rp.getRoleId(), rp.getRoleName(), roleUpdatedAt, u.getUserId()));
                  }
                }
              }
              if (gNames != null) {
                for (String gn : gNames) {
                  GroupUpdatedAt g = groupMetaMapper.getGroupUpdatedAt(mlk, gn);
                  if (g != null) {
                    rows.add(AuthPrefetchRow.forGroup(g.getGroupId(), gn, g.getUpdatedAt()));
                    List<RolePO> groupRoles = roleMetaMapper.listRolesByGroupId(g.getGroupId());
                    if (groupRoles != null) {
                      for (RolePO rp : groupRoles) {
                        RoleUpdatedAt rv = mockedRoleVersions.get(rp.getRoleId());
                        long roleUpdatedAt = rv != null ? rv.getUpdatedAt() : 0L;
                        rows.add(
                            AuthPrefetchRow.forGroupRole(
                                rp.getRoleId(), rp.getRoleName(), roleUpdatedAt, g.getGroupId()));
                      }
                    }
                  }
                }
              }
              return rows;
            });

    // Default: no roles assigned initially
    when(roleMetaMapper.listRolesByUserId(eq(USER_ID))).thenReturn(ImmutableList.of());
    // Default answer pulls versions from mockedRoleVersions, populated by mockRoleInStore.
    // Use doAnswer to avoid eager invocation of any previous stub when re-stubbing.
    doAnswer(
            invocation -> {
              List<Long> ids = invocation.getArgument(0);
              if (ids == null) {
                return ImmutableList.of();
              }
              return ids.stream()
                  .map(mockedRoleVersions::get)
                  .filter(Objects::nonNull)
                  .collect(Collectors.toList());
            })
        .when(roleMetaMapper)
        .batchGetRoleUpdatedAt(any());

    gravitinoEnvMockedStatic = mockStatic(GravitinoEnv.class);
    gravitinoEnvMockedStatic.when(GravitinoEnv::getInstance).thenReturn(gravitinoEnv);
    when(gravitinoEnv.config()).thenReturn(new ServerConfig());
    principalUtilsMockedStatic = mockStatic(PrincipalUtils.class);
    metadataIdConverterMockedStatic = mockStatic(MetadataIdConverter.class);
    principalUtilsMockedStatic
        .when(PrincipalUtils::getCurrentPrincipal)
        .thenReturn(new UserPrincipal(USERNAME));
    principalUtilsMockedStatic.when(() -> PrincipalUtils.doAs(any(), any())).thenCallRealMethod();
    principalUtilsMockedStatic.when(PrincipalUtils::getCurrentUserName).thenCallRealMethod();
    metadataIdConverterMockedStatic
        .when(() -> MetadataIdConverter.getID(any(), eq(METALAKE)))
        .thenReturn(Optional.of(CATALOG_ID));
    when(gravitinoEnv.entityStore()).thenReturn(entityStore);
    when(entityStore.relationOperations()).thenReturn(supportsRelationOperations);
    when(entityStore.get(
            eq(NameIdentifierUtil.ofUser(METALAKE, USERNAME)),
            eq(Entity.EntityType.USER),
            eq(UserEntity.class)))
        .thenReturn(getUserEntity());
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
    // jcasbinAuthorizer is per-test (see @AfterEach); only static mocks need cleanup here.
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

  @BeforeEach
  public void createAuthorizer() throws Exception {
    // Build a fresh authorizer per test so enforcer g-rows and version-validated cache state can
    // never bleed across cases regardless of the JUnit execution order.
    jcasbinAuthorizer = new JcasbinAuthorizer();
    jcasbinAuthorizer.initialize();
    restoreDefaultPrincipal();
    // Reset role-user relation mock to return empty list (no roles) by default; individual tests
    // can override as needed.
    NameIdentifier userNameIdentifier = NameIdentifierUtil.ofUser(METALAKE, USERNAME);
    when(supportsRelationOperations.listEntitiesByRelation(
            eq(SupportsRelationOperations.Type.ROLE_USER_REL),
            eq(userNameIdentifier),
            eq(Entity.EntityType.USER)))
        .thenReturn(ImmutableList.of());
    // Reset role version map and re-stub the answer to keep tests isolated from each other.
    mockedRoleVersions.clear();
    doAnswer(
            invocation -> {
              List<Long> ids = invocation.getArgument(0);
              if (ids == null) {
                return ImmutableList.of();
              }
              return ids.stream()
                  .map(mockedRoleVersions::get)
                  .filter(Objects::nonNull)
                  .collect(Collectors.toList());
            })
        .when(roleMetaMapper)
        .batchGetRoleUpdatedAt(any());
  }

  @AfterEach
  public void closeAuthorizer() throws IOException {
    if (jcasbinAuthorizer != null) {
      jcasbinAuthorizer.close();
      jcasbinAuthorizer = null;
    }
  }

  @Test
  public void testIsMetalakeUserUsesUserInfoCache() {
    assertTrue(jcasbinAuthorizer.isMetalakeUser(METALAKE, new AuthorizationRequestContext()));
    verify(userMetaMapper).getUserUpdatedAt(METALAKE, USERNAME);
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
    long roleVersion = nextRoleVersion();
    RolePO allowRolePO = buildRolePO(ALLOW_ROLE_ID, "allowRole");
    when(roleMetaMapper.listRolesByUserId(eq(USER_ID))).thenReturn(ImmutableList.of(allowRolePO));
    when(roleMetaMapper.batchGetRoleUpdatedAt(any()))
        .thenReturn(ImmutableList.of(new RoleUpdatedAt(ALLOW_ROLE_ID, "allowRole", roleVersion)));
    // Bump user version to invalidate userRoleCache
    when(userMetaMapper.getUserUpdatedAt(eq(METALAKE), eq(USERNAME)))
        .thenReturn(new UserUpdatedAt(USER_ID, nextUserVersion()));

    assertTrue(doAuthorize(currentPrincipal));

    // Test role cache.
    // When the user's role changes to one with no privileges, the prune step removes
    // the stale role's g-rows from the enforcer, so authorization fails immediately.
    Long newRoleId = -1L;
    RoleEntity tempNewRole = getRoleEntity(newRoleId, "tempNewRole", ImmutableList.of());
    when(entityStore.get(
            eq(NameIdentifierUtil.ofRole(METALAKE, tempNewRole.name())),
            eq(Entity.EntityType.ROLE),
            eq(RoleEntity.class)))
        .thenReturn(tempNewRole);
    RolePO tempNewRolePO = buildRolePO(newRoleId, "tempNewRole");
    when(roleMetaMapper.listRolesByUserId(eq(USER_ID))).thenReturn(ImmutableList.of(tempNewRolePO));
    long roleVersion2 = nextRoleVersion();
    when(roleMetaMapper.batchGetRoleUpdatedAt(any()))
        .thenReturn(ImmutableList.of(new RoleUpdatedAt(newRoleId, "tempNewRole", roleVersion2)));
    when(userMetaMapper.getUserUpdatedAt(eq(METALAKE), eq(USERNAME)))
        .thenReturn(new UserUpdatedAt(USER_ID, nextUserVersion()));
    // tempNewRole has no privileges; prune step removes stale allowRole g-row, so authz fails.
    assertFalse(doAuthorize(currentPrincipal));

    // After clearing the role policy cache, the next authorize forces a reload.
    jcasbinAuthorizer.handleRolePrivilegeChange(ALLOW_ROLE_ID);

    // Re-assign allowRole, the authorization will succeed
    when(roleMetaMapper.listRolesByUserId(eq(USER_ID))).thenReturn(ImmutableList.of(allowRolePO));
    long roleVersion3 = nextRoleVersion();
    when(roleMetaMapper.batchGetRoleUpdatedAt(any()))
        .thenReturn(ImmutableList.of(new RoleUpdatedAt(ALLOW_ROLE_ID, "allowRole", roleVersion3)));
    when(userMetaMapper.getUserUpdatedAt(eq(METALAKE), eq(USERNAME)))
        .thenReturn(new UserUpdatedAt(USER_ID, nextUserVersion()));
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
    long roleVersion4 = nextRoleVersion();
    when(roleMetaMapper.batchGetRoleUpdatedAt(any()))
        .thenReturn(
            ImmutableList.of(
                new RoleUpdatedAt(ALLOW_ROLE_ID, "allowRole", roleVersion4),
                new RoleUpdatedAt(DENY_ROLE_ID, "denyRole", roleVersion4)));
    when(userMetaMapper.getUserUpdatedAt(eq(METALAKE), eq(USERNAME)))
        .thenReturn(new UserUpdatedAt(USER_ID, nextUserVersion()));
    assertFalse(doAuthorize(currentPrincipal));
  }

  @Test
  public void testHasDenyPolicy() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();

    // With no roles assigned, the user can hold no deny policy.
    assertFalse(
        jcasbinAuthorizer.hasDenyPolicy(
            currentPrincipal,
            METALAKE,
            ImmutableSet.of(USE_CATALOG),
            new AuthorizationRequestContext()));

    // Assign a role that DENIES USE_CATALOG at the catalog scope.
    mockRoleInStore(DENY_ROLE_ID, "denyRole", ImmutableList.of(getDenySecurableObject()));
    when(roleMetaMapper.listRolesByUserId(eq(USER_ID)))
        .thenReturn(ImmutableList.of(buildRolePO(DENY_ROLE_ID, "denyRole")));
    when(userMetaMapper.getUserUpdatedAt(eq(METALAKE), eq(USERNAME)))
        .thenReturn(new UserUpdatedAt(USER_ID, nextUserVersion()));

    // The deny on USE_CATALOG is detected. The match is scope-agnostic: the deny lives on a
    // catalog, which is a parent scope for a schema/catalog list, so it must be reported and
    // disable the short-circuit.
    assertTrue(
        jcasbinAuthorizer.hasDenyPolicy(
            currentPrincipal,
            METALAKE,
            ImmutableSet.of(USE_CATALOG),
            new AuthorizationRequestContext()));

    // A deny on USE_CATALOG must not be reported when querying a different privilege.
    assertFalse(
        jcasbinAuthorizer.hasDenyPolicy(
            currentPrincipal,
            METALAKE,
            ImmutableSet.of(SELECT_TABLE),
            new AuthorizationRequestContext()));
  }

  @Test
  public void testHasDenyPolicyDetectsGroupInheritedDeny() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    getLoadedRolesCache(jcasbinAuthorizer).invalidateAll();

    // The user holds no direct roles; the deny role is only reachable through group membership.
    UserPrincipal groupPrincipal = setCurrentPrincipalWithGroup(GROUP_NAME);
    mockRoleInStore(DENY_ROLE_ID, "denyRole", ImmutableList.of(getDenySecurableObject()));
    mockNoDirectUserRoles();
    mockGroupWithRoles(GROUP_NAME, ImmutableList.of(DENY_ROLE_ID), ImmutableList.of("denyRole"));

    // A deny inherited via a group must still be detected, otherwise the list short-circuit would
    // over-expose objects that a group-level deny is meant to hide.
    assertTrue(
        jcasbinAuthorizer.hasDenyPolicy(
            groupPrincipal,
            METALAKE,
            ImmutableSet.of(USE_CATALOG),
            new AuthorizationRequestContext()));

    restoreDefaultPrincipal();
    getLoadedRolesCache(jcasbinAuthorizer).invalidateAll();
  }

  @Test
  public void testUserRoleCacheDoesNotReuseRolesAfterUsernameRecreate() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();

    RoleEntity allowRole =
        mockRoleInStore(
            ALLOW_ROLE_ID,
            "allowRoleBeforeUserRecreate",
            ImmutableList.of(getAllowSecurableObject()));
    when(userMetaMapper.getUserUpdatedAt(eq(METALAKE), eq(USERNAME)))
        .thenReturn(new UserUpdatedAt(USER_ID, 1000L));
    when(roleMetaMapper.listRolesByUserId(eq(USER_ID)))
        .thenReturn(ImmutableList.of(buildRolePO(allowRole.id(), allowRole.name())));

    assertTrue(doAuthorize(currentPrincipal));

    long recreatedUserId = USER_ID + 1000L;
    when(userMetaMapper.getUserUpdatedAt(eq(METALAKE), eq(USERNAME)))
        .thenReturn(new UserUpdatedAt(recreatedUserId, 0L));
    when(roleMetaMapper.listRolesByUserId(eq(recreatedUserId))).thenReturn(ImmutableList.of());

    assertFalse(doAuthorize(currentPrincipal));
    verify(roleMetaMapper).listRolesByUserId(eq(recreatedUserId));
  }

  @Test
  public void testVersionCheckEvictsPoliciesOfRolesMissingFromDb() throws Exception {
    // Regression test for the cross-instance role-delete invalidation gap. The
    // happy path is already handled by the fat-JOIN inside prefetchUserAndGroupInfo,
    // which excludes soft-/hard-deleted roles via "role_meta.deleted_at = 0" and
    // re-primes userRoleCache before the next loadUserRoles call. This test covers
    // the defence-in-depth tier: if versionCheckAndLoadRoles is ever invoked with
    // a roleId whose version probe row is missing (e.g. cache window race, future
    // code path bypassing the fat-JOIN), the fix must still clear that role's
    // p-rows from both enforcers and evict its loadedRoles entry so that any
    // residual user → deleted-role g-row grants nothing on subsequent enforce()s.
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();

    // 1. Authorize once via the normal flow to populate loadedRoles + enforcer p-rows
    //    for allowRole.
    RoleEntity allowRole =
        mockRoleInStore(ALLOW_ROLE_ID, "allowRole", ImmutableList.of(getAllowSecurableObject()));
    long userVersion = nextUserVersion();
    when(userMetaMapper.getUserUpdatedAt(eq(METALAKE), eq(USERNAME)))
        .thenReturn(new UserUpdatedAt(USER_ID, userVersion));
    when(roleMetaMapper.listRolesByUserId(eq(USER_ID)))
        .thenReturn(ImmutableList.of(buildRolePO(ALLOW_ROLE_ID, allowRole.name())));
    assertTrue(doAuthorize(currentPrincipal));

    // Sanity: loadedRoles now has an entry for ALLOW_ROLE_ID and the allowEnforcer
    // contains a p-row whose subject (column 0) equals the role id.
    Assertions.assertTrue(
        getLoadedRolesCache(jcasbinAuthorizer).getIfPresent(ALLOW_ROLE_ID).isPresent(),
        "loadedRoles must be primed before the test");
    Enforcer allowEnforcer = getAllowEnforcer(jcasbinAuthorizer);
    Assertions.assertFalse(
        allowEnforcer.getFilteredPolicy(0, String.valueOf(ALLOW_ROLE_ID)).isEmpty(),
        "allowEnforcer must hold p-rows for allowRole before the test");

    // 2. Simulate the bug-trigger: batchGetRoleUpdatedAt returns NO row for the role
    //    (i.e. the role row is gone from role_meta), even though something is still
    //    asking us to version-check it.
    when(roleMetaMapper.batchGetRoleUpdatedAt(any())).thenReturn(ImmutableList.of());

    // Invoke versionCheckAndLoadRoles directly with the "deleted" role id and a
    // fresh AuthorizationRequestContext that has NO prefetched role versions, so
    // the method falls through to the batch probe and observes the empty result.
    AuthorizationRequestContext freshCtx = new AuthorizationRequestContext();
    invokeVersionCheckAndLoadRoles(
        jcasbinAuthorizer, METALAKE, ImmutableList.of(ALLOW_ROLE_ID), freshCtx);

    // 3. The fix must have cleared the role's p-rows and evicted loadedRoles.
    Assertions.assertTrue(
        allowEnforcer.getFilteredPolicy(0, String.valueOf(ALLOW_ROLE_ID)).isEmpty(),
        "allowEnforcer p-rows for the deleted role must be cleared");
    Assertions.assertFalse(
        getLoadedRolesCache(jcasbinAuthorizer).getIfPresent(ALLOW_ROLE_ID).isPresent(),
        "loadedRoles entry for the deleted role must be evicted");
  }

  /** Reflectively invoke the private versionCheckAndLoadRoles. */
  private static void invokeVersionCheckAndLoadRoles(
      JcasbinAuthorizer authorizer,
      String metalake,
      List<Long> roleIds,
      AuthorizationRequestContext requestContext)
      throws Exception {
    Method m =
        JcasbinAuthorizer.class.getDeclaredMethod(
            "versionCheckAndLoadRoles",
            String.class,
            List.class,
            AuthorizationRequestContext.class);
    m.setAccessible(true);
    m.invoke(authorizer, metalake, roleIds, requestContext);
  }

  @Test
  public void testAuthorizeByOwner() throws Exception {
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    // No owner set — should fail
    when(ownerMetaMapper.selectOwnerByMetadataObjectIdAndType(eq(CATALOG_ID), eq("CATALOG")))
        .thenReturn(null);
    getOwnerRelCache(jcasbinAuthorizer).invalidateAll();
    assertFalse(doAuthorizeOwner(currentPrincipal));

    // Set owner to current user
    OwnerInfo ownerInfo = new OwnerInfo(USER_ID, "USER");
    when(ownerMetaMapper.selectOwnerByMetadataObjectIdAndType(eq(CATALOG_ID), eq("CATALOG")))
        .thenReturn(ownerInfo);
    getOwnerRelCache(jcasbinAuthorizer).invalidateAll();
    assertTrue(doAuthorizeOwner(currentPrincipal));

    // Matching ID with a GROUP owner type must not grant user ownership.
    OwnerInfo collidingGroupOwnerInfo = new OwnerInfo(USER_ID, "GROUP");
    when(ownerMetaMapper.selectOwnerByMetadataObjectIdAndType(eq(CATALOG_ID), eq("CATALOG")))
        .thenReturn(collidingGroupOwnerInfo);
    getOwnerRelCache(jcasbinAuthorizer).invalidateAll();
    assertFalse(doAuthorizeOwner(currentPrincipal));

    // Remove owner
    when(ownerMetaMapper.selectOwnerByMetadataObjectIdAndType(eq(CATALOG_ID), eq("CATALOG")))
        .thenReturn(null);
    NameIdentifier catalogIdent = NameIdentifierUtil.ofCatalog(METALAKE, "testCatalog");
    jcasbinAuthorizer.handleMetadataOwnerChange(
        METALAKE, USER_ID, catalogIdent, Entity.EntityType.CATALOG);
    assertFalse(doAuthorizeOwner(currentPrincipal));
  }

  @Test
  public void testPrefetchRunsAfterOwnerUserInfoLookup() throws Exception {
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    RoleEntity allowRole =
        mockRoleInStore(ALLOW_ROLE_ID, "allowRole", ImmutableList.of(getAllowSecurableObject()));
    mockDirectUserRoles(allowRole);

    when(ownerMetaMapper.selectOwnerByMetadataObjectIdAndType(eq(CATALOG_ID), eq("CATALOG")))
        .thenReturn(new OwnerInfo(USER_ID + 1L, "USER"));
    getOwnerRelCache(jcasbinAuthorizer).invalidateAll();

    AuthorizationRequestContext requestContext = new AuthorizationRequestContext();
    MetadataObject catalog = MetadataObjects.of(null, "testCatalog", MetadataObject.Type.CATALOG);

    assertFalse(jcasbinAuthorizer.isOwner(currentPrincipal, METALAKE, catalog, requestContext));
    Mockito.clearInvocations(userMetaMapper, roleMetaMapper);

    assertTrue(
        jcasbinAuthorizer.authorize(
            currentPrincipal, METALAKE, catalog, USE_CATALOG, requestContext));
    verify(userMetaMapper).batchGetAuthSubjectsForUser(eq(METALAKE), eq(USERNAME), anyList());
    verify(roleMetaMapper, Mockito.never()).batchGetRoleUpdatedAt(any());
  }

  @Test
  public void testAuthorizeByGroupOwner() throws Exception {
    // Set up a UserPrincipal whose groups include GROUP_NAME
    UserPrincipal groupPrincipal =
        new UserPrincipal(USERNAME, ImmutableList.of(new UserGroup(Optional.empty(), GROUP_NAME)));
    principalUtilsMockedStatic.when(PrincipalUtils::getCurrentPrincipal).thenReturn(groupPrincipal);

    NameIdentifier catalogIdent = NameIdentifierUtil.ofCatalog(METALAKE, "testCatalog");

    // Group identity is now resolved via groupMetaMapper.getGroupUpdatedAt (per-request cache path)
    // instead of entityStore.batchGet(GROUP); verify the new path is wired correctly.
    when(groupMetaMapper.getGroupUpdatedAt(eq(METALAKE), eq(GROUP_NAME)))
        .thenReturn(new GroupUpdatedAt(GROUP_ID, groupVersionCounter.incrementAndGet()));
    when(groupMetaMapper.getGroupUpdatedAt(eq(METALAKE), eq("otherGroup")))
        .thenReturn(new GroupUpdatedAt(99L, groupVersionCounter.incrementAndGet()));

    // Mock owner_meta lookup returning a GROUP-typed OwnerInfo (the owner is GROUP_ID).
    OwnerInfo groupOwnerInfo = new OwnerInfo(GROUP_ID, "GROUP");
    when(ownerMetaMapper.selectOwnerByMetadataObjectIdAndType(eq(CATALOG_ID), eq("CATALOG")))
        .thenReturn(groupOwnerInfo);
    getOwnerRelCache(jcasbinAuthorizer).invalidateAll();

    // The principal belongs to the owning group, so isOwner should return true
    assertTrue(doAuthorizeOwner(groupPrincipal));

    // entityStore.batchGet must NOT be called for GROUP entity lookups in the owner-check path
    Mockito.verify(entityStore, Mockito.never())
        .batchGet(anyList(), eq(Entity.EntityType.GROUP), eq(GroupEntity.class));

    // Clear owner and verify it returns false
    when(ownerMetaMapper.selectOwnerByMetadataObjectIdAndType(eq(CATALOG_ID), eq("CATALOG")))
        .thenReturn(null);
    jcasbinAuthorizer.handleMetadataOwnerChange(
        METALAKE, GROUP_ID, catalogIdent, Entity.EntityType.CATALOG);
    assertFalse(doAuthorizeOwner(groupPrincipal));

    // Verify a principal whose groups do NOT include the owner group gets denied
    UserPrincipal nonMemberPrincipal =
        new UserPrincipal(
            USERNAME, ImmutableList.of(new UserGroup(Optional.empty(), "otherGroup")));
    principalUtilsMockedStatic
        .when(PrincipalUtils::getCurrentPrincipal)
        .thenReturn(nonMemberPrincipal);
    // Re-populate the owner cache with the group owner
    when(ownerMetaMapper.selectOwnerByMetadataObjectIdAndType(eq(CATALOG_ID), eq("CATALOG")))
        .thenReturn(groupOwnerInfo);
    getOwnerRelCache(jcasbinAuthorizer).invalidateAll();
    assertFalse(doAuthorizeOwner(nonMemberPrincipal));

    // Restore the original principal mock
    principalUtilsMockedStatic
        .when(PrincipalUtils::getCurrentPrincipal)
        .thenReturn(new UserPrincipal(USERNAME));
  }

  @Test
  public void testGroupOwnerCheckDeduplicatesGroupInfoWithinRequest() throws Exception {
    // Verify that repeated isOwner calls within the same AuthorizationRequestContext
    // do not re-query group_meta; groupMetaMapper.getGroupUpdatedAt should be called at most once
    // per (metalake, groupName) per request thanks to requestContext.groupInfoCache.
    Mockito.clearInvocations(groupMetaMapper);

    UserPrincipal groupPrincipal =
        new UserPrincipal(USERNAME, ImmutableList.of(new UserGroup(Optional.empty(), GROUP_NAME)));
    principalUtilsMockedStatic.when(PrincipalUtils::getCurrentPrincipal).thenReturn(groupPrincipal);

    when(groupMetaMapper.getGroupUpdatedAt(eq(METALAKE), eq(GROUP_NAME)))
        .thenReturn(new GroupUpdatedAt(GROUP_ID, groupVersionCounter.incrementAndGet()));

    OwnerInfo groupOwnerInfo = new OwnerInfo(GROUP_ID, "GROUP");
    when(ownerMetaMapper.selectOwnerByMetadataObjectIdAndType(eq(CATALOG_ID), eq("CATALOG")))
        .thenReturn(groupOwnerInfo);
    getOwnerRelCache(jcasbinAuthorizer).invalidateAll();

    MetadataObject catalog = MetadataObjects.of(null, "testCatalog", MetadataObject.Type.CATALOG);

    // Call isOwner twice within the same request context
    AuthorizationRequestContext sharedContext = new AuthorizationRequestContext();
    assertTrue(jcasbinAuthorizer.isOwner(groupPrincipal, METALAKE, catalog, sharedContext));
    assertTrue(jcasbinAuthorizer.isOwner(groupPrincipal, METALAKE, catalog, sharedContext));

    // group_meta should have been queried exactly once despite two isOwner calls
    Mockito.verify(groupMetaMapper, Mockito.times(1))
        .getGroupUpdatedAt(eq(METALAKE), eq(GROUP_NAME));
  }

  @Test
  public void testGroupOwnerCheckUsesProvidedPrincipalGroups() throws Exception {
    UserPrincipal ownerGroupPrincipal =
        new UserPrincipal(USERNAME, ImmutableList.of(new UserGroup(Optional.empty(), GROUP_NAME)));
    UserPrincipal currentPrincipalWithoutOwnerGroup =
        new UserPrincipal(
            USERNAME, ImmutableList.of(new UserGroup(Optional.empty(), "otherGroup")));
    principalUtilsMockedStatic
        .when(PrincipalUtils::getCurrentPrincipal)
        .thenReturn(currentPrincipalWithoutOwnerGroup);

    when(groupMetaMapper.getGroupUpdatedAt(eq(METALAKE), eq(GROUP_NAME)))
        .thenReturn(new GroupUpdatedAt(GROUP_ID, groupVersionCounter.incrementAndGet()));

    OwnerInfo groupOwnerInfo = new OwnerInfo(GROUP_ID, "GROUP");
    when(ownerMetaMapper.selectOwnerByMetadataObjectIdAndType(eq(CATALOG_ID), eq("CATALOG")))
        .thenReturn(groupOwnerInfo);
    getOwnerRelCache(jcasbinAuthorizer).invalidateAll();

    assertTrue(doAuthorizeOwner(ownerGroupPrincipal));
  }

  @Test
  public void testAuthorizeByGroupRole() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);

    UserPrincipal groupPrincipal = setCurrentPrincipalWithGroup(GROUP_NAME);

    // Create a role with USE_CATALOG privilege
    Long groupRoleId = 7L;
    RoleEntity groupRole =
        mockRoleInStore(groupRoleId, "groupRole", ImmutableList.of(getAllowSecurableObject()));

    mockNoDirectUserRoles();
    mockGroupWithRoles(
        GROUP_NAME, ImmutableList.of(groupRoleId), ImmutableList.of(groupRole.name()));

    // Authorization should succeed via group-inherited role
    assertTrue(doAuthorize(groupPrincipal));

    // A principal with no groups should fail
    UserPrincipal noGroupPrincipal = setCurrentPrincipalWithGroup(null);
    // Clear role caches to force re-evaluation
    getLoadedRolesCache(jcasbinAuthorizer).invalidateAll();
    assertFalse(doAuthorize(noGroupPrincipal));

    restoreDefaultPrincipal();
  }

  @Test
  public void testGroupRoleSkippedWhenRoleIdsAndNamesMismatch() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    getLoadedRolesCache(jcasbinAuthorizer).invalidateAll();

    String mismatchGroupName = "mismatchGroup";
    UserPrincipal groupPrincipal = setCurrentPrincipalWithGroup(mismatchGroupName);

    mockNoDirectUserRoles();
    // Mismatched roleIds (1) and roleNames (2) -- the whole group should be skipped
    mockGroupWithRoles(
        mismatchGroupName, ImmutableList.of(101L), ImmutableList.of("roleA", "roleB"));

    // Authorization denied -- the mismatched group is skipped, so no role is loaded
    assertFalse(doAuthorize(groupPrincipal));

    restoreDefaultPrincipal();
    getLoadedRolesCache(jcasbinAuthorizer).invalidateAll();
  }

  @Test
  public void testAuthorizeByDirectAndGroupRoles() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    getLoadedRolesCache(jcasbinAuthorizer).invalidateAll();

    UserPrincipal groupPrincipal = setCurrentPrincipalWithGroup(GROUP_NAME);

    // Direct role has no privilege; group role grants USE_CATALOG
    Long directRoleId = 8L;
    RoleEntity directRole = mockRoleInStore(directRoleId, "directRole", ImmutableList.of());
    Long groupRoleId = 9L;
    RoleEntity groupRole =
        mockRoleInStore(
            groupRoleId, "groupCatalogRole", ImmutableList.of(getAllowSecurableObject()));

    mockDirectUserRoles(directRole);
    mockGroupWithRoles(
        GROUP_NAME, ImmutableList.of(groupRoleId), ImmutableList.of(groupRole.name()));

    // Authorization should succeed -- direct role has no privilege, but group role does
    assertTrue(doAuthorize(groupPrincipal));

    restoreDefaultPrincipal();
    getLoadedRolesCache(jcasbinAuthorizer).invalidateAll();
  }

  @Test
  public void testIsSelfRoleViaGroup() throws Exception {
    // Use a role whose MetadataIdConverter.getID resolves to CATALOG_ID (catch-all mock)
    Long groupRoleId = CATALOG_ID;
    String groupRoleName = "groupSelfRole";
    NameIdentifier roleIdent = NameIdentifierUtil.ofRole(METALAKE, groupRoleName);

    mockNoDirectUserRoles();
    setCurrentPrincipalWithGroup(GROUP_NAME);
    mockGroupWithRoles(GROUP_NAME, ImmutableList.of(groupRoleId), ImmutableList.of(groupRoleName));

    // isSelf should return true -- role is assigned to user's group
    assertTrue(
        jcasbinAuthorizer.isSelf(
            Entity.EntityType.ROLE, roleIdent, new AuthorizationRequestContext()));

    // A principal with no groups should fail
    setCurrentPrincipalWithGroup(null);
    assertFalse(
        jcasbinAuthorizer.isSelf(
            Entity.EntityType.ROLE, roleIdent, new AuthorizationRequestContext()));

    restoreDefaultPrincipal();
  }

  @Test
  public void testIsSelfRoleReusesCacheAcrossCalls() throws Exception {
    // Acceptance criterion for #11088: repeated isSelf(ROLE) calls in the same logical request
    // must not re-issue the role-list DB queries (listRolesByUserId / listRolesByGroupId).
    // The version-validated userRoleCache / groupRoleCache are process-wide, so the second call
    // hits cache even though each isSelf creates a fresh AuthorizationRequestContext.
    //
    // Use CATALOG_ID so the role id matches the catch-all MetadataIdConverter.getID mock.
    Long directRoleId = CATALOG_ID;
    String directRoleName = "selfDedupRole";
    NameIdentifier roleIdent = NameIdentifierUtil.ofRole(METALAKE, directRoleName);

    // Direct user-role assignment via the version-validated cache path.
    mockUserRoles(directRoleId, directRoleName);

    // Use a fresh authorizer + principal to ensure the userRoleCache starts cold.
    setCurrentPrincipalWithGroup(null);
    Mockito.clearInvocations(roleMetaMapper);

    // 1st call: miss → listRolesByUserId; 2nd call: cache hit → no extra listRolesByUserId.
    AuthorizationRequestContext ctx1 = new AuthorizationRequestContext();
    AuthorizationRequestContext ctx2 = new AuthorizationRequestContext();
    assertTrue(jcasbinAuthorizer.isSelf(Entity.EntityType.ROLE, roleIdent, ctx1));
    assertTrue(jcasbinAuthorizer.isSelf(Entity.EntityType.ROLE, roleIdent, ctx2));

    Mockito.verify(roleMetaMapper, Mockito.times(1)).listRolesByUserId(eq(USER_ID));

    restoreDefaultPrincipal();
  }

  @Test
  public void testIsSelfRoleDoesNotCallListEntitiesByRelation() throws Exception {
    // #11088: isSelf(ROLE) must not bypass the cache by going straight to
    // entityStore.relationOperations().listEntitiesByRelation(ROLE_USER_REL, ...).
    Long directRoleId = CATALOG_ID;
    String directRoleName = "noBypassRole";
    NameIdentifier roleIdent = NameIdentifierUtil.ofRole(METALAKE, directRoleName);

    mockUserRoles(directRoleId, directRoleName);
    setCurrentPrincipalWithGroup(null);
    Mockito.clearInvocations(supportsRelationOperations);

    assertTrue(
        jcasbinAuthorizer.isSelf(
            Entity.EntityType.ROLE, roleIdent, new AuthorizationRequestContext()));

    Mockito.verify(supportsRelationOperations, Mockito.never())
        .listEntitiesByRelation(any(), any(), any());

    restoreDefaultPrincipal();
  }

  @Test
  public void testStaleGroupSkippedWhenNotInStore() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    getLoadedRolesCache(jcasbinAuthorizer).invalidateAll();

    String staleGroupName = "staleGroup";
    UserPrincipal groupPrincipal = setCurrentPrincipalWithGroup(staleGroupName);

    mockNoDirectUserRoles();
    // group_meta lookup returns null -- the stale group has no row in the DB.
    when(groupMetaMapper.getGroupUpdatedAt(eq(METALAKE), eq(staleGroupName))).thenReturn(null);

    // Authorization denied without throwing -- the stale group is silently skipped
    assertFalse(doAuthorize(groupPrincipal));

    restoreDefaultPrincipal();
    getLoadedRolesCache(jcasbinAuthorizer).invalidateAll();
  }

  @Test
  public void testGroupRoleRevokedDeniesAccess() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    getLoadedRolesCache(jcasbinAuthorizer).invalidateAll();

    UserPrincipal groupPrincipal = setCurrentPrincipalWithGroup(GROUP_NAME);

    // Group has a role that grants USE_CATALOG
    Long groupRoleId = 10L;
    RoleEntity groupRole =
        mockRoleInStore(groupRoleId, "revokableRole", ImmutableList.of(getAllowSecurableObject()));

    mockNoDirectUserRoles();
    mockGroupWithRoles(
        GROUP_NAME, ImmutableList.of(groupRoleId), ImmutableList.of(groupRole.name()));

    // Authorization succeeds via group-inherited role
    assertTrue(doAuthorize(groupPrincipal));

    // Simulate group removing the role: invalidate the cache (same as handleRolePrivilegeChange)
    // and update the group mock to have no roles
    mockGroupWithRoles(GROUP_NAME, ImmutableList.of(), ImmutableList.of());
    jcasbinAuthorizer.handleRolePrivilegeChange(groupRoleId);

    // Authorization should now be denied -- the role was removed from the group
    assertFalse(doAuthorize(groupPrincipal));

    restoreDefaultPrincipal();
    getLoadedRolesCache(jcasbinAuthorizer).invalidateAll();
  }

  @Test
  public void testRecreatedGroupWithSameNameDoesNotReuseOldRoleCache() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    getLoadedRolesCache(jcasbinAuthorizer).invalidateAll();

    Long oldGroupId = 201L;
    Long newGroupId = 202L;
    Long oldGroupRoleId = 203L;
    RoleEntity oldGroupRole =
        mockRoleInStore(
            oldGroupRoleId, "oldGroupRole", ImmutableList.of(getAllowSecurableObject()));
    UserPrincipal groupPrincipal = setCurrentPrincipalWithGroup(GROUP_NAME);

    mockNoDirectUserRoles();
    mockGroupWithRoles(
        oldGroupId,
        GROUP_NAME,
        ImmutableList.of(oldGroupRoleId),
        ImmutableList.of(oldGroupRole.name()));

    assertTrue(doAuthorize(groupPrincipal));

    // The group is deleted and recreated with the same name but a new id. Keep updated_at lower
    // than the old cache snapshot to verify the group id, not only updated_at, controls reuse.
    when(groupMetaMapper.getGroupUpdatedAt(eq(METALAKE), eq(GROUP_NAME)))
        .thenReturn(new GroupUpdatedAt(newGroupId, 0L));
    when(roleMetaMapper.listRolesByGroupId(eq(newGroupId))).thenReturn(ImmutableList.of());

    assertFalse(doAuthorize(groupPrincipal));

    restoreDefaultPrincipal();
    getLoadedRolesCache(jcasbinAuthorizer).invalidateAll();
  }

  @Test
  public void testRoleSharedByUserAndGroupSurvivesGroupRevocation() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    getLoadedRolesCache(jcasbinAuthorizer).invalidateAll();

    UserPrincipal groupPrincipal = setCurrentPrincipalWithGroup(GROUP_NAME);

    // The same role is assigned both directly to the user AND to the user's group
    Long sharedRoleId = 11L;
    RoleEntity sharedRole =
        mockRoleInStore(sharedRoleId, "sharedRole", ImmutableList.of(getAllowSecurableObject()));

    mockDirectUserRoles(sharedRole);
    mockGroupWithRoles(
        GROUP_NAME, ImmutableList.of(sharedRoleId), ImmutableList.of(sharedRole.name()));

    // Authorization succeeds (role via both direct and group)
    assertTrue(doAuthorize(groupPrincipal));

    // Group removes the role; user still has it directly
    mockGroupWithRoles(GROUP_NAME, ImmutableList.of(), ImmutableList.of());
    jcasbinAuthorizer.handleRolePrivilegeChange(sharedRoleId);

    // Authorization should still succeed -- role is retained via direct assignment
    assertTrue(doAuthorize(groupPrincipal));

    restoreDefaultPrincipal();
    getLoadedRolesCache(jcasbinAuthorizer).invalidateAll();
  }

  /**
   * Inverse of {@link #testRoleSharedByUserAndGroupSurvivesGroupRevocation}: the same role is
   * assigned to both the user directly AND the user's group. When the role is revoked from the
   * user, the group still has it. Access should survive via group inheritance.
   */
  @Test
  public void testRoleSharedByUserAndGroupSurvivesUserRevocation() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    getLoadedRolesCache(jcasbinAuthorizer).invalidateAll();

    UserPrincipal groupPrincipal = setCurrentPrincipalWithGroup(GROUP_NAME);

    // The same role is assigned both directly to the user AND to the user's group
    Long sharedRoleId = 12L;
    RoleEntity sharedRole =
        mockRoleInStore(
            sharedRoleId, "sharedRoleForUserRevoke", ImmutableList.of(getAllowSecurableObject()));

    mockDirectUserRoles(sharedRole);
    mockGroupWithRoles(
        GROUP_NAME, ImmutableList.of(sharedRoleId), ImmutableList.of(sharedRole.name()));

    // Authorization succeeds (role via both direct and group)
    assertTrue(doAuthorize(groupPrincipal));

    // User loses the role directly; group still has it. Bumping the user version forces the
    // userRoleCache to miss and reload the (now-empty) direct role list.
    mockDirectUserRoles();
    jcasbinAuthorizer.handleRolePrivilegeChange(sharedRoleId);

    // Authorization should still succeed -- role is retained via group inheritance
    assertTrue(doAuthorize(groupPrincipal));

    restoreDefaultPrincipal();
    getLoadedRolesCache(jcasbinAuthorizer).invalidateAll();
  }

  @Test
  public void testUserRoleRelChangeInvalidatesUserRoleCache() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    getLoadedRolesCache(jcasbinAuthorizer).invalidateAll();

    UserPrincipal noGroupPrincipal = setCurrentPrincipalWithGroup(null);
    long userVersion = nextUserVersion();
    when(userMetaMapper.getUserUpdatedAt(eq(METALAKE), eq(USERNAME)))
        .thenReturn(new UserUpdatedAt(USER_ID, userVersion));
    when(roleMetaMapper.listRolesByUserId(eq(USER_ID))).thenReturn(ImmutableList.of());

    assertFalse(doAuthorize(noGroupPrincipal));

    Long grantedRoleId = 20L;
    RoleEntity grantedRole =
        mockRoleInStore(
            grantedRoleId, "userRelGrantedRole", ImmutableList.of(getAllowSecurableObject()));
    when(roleMetaMapper.listRolesByUserId(eq(USER_ID)))
        .thenReturn(ImmutableList.of(buildRolePO(grantedRoleId, grantedRole.name())));

    jcasbinAuthorizer.handleUserRoleRelChange(METALAKE, USERNAME);

    assertTrue(doAuthorize(noGroupPrincipal));

    restoreDefaultPrincipal();
    getLoadedRolesCache(jcasbinAuthorizer).invalidateAll();
  }

  @Test
  public void testGroupRoleRelChangeInvalidatesGroupRoleCache() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    getLoadedRolesCache(jcasbinAuthorizer).invalidateAll();

    UserPrincipal groupPrincipal = setCurrentPrincipalWithGroup(GROUP_NAME);
    mockNoDirectUserRoles();
    long groupVersion = groupVersionCounter.incrementAndGet();
    when(groupMetaMapper.getGroupUpdatedAt(eq(METALAKE), eq(GROUP_NAME)))
        .thenReturn(new GroupUpdatedAt(GROUP_ID, groupVersion));
    when(roleMetaMapper.listRolesByGroupId(eq(GROUP_ID))).thenReturn(ImmutableList.of());

    assertFalse(doAuthorize(groupPrincipal));

    Long grantedRoleId = 21L;
    RoleEntity grantedRole =
        mockRoleInStore(
            grantedRoleId, "groupRelGrantedRole", ImmutableList.of(getAllowSecurableObject()));
    when(roleMetaMapper.listRolesByGroupId(eq(GROUP_ID)))
        .thenReturn(ImmutableList.of(buildRolePO(grantedRoleId, grantedRole.name())));

    jcasbinAuthorizer.handleGroupRoleRelChange(METALAKE, GROUP_NAME);

    assertTrue(doAuthorize(groupPrincipal));

    restoreDefaultPrincipal();
    getLoadedRolesCache(jcasbinAuthorizer).invalidateAll();
  }

  /**
   * When the user is removed from a group at the IdP level (e.g. Azure AD), the next JWT token
   * won't include that group. On the next request the group's roles should no longer be available.
   */
  @Test
  public void testUserRemovedFromGroupAtIdpDeniesAccess() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    getLoadedRolesCache(jcasbinAuthorizer).invalidateAll();

    UserPrincipal groupPrincipal = setCurrentPrincipalWithGroup(GROUP_NAME);

    // Group has a role that grants USE_CATALOG; user has no direct roles
    Long groupRoleId = 13L;
    RoleEntity groupRole =
        mockRoleInStore(groupRoleId, "idpGroupRole", ImmutableList.of(getAllowSecurableObject()));

    mockNoDirectUserRoles();
    mockGroupWithRoles(
        GROUP_NAME, ImmutableList.of(groupRoleId), ImmutableList.of(groupRole.name()));

    // Authorization succeeds via group-inherited role
    assertTrue(doAuthorize(groupPrincipal));

    // User is removed from the group at the IdP level -- next token has no groups.
    UserPrincipal noGroupPrincipal = setCurrentPrincipalWithGroup(null);

    // The prune step detects that the group-inherited role is no longer valid
    // (group not in token → role not in desiredRoleIds) and removes the stale g-rows.
    // Access is denied immediately without waiting for cache TTL expiry.
    assertFalse(doAuthorize(noGroupPrincipal));

    restoreDefaultPrincipal();
    getLoadedRolesCache(jcasbinAuthorizer).invalidateAll();
  }

  /**
   * When a user belongs to multiple groups and one group's role is revoked, roles from the other
   * group should still grant access.
   */
  @Test
  public void testMultipleGroupsPartialRevocationRetainsAccess() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    getLoadedRolesCache(jcasbinAuthorizer).invalidateAll();

    String groupA = "groupA";
    String groupB = "groupB";
    Long groupAId = 14L;
    Long groupBId = 15L;

    // Principal belongs to two groups
    UserPrincipal multiGroupPrincipal =
        new UserPrincipal(
            USERNAME,
            ImmutableList.of(
                new UserGroup(Optional.empty(), groupA), new UserGroup(Optional.empty(), groupB)));
    principalUtilsMockedStatic
        .when(PrincipalUtils::getCurrentPrincipal)
        .thenReturn(multiGroupPrincipal);

    // Group A has a role with USE_CATALOG; Group B has a different role also with USE_CATALOG
    Long roleAId = 16L;
    Long roleBId = 17L;
    RoleEntity roleA =
        mockRoleInStore(roleAId, "roleA", ImmutableList.of(getAllowSecurableObject()));
    RoleEntity roleB =
        mockRoleInStore(roleBId, "roleB", ImmutableList.of(getAllowSecurableObject()));

    mockNoDirectUserRoles();

    mockGroupWithRoles(groupAId, groupA, ImmutableList.of(roleAId), ImmutableList.of(roleA.name()));
    mockGroupWithRoles(groupBId, groupB, ImmutableList.of(roleBId), ImmutableList.of(roleB.name()));

    // Authorization succeeds
    assertTrue(doAuthorize(multiGroupPrincipal));

    // Revoke roleA from groupA; groupB still has roleB. Bumping the group_meta version forces
    // the groupRoleCache to miss and reload the now-empty role list for groupA.
    when(groupMetaMapper.getGroupUpdatedAt(eq(METALAKE), eq(groupA)))
        .thenReturn(new GroupUpdatedAt(groupAId, groupVersionCounter.incrementAndGet()));
    when(roleMetaMapper.listRolesByGroupId(eq(groupAId))).thenReturn(ImmutableList.of());
    jcasbinAuthorizer.handleRolePrivilegeChange(roleAId);

    // Authorization should still succeed via groupB's role
    assertTrue(doAuthorize(multiGroupPrincipal));

    restoreDefaultPrincipal();
    getLoadedRolesCache(jcasbinAuthorizer).invalidateAll();
  }

  /**
   * When a group grants an ALLOW privilege and a user has a direct DENY on the same resource, the
   * deny should take precedence (deny wins over allow).
   */
  @Test
  public void testDenyRoleOnUserOverridesAllowFromGroup() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    getLoadedRolesCache(jcasbinAuthorizer).invalidateAll();

    UserPrincipal groupPrincipal = setCurrentPrincipalWithGroup(GROUP_NAME);

    // Group has an allow role
    Long allowRoleId = 18L;
    RoleEntity allowRole =
        mockRoleInStore(allowRoleId, "groupAllowRole", ImmutableList.of(getAllowSecurableObject()));
    mockGroupWithRoles(
        GROUP_NAME, ImmutableList.of(allowRoleId), ImmutableList.of(allowRole.name()));

    // User has a deny role directly
    Long denyRoleId = 19L;
    RoleEntity denyRole =
        mockRoleInStore(denyRoleId, "userDenyRole", ImmutableList.of(getDenySecurableObject()));
    mockDirectUserRoles(denyRole);

    // Deny should win -- user has explicit deny even though group provides allow
    assertFalse(doAuthorize(groupPrincipal));

    restoreDefaultPrincipal();
    getLoadedRolesCache(jcasbinAuthorizer).invalidateAll();
  }

  /** Inverse: group has a DENY role while user has a direct ALLOW role. Deny should still win. */
  @Test
  public void testDenyRoleFromGroupOverridesAllowOnUser() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    getLoadedRolesCache(jcasbinAuthorizer).invalidateAll();

    UserPrincipal groupPrincipal = setCurrentPrincipalWithGroup(GROUP_NAME);

    // Group has a deny role
    Long denyRoleId = 20L;
    RoleEntity denyRole =
        mockRoleInStore(denyRoleId, "groupDenyRole", ImmutableList.of(getDenySecurableObject()));
    mockGroupWithRoles(GROUP_NAME, ImmutableList.of(denyRoleId), ImmutableList.of(denyRole.name()));

    // User has an allow role directly
    Long allowRoleId = 21L;
    RoleEntity allowRole =
        mockRoleInStore(allowRoleId, "userAllowRole", ImmutableList.of(getAllowSecurableObject()));
    mockDirectUserRoles(allowRole);

    // Deny should win -- group provides deny even though user has direct allow
    assertFalse(doAuthorize(groupPrincipal));

    restoreDefaultPrincipal();
    getLoadedRolesCache(jcasbinAuthorizer).invalidateAll();
  }

  /**
   * Role assumption narrows allows to the active subset. The caller holds a granting and a
   * non-granting role; the per-assertion comments below cover each case.
   */
  @Test
  public void testActiveRolesNarrowAllowToNamedRole() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    getLoadedRolesCache(jcasbinAuthorizer).invalidateAll();
    Principal currentPrincipal = setCurrentPrincipalWithGroup(null);

    RoleEntity grantingRole =
        mockRoleInStore(ALLOW_ROLE_ID, "grantingRole", ImmutableList.of(getAllowSecurableObject()));
    RoleEntity nonGrantingRole = mockRoleInStore(30L, "nonGrantingRole", ImmutableList.of());
    mockDirectUserRoles(grantingRole, nonGrantingRole);

    // Default (ALL) evaluates every role -> allowed.
    assertTrue(doAuthorizeWithActiveRoles(currentPrincipal, ActiveRoles.all()));

    // Narrowing to the non-granting role removes the granting role's allow -> denied.
    assertFalse(
        doAuthorizeWithActiveRoles(
            currentPrincipal, ActiveRoles.of(ImmutableList.of("nonGrantingRole"))));

    // Narrowing to the granting role keeps the allow -> allowed.
    assertTrue(
        doAuthorizeWithActiveRoles(
            currentPrincipal, ActiveRoles.of(ImmutableList.of("grantingRole"))));

    restoreDefaultPrincipal();
    getLoadedRolesCache(jcasbinAuthorizer).invalidateAll();
  }

  /** {@code NONE} activates no role, so every role-derived allow is dropped. */
  @Test
  public void testActiveRolesNoneDeniesRoleDerivedAccess() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    getLoadedRolesCache(jcasbinAuthorizer).invalidateAll();
    Principal currentPrincipal = setCurrentPrincipalWithGroup(null);

    RoleEntity grantingRole =
        mockRoleInStore(
            ALLOW_ROLE_ID, "noneGrantingRole", ImmutableList.of(getAllowSecurableObject()));
    mockDirectUserRoles(grantingRole);

    assertTrue(doAuthorizeWithActiveRoles(currentPrincipal, ActiveRoles.all()));
    assertFalse(doAuthorizeWithActiveRoles(currentPrincipal, ActiveRoles.none()));

    restoreDefaultPrincipal();
    getLoadedRolesCache(jcasbinAuthorizer).invalidateAll();
  }

  /**
   * Deny stays global: a deny carried by a role that is <em>not</em> in the active set still blocks
   * access. Here only the granting role is active, but the caller also holds a deny role, so the
   * request is denied.
   */
  @Test
  public void testActiveRolesDenyStaysGlobalWhenAllowNarrowed() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    getLoadedRolesCache(jcasbinAuthorizer).invalidateAll();
    Principal currentPrincipal = setCurrentPrincipalWithGroup(null);

    RoleEntity grantingRole =
        mockRoleInStore(
            ALLOW_ROLE_ID, "denyGlobalAllowRole", ImmutableList.of(getAllowSecurableObject()));
    RoleEntity denyRole =
        mockRoleInStore(
            DENY_ROLE_ID, "denyGlobalDenyRole", ImmutableList.of(getDenySecurableObject()));
    mockDirectUserRoles(grantingRole, denyRole);

    assertFalse(
        doAuthorizeWithActiveRoles(
            currentPrincipal, ActiveRoles.of(ImmutableList.of("denyGlobalAllowRole"))));

    restoreDefaultPrincipal();
    getLoadedRolesCache(jcasbinAuthorizer).invalidateAll();
  }

  /** A group-inherited role can be activated by name exactly like a directly-granted role. */
  @Test
  public void testActiveRolesNarrowingCoversGroupInheritedRole() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    getLoadedRolesCache(jcasbinAuthorizer).invalidateAll();

    UserPrincipal groupPrincipal = setCurrentPrincipalWithGroup(GROUP_NAME);

    Long groupRoleId = 31L;
    RoleEntity groupRole =
        mockRoleInStore(
            groupRoleId, "activeGroupRole", ImmutableList.of(getAllowSecurableObject()));
    mockNoDirectUserRoles();
    mockGroupWithRoles(
        GROUP_NAME, ImmutableList.of(groupRoleId), ImmutableList.of(groupRole.name()));

    assertTrue(
        doAuthorizeWithActiveRoles(
            groupPrincipal, ActiveRoles.of(ImmutableList.of("activeGroupRole"))));

    restoreDefaultPrincipal();
    getLoadedRolesCache(jcasbinAuthorizer).invalidateAll();
  }

  /**
   * Narrowing is subtractive: naming only a role the caller does not hold activates nothing. The
   * {@code 403} rejection for unheld roles happens earlier in the request pipeline, not here.
   */
  @Test
  public void testActiveRolesUnheldRoleActivatesNothing() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    getLoadedRolesCache(jcasbinAuthorizer).invalidateAll();
    Principal currentPrincipal = setCurrentPrincipalWithGroup(null);

    RoleEntity grantingRole =
        mockRoleInStore(
            ALLOW_ROLE_ID, "heldGrantingRole", ImmutableList.of(getAllowSecurableObject()));
    mockDirectUserRoles(grantingRole);

    assertFalse(
        doAuthorizeWithActiveRoles(
            currentPrincipal, ActiveRoles.of(ImmutableList.of("roleTheUserDoesNotHold"))));

    restoreDefaultPrincipal();
    getLoadedRolesCache(jcasbinAuthorizer).invalidateAll();
  }

  /**
   * Sets the current principal mock to a {@link UserPrincipal} with the given group, or with no
   * groups when {@code groupName} is null. Returns the principal for use in assertions.
   */
  private static UserPrincipal setCurrentPrincipalWithGroup(String groupName) {
    UserPrincipal principal =
        groupName == null
            ? new UserPrincipal(USERNAME)
            : new UserPrincipal(
                USERNAME, ImmutableList.of(new UserGroup(Optional.empty(), groupName)));
    principalUtilsMockedStatic.when(PrincipalUtils::getCurrentPrincipal).thenReturn(principal);
    return principal;
  }

  /** Restores the default principal mock used by other tests. */
  private static void restoreDefaultPrincipal() {
    principalUtilsMockedStatic
        .when(PrincipalUtils::getCurrentPrincipal)
        .thenReturn(new UserPrincipal(USERNAME));
  }

  /**
   * Mocks the user as having no directly-assigned roles. Bumps userMetaMapper.getUserUpdatedAt to
   * force the userRoleCache to miss and re-read from the (empty) roleMetaMapper.listRolesByUserId.
   */
  private static void mockNoDirectUserRoles() throws IOException {
    when(roleMetaMapper.listRolesByUserId(eq(USER_ID))).thenReturn(ImmutableList.of());
    when(userMetaMapper.getUserUpdatedAt(eq(METALAKE), eq(USERNAME)))
        .thenReturn(new UserUpdatedAt(USER_ID, nextUserVersion()));
  }

  /**
   * Mocks the user as having the given roles directly assigned via the version-validated cache
   * path. Bumps {@code userMetaMapper.getUserUpdatedAt} to force a userRoleCache miss.
   */
  private static void mockDirectUserRoles(RoleEntity... roles) {
    List<RolePO> rolePOs = new ArrayList<>();
    for (RoleEntity role : roles) {
      rolePOs.add(buildRolePO(role.id(), role.name()));
    }
    when(roleMetaMapper.listRolesByUserId(eq(USER_ID))).thenReturn(rolePOs);
    when(userMetaMapper.getUserUpdatedAt(eq(METALAKE), eq(USERNAME)))
        .thenReturn(new UserUpdatedAt(USER_ID, nextUserVersion()));
  }

  /**
   * Builds a {@link RoleEntity}, registers it in the mocked entity store, and records its version
   * so {@code batchGetRoleUpdatedAt} returns it during version-check.
   */
  private static RoleEntity mockRoleInStore(
      Long roleId, String roleName, List<SecurableObject> securableObjects) throws IOException {
    RoleEntity role = getRoleEntity(roleId, roleName, securableObjects);
    when(entityStore.get(
            eq(NameIdentifierUtil.ofRole(METALAKE, roleName)),
            eq(Entity.EntityType.ROLE),
            eq(RoleEntity.class)))
        .thenReturn(role);
    mockedRoleVersions.put(roleId, new RoleUpdatedAt(roleId, roleName, nextRoleVersion()));
    return role;
  }

  /**
   * Mocks the group as carrying the given roles via the version-validated cache path. The {@code
   * group_meta.updated_at} sentinel is bumped so that the groupRoleCache misses and re-reads from
   * {@code roleMetaMapper.listRolesByGroupId}. The {@code entityStore.batchGet} mock is also wired
   * because {@code isSelf(ROLE)} and {@code ownerMatchesUserOrGroups} still resolve full group
   * entities through the relation store.
   */
  private static GroupEntity mockGroupWithRoles(
      String groupName, List<Long> roleIds, List<String> roleNames) throws IOException {
    return mockGroupWithRoles(GROUP_ID, groupName, roleIds, roleNames);
  }

  private static GroupEntity mockGroupWithRoles(
      Long groupId, String groupName, List<Long> roleIds, List<String> roleNames)
      throws IOException {
    GroupEntity group =
        GroupEntity.builder()
            .withId(groupId)
            .withName(groupName)
            .withNamespace(Namespace.of(METALAKE, "group"))
            .withAuditInfo(AuditInfo.EMPTY)
            .withRoleNames(roleNames)
            .withRoleIds(roleIds)
            .build();
    when(entityStore.batchGet(
            eq(ImmutableList.of(NameIdentifierUtil.ofGroup(METALAKE, groupName))),
            eq(Entity.EntityType.GROUP),
            eq(GroupEntity.class)))
        .thenReturn(ImmutableList.of(group));

    // Version-validated path: group_meta.updated_at sentinel + listRolesByGroupId.
    // Use a monotonic counter so successive calls always advance the version.
    when(groupMetaMapper.getGroupUpdatedAt(eq(METALAKE), eq(groupName)))
        .thenReturn(new GroupUpdatedAt(groupId, groupVersionCounter.incrementAndGet()));
    List<RolePO> rolePOs = new ArrayList<>();
    for (int i = 0; i < roleIds.size(); i++) {
      String name = i < roleNames.size() ? roleNames.get(i) : "role" + roleIds.get(i);
      rolePOs.add(buildRolePO(roleIds.get(i), name));
    }
    when(roleMetaMapper.listRolesByGroupId(eq(groupId))).thenReturn(rolePOs);
    return group;
  }

  private Boolean doAuthorize(Principal currentPrincipal) {
    return jcasbinAuthorizer.authorize(
        currentPrincipal,
        "testMetalake",
        MetadataObjects.of(null, "testCatalog", MetadataObject.Type.CATALOG),
        USE_CATALOG,
        new AuthorizationRequestContext());
  }

  private Boolean doAuthorizeWithActiveRoles(Principal currentPrincipal, ActiveRoles activeRoles) {
    AuthorizationRequestContext requestContext = new AuthorizationRequestContext();
    requestContext.setActiveRoles(activeRoles);
    return jcasbinAuthorizer.authorize(
        currentPrincipal,
        "testMetalake",
        MetadataObjects.of(null, "testCatalog", MetadataObject.Type.CATALOG),
        USE_CATALOG,
        requestContext);
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

  private static GroupEntity getGroupEntity() {
    return GroupEntity.builder()
        .withId(GROUP_ID)
        .withName(GROUP_NAME)
        .withNamespace(Namespace.of(METALAKE, "group"))
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
    GravitinoCache<Long, Optional<OwnerInfo>> ownerRel = getOwnerRelCache(jcasbinAuthorizer);

    // Manually add an owner relation to the cache
    ownerRel.put(CATALOG_ID, Optional.of(new OwnerInfo(USER_ID, "USER")));

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
  public void testOwnerChangeBestEffortWhenMetadataIdLookupFails() throws Exception {
    GravitinoCache<String, Long> metadataIdCache = getMetadataIdCache(jcasbinAuthorizer);
    GravitinoCache<Long, Optional<OwnerInfo>> ownerRelCache = getOwnerRelCache(jcasbinAuthorizer);
    NameIdentifier catalogIdent = NameIdentifierUtil.ofCatalog(METALAKE, "testCatalog");
    String cacheKey =
        JcasbinAuthorizationCacheKeys.metadataIdCacheKey(
            METALAKE, NameIdentifierUtil.toMetadataObject(catalogIdent, Entity.EntityType.CATALOG));

    metadataIdCache.put(cacheKey, CATALOG_ID);
    ownerRelCache.put(CATALOG_ID, Optional.of(new OwnerInfo(USER_ID, "USER")));
    metadataIdConverterMockedStatic
        .when(() -> MetadataIdConverter.getID(any(), eq(METALAKE)))
        .thenThrow(new RuntimeException("lookup failed"));

    try {
      Assertions.assertDoesNotThrow(
          () ->
              jcasbinAuthorizer.handleMetadataOwnerChange(
                  METALAKE, USER_ID, catalogIdent, Entity.EntityType.CATALOG));

      assertFalse(metadataIdCache.getIfPresent(cacheKey).isPresent());
      assertTrue(ownerRelCache.getIfPresent(CATALOG_ID).isPresent());
    } finally {
      metadataIdConverterMockedStatic
          .when(() -> MetadataIdConverter.getID(any(), eq(METALAKE)))
          .thenReturn(Optional.of(CATALOG_ID));
    }
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
    String userIdStr = String.valueOf(USER_ID);

    // Add a policy and a user-role binding for this role.
    allowEnforcer.addRoleForUser(userIdStr, roleIdStr);
    denyEnforcer.addRoleForUser(userIdStr, roleIdStr);
    allowEnforcer.addPolicy(roleIdStr, "CATALOG", "999", "USE_CATALOG", "allow");
    denyEnforcer.addPolicy(roleIdStr, "CATALOG", "999", "USE_CATALOG", "allow");

    // Add role to cache
    loadedRoles.put(testRoleId, System.currentTimeMillis());

    // Verify role exists in enforcer (has policy and grouping).
    assertTrue(allowEnforcer.hasPolicy(roleIdStr, "CATALOG", "999", "USE_CATALOG", "allow"));
    assertTrue(denyEnforcer.hasPolicy(roleIdStr, "CATALOG", "999", "USE_CATALOG", "allow"));
    assertTrue(allowEnforcer.getRolesForUser(userIdStr).contains(roleIdStr));
    assertTrue(denyEnforcer.getRolesForUser(userIdStr).contains(roleIdStr));

    // Invalidate the cache entry - this triggers the synchronous removal listener
    // (using executor(Runnable::run) to ensure synchronous execution)
    loadedRoles.invalidate(testRoleId);

    // Verify the role's policies have been deleted from enforcers (synchronous, no need to wait),
    // but user-role bindings are preserved because loadedRoles owns role policies only.
    assertFalse(allowEnforcer.hasPolicy(roleIdStr, "CATALOG", "999", "USE_CATALOG", "allow"));
    assertFalse(denyEnforcer.hasPolicy(roleIdStr, "CATALOG", "999", "USE_CATALOG", "allow"));
    assertTrue(allowEnforcer.getRolesForUser(userIdStr).contains(roleIdStr));
    assertTrue(denyEnforcer.getRolesForUser(userIdStr).contains(roleIdStr));
  }

  @Test
  public void testRoleCacheReplacementDoesNotDeletePolicy() throws Exception {
    Enforcer allowEnforcer = getAllowEnforcer(jcasbinAuthorizer);
    Enforcer denyEnforcer = getDenyEnforcer(jcasbinAuthorizer);
    GravitinoCache<Long, Long> loadedRoles = getLoadedRolesCache(jcasbinAuthorizer);

    Long testRoleId = 301L;
    String roleIdStr = String.valueOf(testRoleId);
    loadedRoles.put(testRoleId, 1L);

    allowEnforcer.addPolicy(roleIdStr, "CATALOG", "999", "USE_CATALOG", "allow");
    denyEnforcer.addPolicy(roleIdStr, "CATALOG", "999", "USE_CATALOG", "allow");

    loadedRoles.put(testRoleId, 2L);

    assertTrue(allowEnforcer.hasPolicy(roleIdStr, "CATALOG", "999", "USE_CATALOG", "allow"));
    assertTrue(denyEnforcer.hasPolicy(roleIdStr, "CATALOG", "999", "USE_CATALOG", "allow"));
  }

  @Test
  public void testClearRolePoliciesPreservesUserRoleBindings() throws Exception {
    Enforcer allowEnforcer = getAllowEnforcer(jcasbinAuthorizer);
    Enforcer denyEnforcer = getDenyEnforcer(jcasbinAuthorizer);

    Long testRoleId = 302L;
    String roleIdStr = String.valueOf(testRoleId);
    String userIdStr = String.valueOf(USER_ID);
    allowEnforcer.addRoleForUser(userIdStr, roleIdStr);
    denyEnforcer.addRoleForUser(userIdStr, roleIdStr);
    allowEnforcer.addPolicy(roleIdStr, "CATALOG", "999", "USE_CATALOG", "allow");
    denyEnforcer.addPolicy(roleIdStr, "CATALOG", "999", "USE_CATALOG", "allow");

    Method clearRolePolicies =
        JcasbinAuthorizer.class.getDeclaredMethod("clearRolePolicies", long.class);
    clearRolePolicies.setAccessible(true);
    clearRolePolicies.invoke(jcasbinAuthorizer, testRoleId);

    assertFalse(allowEnforcer.hasPolicy(roleIdStr, "CATALOG", "999", "USE_CATALOG", "allow"));
    assertFalse(denyEnforcer.hasPolicy(roleIdStr, "CATALOG", "999", "USE_CATALOG", "allow"));
    assertTrue(allowEnforcer.getRolesForUser(userIdStr).contains(roleIdStr));
    assertTrue(denyEnforcer.getRolesForUser(userIdStr).contains(roleIdStr));
  }

  @Test
  public void testCacheInitialization() throws Exception {
    // Verify that caches are initialized
    GravitinoCache<Long, Long> loadedRolesCache = getLoadedRolesCache(jcasbinAuthorizer);
    GravitinoCache<Long, Optional<OwnerInfo>> ownerRelCache = getOwnerRelCache(jcasbinAuthorizer);

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
                    metalakeGrantRoleId, MetadataObject.Type.METALAKE, METALAKE),
                buildSecurableObject(
                    metalakeGrantRoleId,
                    MetadataObject.Type.SCHEMA,
                    "testCatalog.testSchema",
                    USE_SCHEMA,
                    "ALLOW")));
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
                    catalogGrantRoleId, MetadataObject.Type.CATALOG, "testCatalog"),
                buildSecurableObject(
                    catalogGrantRoleId,
                    MetadataObject.Type.CATALOG,
                    "testCatalog",
                    USE_CATALOG,
                    "ALLOW"),
                buildSecurableObject(
                    catalogGrantRoleId,
                    MetadataObject.Type.SCHEMA,
                    "testCatalog.testSchema",
                    USE_SCHEMA,
                    "ALLOW")));
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
                    "testCatalog.testSchema.testTable"),
                buildSecurableObject(
                    tableGrantRoleId,
                    MetadataObject.Type.SCHEMA,
                    "testCatalog.testSchema",
                    USE_SCHEMA,
                    "ALLOW")));
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

  @Test
  public void testHasMetadataPrivilegePermissionRejectsDenyManageGrants() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);

    Long allowRoleId = 203L;
    RoleEntity allowRole =
        mockRoleInStore(
            allowRoleId,
            "allowManageGrantsRole",
            ImmutableList.of(
                buildSecurableObject(
                    allowRoleId,
                    MetadataObject.Type.TABLE,
                    "testCatalog.testSchema.testTable",
                    Privilege.Name.MANAGE_GRANTS,
                    "ALLOW"),
                buildSecurableObject(
                    allowRoleId,
                    MetadataObject.Type.SCHEMA,
                    "testCatalog.testSchema",
                    USE_SCHEMA,
                    "ALLOW")));
    Long denyRoleId = 204L;
    RoleEntity denyRole =
        mockRoleInStore(
            denyRoleId,
            "denyManageGrantsRole",
            ImmutableList.of(
                buildSecurableObject(
                    denyRoleId,
                    MetadataObject.Type.METALAKE,
                    METALAKE,
                    Privilege.Name.MANAGE_GRANTS,
                    "DENY")));
    mockDirectUserRoles(allowRole, denyRole);

    assertFalse(
        jcasbinAuthorizer.hasMetadataPrivilegePermission(
            METALAKE,
            "TABLE",
            "testCatalog.testSchema.testTable",
            new AuthorizationRequestContext()),
        "DENY MANAGE_GRANTS should override a narrower ALLOW MANAGE_GRANTS");
  }

  @Test
  public void testHasMetadataPrivilegePermissionRejectsMissingParentUsage() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);

    Long allowRoleId = 209L;
    RoleEntity allowRole =
        mockRoleInStore(
            allowRoleId,
            "allowManageGrantsWithoutUseRole",
            ImmutableList.of(
                buildSecurableObject(
                    allowRoleId,
                    MetadataObject.Type.TABLE,
                    "testCatalog.testSchema.testTable",
                    Privilege.Name.MANAGE_GRANTS,
                    "ALLOW")));
    mockDirectUserRoles(allowRole);

    assertFalse(
        jcasbinAuthorizer.hasMetadataPrivilegePermission(
            METALAKE,
            "TABLE",
            "testCatalog.testSchema.testTable",
            new AuthorizationRequestContext()),
        "MANAGE_GRANTS on a table should also require parent USE_SCHEMA");
  }

  @Test
  public void testHasMetadataPrivilegePermissionRejectsDenyParentUsageForFunction()
      throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);

    Long allowRoleId = 210L;
    RoleEntity allowRole =
        mockRoleInStore(
            allowRoleId,
            "allowFunctionManageGrantsRole",
            ImmutableList.of(
                buildSecurableObject(
                    allowRoleId,
                    MetadataObject.Type.FUNCTION,
                    "testCatalog.testSchema.testFunction",
                    Privilege.Name.MANAGE_GRANTS,
                    "ALLOW"),
                buildSecurableObject(
                    allowRoleId,
                    MetadataObject.Type.SCHEMA,
                    "testCatalog.testSchema",
                    USE_SCHEMA,
                    "ALLOW")));
    Long denyRoleId = 211L;
    RoleEntity denyRole =
        mockRoleInStore(
            denyRoleId,
            "denyUseSchemaForFunctionRole",
            ImmutableList.of(
                buildSecurableObject(
                    denyRoleId, MetadataObject.Type.METALAKE, METALAKE, USE_SCHEMA, "DENY")));
    mockDirectUserRoles(allowRole, denyRole);

    assertFalse(
        jcasbinAuthorizer.hasMetadataPrivilegePermission(
            METALAKE,
            "FUNCTION",
            "testCatalog.testSchema.testFunction",
            new AuthorizationRequestContext()),
        "DENY USE_SCHEMA should override FUNCTION-level MANAGE_GRANTS");
  }

  @Test
  public void testHasMetadataPrivilegePermissionAllowsOwnerWithDenyManageGrants() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);

    Long denyRoleId = 205L;
    RoleEntity denyRole =
        mockRoleInStore(
            denyRoleId,
            "denyManageGrantsForOwnerRole",
            ImmutableList.of(
                buildSecurableObject(
                    denyRoleId,
                    MetadataObject.Type.METALAKE,
                    METALAKE,
                    Privilege.Name.MANAGE_GRANTS,
                    "DENY")));
    mockDirectUserRoles(denyRole);
    GravitinoCache<Long, Optional<OwnerInfo>> ownerRelCache = getOwnerRelCache(jcasbinAuthorizer);
    ownerRelCache.invalidateAll();
    ownerRelCache.put(CATALOG_ID, Optional.of(new OwnerInfo(USER_ID, "USER")));

    assertTrue(
        jcasbinAuthorizer.hasMetadataPrivilegePermission(
            METALAKE, "CATALOG", "testCatalog", new AuthorizationRequestContext()),
        "Owner should be able to manage privileges without checking DENY MANAGE_GRANTS");
  }

  @Test
  public void testHasSetOwnerPermissionRejectsDenyUseCatalogForTableOwner() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);

    Long allowRoleId = 206L;
    RoleEntity allowRole =
        mockRoleInStore(
            allowRoleId,
            "allowUseSchemaRole",
            ImmutableList.of(
                buildSecurableObject(
                    allowRoleId,
                    MetadataObject.Type.SCHEMA,
                    "testCatalog.testSchema",
                    USE_SCHEMA,
                    "ALLOW")));
    Long denyRoleId = 207L;
    RoleEntity denyRole =
        mockRoleInStore(
            denyRoleId,
            "denyUseCatalogRole",
            ImmutableList.of(
                buildSecurableObject(
                    denyRoleId, MetadataObject.Type.METALAKE, METALAKE, USE_CATALOG, "DENY")));
    mockDirectUserRoles(allowRole, denyRole);
    when(ownerMetaMapper.selectOwnerByMetadataObjectIdAndType(eq(CATALOG_ID), eq("TABLE")))
        .thenReturn(new OwnerInfo(USER_ID, "USER"));
    getOwnerRelCache(jcasbinAuthorizer).invalidateAll();

    assertFalse(
        jcasbinAuthorizer.hasSetOwnerPermission(
            METALAKE,
            "TABLE",
            "testCatalog.testSchema.testTable",
            new AuthorizationRequestContext()),
        "DENY USE_CATALOG should override table ownership when setting owner");
  }

  @Test
  public void testHasSetOwnerPermissionRejectsDenyUseCatalogForFunctionOwner() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);

    Long allowRoleId = 212L;
    RoleEntity allowRole =
        mockRoleInStore(
            allowRoleId,
            "allowUseSchemaForFunctionOwnerRole",
            ImmutableList.of(
                buildSecurableObject(
                    allowRoleId,
                    MetadataObject.Type.SCHEMA,
                    "testCatalog.testSchema",
                    USE_SCHEMA,
                    "ALLOW")));
    Long denyRoleId = 213L;
    RoleEntity denyRole =
        mockRoleInStore(
            denyRoleId,
            "denyUseCatalogForFunctionOwnerRole",
            ImmutableList.of(
                buildSecurableObject(
                    denyRoleId, MetadataObject.Type.METALAKE, METALAKE, USE_CATALOG, "DENY")));
    mockDirectUserRoles(allowRole, denyRole);
    when(ownerMetaMapper.selectOwnerByMetadataObjectIdAndType(eq(CATALOG_ID), eq("FUNCTION")))
        .thenReturn(new OwnerInfo(USER_ID, "USER"));
    getOwnerRelCache(jcasbinAuthorizer).invalidateAll();

    assertFalse(
        jcasbinAuthorizer.hasSetOwnerPermission(
            METALAKE,
            "FUNCTION",
            "testCatalog.testSchema.testFunction",
            new AuthorizationRequestContext()),
        "DENY USE_CATALOG should override function ownership when setting owner");
  }

  @Test
  public void testHasSetOwnerPermissionAllowsCatalogOwnerWithDenyUseCatalog() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);

    Long denyRoleId = 208L;
    RoleEntity denyRole =
        mockRoleInStore(
            denyRoleId,
            "denyUseCatalogForCatalogOwnerRole",
            ImmutableList.of(
                buildSecurableObject(
                    denyRoleId, MetadataObject.Type.METALAKE, METALAKE, USE_CATALOG, "DENY")));
    mockDirectUserRoles(denyRole);
    GravitinoCache<Long, Optional<OwnerInfo>> ownerRelCache = getOwnerRelCache(jcasbinAuthorizer);
    ownerRelCache.invalidateAll();
    ownerRelCache.put(CATALOG_ID, Optional.of(new OwnerInfo(USER_ID, "USER")));

    assertTrue(
        jcasbinAuthorizer.hasSetOwnerPermission(
            METALAKE, "CATALOG", "testCatalog", new AuthorizationRequestContext()),
        "Catalog owner should be able to set owner without checking DENY USE_CATALOG");
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

  private static SecurableObject buildSecurableObject(
      Long roleId,
      MetadataObject.Type type,
      String objectName,
      Privilege.Name privilege,
      String condition) {
    try {
      SecurableObjectPO po =
          SecurableObjectPO.builder()
              .withType(String.valueOf(type))
              .withMetadataObjectId(CATALOG_ID)
              .withRoleId(roleId)
              .withPrivilegeNames(objectMapper.writeValueAsString(ImmutableList.of(privilege)))
              .withPrivilegeConditions(objectMapper.writeValueAsString(ImmutableList.of(condition)))
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
  private static GravitinoCache<Long, Optional<OwnerInfo>> getOwnerRelCache(
      JcasbinAuthorizer authorizer) throws Exception {
    Field field = JcasbinAuthorizer.class.getDeclaredField("ownerRelCache");
    field.setAccessible(true);
    return (GravitinoCache<Long, Optional<OwnerInfo>>) field.get(authorizer);
  }

  /** Mock mapper to assign zero roles. Bumps user version to invalidate cache. */
  private static void mockUserRoles() {
    when(roleMetaMapper.listRolesByUserId(eq(USER_ID))).thenReturn(ImmutableList.of());
    when(roleMetaMapper.batchGetRoleUpdatedAt(any())).thenReturn(ImmutableList.of());
    when(userMetaMapper.getUserUpdatedAt(eq(METALAKE), eq(USERNAME)))
        .thenReturn(new UserUpdatedAt(USER_ID, nextUserVersion()));
  }

  /** Mock mapper to assign a single role. Bumps user version to invalidate cache. */
  private static void mockUserRoles(Long roleId, String roleName) {
    long roleVersion = nextRoleVersion();
    when(roleMetaMapper.listRolesByUserId(eq(USER_ID)))
        .thenReturn(ImmutableList.of(buildRolePO(roleId, roleName)));
    when(roleMetaMapper.batchGetRoleUpdatedAt(any()))
        .thenReturn(ImmutableList.of(new RoleUpdatedAt(roleId, roleName, roleVersion)));
    // Also register the role in mockedRoleVersions so the fat-JOIN test stub for
    // batchGetAuthSubjectsForUser surfaces it; otherwise prefetch's role-version map would
    // miss this role and downstream loadPolicyByRoleEntity would never run.
    mockedRoleVersions.put(roleId, new RoleUpdatedAt(roleId, roleName, roleVersion));
    when(userMetaMapper.getUserUpdatedAt(eq(METALAKE), eq(USERNAME)))
        .thenReturn(new UserUpdatedAt(USER_ID, nextUserVersion()));
  }

  private static long nextRoleVersion() {
    return roleVersionCounter.incrementAndGet();
  }

  private static long nextUserVersion() {
    return userVersionCounter.incrementAndGet();
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

  @SuppressWarnings("unchecked")
  private static GravitinoCache<String, Long> getMetadataIdCache(JcasbinAuthorizer authorizer)
      throws Exception {
    Field field = JcasbinAuthorizer.class.getDeclaredField("metadataIdCache");
    field.setAccessible(true);
    return (GravitinoCache<String, Long>) field.get(authorizer);
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
