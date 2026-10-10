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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
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
import org.apache.gravitino.authorization.AccessControlDispatcher;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.cache.GravitinoCache;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.catalog.SemanticModelDispatcher;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.hook.SemanticModelHookDispatcher;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.semantic.SemanticModel;
import org.apache.gravitino.semantic.SemanticModelChange;
import org.apache.gravitino.server.ServerConfig;
import org.apache.gravitino.server.authorization.AuthorizationRequestScope;
import org.apache.gravitino.server.authorization.GravitinoAuthorizerProvider;
import org.apache.gravitino.server.authorization.MetadataAuthzHelper;
import org.apache.gravitino.server.authorization.MetadataIdConverter;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;
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
import org.apache.gravitino.utils.ThrowableFunction;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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
    metadataIdConverterMockedStatic =
        mockStatic(
            MetadataIdConverter.class,
            invocation -> {
              // Keep ID-loading fixtures, but execute all normalization helpers (including nested
              // static calls) so capability rules are tested rather than stubbed away.
              if (invocation.getMethod().getName().equals("getIdForNormalizedObject")) {
                return MetadataIdConverter.getID(
                    invocation.getArgument(0), invocation.getArgument(1));
              }
              if (invocation.getMethod().getName().equals("getID")) {
                return Optional.empty();
              }
              return invocation.callRealMethod();
            });
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
    doAnswer(
            invocation -> {
              List<Long> ids = invocation.getArgument(0);
              return ids.stream()
                  .map(mockedRoleVersions::get)
                  .filter(Objects::nonNull)
                  .collect(Collectors.toList());
            })
        .when(roleMetaMapper)
        .batchGetRoleUpdatedAt(any());
    when(entityStore.batchGet(anyList(), eq(Entity.EntityType.ROLE), eq(RoleEntity.class)))
        .thenReturn(Collections.emptyList());
    CatalogManager catalogs = mock(CatalogManager.class);
    BaseCatalog<?> catalog = mock(BaseCatalog.class);
    when(catalog.capability()).thenReturn(Capability.DEFAULT);
    doAnswer(
            invocation -> {
              ThrowableFunction<BaseCatalog<?>, Object> operation = invocation.getArgument(1);
              return operation.apply(catalog);
            })
        .when(catalogs)
        .doWithCatalog(any(), any());
    when(gravitinoEnv.catalogManager()).thenReturn(catalogs);
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
    // userMetaMapper is shared by every test, so only count the calls made by this one.
    Mockito.clearInvocations(userMetaMapper);
    assertTrue(jcasbinAuthorizer.isMetalakeUser(METALAKE, new AuthorizationRequestContext()));
    verify(userMetaMapper).getUserUpdatedAt(METALAKE, USERNAME);
  }

  @Test
  public void testIsServiceAdminUsesInternalDispatcher() {
    AccessControlDispatcher dispatcher = mock(AccessControlDispatcher.class);
    when(gravitinoEnv.internalAccessControlDispatcher()).thenReturn(dispatcher);
    when(dispatcher.isServiceAdmin(USERNAME)).thenReturn(true);

    assertTrue(jcasbinAuthorizer.isServiceAdmin());

    verify(dispatcher).isServiceAdmin(USERNAME);
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
    // code path bypassing the fat-JOIN), the fix must still evict that role's
    // loadedRoles index entry so the request-local role ID cannot resolve any privilege.
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();

    // 1. Authorize once via the normal flow to populate the loadedRoles index for allowRole.
    RoleEntity allowRole =
        mockRoleInStore(ALLOW_ROLE_ID, "allowRole", ImmutableList.of(getAllowSecurableObject()));
    long userVersion = nextUserVersion();
    when(userMetaMapper.getUserUpdatedAt(eq(METALAKE), eq(USERNAME)))
        .thenReturn(new UserUpdatedAt(USER_ID, userVersion));
    when(roleMetaMapper.listRolesByUserId(eq(USER_ID)))
        .thenReturn(ImmutableList.of(buildRolePO(ALLOW_ROLE_ID, allowRole.name())));
    assertTrue(doAuthorize(currentPrincipal));

    // Sanity: loadedRoles now has an index entry for ALLOW_ROLE_ID.
    Assertions.assertTrue(
        getLoadedRolesCache(jcasbinAuthorizer).getIfPresent(ALLOW_ROLE_ID).isPresent(),
        "loadedRoles must be primed before the test");

    // 2. Simulate the bug-trigger: batchGetRoleUpdatedAt returns NO row for the role
    //    (i.e. the role row is gone from role_meta), even though something is still
    //    asking us to version-check it.
    when(roleMetaMapper.batchGetRoleUpdatedAt(any())).thenReturn(ImmutableList.of());

    // Invoke versionCheckAndLoadRoles directly with the "deleted" role id and a
    // fresh AuthorizationRequestContext that has NO prefetched role versions, so
    // the method falls through to the batch probe and observes the empty result.
    AuthorizationRequestContext freshCtx = new AuthorizationRequestContext();
    invokeLoadRequestPolicies(
        jcasbinAuthorizer, METALAKE, ImmutableList.of(ALLOW_ROLE_ID), freshCtx);

    // 3. The fix must have evicted the deleted role's index entry.
    Assertions.assertFalse(
        getLoadedRolesCache(jcasbinAuthorizer).getIfPresent(ALLOW_ROLE_ID).isPresent(),
        "loadedRoles entry for the deleted role must be evicted");
  }

  /** Verifies that a connector failure does not suppress healthy catalog grants. */
  @Test
  public void testCapabilityFailureDoesNotAbortHealthyCatalogPolicyLoading() throws Exception {
    CatalogManager catalogs = gravitinoEnv.catalogManager();
    Mockito.doThrow(new IllegalStateException("Connector initialization failed"))
        .when(catalogs)
        .doWithCatalog(eq(NameIdentifier.of(METALAKE, "broken")), any());
    RoleEntity role =
        mockRoleInStore(
            ALLOW_ROLE_ID,
            "mixedCatalogRole",
            ImmutableList.of(
                buildSecurableObject(
                    ALLOW_ROLE_ID,
                    MetadataObject.Type.TABLE,
                    "broken.schema.table",
                    Privilege.Name.SELECT_TABLE,
                    "ALLOW"),
                buildSecurableObject(
                    ALLOW_ROLE_ID,
                    MetadataObject.Type.TABLE,
                    "healthy.schema.table",
                    Privilege.Name.SELECT_TABLE,
                    "ALLOW")));
    mockDirectUserRoles(role);
    Principal principal = PrincipalUtils.getCurrentPrincipal();
    assertTrue(
        jcasbinAuthorizer.authorize(
            principal,
            METALAKE,
            MetadataObjects.parse("healthy.schema.table", MetadataObject.Type.TABLE),
            Privilege.Name.SELECT_TABLE,
            new AuthorizationRequestContext()));
    assertFalse(
        jcasbinAuthorizer.authorize(
            principal,
            METALAKE,
            MetadataObjects.parse("broken.schema.table", MetadataObject.Type.TABLE),
            Privilege.Name.SELECT_TABLE,
            new AuthorizationRequestContext()));
    assertFalse(
        getLoadedRolesCache(jcasbinAuthorizer).getIfPresent(ALLOW_ROLE_ID).get().isComplete());
    assertTrue(
        getPartialRoleLoadBackoffCache(jcasbinAuthorizer).getIfPresent(ALLOW_ROLE_ID).isPresent());
  }

  /** Verifies conservative DENY scope, backoff, role narrowing and recovery. */

  /** Verifies that normalization failure cannot bypass a cached DENY policy. */
  @Test
  public void testNormalizationFailureCannotBypassAlreadyLoadedDeny() throws Exception {
    MetadataObject denied =
        MetadataObjects.parse("broken.schema.denied", MetadataObject.Type.TABLE);
    MetadataObject sibling =
        MetadataObjects.parse("broken.schema.sibling", MetadataObject.Type.TABLE);
    MetadataObject healthy = MetadataObjects.of(null, "healthy", MetadataObject.Type.CATALOG);
    metadataIdConverterMockedStatic
        .when(() -> MetadataIdConverter.getID(denied, METALAKE))
        .thenReturn(Optional.of(10L));
    metadataIdConverterMockedStatic
        .when(() -> MetadataIdConverter.getID(sibling, METALAKE))
        .thenReturn(Optional.of(11L));
    metadataIdConverterMockedStatic
        .when(() -> MetadataIdConverter.getID(healthy, METALAKE))
        .thenReturn(Optional.of(20L));
    RoleEntity role =
        mockRoleInStore(
            ALLOW_ROLE_ID,
            "completeRole",
            ImmutableList.of(
                buildSecurableObject(
                    ALLOW_ROLE_ID,
                    MetadataObject.Type.METALAKE,
                    METALAKE,
                    Privilege.Name.SELECT_TABLE,
                    "ALLOW"),
                buildSecurableObject(
                    ALLOW_ROLE_ID,
                    MetadataObject.Type.TABLE,
                    denied.fullName(),
                    Privilege.Name.SELECT_TABLE,
                    "DENY")));
    mockDirectUserRoles(role);
    AuthorizationExpressionEvaluator evaluator =
        new AuthorizationExpressionEvaluator("ANY_SELECT_TABLE", jcasbinAuthorizer);
    try {
      assertFalse(
          evaluator.evaluate(
              tableMetadataNames("broken", "denied"), new AuthorizationRequestContext()));
      assertTrue(
          evaluator.evaluate(
              tableMetadataNames("broken", "sibling"), new AuthorizationRequestContext()));
      assertTrue(getLoadedRolesCache(jcasbinAuthorizer).getIfPresent(ALLOW_ROLE_ID).isPresent());
      CatalogManager catalogs = gravitinoEnv.catalogManager();
      Mockito.doThrow(new IllegalStateException("Connector initialization failed"))
          .when(catalogs)
          .doWithCatalog(eq(NameIdentifier.of(METALAKE, "broken")), any());
      assertFalse(
          evaluator.evaluate(
              tableMetadataNames("broken", "denied"), new AuthorizationRequestContext()));
      assertFalse(
          evaluator.evaluate(
              tableMetadataNames("broken", "sibling"), new AuthorizationRequestContext()));
      assertTrue(
          evaluator.evaluate(
              tableMetadataNames("healthy", "table"), new AuthorizationRequestContext()));
      assertTrue(getLoadedRolesCache(jcasbinAuthorizer).getIfPresent(ALLOW_ROLE_ID).isPresent());
    } finally {
      metadataIdConverterMockedStatic
          .when(() -> MetadataIdConverter.getID(denied, METALAKE))
          .thenReturn(Optional.of(CATALOG_ID));
      metadataIdConverterMockedStatic
          .when(() -> MetadataIdConverter.getID(sibling, METALAKE))
          .thenReturn(Optional.of(CATALOG_ID));
      metadataIdConverterMockedStatic
          .when(() -> MetadataIdConverter.getID(healthy, METALAKE))
          .thenReturn(Optional.of(CATALOG_ID));
    }
  }

  /** Verifies that a missing entity does not block inherited creation privileges. */
  @Test
  public void testMissingEntityStillAllowsInheritedCreatePrivilege() throws Exception {
    MetadataObject missing =
        MetadataObjects.parse("catalog.schema.newTable", MetadataObject.Type.TABLE);
    RoleEntity role =
        mockRoleInStore(
            ALLOW_ROLE_ID,
            "createRole",
            ImmutableList.of(
                buildSecurableObject(
                    ALLOW_ROLE_ID,
                    MetadataObject.Type.METALAKE,
                    METALAKE,
                    Privilege.Name.CREATE_TABLE,
                    "ALLOW")));
    mockDirectUserRoles(role);
    metadataIdConverterMockedStatic
        .when(() -> MetadataIdConverter.getID(missing, METALAKE))
        .thenReturn(Optional.empty());
    try {
      assertTrue(
          new AuthorizationExpressionEvaluator("ANY_CREATE_TABLE", jcasbinAuthorizer)
              .evaluate(
                  tableMetadataNames("catalog", "newTable"), new AuthorizationRequestContext()));
    } finally {
      metadataIdConverterMockedStatic
          .when(() -> MetadataIdConverter.getID(missing, METALAKE))
          .thenReturn(Optional.of(CATALOG_ID));
    }
  }

  /** Verifies that a failed DENY guard installation cannot publish a partial role. */
  @Test
  public void testRolePrivilegeChangeMidRequestDoesNotDenyLaterCheck() throws Exception {
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    RoleEntity allowRole =
        mockRoleInStore(ALLOW_ROLE_ID, "allowRole", ImmutableList.of(getAllowSecurableObject()));
    mockDirectUserRoles(allowRole);
    AuthorizationRequestContext requestContext = new AuthorizationRequestContext();

    assertFalse(
        jcasbinAuthorizer.authorize(
            currentPrincipal, METALAKE, metalakeObject(), USE_CATALOG, requestContext));

    // A grant or revoke on the role elsewhere on this node clears its policies immediately.
    jcasbinAuthorizer.handleRolePrivilegeChange(ALLOW_ROLE_ID);

    assertTrue(
        jcasbinAuthorizer.authorize(
            currentPrincipal, METALAKE, catalogObject(), USE_CATALOG, requestContext));
  }

  @Test
  public void testDenyPoliciesEvictedMidRequestStillDeny() throws Exception {
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    RoleEntity allowRole =
        mockRoleInStore(ALLOW_ROLE_ID, "allowRole", ImmutableList.of(getAllowSecurableObject()));
    RoleEntity denyRole =
        mockRoleInStore(DENY_ROLE_ID, "denyRole", ImmutableList.of(getDenySecurableObject()));
    mockDirectUserRoles(allowRole, denyRole);
    AuthorizationRequestContext requestContext = new AuthorizationRequestContext();

    assertFalse(
        jcasbinAuthorizer.authorize(
            currentPrincipal, METALAKE, metalakeObject(), USE_CATALOG, requestContext));

    // Evicting the deny role between two checks must not let the allow role win: that would turn a
    // cache eviction into privilege escalation.
    getLoadedRolesCache(jcasbinAuthorizer).invalidate(DENY_ROLE_ID);

    assertTrue(
        jcasbinAuthorizer.deny(
            currentPrincipal, METALAKE, catalogObject(), USE_CATALOG, requestContext),
        "the deny check must still see the evicted deny role");
    assertFalse(
        jcasbinAuthorizer.authorize(
            currentPrincipal, METALAKE, catalogObject(), USE_CATALOG, requestContext),
        "the allow check must still honor the evicted deny role");
  }

  @Test
  public void testReloadReadFailureMustNotLoseDeny() throws Exception {
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    String roleName = "unavailableDenyRole";
    RoleEntity denyRole =
        mockRoleInStore(DENY_ROLE_ID, roleName, ImmutableList.of(getDenySecurableObject()));
    mockDirectUserRoles(denyRole);
    AuthorizationRequestContext requestContext = new AuthorizationRequestContext();

    assertFalse(
        jcasbinAuthorizer.authorize(
            currentPrincipal, METALAKE, metalakeObject(), USE_CATALOG, requestContext));
    jcasbinAuthorizer.handleRolePrivilegeChange(DENY_ROLE_ID);
    NameIdentifier roleIdent = NameIdentifierUtil.ofRole(METALAKE, roleName);
    when(entityStore.get(eq(roleIdent), eq(Entity.EntityType.ROLE), eq(RoleEntity.class)))
        .thenThrow(new IllegalStateException("Role store temporarily unavailable"));

    assertTrue(
        jcasbinAuthorizer.deny(
            currentPrincipal, METALAKE, catalogObject(), USE_CATALOG, requestContext),
        "an unsuccessful reload must fail closed instead of treating missing p-rows as no deny");
  }

  @Test
  public void testInitialRoleReadFailureMustNotLoseDeny() throws Exception {
    Principal principal = PrincipalUtils.getCurrentPrincipal();
    String roleName = "unavailableInitialDenyRole";
    RoleEntity allowRole =
        mockRoleInStore(ALLOW_ROLE_ID, "allowRole", ImmutableList.of(getAllowSecurableObject()));
    RoleEntity denyRole =
        mockRoleInStore(DENY_ROLE_ID, roleName, ImmutableList.of(getDenySecurableObject()));
    mockDirectUserRoles(allowRole, denyRole);
    when(entityStore.get(
            eq(NameIdentifierUtil.ofRole(METALAKE, roleName)),
            eq(Entity.EntityType.ROLE),
            eq(RoleEntity.class)))
        .thenThrow(new IllegalStateException("Role store temporarily unavailable"));

    // Each entry point must preserve the failure from its own initial load, including when
    // another readable role grants the privilege. Subsequent checks of that request stay closed.
    AuthorizationRequestContext allowContext = new AuthorizationRequestContext();
    assertFalse(
        jcasbinAuthorizer.authorize(
            principal, METALAKE, catalogObject(), USE_CATALOG, allowContext));
    assertTrue(
        jcasbinAuthorizer.deny(principal, METALAKE, catalogObject(), USE_CATALOG, allowContext));
    assertTrue(
        jcasbinAuthorizer.hasDenyPolicy(
            principal, METALAKE, ImmutableSet.of(USE_CATALOG), allowContext));
    assertTrue(
        jcasbinAuthorizer.deny(
            principal, METALAKE, catalogObject(), USE_CATALOG, new AuthorizationRequestContext()));
    assertTrue(
        jcasbinAuthorizer.hasDenyPolicy(
            principal, METALAKE, ImmutableSet.of(USE_CATALOG), new AuthorizationRequestContext()));

    Mockito.doReturn(denyRole)
        .when(entityStore)
        .get(
            eq(NameIdentifierUtil.ofRole(METALAKE, roleName)),
            eq(Entity.EntityType.ROLE),
            eq(RoleEntity.class));
    AuthorizationRequestContext recoveredContext = new AuthorizationRequestContext();
    assertFalse(
        jcasbinAuthorizer.hasDenyPolicy(
            principal, METALAKE, ImmutableSet.of(USE_SCHEMA), recoveredContext));
    assertTrue(recoveredContext.getUnreadableRoleIds().isEmpty());
    assertTrue(
        jcasbinAuthorizer.hasDenyPolicy(
            principal, METALAKE, ImmutableSet.of(USE_SCHEMA), allowContext),
        "the failed request stays closed even after another request successfully loads the role");
  }

  @Test
  public void testReloadExceptionFailsClosed() throws Exception {
    Principal principal = PrincipalUtils.getCurrentPrincipal();
    RoleEntity role =
        mockRoleInStore(ALLOW_ROLE_ID, "allowRole", ImmutableList.of(getAllowSecurableObject()));
    mockDirectUserRoles(role);
    AuthorizationRequestContext context = new AuthorizationRequestContext();
    assertFalse(
        jcasbinAuthorizer.authorize(principal, METALAKE, metalakeObject(), USE_CATALOG, context));
    jcasbinAuthorizer.handleRolePrivilegeChange(ALLOW_ROLE_ID);
    // Force the reload to probe versions rather than use the request's fat-JOIN snapshot.
    context.setPrefetchedRoleVersions(Collections.emptyMap());
    when(roleMetaMapper.batchGetRoleUpdatedAt(any()))
        .thenThrow(new IllegalStateException("Role version probe unavailable"));

    assertFalse(
        jcasbinAuthorizer.authorize(principal, METALAKE, catalogObject(), USE_CATALOG, context));
    assertTrue(jcasbinAuthorizer.deny(principal, METALAKE, catalogObject(), USE_CATALOG, context));
    assertTrue(
        jcasbinAuthorizer.hasDenyPolicy(
            principal, METALAKE, ImmutableSet.of(USE_CATALOG), context));
  }

  @Test
  public void testNewDenyOnInitiallyEmptyRoleIsSeenMidRequest() throws Exception {
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    Long roleId = 32L;
    String roleName = "newDenyRole";
    RoleEntity emptyRole = mockRoleInStore(roleId, roleName, ImmutableList.of());
    mockDirectUserRoles(emptyRole);
    AuthorizationRequestContext requestContext = new AuthorizationRequestContext();

    assertFalse(
        jcasbinAuthorizer.authorize(
            currentPrincipal, METALAKE, metalakeObject(), USE_CATALOG, requestContext));

    mockRoleInStore(
        roleId,
        roleName,
        ImmutableList.of(
            buildSecurableObject(
                roleId, MetadataObject.Type.CATALOG, "testCatalog", USE_CATALOG, "DENY")));
    jcasbinAuthorizer.handleRolePrivilegeChange(roleId);

    assertTrue(
        jcasbinAuthorizer.deny(
            currentPrincipal, METALAKE, catalogObject(), USE_CATALOG, requestContext),
        "a privilege change on an empty role must trigger a reload in the same request");
  }

  @Test
  public void testPartiallyLoadedRoleDoesNotFailClosedAfterEviction() throws Exception {
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    RoleEntity allowRole =
        mockRoleInStore(ALLOW_ROLE_ID, "allowRole", ImmutableList.of(getAllowSecurableObject()));
    // A role that still references a dropped catalog loads partially and never gets a loaded
    // marker. It must not turn every mid-request eviction of another role into a denial.
    long partialRoleId = 40L;
    RoleEntity partialRole =
        mockRoleInStore(
            partialRoleId,
            "partialRole",
            ImmutableList.of(
                buildSecurableObject(
                    partialRoleId,
                    MetadataObject.Type.CATALOG,
                    "droppedCatalog",
                    USE_CATALOG,
                    "ALLOW")));
    metadataIdConverterMockedStatic
        .when(
            () ->
                MetadataIdConverter.getID(
                    Mockito.argThat(
                        object -> object != null && "droppedCatalog".equals(object.name())),
                    eq(METALAKE)))
        .thenReturn(Optional.empty());
    try {
      mockDirectUserRoles(allowRole, partialRole);
      AuthorizationRequestContext requestContext = new AuthorizationRequestContext();

      assertFalse(
          jcasbinAuthorizer.authorize(
              currentPrincipal, METALAKE, metalakeObject(), USE_CATALOG, requestContext));
      assertFalse(
          getLoadedRolesCache(jcasbinAuthorizer).getIfPresent(partialRoleId).get().isComplete());
      getLoadedRolesCache(jcasbinAuthorizer).invalidate(ALLOW_ROLE_ID);

      assertTrue(
          jcasbinAuthorizer.authorize(
              currentPrincipal, METALAKE, catalogObject(), USE_CATALOG, requestContext));
    } finally {
      metadataIdConverterMockedStatic
          .when(() -> MetadataIdConverter.getID(any(), eq(METALAKE)))
          .thenReturn(Optional.of(CATALOG_ID));
    }
  }

  @Test
  public void testRoleClearGenerationsArePrunedWithoutMissingClears() throws Exception {
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    String roleName = "prunedGenerationRole";
    RoleEntity allowRole =
        mockRoleInStore(ALLOW_ROLE_ID, roleName, ImmutableList.of(getAllowSecurableObject()));
    mockDirectUserRoles(allowRole);
    Field maxField = JcasbinAuthorizer.class.getDeclaredField("maxRoleClearGenerations");
    maxField.setAccessible(true);
    maxField.setLong(jcasbinAuthorizer, 4L);
    Mockito.clearInvocations(entityStore);
    AuthorizationRequestContext requestContext = new AuthorizationRequestContext();

    assertFalse(
        jcasbinAuthorizer.authorize(
            currentPrincipal, METALAKE, metalakeObject(), USE_CATALOG, requestContext));

    // Clear the request's role first, then enough other roles to prune its generation away.
    jcasbinAuthorizer.handleRolePrivilegeChange(ALLOW_ROLE_ID);
    for (long roleId = 100L; roleId < 110L; roleId++) {
      jcasbinAuthorizer.handleRolePrivilegeChange(roleId);
    }
    Field generationsField = JcasbinAuthorizer.class.getDeclaredField("roleClearGenerations");
    generationsField.setAccessible(true);
    Map<?, ?> generations = (Map<?, ?>) generationsField.get(jcasbinAuthorizer);
    assertTrue(generations.size() <= 4, "the generation map must stay bounded");
    assertFalse(
        generations.containsKey(ALLOW_ROLE_ID), "the role's own clear must have been pruned");

    assertTrue(
        jcasbinAuthorizer.authorize(
            currentPrincipal, METALAKE, catalogObject(), USE_CATALOG, requestContext),
        "a pruned clear must still make the request reload the role");
    verify(entityStore, Mockito.times(2))
        .get(
            eq(NameIdentifierUtil.ofRole(METALAKE, roleName)),
            eq(Entity.EntityType.ROLE),
            eq(RoleEntity.class));
  }

  /** Deny summaries survive shared-cache eviction. */
  @Test
  public void testHasDenyPolicyPinsEvictedDenyPolicies() throws Exception {
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    RoleEntity denyRole =
        mockRoleInStore(DENY_ROLE_ID, "denyRole", ImmutableList.of(getDenySecurableObject()));
    mockDirectUserRoles(denyRole);
    AuthorizationRequestContext requestContext = new AuthorizationRequestContext();

    assertTrue(
        jcasbinAuthorizer.hasDenyPolicy(
            currentPrincipal, METALAKE, ImmutableSet.of(USE_CATALOG), requestContext));

    getLoadedRolesCache(jcasbinAuthorizer).invalidate(DENY_ROLE_ID);

    assertTrue(
        jcasbinAuthorizer.hasDenyPolicy(
            currentPrincipal, METALAKE, ImmutableSet.of(USE_CATALOG), requestContext),
        "an evicted deny role must not disable the list short-circuit guard");
  }

  /** Repeated invalidation during repair must fail closed. */
  @Test
  public void testRepeatedPrivilegeChangesFailClosed() throws Exception {
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    long otherRoleId = 31L;
    String roleName = "pingRole";
    String otherRoleName = "pongRole";
    RoleEntity role =
        getRoleEntity(ALLOW_ROLE_ID, roleName, ImmutableList.of(getAllowSecurableObject()));
    RoleEntity otherRole =
        getRoleEntity(otherRoleId, otherRoleName, ImmutableList.of(getAllowSecurableObject()));
    mockedRoleVersions.put(
        ALLOW_ROLE_ID, new RoleUpdatedAt(ALLOW_ROLE_ID, roleName, nextRoleVersion()));
    mockedRoleVersions.put(
        otherRoleId, new RoleUpdatedAt(otherRoleId, otherRoleName, nextRoleVersion()));
    AtomicReference<Boolean> armed = new AtomicReference<>(false);
    // Reloading either role explicitly invalidates the other, preventing a stable policy view.
    doAnswer(
            invocation -> {
              if (armed.get()) {
                jcasbinAuthorizer.handleRolePrivilegeChange(otherRoleId);
              }
              return role;
            })
        .when(entityStore)
        .get(
            eq(NameIdentifierUtil.ofRole(METALAKE, roleName)),
            eq(Entity.EntityType.ROLE),
            eq(RoleEntity.class));
    doAnswer(
            invocation -> {
              if (armed.get()) {
                jcasbinAuthorizer.handleRolePrivilegeChange(ALLOW_ROLE_ID);
              }
              return otherRole;
            })
        .when(entityStore)
        .get(
            eq(NameIdentifierUtil.ofRole(METALAKE, otherRoleName)),
            eq(Entity.EntityType.ROLE),
            eq(RoleEntity.class));
    mockDirectUserRoles(role, otherRole);

    AuthorizationRequestContext allowContext = new AuthorizationRequestContext();
    assertFalse(
        jcasbinAuthorizer.authorize(
            currentPrincipal, METALAKE, metalakeObject(), USE_CATALOG, allowContext));
    armed.set(true);
    jcasbinAuthorizer.handleRolePrivilegeChange(ALLOW_ROLE_ID);
    assertFalse(
        jcasbinAuthorizer.authorize(
            currentPrincipal, METALAKE, catalogObject(), USE_CATALOG, allowContext),
        "an allow check must fail closed when the request's policies never stabilize");

    armed.set(false);
    AuthorizationRequestContext denyContext = new AuthorizationRequestContext();
    assertFalse(
        jcasbinAuthorizer.authorize(
            currentPrincipal, METALAKE, metalakeObject(), USE_CATALOG, denyContext));
    armed.set(true);
    jcasbinAuthorizer.handleRolePrivilegeChange(ALLOW_ROLE_ID);
    assertTrue(
        jcasbinAuthorizer.deny(
            currentPrincipal, METALAKE, catalogObject(), USE_CATALOG, denyContext),
        "a deny check must fail closed when the request's policies never stabilize");
    armed.set(false);
    AuthorizationRequestContext scanContext = new AuthorizationRequestContext();
    assertFalse(
        jcasbinAuthorizer.hasDenyPolicy(
            currentPrincipal, METALAKE, ImmutableSet.of(USE_CATALOG), scanContext));
    armed.set(true);
    jcasbinAuthorizer.handleRolePrivilegeChange(ALLOW_ROLE_ID);
    assertTrue(
        jcasbinAuthorizer.hasDenyPolicy(
            currentPrincipal, METALAKE, ImmutableSet.of(USE_CATALOG), scanContext),
        "a deny scan must fail closed when the request's policies never stabilize");
    armed.set(false);
  }

  @Test
  public void testRoleDeletedMidRequestDeniesWithoutLooping() throws Exception {
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    String roleName = "deletedMidRequestRole";
    RoleEntity allowRole =
        mockRoleInStore(ALLOW_ROLE_ID, roleName, ImmutableList.of(getAllowSecurableObject()));
    mockDirectUserRoles(allowRole);
    Mockito.clearInvocations(entityStore);
    AuthorizationRequestContext requestContext = new AuthorizationRequestContext();

    assertFalse(
        jcasbinAuthorizer.authorize(
            currentPrincipal, METALAKE, metalakeObject(), USE_CATALOG, requestContext));

    NameIdentifier roleIdent = NameIdentifierUtil.ofRole(METALAKE, roleName);
    when(entityStore.get(eq(roleIdent), eq(Entity.EntityType.ROLE), eq(RoleEntity.class)))
        .thenThrow(new NoSuchEntityException("Role %s is dropped", roleName));
    Mockito.clearInvocations(entityStore);
    jcasbinAuthorizer.handleRolePrivilegeChange(ALLOW_ROLE_ID);

    assertFalse(
        jcasbinAuthorizer.authorize(
            currentPrincipal, METALAKE, catalogObject(), USE_CATALOG, requestContext));
    verify(entityStore, Mockito.times(1))
        .get(eq(roleIdent), eq(Entity.EntityType.ROLE), eq(RoleEntity.class));
  }

  @Test
  public void testDenialByEmptyRoleDoesNotReloadIt() throws Exception {
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    RoleEntity emptyRole = mockRoleInStore(32L, "emptyRole", ImmutableList.of());
    mockDirectUserRoles(emptyRole);
    Mockito.clearInvocations(entityStore);
    AuthorizationRequestContext requestContext = new AuthorizationRequestContext();

    assertFalse(
        jcasbinAuthorizer.authorize(
            currentPrincipal, METALAKE, metalakeObject(), USE_CATALOG, requestContext));
    assertFalse(
        jcasbinAuthorizer.authorize(
            currentPrincipal, METALAKE, catalogObject(), USE_CATALOG, requestContext));
    assertFalse(
        jcasbinAuthorizer.deny(
            currentPrincipal, METALAKE, catalogObject(), USE_CATALOG, requestContext));

    verify(entityStore, Mockito.times(1))
        .get(
            eq(NameIdentifierUtil.ofRole(METALAKE, "emptyRole")),
            eq(Entity.EntityType.ROLE),
            eq(RoleEntity.class));
  }

  @Test
  public void testEvictionDuringRoleLoadIsDetected() throws Exception {
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    RoleEntity allowRole =
        mockRoleInStore(ALLOW_ROLE_ID, "allowRole", ImmutableList.of(getAllowSecurableObject()));
    mockDirectUserRoles(allowRole);
    assertTrue(doAuthorize(currentPrincipal));

    // The next request loads a second role. While that load is in progress, the already loaded
    // role is evicted; the eviction must not slip between the load and the first check.
    long otherRoleId = 33L;
    String otherRoleName = "roleLoadedDuringEviction";
    RoleEntity otherRole = getRoleEntity(otherRoleId, otherRoleName, ImmutableList.of());
    mockedRoleVersions.put(
        otherRoleId, new RoleUpdatedAt(otherRoleId, otherRoleName, nextRoleVersion()));
    GravitinoCache<Long, CachedRolePolicies> loadedRoles = getLoadedRolesCache(jcasbinAuthorizer);
    doAnswer(
            invocation -> {
              loadedRoles.invalidate(ALLOW_ROLE_ID);
              return otherRole;
            })
        .when(entityStore)
        .get(
            eq(NameIdentifierUtil.ofRole(METALAKE, otherRoleName)),
            eq(Entity.EntityType.ROLE),
            eq(RoleEntity.class));
    mockDirectUserRoles(allowRole, otherRole);

    assertTrue(
        jcasbinAuthorizer.authorize(
            currentPrincipal,
            METALAKE,
            catalogObject(),
            USE_CATALOG,
            new AuthorizationRequestContext()));
  }

  /** Named-role authorization retains pinned policies after eviction. */
  @Test
  public void testNarrowedAuthorizationPinsEvictedPolicies() throws Exception {
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    RoleEntity allowRole =
        mockRoleInStore(ALLOW_ROLE_ID, "narrowedRole", ImmutableList.of(getAllowSecurableObject()));
    mockDirectUserRoles(allowRole);
    AuthorizationRequestContext requestContext = new AuthorizationRequestContext();
    requestContext.setActiveRoles(ActiveRoles.of(ImmutableList.of("narrowedRole")));

    assertFalse(
        jcasbinAuthorizer.authorize(
            currentPrincipal, METALAKE, metalakeObject(), USE_CATALOG, requestContext));
    getLoadedRolesCache(jcasbinAuthorizer).invalidate(ALLOW_ROLE_ID);

    assertTrue(
        jcasbinAuthorizer.authorize(
            currentPrincipal, METALAKE, catalogObject(), USE_CATALOG, requestContext));
  }

  /** Verifies bounded cache eviction races preserve composite authorization decisions. */
  @Test
  public void testConcurrentEvictionNeverFlipsCompositeDecisions() throws Exception {
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    RoleEntity allowRole =
        mockRoleInStore(ALLOW_ROLE_ID, "allowRole", ImmutableList.of(getAllowSecurableObject()));
    RoleEntity denyRole =
        mockRoleInStore(DENY_ROLE_ID, "denyRole", ImmutableList.of(getDenySecurableObject()));
    GravitinoCache<Long, CachedRolePolicies> loadedRoles = getLoadedRolesCache(jcasbinAuthorizer);

    // Requests run on this thread because the static mocks are thread-local. The evictor only
    // removes shared cache entries, as TTL or size eviction does. Pinned request views must retain
    // their policies without reloading, regardless of when this ordinary eviction happens.
    // Each request waits for its eviction round to finish before the next request starts, so a
    // delayed eviction cannot spill into another request.
    Semaphore evictionRequests = new Semaphore(0);
    Semaphore completedEvictions = new Semaphore(0);
    AtomicLong evictions = new AtomicLong();
    Thread evictor =
        new Thread(
            () -> {
              try {
                while (!Thread.currentThread().isInterrupted()) {
                  evictionRequests.acquire();
                  LockSupport.parkNanos(ThreadLocalRandom.current().nextLong(200_000L));
                  loadedRoles.invalidate(ALLOW_ROLE_ID);
                  loadedRoles.invalidate(DENY_ROLE_ID);
                  evictions.incrementAndGet();
                  completedEvictions.release();
                }
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });
    evictor.setDaemon(true);
    try {
      evictor.start();

      // Granted: only the allow role is held. Every composite decision must be allowed.
      mockDirectUserRoles(allowRole);
      for (int i = 0; i < 500; i++) {
        AuthorizationRequestContext ctx = new AuthorizationRequestContext();
        evictionRequests.release();
        assertFalse(
            jcasbinAuthorizer.authorize(
                currentPrincipal, METALAKE, metalakeObject(), USE_CATALOG, ctx));
        assertTrue(
            jcasbinAuthorizer.authorize(
                currentPrincipal, METALAKE, catalogObject(), USE_CATALOG, ctx),
            "an eviction must not deny a granted check");
        assertTrue(completedEvictions.tryAcquire(5, TimeUnit.SECONDS));
      }

      // Denied: the allow role and the deny role are both held. No decision may be allowed.
      mockDirectUserRoles(allowRole, denyRole);
      for (int i = 0; i < 500; i++) {
        AuthorizationRequestContext ctx = new AuthorizationRequestContext();
        evictionRequests.release();
        assertFalse(
            jcasbinAuthorizer.authorize(
                currentPrincipal, METALAKE, metalakeObject(), USE_CATALOG, ctx));
        assertFalse(
            jcasbinAuthorizer.authorize(
                    currentPrincipal, METALAKE, catalogObject(), USE_CATALOG, ctx)
                && !jcasbinAuthorizer.deny(
                    currentPrincipal, METALAKE, catalogObject(), USE_CATALOG, ctx),
            "an eviction must not allow a denied check");
        assertTrue(completedEvictions.tryAcquire(5, TimeUnit.SECONDS));
      }
    } finally {
      evictor.interrupt();
      evictor.join(5000L);
    }
    assertFalse(evictor.isAlive(), "the evictor must stop before the test exits");
    assertEquals(1000L, evictions.get(), "each request must complete exactly one eviction round");
  }

  /** Late partial resolution must preserve a concurrently completed index. */
  @Test
  public void testLatePartialLoadCannotReplaceCompleteLoad() throws Exception {
    RoleEntity complete =
        mockRoleInStore(ALLOW_ROLE_ID, "racingRole", ImmutableList.of(getAllowSecurableObject()));
    RoleEntity partial =
        getRoleEntity(
            ALLOW_ROLE_ID,
            "racingRole",
            ImmutableList.of(
                buildSecurableObject(
                    ALLOW_ROLE_ID,
                    MetadataObject.Type.TABLE,
                    "missing.schema.table",
                    USE_CATALOG,
                    "DENY")));
    mockDirectUserRoles(complete);
    MetadataObject missing =
        MetadataObjects.parse("missing.schema.table", MetadataObject.Type.TABLE);
    metadataIdConverterMockedStatic
        .when(() -> MetadataIdConverter.getID(missing, METALAKE))
        .thenReturn(Optional.empty());
    AtomicBoolean nested = new AtomicBoolean();
    doAnswer(
            invocation -> {
              if (nested.compareAndSet(false, true)) {
                invokeLoadRequestPolicies(
                    jcasbinAuthorizer,
                    METALAKE,
                    ImmutableList.of(ALLOW_ROLE_ID),
                    new AuthorizationRequestContext());
                return partial;
              }
              return complete;
            })
        .when(entityStore)
        .get(
            eq(NameIdentifierUtil.ofRole(METALAKE, "racingRole")),
            eq(Entity.EntityType.ROLE),
            eq(RoleEntity.class));
    AuthorizationRequestContext context = new AuthorizationRequestContext();
    invokeLoadRequestPolicies(
        jcasbinAuthorizer, METALAKE, ImmutableList.of(ALLOW_ROLE_ID), context);
    CachedRolePolicies cached =
        getLoadedRolesCache(jcasbinAuthorizer).getIfPresent(ALLOW_ROLE_ID).get();
    assertTrue(cached.isComplete());
    assertEquals(
        Effect.ALLOW,
        cached.getIndex().get(new PolicyKey("CATALOG", CATALOG_ID, USE_CATALOG.name())));
    assertTrue(
        invokeAuthorizeByPolicyIndex(
            jcasbinAuthorizer,
            USER_ID,
            METALAKE,
            catalogObject(),
            CATALOG_ID,
            USE_CATALOG,
            context));
  }

  /** A delayed older load must retain a newer deny after shared-cache eviction. */
  @Test
  public void testOlderLoadCannotOverwriteNewerEvictedDeny() throws Exception {
    RoleEntity older =
        mockRoleInStore(ALLOW_ROLE_ID, "racingRole", ImmutableList.of(getAllowSecurableObject()));
    RoleEntity newer =
        getRoleEntity(
            ALLOW_ROLE_ID,
            "racingRole",
            ImmutableList.of(
                buildSecurableObject(
                    ALLOW_ROLE_ID,
                    MetadataObject.Type.CATALOG,
                    "testCatalog",
                    USE_CATALOG,
                    "DENY")));
    mockDirectUserRoles(older);
    AtomicBoolean nested = new AtomicBoolean();
    doAnswer(
            invocation -> {
              if (nested.compareAndSet(false, true)) {
                mockedRoleVersions.put(
                    ALLOW_ROLE_ID,
                    new RoleUpdatedAt(ALLOW_ROLE_ID, "racingRole", nextRoleVersion()));
                invokeLoadRequestPolicies(
                    jcasbinAuthorizer,
                    METALAKE,
                    ImmutableList.of(ALLOW_ROLE_ID),
                    new AuthorizationRequestContext());
                getLoadedRolesCache(jcasbinAuthorizer).invalidate(ALLOW_ROLE_ID);
                return older;
              }
              return newer;
            })
        .when(entityStore)
        .get(
            eq(NameIdentifierUtil.ofRole(METALAKE, "racingRole")),
            eq(Entity.EntityType.ROLE),
            eq(RoleEntity.class));
    AuthorizationRequestContext context = new AuthorizationRequestContext();
    invokeLoadRequestPolicies(
        jcasbinAuthorizer, METALAKE, ImmutableList.of(ALLOW_ROLE_ID), context);
    assertFalse(
        invokeAuthorizeByPolicyIndex(
            jcasbinAuthorizer,
            USER_ID,
            METALAKE,
            catalogObject(),
            CATALOG_ID,
            USE_CATALOG,
            context));
    assertTrue(
        jcasbinAuthorizer.deny(
            PrincipalUtils.getCurrentPrincipal(), METALAKE, catalogObject(), USE_CATALOG, context));
  }

  /** Explicit invalidation must reject a resolution that started before it. */
  @Test
  public void testInvalidationDuringInitialResolutionRejectsStalePublication() throws Exception {
    RoleEntity role =
        mockRoleInStore(
            ALLOW_ROLE_ID, "invalidatedLoader", ImmutableList.of(getAllowSecurableObject()));
    mockDirectUserRoles(role);
    doAnswer(
            invocation -> {
              jcasbinAuthorizer.handleRolePrivilegeChange(ALLOW_ROLE_ID);
              return role;
            })
        .when(entityStore)
        .get(
            eq(NameIdentifierUtil.ofRole(METALAKE, "invalidatedLoader")),
            eq(Entity.EntityType.ROLE),
            eq(RoleEntity.class));
    AuthorizationRequestContext context = new AuthorizationRequestContext();
    assertFalse(
        jcasbinAuthorizer.authorize(
            PrincipalUtils.getCurrentPrincipal(), METALAKE, catalogObject(), USE_CATALOG, context));
    assertTrue(
        jcasbinAuthorizer.hasDenyPolicy(
            PrincipalUtils.getCurrentPrincipal(), METALAKE, ImmutableSet.of(USE_CATALOG), context));
    assertFalse(getLoadedRolesCache(jcasbinAuthorizer).getIfPresent(ALLOW_ROLE_ID).isPresent());
  }

  /** A partial batch result must retry omitted roles and retain read failures. */
  @Test
  public void testPartialBatchReadCannotOmitUnreadableDeny() throws Exception {
    RoleEntity allow =
        mockRoleInStore(
            ALLOW_ROLE_ID, "readableAllow", ImmutableList.of(getAllowSecurableObject()));
    RoleEntity deny =
        mockRoleInStore(DENY_ROLE_ID, "unreadableDeny", ImmutableList.of(getDenySecurableObject()));
    mockDirectUserRoles(allow, deny);
    when(entityStore.batchGet(anyList(), eq(Entity.EntityType.ROLE), eq(RoleEntity.class)))
        .thenReturn(ImmutableList.of(allow));
    when(entityStore.get(
            eq(NameIdentifierUtil.ofRole(METALAKE, "unreadableDeny")),
            eq(Entity.EntityType.ROLE),
            eq(RoleEntity.class)))
        .thenThrow(new IllegalStateException("Unavailable"));
    AuthorizationRequestContext context = new AuthorizationRequestContext();
    assertFalse(
        jcasbinAuthorizer.authorize(
            PrincipalUtils.getCurrentPrincipal(), METALAKE, catalogObject(), USE_CATALOG, context));
    assertTrue(
        jcasbinAuthorizer.deny(
            PrincipalUtils.getCurrentPrincipal(), METALAKE, catalogObject(), USE_CATALOG, context));
    assertEquals(ImmutableSet.of(DENY_ROLE_ID), context.getUnreadableRoleIds());
  }

  /** A small shared cache must not truncate the request policy union. */
  @Test
  public void testRequestPinsAllRolesWhenCacheIsSmallerThanMembership() throws Exception {
    RoleEntity allow =
        mockRoleInStore(
            ALLOW_ROLE_ID, "smallCacheAllow", ImmutableList.of(getAllowSecurableObject()));
    RoleEntity deny =
        mockRoleInStore(DENY_ROLE_ID, "smallCacheDeny", ImmutableList.of(getDenySecurableObject()));
    RoleEntity empty = mockRoleInStore(99L, "smallCacheEmpty", ImmutableList.of());
    mockDirectUserRoles(allow, deny, empty);
    Field field = JcasbinAuthorizer.class.getDeclaredField("loadedRoles");
    field.setAccessible(true);
    field.set(jcasbinAuthorizer, new JcasbinLoadedRolesCache(60_000L, 1L));
    AuthorizationRequestContext context = new AuthorizationRequestContext();
    assertFalse(
        jcasbinAuthorizer.authorize(
            PrincipalUtils.getCurrentPrincipal(),
            METALAKE,
            metalakeObject(),
            USE_CATALOG,
            context));
    getLoadedRolesCache(jcasbinAuthorizer).invalidateAll();
    assertTrue(
        jcasbinAuthorizer.deny(
            PrincipalUtils.getCurrentPrincipal(), METALAKE, catalogObject(), USE_CATALOG, context));
    assertFalse(
        jcasbinAuthorizer.authorize(
            PrincipalUtils.getCurrentPrincipal(), METALAKE, catalogObject(), USE_CATALOG, context));
    assertTrue(
        jcasbinAuthorizer.hasDenyPolicy(
            PrincipalUtils.getCurrentPrincipal(), METALAKE, ImmutableSet.of(USE_CATALOG), context));
  }

  /** Unresolved deny existence must survive retry-marker expiry and cache eviction. */
  @Test
  public void testUnresolvedDenySummarySurvivesBackoffExpiry() throws Exception {
    RoleEntity allow =
        mockRoleInStore(
            ALLOW_ROLE_ID, "ancestorAllow", ImmutableList.of(getAllowSecurableObject()));
    RoleEntity deny =
        mockRoleInStore(
            DENY_ROLE_ID,
            "unresolvedDeny",
            ImmutableList.of(
                buildSecurableObject(
                    DENY_ROLE_ID,
                    MetadataObject.Type.TABLE,
                    "missing.schema.table",
                    USE_CATALOG,
                    "DENY")));
    MetadataObject missing =
        MetadataObjects.parse("missing.schema.table", MetadataObject.Type.TABLE);
    metadataIdConverterMockedStatic
        .when(() -> MetadataIdConverter.getID(missing, METALAKE))
        .thenReturn(Optional.empty());
    mockDirectUserRoles(allow, deny);
    AuthorizationRequestContext context = new AuthorizationRequestContext();
    assertTrue(
        jcasbinAuthorizer.hasDenyPolicy(
            PrincipalUtils.getCurrentPrincipal(), METALAKE, ImmutableSet.of(USE_CATALOG), context));
    CachedRolePolicies partial =
        getLoadedRolesCache(jcasbinAuthorizer).getIfPresent(DENY_ROLE_ID).get();
    assertFalse(partial.isComplete());
    getPartialRoleLoadBackoffCache(jcasbinAuthorizer).invalidateAll();
    getLoadedRolesCache(jcasbinAuthorizer).invalidateAll();
    assertTrue(
        jcasbinAuthorizer.hasDenyPolicy(
            PrincipalUtils.getCurrentPrincipal(), METALAKE, ImmutableSet.of(USE_CATALOG), context));
    assertTrue(
        jcasbinAuthorizer.hasDenyPolicy(
            PrincipalUtils.getCurrentPrincipal(),
            METALAKE,
            ImmutableSet.of(USE_CATALOG),
            new AuthorizationRequestContext()));
  }

  /** A newer request's deny refreshes older views even after ordinary cache eviction. */
  @Test
  public void testNewerDenyPublicationRefreshesEvictedRequestView() throws Exception {
    RoleEntity role =
        mockRoleInStore(
            ALLOW_ROLE_ID, "publishedDeny", ImmutableList.of(getAllowSecurableObject()));
    mockDirectUserRoles(role);
    Principal principal = PrincipalUtils.getCurrentPrincipal();
    AuthorizationRequestContext older = new AuthorizationRequestContext();
    assertFalse(
        jcasbinAuthorizer.authorize(principal, METALAKE, metalakeObject(), USE_CATALOG, older));
    getLoadedRolesCache(jcasbinAuthorizer).invalidate(ALLOW_ROLE_ID);
    mockRoleInStore(
        ALLOW_ROLE_ID,
        "publishedDeny",
        ImmutableList.of(
            buildSecurableObject(
                ALLOW_ROLE_ID, MetadataObject.Type.CATALOG, "testCatalog", USE_CATALOG, "DENY")));
    AuthorizationRequestContext newer = new AuthorizationRequestContext();
    assertTrue(jcasbinAuthorizer.deny(principal, METALAKE, catalogObject(), USE_CATALOG, newer));
    assertTrue(jcasbinAuthorizer.deny(principal, METALAKE, catalogObject(), USE_CATALOG, older));
    assertFalse(
        jcasbinAuthorizer.authorize(principal, METALAKE, catalogObject(), USE_CATALOG, older));
  }

  /** Unrelated privilege changes advance validation without re-reading held roles. */
  @Test
  public void testUnrelatedPrivilegeChangeAdvancesViewValidation() throws Exception {
    RoleEntity role =
        mockRoleInStore(
            ALLOW_ROLE_ID, "stableHeldRole", ImmutableList.of(getAllowSecurableObject()));
    mockDirectUserRoles(role);
    AuthorizationRequestContext context = new AuthorizationRequestContext();
    Principal principal = PrincipalUtils.getCurrentPrincipal();
    assertFalse(
        jcasbinAuthorizer.authorize(principal, METALAKE, metalakeObject(), USE_CATALOG, context));
    long before = context.getRolePolicyView().generation();
    Mockito.clearInvocations(entityStore);
    jcasbinAuthorizer.handleRolePrivilegeChange(999L);
    assertTrue(
        jcasbinAuthorizer.authorize(principal, METALAKE, catalogObject(), USE_CATALOG, context));
    assertTrue(context.getRolePolicyView().generation() > before);
    assertEquals(context.getRolePolicyView().generation(), context.getRolePolicyGeneration());
    verify(entityStore, Mockito.never())
        .batchGet(anyList(), eq(Entity.EntityType.ROLE), eq(RoleEntity.class));
    verify(entityStore, Mockito.never())
        .get(any(), eq(Entity.EntityType.ROLE), eq(RoleEntity.class));
  }

  /** Subject read failures must not be interpreted as an absence of deny policies. */
  @Test
  public void testSubjectReadFailureFailsClosedAndDisablesShortcut() {
    UserUpdatedAt previous = userMetaMapper.getUserUpdatedAt(METALAKE, USERNAME);
    Mockito.doThrow(new IllegalStateException("Subject store unavailable"))
        .when(userMetaMapper)
        .getUserUpdatedAt(METALAKE, USERNAME);
    try {
      AuthorizationRequestContext context = new AuthorizationRequestContext();
      Principal principal = PrincipalUtils.getCurrentPrincipal();
      assertFalse(
          jcasbinAuthorizer.authorize(principal, METALAKE, catalogObject(), USE_CATALOG, context));
      assertTrue(
          jcasbinAuthorizer.deny(principal, METALAKE, catalogObject(), USE_CATALOG, context));
      assertTrue(
          jcasbinAuthorizer.hasDenyPolicy(
              principal, METALAKE, ImmutableSet.of(USE_CATALOG), context));
    } finally {
      Mockito.doReturn(previous).when(userMetaMapper).getUserUpdatedAt(METALAKE, USERNAME);
    }
  }

  /** Readiness belongs to the atomically published view, independently of diagnostic flags. */
  @Test
  public void testUnreadableViewCannotUseStaleDiagnosticFlagsToGrant() throws Exception {
    RoleEntity role =
        mockRoleInStore(
            ALLOW_ROLE_ID, "snapshotReadiness", ImmutableList.of(getAllowSecurableObject()));
    mockDirectUserRoles(role);
    Principal principal = PrincipalUtils.getCurrentPrincipal();
    AuthorizationRequestContext context = new AuthorizationRequestContext();
    assertFalse(
        jcasbinAuthorizer.authorize(principal, METALAKE, metalakeObject(), USE_CATALOG, context));
    CachedRolePolicies cached =
        getLoadedRolesCache(jcasbinAuthorizer).getIfPresent(ALLOW_ROLE_ID).get();
    // A worker may have read the old empty diagnostic set before another worker publishes a
    // failed refresh. Policies and readiness must be read from the same immutable view.
    assertTrue(context.getUnreadableRoleIds().isEmpty());
    context.setRolePolicyView(
        new RequestRolePolicies(
            context.getRolePolicyView().generation(),
            Map.of(ALLOW_ROLE_ID, cached),
            ActiveRoles.all(),
            Collections.emptyMap(),
            false));
    assertFalse(
        jcasbinAuthorizer.authorize(principal, METALAKE, catalogObject(), USE_CATALOG, context));
    assertTrue(jcasbinAuthorizer.deny(principal, METALAKE, catalogObject(), USE_CATALOG, context));
    assertTrue(
        jcasbinAuthorizer.hasDenyPolicy(
            principal, METALAKE, ImmutableSet.of(USE_CATALOG), context));
  }

  private static MetadataObject metalakeObject() {
    return MetadataObjects.of(null, METALAKE, MetadataObject.Type.METALAKE);
  }

  private static MetadataObject catalogObject() {
    return MetadataObjects.of(null, "testCatalog", MetadataObject.Type.CATALOG);
  }

  /** Loads a pinned view through the real request policy loader. */
  private static void invokeLoadRequestPolicies(
      JcasbinAuthorizer authorizer,
      String metalake,
      List<Long> roleIds,
      AuthorizationRequestContext requestContext)
      throws Exception {
    requestContext.setBoundRoleIds(roleIds);
    Method m =
        JcasbinAuthorizer.class.getDeclaredMethod(
            "loadRequestPolicies", String.class, AuthorizationRequestContext.class, long.class);
    m.setAccessible(true);
    m.invoke(authorizer, metalake, requestContext, 0L);
  }

  /** Evaluates the allow side of the indexed policy lookup. */
  private static boolean invokeAuthorizeByPolicyIndex(
      JcasbinAuthorizer authorizer,
      long userId,
      String metalake,
      MetadataObject metadataObject,
      Long metadataId,
      Privilege.Name privilege,
      AuthorizationRequestContext requestContext)
      throws Exception {
    Field field = JcasbinAuthorizer.class.getDeclaredField("allowInternalAuthorizer");
    field.setAccessible(true);
    Object allowInternalAuthorizer = field.get(authorizer);
    Method method =
        allowInternalAuthorizer
            .getClass()
            .getDeclaredMethod(
                "authorizeByPolicyIndex",
                long.class,
                String.class,
                MetadataObject.class,
                Long.class,
                String.class,
                AuthorizationRequestContext.class);
    method.setAccessible(true);
    return (Boolean)
        method.invoke(
            allowInternalAuthorizer,
            userId,
            metalake,
            metadataObject,
            metadataId,
            privilege.name(),
            requestContext);
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

  /** Reusing entry state avoids another SQL prefetch even for a different privilege check. */
  @Test
  public void testReadScopeReusesEntryRolePrefetch() throws Exception {
    Principal principal = PrincipalUtils.getCurrentPrincipal();
    RoleEntity role =
        mockRoleInStore(ALLOW_ROLE_ID, "allowRole", ImmutableList.of(getAllowSecurableObject()));
    mockDirectUserRoles(role);
    MetadataObject catalog = MetadataObjects.of(null, "testCatalog", MetadataObject.Type.CATALOG);
    AuthorizationRequestContext entryContext = new AuthorizationRequestContext();
    assertTrue(
        jcasbinAuthorizer.authorize(principal, METALAKE, catalog, USE_CATALOG, entryContext));
    Mockito.clearInvocations(userMetaMapper, roleMetaMapper);

    try (AuthorizationRequestScope scope = AuthorizationRequestScope.open()) {
      scope.bind(METALAKE, entryContext);
      AuthorizationRequestContext filterContext = AuthorizationRequestScope.getOrCreate(METALAKE);
      assertSame(entryContext, filterContext);
      assertFalse(
          jcasbinAuthorizer.authorize(principal, METALAKE, catalog, SELECT_TABLE, filterContext));
      verify(userMetaMapper, Mockito.never())
          .batchGetAuthSubjectsForUser(anyString(), anyString(), anyList());
      verify(roleMetaMapper, Mockito.never()).batchGetRoleUpdatedAt(any());
    }

    // A subsequent request must revalidate SQL versions, even with warm shared role caches.
    AuthorizationRequestContext nextContext = AuthorizationRequestScope.getOrCreate(METALAKE);
    assertTrue(jcasbinAuthorizer.authorize(principal, METALAKE, catalog, USE_CATALOG, nextContext));
    verify(userMetaMapper).batchGetAuthSubjectsForUser(eq(METALAKE), eq(USERNAME), anyList());
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
  public void testIsSelfGroupViaPrincipalGroup() throws Exception {
    NameIdentifier groupIdent = NameIdentifierUtil.ofGroup(METALAKE, GROUP_NAME);

    setCurrentPrincipalWithGroup(GROUP_NAME);
    Mockito.clearInvocations(groupMetaMapper);
    assertTrue(
        jcasbinAuthorizer.isSelf(
            Entity.EntityType.GROUP, groupIdent, new AuthorizationRequestContext()));
    Mockito.verify(groupMetaMapper, Mockito.never()).getGroupUpdatedAt(anyString(), anyString());

    setCurrentPrincipalWithGroup("otherGroup");
    assertFalse(
        jcasbinAuthorizer.isSelf(
            Entity.EntityType.GROUP, groupIdent, new AuthorizationRequestContext()));
    Mockito.verify(groupMetaMapper, Mockito.never()).getGroupUpdatedAt(anyString(), anyString());

    restoreDefaultPrincipal();
  }

  @Test
  public void testIsSelfGroupDoesNotRequireCurrentUserName() throws Exception {
    NameIdentifier groupIdent = NameIdentifierUtil.ofGroup(METALAKE, GROUP_NAME);

    UserPrincipal groupPrincipal = mock(UserPrincipal.class);
    when(groupPrincipal.getGroups())
        .thenReturn(ImmutableList.of(new UserGroup(Optional.empty(), GROUP_NAME)));
    when(groupPrincipal.getName())
        .thenThrow(new AssertionError("GROUP self check should only use principal groups"));
    principalUtilsMockedStatic.when(PrincipalUtils::getCurrentPrincipal).thenReturn(groupPrincipal);

    try {
      assertTrue(
          jcasbinAuthorizer.isSelf(
              Entity.EntityType.GROUP, groupIdent, new AuthorizationRequestContext()));
    } finally {
      restoreDefaultPrincipal();
    }
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

  /** All declared active roles are held (directly), so nothing is unheld. */
  @Test
  public void testFindUnheldRolesEmptyWhenAllRolesHeld() throws Exception {
    getLoadedRolesCache(jcasbinAuthorizer).invalidateAll();
    Principal principal = setCurrentPrincipalWithGroup(null);
    RoleEntity held = mockRoleInStore(ALLOW_ROLE_ID, "heldRole", ImmutableList.of());
    mockDirectUserRoles(held);
    Mockito.clearInvocations(userMetaMapper);
    metadataIdConverterMockedStatic.clearInvocations();

    AuthorizationRequestContext requestContext = new AuthorizationRequestContext();
    Set<String> unheld =
        jcasbinAuthorizer.findUnheldRoles(
            principal, METALAKE, ImmutableSet.of("heldRole"), requestContext);

    assertTrue(unheld.isEmpty());
    metadataIdConverterMockedStatic.verify(
        () -> MetadataIdConverter.getID(any(), eq(METALAKE)), Mockito.never());
    jcasbinAuthorizer.authorize(
        principal,
        METALAKE,
        MetadataObjects.of(null, "testCatalog", MetadataObject.Type.CATALOG),
        USE_CATALOG,
        requestContext);
    verify(userMetaMapper).batchGetAuthSubjectsForUser(eq(METALAKE), eq(USERNAME), anyList());
    restoreDefaultPrincipal();
    getLoadedRolesCache(jcasbinAuthorizer).invalidateAll();
  }

  /** A declared role that exists but is not assigned to the caller is reported as unheld. */
  @Test
  public void testFindUnheldRolesReturnsRolesNotAssigned() throws Exception {
    getLoadedRolesCache(jcasbinAuthorizer).invalidateAll();
    Principal principal = setCurrentPrincipalWithGroup(null);
    RoleEntity held = mockRoleInStore(ALLOW_ROLE_ID, "heldRole", ImmutableList.of());
    mockDirectUserRoles(held);

    Set<String> unheld =
        jcasbinAuthorizer.findUnheldRoles(
            principal,
            METALAKE,
            ImmutableSet.of("heldRole", "otherRole"),
            new AuthorizationRequestContext());

    assertEquals(ImmutableSet.of("otherRole"), unheld);
    restoreDefaultPrincipal();
    getLoadedRolesCache(jcasbinAuthorizer).invalidateAll();
  }

  /** A declared role that does not exist (no id) is treated as unheld. */
  @Test
  public void testFindUnheldRolesTreatsNonExistentRoleAsUnheld() throws Exception {
    getLoadedRolesCache(jcasbinAuthorizer).invalidateAll();
    Principal principal = setCurrentPrincipalWithGroup(null);
    RoleEntity held = mockRoleInStore(ALLOW_ROLE_ID, "heldRole", ImmutableList.of());
    mockDirectUserRoles(held);

    Set<String> unheld =
        jcasbinAuthorizer.findUnheldRoles(
            principal, METALAKE, ImmutableSet.of("ghostRole"), new AuthorizationRequestContext());

    assertEquals(ImmutableSet.of("ghostRole"), unheld);
    restoreDefaultPrincipal();
    getLoadedRolesCache(jcasbinAuthorizer).invalidateAll();
  }

  /** A group-inherited role counts as held, so it is not reported as unheld. */
  @Test
  public void testFindUnheldRolesCoversGroupInheritedRole() throws Exception {
    getLoadedRolesCache(jcasbinAuthorizer).invalidateAll();
    UserPrincipal groupPrincipal = setCurrentPrincipalWithGroup(GROUP_NAME);
    Long groupRoleId = 31L;
    mockRoleInStore(groupRoleId, "groupRole", ImmutableList.of());
    mockNoDirectUserRoles();
    mockGroupWithRoles(GROUP_NAME, ImmutableList.of(groupRoleId), ImmutableList.of("groupRole"));
    Mockito.clearInvocations(userMetaMapper);
    metadataIdConverterMockedStatic.clearInvocations();

    Set<String> unheld =
        jcasbinAuthorizer.findUnheldRoles(
            groupPrincipal,
            METALAKE,
            ImmutableSet.of("groupRole"),
            new AuthorizationRequestContext());

    assertTrue(unheld.isEmpty());
    verify(userMetaMapper).batchGetAuthSubjectsForUser(eq(METALAKE), eq(USERNAME), anyList());
    metadataIdConverterMockedStatic.verify(
        () -> MetadataIdConverter.getID(any(), eq(METALAKE)), Mockito.never());
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

  private static Map<Entity.EntityType, NameIdentifier> tableMetadataNames(
      String catalog, String table) {
    return ImmutableMap.of(
        Entity.EntityType.METALAKE, NameIdentifierUtil.ofMetalake(METALAKE),
        Entity.EntityType.CATALOG, NameIdentifierUtil.ofCatalog(METALAKE, catalog),
        Entity.EntityType.SCHEMA, NameIdentifierUtil.ofSchema(METALAKE, catalog, "schema"),
        Entity.EntityType.TABLE, NameIdentifier.of(METALAKE, catalog, "schema", table));
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
    GravitinoCache<Long, CachedRolePolicies> loadedRoles = getLoadedRolesCache(jcasbinAuthorizer);

    // Manually add a role to the cache
    Long testRoleId = 100L;
    loadedRoles.put(
        testRoleId, new CachedRolePolicies(System.currentTimeMillis(), Collections.emptyMap()));

    // Verify it's in the cache
    assertTrue(loadedRoles.getIfPresent(testRoleId).isPresent());

    // Call handleRolePrivilegeChange which should invalidate the cache entry
    jcasbinAuthorizer.handleRolePrivilegeChange(testRoleId);

    // Verify it's removed from the cache
    assertFalse(loadedRoles.getIfPresent(testRoleId).isPresent());
  }

  @Test
  public void testSemanticModelRenameDropAndNameReuseInvalidateLocalCache() throws Exception {
    GravitinoCache<String, Long> cache = getMetadataIdCache(jcasbinAuthorizer);
    JcasbinAuthorizationLookups lookups =
        new JcasbinAuthorizationLookups(cache, getOwnerRelCache(jcasbinAuthorizer));
    CatalogManager catalogs = mock(CatalogManager.class);
    BaseCatalog<?> catalog = mock(BaseCatalog.class);
    when(catalog.capability())
        .thenReturn(
            new Capability() {
              @Override
              public CapabilityResult caseSensitiveOnName(Scope scope) {
                return CapabilityResult.unsupported("case insensitive");
              }
            });
    Mockito.doAnswer(
            invocation -> {
              ThrowableFunction<BaseCatalog<?>, Object> operation = invocation.getArgument(1);
              return operation.apply(catalog);
            })
        .when(catalogs)
        .doWithCatalog(any(), any());
    when(gravitinoEnv.catalogManager()).thenReturn(catalogs);
    NameIdentifier oldIdent = NameIdentifier.of(METALAKE, "catalog", "schema", "SalesModel");
    NameIdentifier newIdent = NameIdentifier.of(oldIdent.namespace(), "RenamedModel");
    MetadataObject oldObject =
        MetadataObjects.parse("catalog.SCHEMA.SalesModel", MetadataObject.Type.SEMANTIC_MODEL);
    MetadataObject newObject =
        MetadataObjects.parse("catalog.ScHeMa.RenamedModel", MetadataObject.Type.SEMANTIC_MODEL);
    metadataIdConverterMockedStatic
        .when(
            () ->
                MetadataIdConverter.getID(
                    MetadataIdConverter.normalizeMetadataObject(oldObject, METALAKE), METALAKE))
        .thenReturn(Optional.of(100L));
    metadataIdConverterMockedStatic
        .when(
            () ->
                MetadataIdConverter.getID(
                    MetadataIdConverter.normalizeMetadataObject(newObject, METALAKE), METALAKE))
        .thenReturn(Optional.of(200L));
    assertEquals(
        Optional.of(100L),
        lookups.resolveMetadataId(oldObject, METALAKE, new AuthorizationRequestContext()));
    assertEquals(
        Optional.of(200L),
        lookups.resolveMetadataId(newObject, METALAKE, new AuthorizationRequestContext()));
    SemanticModelDispatcher dispatcher = mock(SemanticModelDispatcher.class);
    SemanticModel renamed = mock(SemanticModel.class);
    when(renamed.name()).thenReturn(newIdent.name());
    SemanticModelChange rename = SemanticModelChange.rename(newIdent.name());
    when(dispatcher.alterSemanticModel(oldIdent, rename)).thenReturn(renamed);
    when(dispatcher.dropSemanticModel(newIdent)).thenReturn(true);
    when(gravitinoEnv.gravitinoAuthorizer()).thenReturn(jcasbinAuthorizer);
    SemanticModelHookDispatcher hook = new SemanticModelHookDispatcher(dispatcher, () -> null);
    try {
      hook.alterSemanticModel(oldIdent, rename);
      metadataIdConverterMockedStatic
          .when(
              () ->
                  MetadataIdConverter.getID(
                      MetadataIdConverter.normalizeMetadataObject(oldObject, METALAKE), METALAKE))
          .thenReturn(Optional.empty());
      metadataIdConverterMockedStatic
          .when(
              () ->
                  MetadataIdConverter.getID(
                      MetadataIdConverter.normalizeMetadataObject(newObject, METALAKE), METALAKE))
          .thenReturn(Optional.of(100L));
      assertEquals(
          Optional.empty(),
          lookups.resolveMetadataId(oldObject, METALAKE, new AuthorizationRequestContext()));
      assertEquals(
          Optional.of(100L),
          lookups.resolveMetadataId(newObject, METALAKE, new AuthorizationRequestContext()));
      hook.dropSemanticModel(newIdent);
      metadataIdConverterMockedStatic
          .when(
              () ->
                  MetadataIdConverter.getID(
                      MetadataIdConverter.normalizeMetadataObject(newObject, METALAKE), METALAKE))
          .thenReturn(Optional.of(300L));
      assertEquals(
          Optional.of(300L),
          lookups.resolveMetadataId(newObject, METALAKE, new AuthorizationRequestContext()));
      metadataIdConverterMockedStatic
          .when(
              () ->
                  MetadataIdConverter.getID(
                      MetadataIdConverter.normalizeMetadataObject(oldObject, METALAKE), METALAKE))
          .thenReturn(Optional.of(400L));
      assertEquals(
          Optional.of(400L),
          lookups.resolveMetadataId(oldObject, METALAKE, new AuthorizationRequestContext()));
    } finally {
      when(gravitinoEnv.gravitinoAuthorizer()).thenReturn(null);
      when(gravitinoEnv.catalogManager()).thenReturn(null);
    }
  }

  @Test
  public void testSemanticModelCacheKeepsCaseSensitiveSchemasDistinct() throws Exception {
    CatalogManager catalogs = mock(CatalogManager.class);
    BaseCatalog<?> catalog = mock(BaseCatalog.class);
    when(catalog.capability()).thenReturn(Capability.DEFAULT);
    Mockito.doAnswer(
            invocation -> {
              ThrowableFunction<BaseCatalog<?>, Object> operation = invocation.getArgument(1);
              return operation.apply(catalog);
            })
        .when(catalogs)
        .doWithCatalog(any(), any());
    when(gravitinoEnv.catalogManager()).thenReturn(catalogs);
    JcasbinAuthorizationLookups lookups =
        new JcasbinAuthorizationLookups(
            getMetadataIdCache(jcasbinAuthorizer), getOwnerRelCache(jcasbinAuthorizer));
    MetadataObject lower =
        MetadataObjects.parse("catalog.schema.SalesModel", MetadataObject.Type.SEMANTIC_MODEL);
    MetadataObject upper =
        MetadataObjects.parse("catalog.SCHEMA.SalesModel", MetadataObject.Type.SEMANTIC_MODEL);
    metadataIdConverterMockedStatic
        .when(() -> MetadataIdConverter.getID(lower, METALAKE))
        .thenReturn(Optional.of(100L));
    metadataIdConverterMockedStatic
        .when(() -> MetadataIdConverter.getID(upper, METALAKE))
        .thenReturn(Optional.of(200L));
    try {
      assertEquals(
          Optional.of(100L),
          lookups.resolveMetadataId(lower, METALAKE, new AuthorizationRequestContext()));
      assertEquals(
          Optional.of(200L),
          lookups.resolveMetadataId(upper, METALAKE, new AuthorizationRequestContext()));
      jcasbinAuthorizer.handleEntityNameIdMappingChange(
          METALAKE,
          NameIdentifier.of(METALAKE, "catalog", "SCHEMA", "SalesModel"),
          Entity.EntityType.SEMANTIC_MODEL);
      metadataIdConverterMockedStatic
          .when(() -> MetadataIdConverter.getID(upper, METALAKE))
          .thenReturn(Optional.of(300L));
      assertEquals(
          Optional.of(300L),
          lookups.resolveMetadataId(upper, METALAKE, new AuthorizationRequestContext()));
      assertEquals(
          Optional.of(100L),
          lookups.resolveMetadataId(lower, METALAKE, new AuthorizationRequestContext()));
    } finally {
      when(gravitinoEnv.catalogManager()).thenReturn(null);
    }
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
  public void testCacheInitialization() throws Exception {
    // Verify that caches are initialized
    GravitinoCache<Long, CachedRolePolicies> loadedRolesCache =
        getLoadedRolesCache(jcasbinAuthorizer);
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
  public void testHasSetOwnerPermissionAllowsSchemaAndCatalogOwner() throws Exception {
    MetadataObject metalakeObject =
        MetadataObjects.of(ImmutableList.of(METALAKE), MetadataObject.Type.METALAKE);
    metadataIdConverterMockedStatic
        .when(() -> MetadataIdConverter.getID(eq(metalakeObject), eq(METALAKE)))
        .thenReturn(Optional.of(USER_METALAKE_ID));
    when(ownerMetaMapper.selectOwnerByMetadataObjectIdAndType(eq(CATALOG_ID), eq("SCHEMA")))
        .thenReturn(new OwnerInfo(USER_ID, "USER"));
    when(ownerMetaMapper.selectOwnerByMetadataObjectIdAndType(eq(CATALOG_ID), eq("CATALOG")))
        .thenReturn(new OwnerInfo(USER_ID, "USER"));
    getOwnerRelCache(jcasbinAuthorizer).invalidateAll();

    try {
      assertTrue(
          jcasbinAuthorizer.hasSetOwnerPermission(
              METALAKE, "SCHEMA", "testCatalog.testSchema", new AuthorizationRequestContext()));
    } finally {
      metadataIdConverterMockedStatic
          .when(() -> MetadataIdConverter.getID(eq(metalakeObject), eq(METALAKE)))
          .thenReturn(Optional.of(CATALOG_ID));
    }
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
  /** Eviction must preserve membership already pinned by a request. */
  @Test
  public void testRoleCacheInvalidationDoesNotMutateRequestRoleSnapshot() throws Exception {
    GravitinoCache<Long, CachedRolePolicies> loadedRoles = getLoadedRolesCache(jcasbinAuthorizer);
    Long testRoleId = 300L;
    AuthorizationRequestContext requestContext = new AuthorizationRequestContext();
    requestContext.setBoundRoleIds(Collections.singletonList(testRoleId));
    loadedRoles.put(
        testRoleId, new CachedRolePolicies(System.currentTimeMillis(), new HashMap<>()));

    loadedRoles.invalidate(testRoleId);

    assertFalse(loadedRoles.getIfPresent(testRoleId).isPresent());
    assertEquals(
        Collections.singleton(testRoleId), new HashSet<>(requestContext.getBoundRoleIds()));
  }

  /** Privilege probes use the immutable index of the current request's roles. */
  @Test
  public void testPrivilegeProbesUseIndexAndRequestRoleSnapshot() throws Exception {
    // Guards the O(roles_per_user) optimization: privilege probes resolve against the per-role
    // index using the role IDs captured in the request context.
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();

    mockRoleInStore(ALLOW_ROLE_ID, "allowRole", ImmutableList.of(getAllowSecurableObject()));
    when(userMetaMapper.getUserUpdatedAt(eq(METALAKE), eq(USERNAME)))
        .thenReturn(new UserUpdatedAt(USER_ID, nextUserVersion()));
    when(roleMetaMapper.listRolesByUserId(eq(USER_ID)))
        .thenReturn(ImmutableList.of(buildRolePO(ALLOW_ROLE_ID, "allowRole")));

    AuthorizationRequestContext requestContext =
        assertAuthorizationDecision(
            currentPrincipal,
            MetadataObjects.of(null, "testCatalog", MetadataObject.Type.CATALOG),
            USE_CATALOG,
            true,
            false);

    // The per-role index carries the granted privilege ...
    CachedRolePolicies cached =
        getLoadedRolesCache(jcasbinAuthorizer).getIfPresent(ALLOW_ROLE_ID).orElse(null);
    assertNotNull(cached, "role index must be cached after authorize");
    assertEquals(
        Effect.ALLOW,
        cached.getIndex().get(new PolicyKey("CATALOG", CATALOG_ID, USE_CATALOG.name())),
        "index must resolve the granted allow");
    assertEquals(
        Collections.singleton(ALLOW_ROLE_ID),
        new HashSet<>(requestContext.getBoundRoleIds()),
        "request must retain the effective role snapshot used by the probe");
  }

  /** Deny probes retain the request's role union and indexed deny. */
  @Test
  public void testDenyPolicyUsesDenyIndexAndRequestRoleSnapshot() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    MetadataObject catalog = MetadataObjects.of(null, "testCatalog", MetadataObject.Type.CATALOG);

    RoleEntity denyRole =
        mockRoleInStore(
            DENY_ROLE_ID,
            "denyRole",
            ImmutableList.of(
                buildSecurableObject(
                    DENY_ROLE_ID,
                    MetadataObject.Type.CATALOG,
                    "testCatalog",
                    USE_CATALOG,
                    "DENY")));
    mockDirectUserRoles(denyRole);

    AuthorizationRequestContext requestContext =
        assertAuthorizationDecision(currentPrincipal, catalog, USE_CATALOG, false, true);
    assertCachedRoleEffect(DENY_ROLE_ID, MetadataObject.Type.CATALOG, USE_CATALOG, Effect.DENY);
    assertEquals(
        Collections.singleton(DENY_ROLE_ID), new HashSet<>(requestContext.getBoundRoleIds()));
  }

  /** A verified empty role union grants no allow and contains no deny. */
  @Test
  public void testNoRolesReturnFalseForAllowAndDeny() {
    mockUserRoles();
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    MetadataObject catalog = MetadataObjects.of(null, "testCatalog", MetadataObject.Type.CATALOG);

    assertAuthorizationDecision(currentPrincipal, catalog, USE_CATALOG, false, false);
  }

  /** Deny wins within a role regardless of privilege insertion order. */
  @Test
  public void testSameRoleDenyWinsRegardlessOfPolicyOrder() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    MetadataObject catalog = MetadataObjects.of(null, "testCatalog", MetadataObject.Type.CATALOG);
    Long allowThenDenyRoleId = 301L;
    Long denyThenAllowRoleId = 302L;

    RoleEntity allowThenDenyRole =
        mockRoleInStore(
            allowThenDenyRoleId,
            "allowThenDenyRole",
            ImmutableList.of(
                buildSecurableObject(
                    allowThenDenyRoleId,
                    MetadataObject.Type.CATALOG,
                    "testCatalog",
                    USE_CATALOG,
                    "ALLOW"),
                buildSecurableObject(
                    allowThenDenyRoleId,
                    MetadataObject.Type.CATALOG,
                    "testCatalog",
                    USE_CATALOG,
                    "DENY")));
    RoleEntity denyThenAllowRole =
        mockRoleInStore(
            denyThenAllowRoleId,
            "denyThenAllowRole",
            ImmutableList.of(
                buildSecurableObject(
                    denyThenAllowRoleId,
                    MetadataObject.Type.CATALOG,
                    "testCatalog",
                    USE_CATALOG,
                    "DENY"),
                buildSecurableObject(
                    denyThenAllowRoleId,
                    MetadataObject.Type.CATALOG,
                    "testCatalog",
                    USE_CATALOG,
                    "ALLOW")));
    mockDirectUserRoles(allowThenDenyRole, denyThenAllowRole);

    assertAuthorizationDecision(currentPrincipal, catalog, USE_CATALOG, false, true);
    assertCachedRoleEffect(
        allowThenDenyRoleId, MetadataObject.Type.CATALOG, USE_CATALOG, Effect.DENY);
    assertCachedRoleEffect(
        denyThenAllowRoleId, MetadataObject.Type.CATALOG, USE_CATALOG, Effect.DENY);
  }

  /** Any held role's deny overrides other held roles' allows. */
  @Test
  public void testDenyInOneRoleOverridesAllowInAnotherRole() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    MetadataObject catalog = MetadataObjects.of(null, "testCatalog", MetadataObject.Type.CATALOG);

    RoleEntity allowRole =
        mockRoleInStore(
            ALLOW_ROLE_ID,
            "allowRole",
            ImmutableList.of(
                buildSecurableObject(
                    ALLOW_ROLE_ID,
                    MetadataObject.Type.CATALOG,
                    "testCatalog",
                    USE_CATALOG,
                    "ALLOW")));
    RoleEntity denyRole =
        mockRoleInStore(
            DENY_ROLE_ID,
            "denyRole",
            ImmutableList.of(
                buildSecurableObject(
                    DENY_ROLE_ID,
                    MetadataObject.Type.CATALOG,
                    "testCatalog",
                    USE_CATALOG,
                    "DENY")));
    mockDirectUserRoles(allowRole, denyRole);

    assertAuthorizationDecision(currentPrincipal, catalog, USE_CATALOG, false, true);
    assertCachedRoleEffect(ALLOW_ROLE_ID, MetadataObject.Type.CATALOG, USE_CATALOG, Effect.ALLOW);
    assertCachedRoleEffect(DENY_ROLE_ID, MetadataObject.Type.CATALOG, USE_CATALOG, Effect.DENY);
  }

  /** Policy lookup requires both metadata type and privilege equality. */
  @Test
  public void testPolicyKeyRequiresMatchingTypeAndPrivilege() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    MetadataObject catalog = MetadataObjects.of(null, "testCatalog", MetadataObject.Type.CATALOG);
    MetadataObject schema =
        MetadataObjects.of("testCatalog", "testSchema", MetadataObject.Type.SCHEMA);

    RoleEntity allowRole =
        mockRoleInStore(
            ALLOW_ROLE_ID,
            "allowRole",
            ImmutableList.of(
                buildSecurableObject(
                    ALLOW_ROLE_ID,
                    MetadataObject.Type.CATALOG,
                    "testCatalog",
                    USE_CATALOG,
                    "ALLOW")));
    mockDirectUserRoles(allowRole);

    assertAuthorizationDecision(currentPrincipal, catalog, SELECT_TABLE, false, false);
    assertAuthorizationDecision(currentPrincipal, schema, USE_CATALOG, false, false);
  }

  /** Different privileges retain independent effects in one index. */
  @Test
  public void testDifferentPrivilegesInOneRoleResolveIndependently() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    MetadataObject catalog = MetadataObjects.of(null, "testCatalog", MetadataObject.Type.CATALOG);
    Long mixedRoleId = 303L;

    RoleEntity mixedRole =
        mockRoleInStore(
            mixedRoleId,
            "mixedPrivilegeRole",
            ImmutableList.of(
                buildSecurableObject(
                    mixedRoleId, MetadataObject.Type.CATALOG, "testCatalog", USE_CATALOG, "ALLOW"),
                buildSecurableObject(
                    mixedRoleId, MetadataObject.Type.CATALOG, "testCatalog", USE_SCHEMA, "DENY")));
    mockDirectUserRoles(mixedRole);

    assertAuthorizationDecision(currentPrincipal, catalog, USE_CATALOG, true, false);
    assertAuthorizationDecision(currentPrincipal, catalog, USE_SCHEMA, false, true);
    assertCachedRoleEffect(mixedRoleId, MetadataObject.Type.CATALOG, USE_CATALOG, Effect.ALLOW);
    assertCachedRoleEffect(mixedRoleId, MetadataObject.Type.CATALOG, USE_SCHEMA, Effect.DENY);
  }

  /** A newer complete role index replaces the previous deny. */
  @Test
  public void testRolePolicyReloadChangesDenyToAllow() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    MetadataObject catalog = MetadataObjects.of(null, "testCatalog", MetadataObject.Type.CATALOG);
    Long mutableRoleId = 304L;

    RoleEntity denyRole =
        mockRoleInStore(
            mutableRoleId,
            "mutableRole",
            ImmutableList.of(
                buildSecurableObject(
                    mutableRoleId,
                    MetadataObject.Type.CATALOG,
                    "testCatalog",
                    USE_CATALOG,
                    "DENY")));
    mockDirectUserRoles(denyRole);
    assertAuthorizationDecision(currentPrincipal, catalog, USE_CATALOG, false, true);

    RoleEntity allowRole =
        mockRoleInStore(
            mutableRoleId,
            "mutableRole",
            ImmutableList.of(
                buildSecurableObject(
                    mutableRoleId,
                    MetadataObject.Type.CATALOG,
                    "testCatalog",
                    USE_CATALOG,
                    "ALLOW")));
    mockDirectUserRoles(allowRole);
    jcasbinAuthorizer.handleRolePrivilegeChange(mutableRoleId);

    assertAuthorizationDecision(currentPrincipal, catalog, USE_CATALOG, true, false);
    assertCachedRoleEffect(mutableRoleId, MetadataObject.Type.CATALOG, USE_CATALOG, Effect.ALLOW);
  }

  /** A new request excludes roles removed from the user's membership. */
  @Test
  public void testDenyDisappearsAfterRoleAssignmentIsRemoved() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    MetadataObject catalog = MetadataObjects.of(null, "testCatalog", MetadataObject.Type.CATALOG);

    RoleEntity denyRole =
        mockRoleInStore(
            DENY_ROLE_ID,
            "denyRole",
            ImmutableList.of(
                buildSecurableObject(
                    DENY_ROLE_ID,
                    MetadataObject.Type.CATALOG,
                    "testCatalog",
                    USE_CATALOG,
                    "DENY")));
    mockDirectUserRoles(denyRole);
    AuthorizationRequestContext deniedRequest =
        assertAuthorizationDecision(currentPrincipal, catalog, USE_CATALOG, false, true);
    assertEquals(
        Collections.singleton(DENY_ROLE_ID), new HashSet<>(deniedRequest.getBoundRoleIds()));

    mockNoDirectUserRoles();

    AuthorizationRequestContext nextRequest =
        assertAuthorizationDecision(currentPrincipal, catalog, USE_CATALOG, false, false);
    assertTrue(new HashSet<>(nextRequest.getBoundRoleIds()).isEmpty());
    assertEquals(
        Collections.singleton(DENY_ROLE_ID),
        new HashSet<>(deniedRequest.getBoundRoleIds()),
        "an earlier request must retain its immutable role snapshot");
  }

  /** Dropped references remain absent from object-specific policy lookup. */
  @Test
  public void testDroppedMetadataPolicyIsSkipped() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    MetadataObject catalog = MetadataObjects.of(null, "testCatalog", MetadataObject.Type.CATALOG);
    Long droppedRoleId = 305L;
    SecurableObject droppedObject =
        buildSecurableObject(
            droppedRoleId, MetadataObject.Type.CATALOG, "droppedCatalog", USE_CATALOG, "ALLOW");
    metadataIdConverterMockedStatic
        .when(() -> MetadataIdConverter.getID(eq(droppedObject), eq(METALAKE)))
        .thenReturn(Optional.empty());

    try {
      RoleEntity droppedRole =
          mockRoleInStore(droppedRoleId, "droppedMetadataRole", ImmutableList.of(droppedObject));
      mockDirectUserRoles(droppedRole);

      assertAuthorizationDecision(currentPrincipal, catalog, USE_CATALOG, false, false);
      CachedRolePolicies cached =
          getLoadedRolesCache(jcasbinAuthorizer).getIfPresent(droppedRoleId).orElse(null);
      assertNotNull(cached, "role must still be cached after skipping dropped metadata");
      assertTrue(cached.getIndex().isEmpty(), "dropped metadata must not create an index entry");
    } finally {
      metadataIdConverterMockedStatic
          .when(() -> MetadataIdConverter.getID(eq(droppedObject), eq(METALAKE)))
          .thenReturn(Optional.of(CATALOG_ID));
    }
  }

  /** Equal policy keys share equality and hash semantics. */
  @Test
  public void testPolicyKeyValueSemantics() {
    PolicyKey key = new PolicyKey("CATALOG", CATALOG_ID, USE_CATALOG.name());
    PolicyKey sameKey = new PolicyKey("CATALOG", CATALOG_ID, USE_CATALOG.name());

    assertEquals(key, key);
    assertEquals(key, sameKey);
    assertEquals(key.hashCode(), sameKey.hashCode());
    assertEquals(USE_CATALOG.name(), key.privilege());
    assertNotEquals(key, null);
    assertNotEquals(key, "CATALOG");
    assertNotEquals(key, new PolicyKey("SCHEMA", CATALOG_ID, USE_CATALOG.name()));
    assertNotEquals(key, new PolicyKey("CATALOG", CATALOG_ID + 1, USE_CATALOG.name()));
    assertNotEquals(key, new PolicyKey("CATALOG", CATALOG_ID, USE_SCHEMA.name()));
    assertEquals(
        "PolicyKey{type=CATALOG, id=" + CATALOG_ID + ", priv=USE_CATALOG}", key.toString());
  }

  /** Cached indexes cannot be changed through their source maps. */
  @Test
  public void testCachedRolePoliciesDefensivelyCopiesIndex() {
    Map<PolicyKey, Effect> index = new HashMap<>();
    PolicyKey key = new PolicyKey("CATALOG", CATALOG_ID, USE_CATALOG.name());
    CachedRolePolicies cachedRolePolicies = new CachedRolePolicies(1L, index);

    index.put(key, Effect.ALLOW);

    assertTrue(cachedRolePolicies.getIndex().isEmpty());
    assertThrows(
        UnsupportedOperationException.class,
        () -> cachedRolePolicies.getIndex().put(key, Effect.ALLOW));
  }

  private static GravitinoCache<Long, CachedRolePolicies> getLoadedRolesCache(
      JcasbinAuthorizer authorizer) throws Exception {
    Field field = JcasbinAuthorizer.class.getDeclaredField("loadedRoles");
    field.setAccessible(true);
    return (GravitinoCache<Long, CachedRolePolicies>) field.get(authorizer);
  }

  private static ReentrantReadWriteLock getRolePolicyLock(JcasbinAuthorizer authorizer)
      throws Exception {
    Field field = JcasbinAuthorizer.class.getDeclaredField("rolePolicyLock");
    field.setAccessible(true);
    return (ReentrantReadWriteLock) field.get(authorizer);
  }

  @SuppressWarnings("unchecked")
  private static GravitinoCache<Long, Boolean> getPartialRoleLoadBackoffCache(
      JcasbinAuthorizer authorizer) throws Exception {
    Field field = JcasbinAuthorizer.class.getDeclaredField("partialRoleLoadBackoff");
    field.setAccessible(true);
    return (GravitinoCache<Long, Boolean>) field.get(authorizer);
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

  private AuthorizationRequestContext assertAuthorizationDecision(
      Principal principal,
      MetadataObject metadataObject,
      Privilege.Name privilege,
      boolean expectedAuthorize,
      boolean expectedDeny) {
    AuthorizationRequestContext requestContext = new AuthorizationRequestContext();
    assertEquals(
        expectedAuthorize,
        jcasbinAuthorizer.authorize(principal, METALAKE, metadataObject, privilege, requestContext),
        "unexpected authorize decision");
    assertEquals(
        expectedDeny,
        jcasbinAuthorizer.deny(principal, METALAKE, metadataObject, privilege, requestContext),
        "unexpected deny decision");
    return requestContext;
  }

  private void assertCachedRoleEffect(
      Long roleId, MetadataObject.Type metadataType, Privilege.Name privilege, Effect expected)
      throws Exception {
    CachedRolePolicies cached =
        getLoadedRolesCache(jcasbinAuthorizer).getIfPresent(roleId).orElse(null);
    assertNotNull(cached, "role index must be cached for role " + roleId);
    assertEquals(
        expected,
        cached.getIndex().get(new PolicyKey(metadataType.name(), CATALOG_ID, privilege.name())),
        "unexpected cached effect for role " + roleId);
  }

  @Test
  public void testUnresolvedDenyGuardsCatalogAndRestoresPreciseScopeAfterRetry() throws Exception {
    MetadataObject denied =
        MetadataObjects.parse("broken.schema.denied", MetadataObject.Type.TABLE);
    MetadataObject sibling =
        MetadataObjects.parse("broken.schema.sibling", MetadataObject.Type.TABLE);
    MetadataObject healthy = MetadataObjects.of(null, "healthy", MetadataObject.Type.CATALOG);
    metadataIdConverterMockedStatic
        .when(() -> MetadataIdConverter.getID(denied, METALAKE))
        .thenReturn(Optional.of(10L));
    metadataIdConverterMockedStatic
        .when(() -> MetadataIdConverter.getID(sibling, METALAKE))
        .thenReturn(Optional.of(11L));
    metadataIdConverterMockedStatic
        .when(() -> MetadataIdConverter.getID(healthy, METALAKE))
        .thenReturn(Optional.of(20L));
    RoleEntity allowRole =
        mockRoleInStore(
            ALLOW_ROLE_ID,
            "ancestorAllowRole",
            ImmutableList.of(
                buildSecurableObject(
                    ALLOW_ROLE_ID,
                    MetadataObject.Type.METALAKE,
                    METALAKE,
                    Privilege.Name.SELECT_TABLE,
                    "ALLOW")));
    RoleEntity denyRole =
        mockRoleInStore(
            DENY_ROLE_ID,
            "childDenyRole",
            ImmutableList.of(
                buildSecurableObject(
                    DENY_ROLE_ID,
                    MetadataObject.Type.TABLE,
                    denied.fullName(),
                    Privilege.Name.SELECT_TABLE,
                    "DENY")));
    mockDirectUserRoles(allowRole, denyRole);
    CatalogManager catalogs = gravitinoEnv.catalogManager();
    Mockito.doThrow(new IllegalStateException("Connector initialization failed"))
        .when(catalogs)
        .doWithCatalog(eq(NameIdentifier.of(METALAKE, "broken")), any());
    AuthorizationExpressionEvaluator evaluator =
        new AuthorizationExpressionEvaluator("ANY_SELECT_TABLE", jcasbinAuthorizer);
    try {
      assertFalse(
          evaluator.evaluate(
              tableMetadataNames("broken", "denied"), new AuthorizationRequestContext()));
      // The guard must persist through the partial-role retry backoff and role narrowing.
      AuthorizationRequestContext narrowed = new AuthorizationRequestContext();
      narrowed.setActiveRoles(ActiveRoles.of(Set.of("ancestorAllowRole")));
      assertFalse(evaluator.evaluate(tableMetadataNames("broken", "sibling"), narrowed));
      assertTrue(
          evaluator.evaluate(
              tableMetadataNames("healthy", "table"), new AuthorizationRequestContext()));
      assertTrue(
          jcasbinAuthorizer.hasDenyPolicy(
              PrincipalUtils.getCurrentPrincipal(),
              METALAKE,
              Set.of(Privilege.Name.SELECT_TABLE),
              new AuthorizationRequestContext()));
      assertFalse(
          getLoadedRolesCache(jcasbinAuthorizer).getIfPresent(DENY_ROLE_ID).get().isComplete());

      BaseCatalog<?> catalog = mock(BaseCatalog.class);
      when(catalog.capability()).thenReturn(Capability.DEFAULT);
      doAnswer(
              invocation -> {
                ThrowableFunction<BaseCatalog<?>, Object> operation = invocation.getArgument(1);
                return operation.apply(catalog);
              })
          .when(catalogs)
          .doWithCatalog(eq(NameIdentifier.of(METALAKE, "broken")), any());
      // The catalog has recovered but the role is still in its retry backoff: only the guard keeps
      // the ancestor ALLOW from granting the denied table.
      assertTrue(
          getPartialRoleLoadBackoffCache(jcasbinAuthorizer).getIfPresent(DENY_ROLE_ID).isPresent());
      assertFalse(
          evaluator.evaluate(
              tableMetadataNames("broken", "denied"), new AuthorizationRequestContext()));
      getPartialRoleLoadBackoffCache(jcasbinAuthorizer).invalidate(DENY_ROLE_ID);
      assertFalse(
          evaluator.evaluate(
              tableMetadataNames("broken", "denied"), new AuthorizationRequestContext()));
      assertTrue(
          evaluator.evaluate(
              tableMetadataNames("broken", "sibling"), new AuthorizationRequestContext()));
      assertTrue(
          getLoadedRolesCache(jcasbinAuthorizer).getIfPresent(DENY_ROLE_ID).get().isComplete());

    } finally {
      metadataIdConverterMockedStatic
          .when(() -> MetadataIdConverter.getID(denied, METALAKE))
          .thenReturn(Optional.of(CATALOG_ID));
      metadataIdConverterMockedStatic
          .when(() -> MetadataIdConverter.getID(sibling, METALAKE))
          .thenReturn(Optional.of(CATALOG_ID));
      metadataIdConverterMockedStatic
          .when(() -> MetadataIdConverter.getID(healthy, METALAKE))
          .thenReturn(Optional.of(CATALOG_ID));
    }
  }

  @Test
  public void testUnresolvedDenyWithoutCatalogIdFailsBeforePoliciesAreApplied() throws Exception {
    MetadataObject catalogObject = MetadataObjects.of(null, "broken", MetadataObject.Type.CATALOG);
    CatalogManager catalogs = gravitinoEnv.catalogManager();
    Mockito.doThrow(new IllegalStateException("Connector initialization failed"))
        .when(catalogs)
        .doWithCatalog(eq(NameIdentifier.of(METALAKE, "broken")), any());
    RoleEntity role =
        mockRoleInStore(
            ALLOW_ROLE_ID,
            "unguardedRole",
            ImmutableList.of(
                buildSecurableObject(
                    ALLOW_ROLE_ID,
                    MetadataObject.Type.METALAKE,
                    METALAKE,
                    Privilege.Name.SELECT_TABLE,
                    "ALLOW"),
                buildSecurableObject(
                    ALLOW_ROLE_ID,
                    MetadataObject.Type.TABLE,
                    "broken.schema.table",
                    Privilege.Name.SELECT_TABLE,
                    "DENY")));
    mockDirectUserRoles(role);
    metadataIdConverterMockedStatic
        .when(() -> MetadataIdConverter.getID(catalogObject, METALAKE))
        .thenReturn(Optional.empty());
    try {
      AuthorizationRequestContext context = new AuthorizationRequestContext();
      assertFalse(
          jcasbinAuthorizer.authorize(
              PrincipalUtils.getCurrentPrincipal(),
              METALAKE,
              MetadataObjects.of(null, METALAKE, MetadataObject.Type.METALAKE),
              Privilege.Name.SELECT_TABLE,
              context));
      assertEquals(Set.of(ALLOW_ROLE_ID), context.getUnreadableRoleIds());
      assertTrue(
          jcasbinAuthorizer.hasDenyPolicy(
              PrincipalUtils.getCurrentPrincipal(),
              METALAKE,
              Set.of(Privilege.Name.SELECT_TABLE),
              context));
      assertFalse(getLoadedRolesCache(jcasbinAuthorizer).getIfPresent(ALLOW_ROLE_ID).isPresent());

      metadataIdConverterMockedStatic
          .when(() -> MetadataIdConverter.getID(catalogObject, METALAKE))
          .thenReturn(Optional.of(CATALOG_ID));
      assertTrue(
          jcasbinAuthorizer.authorize(
              PrincipalUtils.getCurrentPrincipal(),
              METALAKE,
              MetadataObjects.of(null, METALAKE, MetadataObject.Type.METALAKE),
              Privilege.Name.SELECT_TABLE,
              new AuthorizationRequestContext()));
    } finally {
      metadataIdConverterMockedStatic
          .when(() -> MetadataIdConverter.getID(catalogObject, METALAKE))
          .thenReturn(Optional.of(CATALOG_ID));
    }
  }

  @Test
  public void testPartialPolicyLoadIsNotRecordedAsLoaded() throws Exception {
    // Regression test for the permanently-denied-role failure mode. When a securable object cannot
    // be resolved to a metadata id, loadPolicyByRoleEntity skips it. Recording the role as loaded
    // anyway pins the broken state forever: role_meta.updated_at never moves, so the version check
    // keeps skipping the reload and the role's privileges never come back on that node.
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);

    mockRoleInStore(ALLOW_ROLE_ID, "allowRole", ImmutableList.of(getAllowSecurableObject()));
    GravitinoCache<Long, CachedRolePolicies> loadedRoles = getLoadedRolesCache(jcasbinAuthorizer);
    GravitinoCache<Long, Boolean> backoff = getPartialRoleLoadBackoffCache(jcasbinAuthorizer);

    // 1. The role's only securable object does not resolve, so nothing can be loaded.
    metadataIdConverterMockedStatic
        .when(() -> MetadataIdConverter.getID(any(), eq(METALAKE)))
        .thenReturn(Optional.empty());
    try {
      invokeLoadRequestPolicies(
          jcasbinAuthorizer,
          METALAKE,
          ImmutableList.of(ALLOW_ROLE_ID),
          new AuthorizationRequestContext());

      assertFalse(
          loadedRoles.getIfPresent(ALLOW_ROLE_ID).get().isComplete(),
          "a role whose policies could not be loaded must not be recorded as loaded");

      assertTrue(
          backoff.getIfPresent(ALLOW_ROLE_ID).isPresent(),
          "the incomplete load must arm the retry backoff");
    } finally {
      metadataIdConverterMockedStatic
          .when(() -> MetadataIdConverter.getID(any(), eq(METALAKE)))
          .thenReturn(Optional.of(CATALOG_ID));
    }

    // 2. While the backoff is armed the role is not re-read, so a request cannot turn into a DB
    //    round-trip per call for a role that stays unresolvable.
    invokeLoadRequestPolicies(
        jcasbinAuthorizer,
        METALAKE,
        ImmutableList.of(ALLOW_ROLE_ID),
        new AuthorizationRequestContext());

    // 3. Once the backoff lapses the role is retried and, now that the object resolves, loads
    //    fully and is recorded — this is the self-healing the old code could never reach.
    backoff.invalidate(ALLOW_ROLE_ID);
    invokeLoadRequestPolicies(
        jcasbinAuthorizer,
        METALAKE,
        ImmutableList.of(ALLOW_ROLE_ID),
        new AuthorizationRequestContext());

    assertTrue(
        loadedRoles.getIfPresent(ALLOW_ROLE_ID).isPresent(),
        "a fully loaded role must be recorded as loaded");
    assertFalse(
        backoff.getIfPresent(ALLOW_ROLE_ID).isPresent(),
        "a successful load must disarm the retry backoff");
  }

  @Test
  public void testAllowPoliciesEvictedMidRequestAreReloadedBeforeLaterCheck() throws Exception {
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    RoleEntity allowRole =
        mockRoleInStore(ALLOW_ROLE_ID, "allowRole", ImmutableList.of(getAllowSecurableObject()));
    mockDirectUserRoles(allowRole);
    AuthorizationRequestContext requestContext = new AuthorizationRequestContext();

    // The first check of a composite expression loads the role but does not match its scope.
    assertFalse(
        jcasbinAuthorizer.authorize(
            currentPrincipal, METALAKE, metalakeObject(), USE_CATALOG, requestContext));

    // TTL expiry or size eviction removes only the shared index. This request retains its view.
    getLoadedRolesCache(jcasbinAuthorizer).invalidate(ALLOW_ROLE_ID);

    assertTrue(
        jcasbinAuthorizer.authorize(
            currentPrincipal, METALAKE, catalogObject(), USE_CATALOG, requestContext),
        "a later check in the same request must see the evicted role's policies again");
  }

  @Test
  public void testUnrelatedRoleEvictionDoesNotReloadRequestRoles() throws Exception {
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();
    RoleEntity allowRole =
        mockRoleInStore(
            ALLOW_ROLE_ID, "unrelatedEvictionRole", ImmutableList.of(getAllowSecurableObject()));
    mockDirectUserRoles(allowRole);
    Mockito.clearInvocations(entityStore);
    AuthorizationRequestContext requestContext = new AuthorizationRequestContext();

    assertFalse(
        jcasbinAuthorizer.authorize(
            currentPrincipal, METALAKE, metalakeObject(), USE_CATALOG, requestContext));

    // Evict a loaded role that this request does not hold.
    long otherRoleId = 99L;

    GravitinoCache<Long, CachedRolePolicies> loadedRoles = getLoadedRolesCache(jcasbinAuthorizer);
    loadedRoles.put(otherRoleId, new CachedRolePolicies(1L, Collections.emptyMap()));
    loadedRoles.invalidate(otherRoleId);

    assertTrue(
        jcasbinAuthorizer.authorize(
            currentPrincipal, METALAKE, catalogObject(), USE_CATALOG, requestContext));
    verify(entityStore, Mockito.times(1))
        .get(
            eq(NameIdentifierUtil.ofRole(METALAKE, "unrelatedEvictionRole")),
            eq(Entity.EntityType.ROLE),
            eq(RoleEntity.class));
  }

  /** Different IdP group snapshots must not inject allows into another request. */
  @Test
  public void testInterleavedGroupRequestsMustNotInjectAllow() throws Exception {
    metadataIdConverterMockedStatic
        .when(() -> MetadataIdConverter.getID(any(), eq(METALAKE)))
        .thenReturn(Optional.of(CATALOG_ID));
    try {
      RoleEntity groupRole =
          mockRoleInStore(901L, "probeGroupAllow", ImmutableList.of(getAllowSecurableObject()));
      mockNoDirectUserRoles();
      mockGroupWithRoles(
          GROUP_NAME, ImmutableList.of(groupRole.id()), ImmutableList.of(groupRole.name()));

      UserPrincipal noGroupPrincipal = setCurrentPrincipalWithGroup(null);
      AuthorizationRequestContext noGroupContext = new AuthorizationRequestContext();
      assertFalse(
          jcasbinAuthorizer.authorize(
              noGroupPrincipal, METALAKE, metalakeObject(), USE_CATALOG, noGroupContext));
      assertTrue(noGroupContext.getBoundRoleIds().isEmpty());

      UserPrincipal groupPrincipal = setCurrentPrincipalWithGroup(GROUP_NAME);
      assertTrue(
          jcasbinAuthorizer.authorize(
              groupPrincipal,
              METALAKE,
              catalogObject(),
              USE_CATALOG,
              new AuthorizationRequestContext()));

      principalUtilsMockedStatic
          .when(PrincipalUtils::getCurrentPrincipal)
          .thenReturn(noGroupPrincipal);
      assertFalse(
          jcasbinAuthorizer.authorize(
              noGroupPrincipal, METALAKE, catalogObject(), USE_CATALOG, noGroupContext),
          "request without groups must not inherit another request's group ALLOW");

    } finally {
      metadataIdConverterMockedStatic
          .when(() -> MetadataIdConverter.getID(any(), eq(METALAKE)))
          .thenReturn(Optional.of(CATALOG_ID));
      restoreDefaultPrincipal();
    }
  }

  /** Different IdP group snapshots must not remove another request's deny. */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testInterleavedGroupRequestsMustNotRemoveDeny(boolean narrow) throws Exception {
    metadataIdConverterMockedStatic
        .when(() -> MetadataIdConverter.getID(any(), eq(METALAKE)))
        .thenReturn(Optional.of(CATALOG_ID));
    try {
      RoleEntity allowRole =
          mockRoleInStore(
              ALLOW_ROLE_ID, "probeDirectAllow", ImmutableList.of(getAllowSecurableObject()));
      RoleEntity denyRole =
          mockRoleInStore(
              DENY_ROLE_ID, "probeGroupDeny", ImmutableList.of(getDenySecurableObject()));
      mockDirectUserRoles(allowRole);
      mockGroupWithRoles(
          GROUP_NAME, ImmutableList.of(denyRole.id()), ImmutableList.of(denyRole.name()));

      UserPrincipal groupPrincipal = setCurrentPrincipalWithGroup(GROUP_NAME);
      AuthorizationRequestContext groupContext = new AuthorizationRequestContext();
      if (narrow) {
        groupContext.setActiveRoles(ActiveRoles.of(ImmutableList.of("probeDirectAllow")));
      }
      assertFalse(
          jcasbinAuthorizer.authorize(
              groupPrincipal, METALAKE, metalakeObject(), USE_CATALOG, groupContext));
      assertTrue(groupContext.getBoundRoleIds().contains(DENY_ROLE_ID));

      UserPrincipal noGroupPrincipal = setCurrentPrincipalWithGroup(null);
      assertTrue(
          jcasbinAuthorizer.authorize(
              noGroupPrincipal,
              METALAKE,
              catalogObject(),
              USE_CATALOG,
              new AuthorizationRequestContext()));

      principalUtilsMockedStatic
          .when(PrincipalUtils::getCurrentPrincipal)
          .thenReturn(groupPrincipal);
      assertTrue(
          jcasbinAuthorizer.deny(
              groupPrincipal, METALAKE, catalogObject(), USE_CATALOG, groupContext),
          "another request must not prune this request's group DENY");
      assertFalse(
          jcasbinAuthorizer.authorize(
              groupPrincipal, METALAKE, catalogObject(), USE_CATALOG, groupContext));
      assertTrue(
          jcasbinAuthorizer.hasDenyPolicy(
              groupPrincipal, METALAKE, ImmutableSet.of(USE_CATALOG), groupContext));
      principalUtilsMockedStatic
          .when(PrincipalUtils::getCurrentPrincipal)
          .thenReturn(noGroupPrincipal);
      assertFalse(
          jcasbinAuthorizer.hasDenyPolicy(
              noGroupPrincipal,
              METALAKE,
              ImmutableSet.of(USE_CATALOG),
              new AuthorizationRequestContext()));

    } finally {
      metadataIdConverterMockedStatic
          .when(() -> MetadataIdConverter.getID(any(), eq(METALAKE)))
          .thenReturn(Optional.of(CATALOG_ID));
      restoreDefaultPrincipal();
    }
  }

  /** The real list filter must retain the current request's group deny. */
  @Test
  public void testInterleavedGroupRequestsMustNotExposeDeniedTable() throws Exception {
    metadataIdConverterMockedStatic
        .when(() -> MetadataIdConverter.getID(any(), eq(METALAKE)))
        .thenReturn(Optional.of(CATALOG_ID));
    Field executorField = MetadataAuthzHelper.class.getDeclaredField("executor");
    executorField.setAccessible(true);
    Object previousExecutor = executorField.get(null);
    executorField.set(null, (Executor) Runnable::run);
    Config previousConfig = gravitinoEnv.config();
    Config config = mock(Config.class);
    when(config.get(eq(Configs.ENABLE_AUTHORIZATION))).thenReturn(true);
    when(gravitinoEnv.config()).thenReturn(config);
    try {
      SecurableObject deniedObject =
          buildSecurableObject(
              DENY_ROLE_ID,
              MetadataObject.Type.TABLE,
              "testCatalog.testSchema.hidden",
              SELECT_TABLE,
              "DENY");
      metadataIdConverterMockedStatic
          .when(() -> MetadataIdConverter.getID(eq(deniedObject), eq(METALAKE)))
          .thenReturn(Optional.of(42L));
      RoleEntity allowRole =
          mockRoleInStore(
              ALLOW_ROLE_ID,
              "probeListParentAllow",
              ImmutableList.of(
                  buildSecurableObject(
                      ALLOW_ROLE_ID,
                      MetadataObject.Type.CATALOG,
                      "testCatalog",
                      SELECT_TABLE,
                      "ALLOW"),
                  buildSecurableObject(
                      ALLOW_ROLE_ID,
                      MetadataObject.Type.CATALOG,
                      "testCatalog",
                      USE_CATALOG,
                      "ALLOW"),
                  buildSecurableObject(
                      ALLOW_ROLE_ID,
                      MetadataObject.Type.SCHEMA,
                      "testCatalog.testSchema",
                      USE_SCHEMA,
                      "ALLOW")));
      RoleEntity denyRole =
          mockRoleInStore(DENY_ROLE_ID, "probeListGroupDeny", ImmutableList.of(deniedObject));
      mockDirectUserRoles(allowRole);
      mockGroupWithRoles(
          GROUP_NAME, ImmutableList.of(denyRole.id()), ImmutableList.of(denyRole.name()));
      UserPrincipal groupPrincipal = setCurrentPrincipalWithGroup(GROUP_NAME);
      AuthorizationRequestContext groupContext = new AuthorizationRequestContext();
      assertFalse(
          jcasbinAuthorizer.authorize(
              groupPrincipal, METALAKE, metalakeObject(), SELECT_TABLE, groupContext));
      assertTrue(
          jcasbinAuthorizer.hasDenyPolicy(
              groupPrincipal, METALAKE, ImmutableSet.of(SELECT_TABLE), groupContext));

      UserPrincipal noGroupPrincipal = setCurrentPrincipalWithGroup(null);
      assertTrue(
          jcasbinAuthorizer.authorize(
              noGroupPrincipal,
              METALAKE,
              catalogObject(),
              SELECT_TABLE,
              new AuthorizationRequestContext()));
      principalUtilsMockedStatic
          .when(PrincipalUtils::getCurrentPrincipal)
          .thenReturn(groupPrincipal);

      GravitinoAuthorizerProvider provider = mock(GravitinoAuthorizerProvider.class);
      when(provider.getGravitinoAuthorizer()).thenReturn(jcasbinAuthorizer);
      try (MockedStatic<GravitinoAuthorizerProvider> providerMock =
              mockStatic(GravitinoAuthorizerProvider.class);
          AuthorizationRequestScope scope = AuthorizationRequestScope.open()) {
        providerMock.when(GravitinoAuthorizerProvider::getInstance).thenReturn(provider);
        scope.bind(METALAKE, groupContext);
        NameIdentifier hidden = NameIdentifier.of(METALAKE, "testCatalog", "testSchema", "hidden");
        NameIdentifier visible =
            NameIdentifier.of(METALAKE, "testCatalog", "testSchema", "visible");
        NameIdentifier[] filtered =
            MetadataAuthzHelper.filterByExpression(
                METALAKE,
                AuthorizationExpressionConstants.LIST_TABLE_LIKE_AUTHORIZATION_EXPRESSION,
                Entity.EntityType.TABLE,
                new NameIdentifier[] {hidden, visible});
        assertTrue(
            Arrays.asList(filtered).contains(visible), "the permitted table must remain visible");
        assertFalse(
            Arrays.asList(filtered).contains(hidden),
            "a concurrent no-group request must not expose the group-denied table identifier");
      }
    } finally {
      executorField.set(null, previousExecutor);
      when(gravitinoEnv.config()).thenReturn(previousConfig);
      metadataIdConverterMockedStatic
          .when(() -> MetadataIdConverter.getID(any(), eq(METALAKE)))
          .thenReturn(Optional.of(CATALOG_ID));
      restoreDefaultPrincipal();
    }
  }
}
