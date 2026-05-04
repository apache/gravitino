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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
import java.util.List;
import java.util.Optional;
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
import org.apache.gravitino.UserGroup;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.server.ServerConfig;
import org.apache.gravitino.server.authorization.MetadataIdConverter;
import org.apache.gravitino.server.authorization.jcasbin.JcasbinAuthorizer.OwnerInfo;
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
    principalUtilsMockedStatic.when(PrincipalUtils::getCurrentUserName).thenCallRealMethod();
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

  @BeforeEach
  public void resetSharedState() throws Exception {
    // Reset shared enforcer and cache state to prevent test ordering contamination.
    getLoadedRolesCache(jcasbinAuthorizer).invalidateAll();
    restoreDefaultPrincipal();
    // Reset role-user relation mock to return empty list (no roles) by default; individual tests
    // can override as needed.
    NameIdentifier userNameIdentifier = NameIdentifierUtil.ofUser(METALAKE, USERNAME);
    when(supportsRelationOperations.listEntitiesByRelation(
            eq(SupportsRelationOperations.Type.ROLE_USER_REL),
            eq(userNameIdentifier),
            eq(Entity.EntityType.USER)))
        .thenReturn(ImmutableList.of());
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

  @Test
  public void testAuthorizeByGroupOwner() throws Exception {
    // Set up a UserPrincipal whose groups include GROUP_NAME
    UserPrincipal groupPrincipal =
        new UserPrincipal(USERNAME, ImmutableList.of(new UserGroup(Optional.empty(), GROUP_NAME)));
    principalUtilsMockedStatic.when(PrincipalUtils::getCurrentPrincipal).thenReturn(groupPrincipal);

    NameIdentifier catalogIdent = NameIdentifierUtil.ofCatalog(METALAKE, "testCatalog");

    // Mock entityStore.batchGet for group entity lookup (needed for ID-based ownership
    // verification)
    when(entityStore.batchGet(
            eq(ImmutableList.of(NameIdentifierUtil.ofGroup(METALAKE, GROUP_NAME))),
            eq(Entity.EntityType.GROUP),
            eq(GroupEntity.class)))
        .thenReturn(ImmutableList.of(getGroupEntity()));

    // For non-member principal, mock batchGet for "otherGroup"
    when(entityStore.batchGet(
            eq(ImmutableList.of(NameIdentifierUtil.ofGroup(METALAKE, "otherGroup"))),
            eq(Entity.EntityType.GROUP),
            eq(GroupEntity.class)))
        .thenReturn(
            ImmutableList.of(
                GroupEntity.builder()
                    .withId(99L)
                    .withName("otherGroup")
                    .withNamespace(Namespace.of(METALAKE, "group"))
                    .withAuditInfo(AuditInfo.EMPTY)
                    .build()));

    // Mock owner relation returning a GroupEntity
    List<GroupEntity> owners = ImmutableList.of(getGroupEntity());
    doReturn(owners)
        .when(supportsRelationOperations)
        .listEntitiesByRelation(
            eq(SupportsRelationOperations.Type.OWNER_REL),
            eq(catalogIdent),
            eq(Entity.EntityType.CATALOG));
    getOwnerRelCache(jcasbinAuthorizer).invalidateAll();

    // The principal belongs to the owning group, so isOwner should return true
    assertTrue(doAuthorizeOwner(groupPrincipal));

    // Clear owner and verify it returns false
    doReturn(new ArrayList<>())
        .when(supportsRelationOperations)
        .listEntitiesByRelation(
            eq(SupportsRelationOperations.Type.OWNER_REL),
            eq(catalogIdent),
            eq(Entity.EntityType.CATALOG));
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
    doReturn(ImmutableList.of(getGroupEntity()))
        .when(supportsRelationOperations)
        .listEntitiesByRelation(
            eq(SupportsRelationOperations.Type.OWNER_REL),
            eq(catalogIdent),
            eq(Entity.EntityType.CATALOG));
    getOwnerRelCache(jcasbinAuthorizer).invalidateAll();
    assertFalse(doAuthorizeOwner(nonMemberPrincipal));

    // Restore the original principal mock
    principalUtilsMockedStatic
        .when(PrincipalUtils::getCurrentPrincipal)
        .thenReturn(new UserPrincipal(USERNAME));
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

    NameIdentifier userNameIdentifier = NameIdentifierUtil.ofUser(METALAKE, USERNAME);
    when(supportsRelationOperations.listEntitiesByRelation(
            eq(SupportsRelationOperations.Type.ROLE_USER_REL),
            eq(userNameIdentifier),
            eq(Entity.EntityType.USER)))
        .thenReturn(ImmutableList.of(directRole));

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
    assertTrue(jcasbinAuthorizer.isSelf(Entity.EntityType.ROLE, roleIdent));

    // A principal with no groups should fail
    setCurrentPrincipalWithGroup(null);
    assertFalse(jcasbinAuthorizer.isSelf(Entity.EntityType.ROLE, roleIdent));

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
  public void testStaleGroupSkippedWhenNotInStore() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    getLoadedRolesCache(jcasbinAuthorizer).invalidateAll();

    String staleGroupName = "staleGroup";
    UserPrincipal groupPrincipal = setCurrentPrincipalWithGroup(staleGroupName);

    mockNoDirectUserRoles();
    // batchGet silently skips missing entities -- the stale group returns an empty list
    when(entityStore.batchGet(
            eq(ImmutableList.of(NameIdentifierUtil.ofGroup(METALAKE, staleGroupName))),
            eq(Entity.EntityType.GROUP),
            eq(GroupEntity.class)))
        .thenReturn(ImmutableList.of());

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
  public void testRoleSharedByUserAndGroupSurvivesGroupRevocation() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    getLoadedRolesCache(jcasbinAuthorizer).invalidateAll();

    UserPrincipal groupPrincipal = setCurrentPrincipalWithGroup(GROUP_NAME);

    // The same role is assigned both directly to the user AND to the user's group
    Long sharedRoleId = 11L;
    RoleEntity sharedRole =
        mockRoleInStore(sharedRoleId, "sharedRole", ImmutableList.of(getAllowSecurableObject()));

    NameIdentifier userNameIdentifier = NameIdentifierUtil.ofUser(METALAKE, USERNAME);
    when(supportsRelationOperations.listEntitiesByRelation(
            eq(SupportsRelationOperations.Type.ROLE_USER_REL),
            eq(userNameIdentifier),
            eq(Entity.EntityType.USER)))
        .thenReturn(ImmutableList.of(sharedRole));
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

    NameIdentifier userNameIdentifier = NameIdentifierUtil.ofUser(METALAKE, USERNAME);
    when(supportsRelationOperations.listEntitiesByRelation(
            eq(SupportsRelationOperations.Type.ROLE_USER_REL),
            eq(userNameIdentifier),
            eq(Entity.EntityType.USER)))
        .thenReturn(ImmutableList.of(sharedRole));
    mockGroupWithRoles(
        GROUP_NAME, ImmutableList.of(sharedRoleId), ImmutableList.of(sharedRole.name()));

    // Authorization succeeds (role via both direct and group)
    assertTrue(doAuthorize(groupPrincipal));

    // User loses the role directly; group still has it
    when(supportsRelationOperations.listEntitiesByRelation(
            eq(SupportsRelationOperations.Type.ROLE_USER_REL),
            eq(userNameIdentifier),
            eq(Entity.EntityType.USER)))
        .thenReturn(ImmutableList.of());
    jcasbinAuthorizer.handleRolePrivilegeChange(sharedRoleId);

    // Authorization should still succeed -- role is retained via group inheritance
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

    // Known limitation: stale g-rows from the previous request still grant access because
    // there is no signal path from the IdP into Gravitino for group membership changes.
    // Access persists until the role's cache entry is evicted (TTL or explicit invalidation).
    assertTrue(doAuthorize(noGroupPrincipal));

    // Simulate cache invalidation by calling handleRolePrivilegeChange.
    // After cache invalidation (e.g. TTL expiry), the role is re-resolved and access is denied.
    jcasbinAuthorizer.handleRolePrivilegeChange(groupRoleId);
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

    GroupEntity groupAEntity =
        GroupEntity.builder()
            .withId(groupAId)
            .withName(groupA)
            .withNamespace(Namespace.of(METALAKE, "group"))
            .withAuditInfo(AuditInfo.EMPTY)
            .withRoleNames(ImmutableList.of(roleA.name()))
            .withRoleIds(ImmutableList.of(roleAId))
            .build();
    GroupEntity groupBEntity =
        GroupEntity.builder()
            .withId(groupBId)
            .withName(groupB)
            .withNamespace(Namespace.of(METALAKE, "group"))
            .withAuditInfo(AuditInfo.EMPTY)
            .withRoleNames(ImmutableList.of(roleB.name()))
            .withRoleIds(ImmutableList.of(roleBId))
            .build();
    when(entityStore.batchGet(
            eq(
                ImmutableList.of(
                    NameIdentifierUtil.ofGroup(METALAKE, groupA),
                    NameIdentifierUtil.ofGroup(METALAKE, groupB))),
            eq(Entity.EntityType.GROUP),
            eq(GroupEntity.class)))
        .thenReturn(ImmutableList.of(groupAEntity, groupBEntity));

    // Authorization succeeds
    assertTrue(doAuthorize(multiGroupPrincipal));

    // Revoke roleA from groupA; groupB still has roleB
    GroupEntity groupANoRoles =
        GroupEntity.builder()
            .withId(groupAId)
            .withName(groupA)
            .withNamespace(Namespace.of(METALAKE, "group"))
            .withAuditInfo(AuditInfo.EMPTY)
            .withRoleNames(ImmutableList.of())
            .withRoleIds(ImmutableList.of())
            .build();
    when(entityStore.batchGet(
            eq(
                ImmutableList.of(
                    NameIdentifierUtil.ofGroup(METALAKE, groupA),
                    NameIdentifierUtil.ofGroup(METALAKE, groupB))),
            eq(Entity.EntityType.GROUP),
            eq(GroupEntity.class)))
        .thenReturn(ImmutableList.of(groupANoRoles, groupBEntity));
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
    NameIdentifier userNameIdentifier = NameIdentifierUtil.ofUser(METALAKE, USERNAME);
    when(supportsRelationOperations.listEntitiesByRelation(
            eq(SupportsRelationOperations.Type.ROLE_USER_REL),
            eq(userNameIdentifier),
            eq(Entity.EntityType.USER)))
        .thenReturn(ImmutableList.of(denyRole));

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
    NameIdentifier userNameIdentifier = NameIdentifierUtil.ofUser(METALAKE, USERNAME);
    when(supportsRelationOperations.listEntitiesByRelation(
            eq(SupportsRelationOperations.Type.ROLE_USER_REL),
            eq(userNameIdentifier),
            eq(Entity.EntityType.USER)))
        .thenReturn(ImmutableList.of(allowRole));

    // Deny should win -- group provides deny even though user has direct allow
    assertFalse(doAuthorize(groupPrincipal));

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

  /** Mocks the user as having no direct (ROLE_USER_REL) role assignments. */
  private static void mockNoDirectUserRoles() throws IOException {
    NameIdentifier userNameIdentifier = NameIdentifierUtil.ofUser(METALAKE, USERNAME);
    when(supportsRelationOperations.listEntitiesByRelation(
            eq(SupportsRelationOperations.Type.ROLE_USER_REL),
            eq(userNameIdentifier),
            eq(Entity.EntityType.USER)))
        .thenReturn(ImmutableList.of());
  }

  /** Builds a {@link RoleEntity} and registers it in the mocked entity store. */
  private static RoleEntity mockRoleInStore(
      Long roleId, String roleName, List<SecurableObject> securableObjects) throws IOException {
    RoleEntity role = getRoleEntity(roleId, roleName, securableObjects);
    when(entityStore.get(
            eq(NameIdentifierUtil.ofRole(METALAKE, roleName)),
            eq(Entity.EntityType.ROLE),
            eq(RoleEntity.class)))
        .thenReturn(role);
    return role;
  }

  /**
   * Builds a {@link GroupEntity} with the given role ids/names and registers it in the mocked
   * entity store via batchGet.
   */
  private static GroupEntity mockGroupWithRoles(
      String groupName, List<Long> roleIds, List<String> roleNames) throws IOException {
    GroupEntity group =
        GroupEntity.builder()
            .withId(GROUP_ID)
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
    Cache<Long, Boolean> loadedRoles = getLoadedRolesCache(jcasbinAuthorizer);

    // Manually add a role to the cache
    Long testRoleId = 100L;
    loadedRoles.put(testRoleId, true);

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
    Cache<Long, Optional<OwnerInfo>> ownerRel = getOwnerRelCache(jcasbinAuthorizer);

    // Manually add an owner relation to the cache
    ownerRel.put(CATALOG_ID, Optional.of(new OwnerInfo(USER_ID, Entity.EntityType.USER, USERNAME)));

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
    Cache<Long, Boolean> loadedRoles = getLoadedRolesCache(jcasbinAuthorizer);

    // Add a role and its policy to the enforcer
    Long testRoleId = 300L;
    String roleIdStr = String.valueOf(testRoleId);

    // Add a policy for this role
    allowEnforcer.addPolicy(roleIdStr, "CATALOG", "999", "USE_CATALOG", "allow");
    denyEnforcer.addPolicy(roleIdStr, "CATALOG", "999", "USE_CATALOG", "allow");

    // Add role to cache
    loadedRoles.put(testRoleId, true);

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
    Cache<Long, Boolean> loadedRoles = getLoadedRolesCache(jcasbinAuthorizer);
    Cache<Long, Optional<OwnerInfo>> ownerRel = getOwnerRelCache(jcasbinAuthorizer);

    assertNotNull(loadedRoles, "loadedRoles cache should be initialized");
    assertNotNull(ownerRel, "ownerRel cache should be initialized");
  }

  /** Tests {@link JcasbinAuthorizer#hasMetadataPrivilegePermission} hierarchy walk */
  @Test
  public void testHasMetadataPrivilegePermission() throws Exception {
    makeCompletableFutureUseCurrentThread(jcasbinAuthorizer);
    NameIdentifier userNameIdentifier = NameIdentifierUtil.ofUser(METALAKE, USERNAME);

    // --- Case 1: no MANAGE_GRANTS anywhere → false ---
    when(supportsRelationOperations.listEntitiesByRelation(
            eq(SupportsRelationOperations.Type.ROLE_USER_REL),
            eq(userNameIdentifier),
            eq(Entity.EntityType.USER)))
        .thenReturn(ImmutableList.of());
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
    when(supportsRelationOperations.listEntitiesByRelation(
            eq(SupportsRelationOperations.Type.ROLE_USER_REL),
            eq(userNameIdentifier),
            eq(Entity.EntityType.USER)))
        .thenReturn(ImmutableList.of(metalakeGrantRole));
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
    when(supportsRelationOperations.listEntitiesByRelation(
            eq(SupportsRelationOperations.Type.ROLE_USER_REL),
            eq(userNameIdentifier),
            eq(Entity.EntityType.USER)))
        .thenReturn(ImmutableList.of(catalogGrantRole));
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
    when(supportsRelationOperations.listEntitiesByRelation(
            eq(SupportsRelationOperations.Type.ROLE_USER_REL),
            eq(userNameIdentifier),
            eq(Entity.EntityType.USER)))
        .thenReturn(ImmutableList.of(tableGrantRole));
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
  private static Cache<Long, Boolean> getLoadedRolesCache(JcasbinAuthorizer authorizer)
      throws Exception {
    Field field = JcasbinAuthorizer.class.getDeclaredField("loadedRoles");
    field.setAccessible(true);
    return (Cache<Long, Boolean>) field.get(authorizer);
  }

  @SuppressWarnings("unchecked")
  private static Cache<Long, Optional<OwnerInfo>> getOwnerRelCache(JcasbinAuthorizer authorizer)
      throws Exception {
    Field field = JcasbinAuthorizer.class.getDeclaredField("ownerRel");
    field.setAccessible(true);
    return (Cache<Long, Optional<OwnerInfo>>) field.get(authorizer);
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
