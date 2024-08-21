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
package org.apache.gravitino.authorization.ranger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.Group;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.RoleChange;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.authorization.ranger.defines.VXGroup;
import org.apache.gravitino.authorization.ranger.defines.VXGroupList;
import org.apache.gravitino.authorization.ranger.defines.VXUser;
import org.apache.gravitino.authorization.ranger.defines.VXUserList;
import org.apache.gravitino.connector.AuthorizationPropertiesMeta;
import org.apache.gravitino.connector.authorization.AuthorizationPlugin;
import org.apache.gravitino.exceptions.AuthorizationPluginException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.ranger.RangerServiceException;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerRole;
import org.apache.ranger.plugin.util.GrantRevokeRoleRequest;
import org.apache.ranger.plugin.util.SearchFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Ranger authorization operations plugin abstract class. */
public abstract class RangerAuthorizationPlugin implements AuthorizationPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(RangerAuthorizationPlugin.class);

  protected String catalogProvider;
  protected RangerClientExtend rangerClient;
  protected String rangerServiceName;
  /** Mapping Gravitino privilege name to the underlying authorization system privileges. */
  protected Map<Privilege.Name, Set<String>> privilegesMapping = null;
  // The owner privileges, the owner can do anything on the metadata object
  protected Set<String> ownerPrivileges = null;

  /**
   * Because Ranger doesn't support the precise filter, Ranger will return the policy meets the
   * wildcard(*,?) conditions, just like `*.*.*` policy will match `db1.table1.column1` So we need
   * to manually precisely filter the policies.
   */
  // Search Ranger policy filter keys
  protected List<String> policyFilterKeys = null;
  // Search Ranger policy precise filter keys
  protected List<String> policyPreciseFilterKeys = null;

  public static final String MANAGED_BY_GRAVITINO = "MANAGED_BY_GRAVITINO";

  // TODO: Maybe need to move to the configuration in the future
  public static final String RANGER_ADMIN_NAME = "admin";

  public RangerAuthorizationPlugin(String catalogProvider, Map<String, String> config) {
    super();
    this.catalogProvider = catalogProvider;
    String rangerUrl = config.get(AuthorizationPropertiesMeta.RANGER_ADMIN_URL);
    String authType = config.get(AuthorizationPropertiesMeta.RANGER_AUTH_TYPE);
    String username = config.get(AuthorizationPropertiesMeta.RANGER_USERNAME);
    // Apache Ranger Password should be minimum 8 characters with min one alphabet and one numeric.
    String password = config.get(AuthorizationPropertiesMeta.RANGER_PASSWORD);
    rangerServiceName = config.get(AuthorizationPropertiesMeta.RANGER_SERVICE_NAME);
    check(rangerUrl != null, "Ranger admin URL is required");
    check(authType != null, "Ranger auth type is required");
    check(username != null, "Ranger username is required");
    check(password != null, "Ranger password is required");
    check(rangerServiceName != null, "Ranger service name is required");

    rangerClient = new RangerClientExtend(rangerUrl, authType, username, password);

    initMapPrivileges();
    initOwnerPrivileges();
    initPolicyFilterKeys();
    initPreciseFilterKeys();
  }

  /**
   * Different underlying permission system may have different privilege names, this function is
   * used to initialize the privilege mapping.
   */
  protected abstract void initMapPrivileges();

  /**
   * Different underlying permission system may have different owner privilege names, this function
   * is used to initialize the owner privilege mapping.
   */
  protected abstract void initOwnerPrivileges();

  // Initial Ranger policy filter keys
  protected abstract void initPolicyFilterKeys();
  // Initial Ranger policy precise filter keys
  protected abstract void initPreciseFilterKeys();

  /**
   * Translate the privilege name to the corresponding privilege name in the underlying permission
   *
   * @param name The privilege name to translate
   * @return The corresponding privilege name in the underlying permission system
   */
  public Set<String> translatePrivilege(Privilege.Name name) {
    return privilegesMapping.get(name);
  }

  /**
   * Whether this privilege is underlying permission system supported
   *
   * @param name The privilege name to check
   * @return true if the privilege is supported, otherwise false
   */
  protected boolean checkPrivilege(Privilege.Name name) {
    return privilegesMapping.containsKey(name);
  }

  @FormatMethod
  protected void check(boolean condition, @FormatString String message, Object... args) {
    if (!condition) {
      throw new AuthorizationPluginException(message, args);
    }
  }

  @VisibleForTesting
  public List<String> getOwnerPrivileges() {
    return Lists.newArrayList(ownerPrivileges);
  }

  /**
   * Because Ranger does not have a Role concept, Each metadata object will have a unique Ranger
   * policy. we can use one or more Ranger policy to simulate the role. <br>
   * 1. Create a policy for each metadata object. <br>
   * 2. Save role name in the Policy properties. <br>
   * 3. Set `MANAGED_BY_GRAVITINO` label in the policy. <br>
   * 4. For easy manage, each privilege will create a RangerPolicyItemAccess in the policy. <br>
   * 5. The policy will only have one user, the user is the {OWNER} of the policy. <br>
   * 6. The policy will not have group. <br>
   */
  @Override
  public Boolean onRoleCreated(Role role) throws RuntimeException {
    createRangerRoleIfNotExists(role.name());
    return onRoleUpdated(
        role,
        role.securableObjects().stream()
            .map(securableObject -> RoleChange.addSecurableObject(role.name(), securableObject))
            .toArray(RoleChange[]::new));
  }

  @Override
  public Boolean onRoleAcquired(Role role) throws RuntimeException {
    try {
      return role.securableObjects().stream().allMatch(object -> findManagedPolicy(object) != null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Because one Ranger policy maybe contain multiple securable objects, so we didn't directly
   * remove the policy. <br>
   */
  @Override
  public Boolean onRoleDeleted(Role role) throws RuntimeException {
    // First, remove the role in the Ranger policy
    onRoleUpdated(
        role,
        role.securableObjects().stream()
            .map(securableObject -> RoleChange.removeSecurableObject(role.name(), securableObject))
            .toArray(RoleChange[]::new));
    // Lastly, remove the role in the Ranger
    try {
      rangerClient.deleteRole(role.name(), RANGER_ADMIN_NAME, rangerServiceName);
    } catch (RangerServiceException e) {
      // Ignore exception to support idempotent operation
    }
    return Boolean.TRUE;
  }

  @Override
  public Boolean onRoleUpdated(Role role, RoleChange... changes) throws RuntimeException {
    for (RoleChange change : changes) {
      boolean execResult;
      if (change instanceof RoleChange.AddSecurableObject) {
        execResult = doAddSecurableObject((RoleChange.AddSecurableObject) change);
      } else if (change instanceof RoleChange.RemoveSecurableObject) {
        execResult =
            doRemoveSecurableObject(role.name(), (RoleChange.RemoveSecurableObject) change);
      } else if (change instanceof RoleChange.UpdateSecurableObject) {
        execResult =
            doUpdateSecurableObject(role.name(), (RoleChange.UpdateSecurableObject) change);
      } else {
        throw new IllegalArgumentException(
            "Unsupported role change type: "
                + (change == null ? "null" : change.getClass().getSimpleName()));
      }
      if (!execResult) {
        return Boolean.FALSE;
      }
    }

    return Boolean.TRUE;
  }

  /**
   * Set or transfer the ownership of the metadata object. <br>
   *
   * @param metadataObject The metadata object to set the owner.
   * @param preOwner The previous owner of the metadata object. If the metadata object doesn't have
   *     an owner, then the preOwner will be null and newOwner will be not null.
   * @param newOwner The new owner of the metadata object. If the metadata object already has an
   *     owner, then the preOwner and newOwner will not be null.
   */
  @Override
  public Boolean onOwnerSet(MetadataObject metadataObject, Owner preOwner, Owner newOwner)
      throws RuntimeException {
    // 1. Set the owner of the metadata object
    // 2. Transfer the ownership from preOwner to newOwner of the metadata object
    check(newOwner != null, "The newOwner must be not null");

    if (newOwner != null) {
      // Add the user or group to the Ranger
      AuditInfo auditInfo =
          AuditInfo.builder()
              .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
              .withCreateTime(Instant.now())
              .build();
      if (newOwner.type() == Owner.Type.USER) {
        UserEntity userEntity =
            UserEntity.builder()
                .withId(1L)
                .withName(newOwner.name())
                .withRoleNames(Collections.emptyList())
                .withRoleIds(Collections.emptyList())
                .withAuditInfo(auditInfo)
                .build();
        onUserAdded(userEntity);
      } else {
        GroupEntity groupEntity =
            GroupEntity.builder()
                .withId(1L)
                .withName(newOwner.name())
                .withRoleNames(Collections.emptyList())
                .withRoleIds(Collections.emptyList())
                .withAuditInfo(auditInfo)
                .build();
        onGroupAdded(groupEntity);
      }
    }

    RangerPolicy policy = findManagedPolicy(metadataObject);
    if (policy != null) {
      // Find matching policy items based on the owner's privileges
      List<RangerPolicy.RangerPolicyItem> matchPolicyItmes =
          policy.getPolicyItems().stream()
              .filter(
                  policyItem -> {
                    return policyItem.getAccesses().stream()
                        .allMatch(
                            policyItemAccess -> {
                              return ownerPrivileges.contains(policyItemAccess.getType());
                            });
                  })
              .collect(Collectors.toList());
      // Add or remove the owner in the policy item
      matchPolicyItmes.forEach(
          policyItem -> {
            if (preOwner != null) {
              if (preOwner.type() == Owner.Type.USER) {
                policyItem.getUsers().removeIf(preOwner.name()::equals);
              } else {
                policyItem.getGroups().removeIf(preOwner.name()::equals);
              }
            }
            if (newOwner != null) {
              if (newOwner.type() == Owner.Type.USER) {
                if (!policyItem.getUsers().contains(newOwner.name())) {
                  policyItem.getUsers().add(newOwner.name());
                }
              } else {
                if (!policyItem.getGroups().contains(newOwner.name())) {
                  policyItem.getGroups().add(newOwner.name());
                }
              }
            }
          });
      // If the policy item is not equal to owner's privileges, then update the policy
      RangerPolicy finalPolicy = policy;
      ownerPrivileges.stream()
          .filter(
              ownerPrivilege -> {
                return matchPolicyItmes.stream()
                    .noneMatch(
                        policyItem -> {
                          return policyItem.getAccesses().stream()
                              .anyMatch(
                                  policyItemAccess -> {
                                    return ownerPrivilege.equals(policyItemAccess.getType());
                                  });
                        });
              })
          .forEach(
              // Add lose owner's privilege to the policy
              ownerPrivilege -> {
                RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem();
                policyItem
                    .getAccesses()
                    .add(new RangerPolicy.RangerPolicyItemAccess(ownerPrivilege));
                if (newOwner != null) {
                  if (newOwner.type() == Owner.Type.USER) {
                    policyItem.getUsers().add(newOwner.name());
                  } else {
                    policyItem.getGroups().add(newOwner.name());
                  }
                }
                finalPolicy.getPolicyItems().add(policyItem);
              });
    } else {
      policy = new RangerPolicy();
      policy.setService(rangerServiceName);
      policy.setName(metadataObject.fullName());
      policy.setPolicyLabels(Lists.newArrayList(MANAGED_BY_GRAVITINO));

      List<String> nsMetadataObject =
          Lists.newArrayList(SecurableObjects.DOT_SPLITTER.splitToList(metadataObject.fullName()));
      if (nsMetadataObject.size() > 4) {
        // The max level of the securable object is `catalog.db.table.column`
        throw new RuntimeException("The securable object than 4");
      }
      nsMetadataObject.remove(0); // remove `catalog`

      for (int i = 0; i < nsMetadataObject.size(); i++) {
        RangerPolicy.RangerPolicyResource policyResource =
            new RangerPolicy.RangerPolicyResource(nsMetadataObject.get(i));
        policy
            .getResources()
            .put(
                i == 0
                    ? RangerDefines.RESOURCE_DATABASE
                    : i == 1 ? RangerDefines.RESOURCE_TABLE : RangerDefines.RESOURCE_COLUMN,
                policyResource);
      }

      RangerPolicy finalPolicy = policy;
      ownerPrivileges.stream()
          .forEach(
              ownerPrivilege -> {
                // Each owner's privilege will create one RangerPolicyItemAccess in the policy
                RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem();
                policyItem
                    .getAccesses()
                    .add(new RangerPolicy.RangerPolicyItemAccess(ownerPrivilege));
                if (newOwner != null) {
                  if (newOwner.type() == Owner.Type.USER) {
                    policyItem.getUsers().add(newOwner.name());
                  } else {
                    policyItem.getGroups().add(newOwner.name());
                  }
                }
                finalPolicy.getPolicyItems().add(policyItem);
              });
    }
    try {
      if (policy.getId() == null) {
        rangerClient.createPolicy(policy);
      } else {
        rangerClient.updatePolicy(policy.getId(), policy);
      }
    } catch (RangerServiceException e) {
      throw new RuntimeException(e);
    }

    return Boolean.TRUE;
  }

  /**
   * Because one Ranger policy maybe contain multiple Gravitino securable objects, <br>
   * So we need to find the corresponding policy item mapping to set the user.
   */
  @Override
  public Boolean onGrantedRolesToUser(List<Role> roles, User user) throws RuntimeException {
    // If the user does not exist, then create it.
    onUserAdded(user);

    roles.stream()
        .forEach(
            role -> {
              createRangerRoleIfNotExists(role.name());
              GrantRevokeRoleRequest grantRevokeRoleRequest =
                  createGrantRevokeRoleRequest(role.name(), user.name(), null);
              try {
                rangerClient.grantRole(rangerServiceName, grantRevokeRoleRequest);
              } catch (RangerServiceException e) {
                // ignore exception, support idempotent operation
                LOG.warn("Grant role to user failed!", e);
              }
            });

    return Boolean.TRUE;
  }

  /**
   * Because one Ranger policy maybe contains multiple Gravitino securable objects, <br>
   * So we need to find the corresponding policy item mapping to remove the user.
   */
  @Override
  public Boolean onRevokedRolesFromUser(List<Role> roles, User user) throws RuntimeException {
    onUserAdded(user);
    roles.stream()
        .forEach(
            role -> {
              checkRangerRole(role.name());
              GrantRevokeRoleRequest grantRevokeRoleRequest =
                  createGrantRevokeRoleRequest(role.name(), user.name(), null);
              try {
                rangerClient.revokeRole(rangerServiceName, grantRevokeRoleRequest);
              } catch (RangerServiceException e) {
                // Ignore exception to support idempotent operation
              }
            });

    return Boolean.TRUE;
  }

  /**
   * Grant the roles to the group. <br>
   * 1. Create a group in the Ranger if the group does not exist. <br>
   * 2. Create a role in the Ranger if the role does not exist. <br>
   * 3. Add this group to the role. <br>
   * 4. Add the role to the policy item base the metadata object. <br>
   *
   * @param roles The roles to grant to the group.
   * @param group The group to grant the roles.
   */
  @Override
  public Boolean onGrantedRolesToGroup(List<Role> roles, Group group) throws RuntimeException {
    // If the group does not exist, then create it.
    onGroupAdded(group);

    roles.stream()
        .forEach(
            role -> {
              createRangerRoleIfNotExists(role.name());
              GrantRevokeRoleRequest grantRevokeRoleRequest =
                  createGrantRevokeRoleRequest(role.name(), null, group.name());
              try {
                rangerClient.grantRole(rangerServiceName, grantRevokeRoleRequest);
              } catch (RangerServiceException e) {
                // Ignore exception to support idempotent operation
              }
            });
    return Boolean.TRUE;
  }

  @Override
  public Boolean onRevokedRolesFromGroup(List<Role> roles, Group group) throws RuntimeException {
    onGroupAdded(group);
    roles.stream()
        .forEach(
            role -> {
              checkRangerRole(role.name());
              GrantRevokeRoleRequest grantRevokeRoleRequest =
                  createGrantRevokeRoleRequest(role.name(), null, group.name());
              try {
                rangerClient.revokeRole(rangerServiceName, grantRevokeRoleRequest);
              } catch (RangerServiceException e) {
                // Ignore exception to support idempotent operation
              }
            });

    return Boolean.TRUE;
  }

  @Override
  public Boolean onUserAdded(User user) throws RuntimeException {
    VXUserList list = rangerClient.searchUser(ImmutableMap.of("name", user.name()));
    if (list.getListSize() > 0) {
      LOG.warn("The user({}) is already exist in the Ranger!", user.name());
      return Boolean.FALSE;
    }

    VXUser rangerUser = VXUser.builder().withName(user.name()).withDescription(user.name()).build();
    return rangerClient.createUser(rangerUser);
  }

  @Override
  public Boolean onUserRemoved(User user) throws RuntimeException {
    VXUserList list = rangerClient.searchUser(ImmutableMap.of("name", user.name()));
    if (list.getListSize() == 0) {
      LOG.warn("The user({}) is not exist in the Ranger!", user);
      return Boolean.FALSE;
    }
    rangerClient.deleteUser(list.getList().get(0).getId());
    return Boolean.TRUE;
  }

  @Override
  public Boolean onUserAcquired(User user) throws RuntimeException {
    VXUserList list = rangerClient.searchUser(ImmutableMap.of("name", user.name()));
    if (list.getListSize() == 0) {
      LOG.warn("The user({}) is not exist in the Ranger!", user);
      return Boolean.FALSE;
    }
    return Boolean.TRUE;
  }

  @Override
  public Boolean onGroupAdded(Group group) throws RuntimeException {
    return rangerClient.createGroup(VXGroup.builder().withName(group.name()).build());
  }

  @Override
  public Boolean onGroupRemoved(Group group) throws RuntimeException {
    VXGroupList list = rangerClient.searchGroup(ImmutableMap.of("name", group.name()));
    if (list.getListSize() == 0) {
      LOG.warn("The group({}) is not exists in the Ranger!", group);
      return Boolean.FALSE;
    }
    return rangerClient.deleteGroup(list.getList().get(0).getId());
  }

  @Override
  public Boolean onGroupAcquired(Group group) {
    VXGroupList vxGroupList = rangerClient.searchGroup(ImmutableMap.of("name", group.name()));
    if (vxGroupList.getListSize() == 0) {
      LOG.warn("The group({}) is not exists in the Ranger!", group);
      return Boolean.FALSE;
    }
    return Boolean.TRUE;
  }

  private boolean checkRangerRole(String roleName) throws AuthorizationPluginException {
    try {
      rangerClient.getRole(roleName, RANGER_ADMIN_NAME, rangerServiceName);
    } catch (RangerServiceException e) {
      throw new AuthorizationPluginException(e);
    }
    return true;
  }

  private GrantRevokeRoleRequest createGrantRevokeRoleRequest(
      String roleName, String userName, String groupName) {
    Set<String> users;
    if (userName == null || userName.isEmpty()) {
      users = new HashSet<>();
    } else {
      users = new HashSet<>(Arrays.asList(userName));
    }
    Set<String> groups;
    if (groupName == null || groupName.isEmpty()) {
      groups = new HashSet<>();
    } else {
      groups = new HashSet<>(Arrays.asList(groupName));
    }

    GrantRevokeRoleRequest roleRequest = new GrantRevokeRoleRequest();
    roleRequest.setUsers(users);
    roleRequest.setGroups(groups);
    roleRequest.setGrantor(RANGER_ADMIN_NAME);
    roleRequest.setTargetRoles(new HashSet<>(Arrays.asList(roleName)));
    return roleRequest;
  }

  /** Create a Ranger role if the role does not exist. */
  private RangerRole createRangerRoleIfNotExists(String roleName) {
    RangerRole rangerRole = null;
    try {
      rangerRole = rangerClient.getRole(roleName, RANGER_ADMIN_NAME, rangerServiceName);
    } catch (RangerServiceException e) {
      // ignore exception, If the role does not exist, then create it.
    }
    try {
      if (rangerRole == null) {
        rangerRole = new RangerRole(roleName, MANAGED_BY_GRAVITINO, null, null, null);
        rangerClient.createRole(rangerServiceName, rangerRole);
      }
    } catch (RangerServiceException e) {
      throw new RuntimeException(e);
    }
    return rangerRole;
  }

  /**
   * Find the managed policy for the metadata object.
   *
   * @param metadataObject The metadata object to find the managed policy.
   * @return The managed policy for the metadata object.
   */
  @VisibleForTesting
  public RangerPolicy findManagedPolicy(MetadataObject metadataObject)
      throws AuthorizationPluginException {
    List<String> nsMetadataObj =
        Lists.newArrayList(SecurableObjects.DOT_SPLITTER.splitToList(metadataObject.fullName()));
    nsMetadataObj.remove(0); // skip `catalog`
    Map<String, String> policyFilter = new HashMap<>();
    Map<String, String> preciseFilterKeysFilter = new HashMap<>();
    policyFilter.put(RangerDefines.SEARCH_FILTER_SERVICE_NAME, this.rangerServiceName);
    policyFilter.put(SearchFilter.POLICY_LABELS_PARTIAL, MANAGED_BY_GRAVITINO);
    for (int i = 0; i < nsMetadataObj.size(); i++) {
      policyFilter.put(policyFilterKeys.get(i), nsMetadataObj.get(i));
      preciseFilterKeysFilter.put(policyPreciseFilterKeys.get(i), nsMetadataObj.get(i));
    }

    try {
      List<RangerPolicy> policies = rangerClient.findPolicies(policyFilter);

      if (!policies.isEmpty()) {
        // Because Ranger doesn't support the precise filter, Ranger will return the policy meets
        // the wildcard(*,?) conditions, just like `*.*.*` policy will match `db1.table1.column1`
        // So we need to manually precisely filter the policies.
        policies =
            policies.stream()
                .filter(
                    policy ->
                        policy.getResources().entrySet().stream()
                            .allMatch(
                                entry ->
                                    preciseFilterKeysFilter.containsKey(entry.getKey())
                                        && entry.getValue().getValues().size() == 1
                                        && entry
                                            .getValue()
                                            .getValues()
                                            .contains(preciseFilterKeysFilter.get(entry.getKey()))))
                .collect(Collectors.toList());
      }

      // Only return the policies that are managed by Gravitino.
      if (policies.size() > 1) {
        throw new AuthorizationPluginException(
            "Each metadata object only have one Gravitino management enable policies.");
      }

      RangerPolicy policy = policies.size() == 1 ? policies.get(0) : null;
      // Didn't contain duplicate privilege in the delegate Gravitino management policy
      if (policy != null) {
        policy.getPolicyItems().forEach(this::checkPolicyItemAccess);
        policy.getDenyPolicyItems().forEach(this::checkPolicyItemAccess);
        policy.getRowFilterPolicyItems().forEach(this::checkPolicyItemAccess);
        policy.getDataMaskPolicyItems().forEach(this::checkPolicyItemAccess);
      }

      return policy;
    } catch (RangerServiceException e) {
      throw new AuthorizationPluginException(e);
    }
  }

  /**
   * For easy management, each privilege will create one RangerPolicyItemAccess in the policy.
   *
   * @param policyItem The policy item to check
   * @throws AuthorizationPluginException If the policy item contains more than one access type
   */
  void checkPolicyItemAccess(RangerPolicy.RangerPolicyItem policyItem)
      throws AuthorizationPluginException {
    if (policyItem.getAccesses().size() != 1) {
      throw new AuthorizationPluginException(
          "The access type only have one in the delegate Gravitino management policy");
    }
    Map<String, Boolean> mapAccesses = new HashMap<>();
    policyItem
        .getAccesses()
        .forEach(
            access -> {
              if (mapAccesses.containsKey(access.getType()) && mapAccesses.get(access.getType())) {
                throw new AuthorizationPluginException(
                    "Contain duplicate privilege(%s) in the delegate Gravitino management policy ",
                    access.getType());
              }
              mapAccesses.put(access.getType(), true);
            });
  }

  /**
   * Add the securable object's privilege to the policy. <br>
   * 1. Find the policy base the metadata object. <br>
   * 2. If the policy exists and has the same privilege, because support idempotent operation, so
   * return true. <br>
   * 3. If the policy exists but has different privileges, also return true. Because one Ranger
   * policy maybe contain multiple Gravitino securable object <br>
   * 4. If the policy does not exist, then create a new policy. <br>
   */
  private boolean doAddSecurableObject(RoleChange.AddSecurableObject change) {
    RangerPolicy policy = findManagedPolicy(change.getSecurableObject());

    if (policy != null) {
      // Check the policy item's accesses and roles equal the Gravitino securable object's privilege
      // and roleName
      Set<String> policyPrivileges =
          policy.getPolicyItems().stream()
              .filter(policyItem -> policyItem.getRoles().contains(change.getRoleName()))
              .flatMap(policyItem -> policyItem.getAccesses().stream())
              .map(RangerPolicy.RangerPolicyItemAccess::getType)
              .collect(Collectors.toSet());
      Set<String> newPrivileges =
          change.getSecurableObject().privileges().stream()
              .filter(Objects::nonNull)
              .flatMap(privilege -> translatePrivilege(privilege.name()).stream())
              .filter(Objects::nonNull)
              .collect(Collectors.toSet());
      if (policyPrivileges.containsAll(newPrivileges)) {
        LOG.info(
            "The privilege({}) already added to Ranger policy({})!",
            policy.getName(),
            change.getSecurableObject().fullName());
        // If exist policy and have same privilege then directly return true, because support
        // idempotent operation.
        return true;
      }
    } else {
      policy = new RangerPolicy();
      policy.setService(rangerServiceName);
      policy.setName(change.getSecurableObject().fullName());
      policy.setPolicyLabels(Lists.newArrayList(MANAGED_BY_GRAVITINO));

      List<String> nsMetadataObject =
          Lists.newArrayList(
              SecurableObjects.DOT_SPLITTER.splitToList(change.getSecurableObject().fullName()));
      if (nsMetadataObject.size() > 4) {
        // The max level of the securable object is `catalog.db.table.column`
        throw new RuntimeException("The securable object than 4");
      }
      nsMetadataObject.remove(0); // remove `catalog`

      List<String> rangerDefinesList =
          Lists.newArrayList(
              RangerDefines.RESOURCE_DATABASE,
              RangerDefines.RESOURCE_TABLE,
              RangerDefines.RESOURCE_COLUMN);
      for (int i = 0; i < nsMetadataObject.size(); i++) {
        RangerPolicy.RangerPolicyResource policyResource =
            new RangerPolicy.RangerPolicyResource(nsMetadataObject.get(i));
        policy.getResources().put(rangerDefinesList.get(i), policyResource);
      }
    }

    addPolicyItem(policy, change.getRoleName(), change.getSecurableObject());
    try {
      if (policy.getId() == null) {
        rangerClient.createPolicy(policy);
      } else {
        rangerClient.updatePolicy(policy.getId(), policy);
      }
    } catch (RangerServiceException e) {
      throw new RuntimeException(e);
    }

    return true;
  }

  /**
   * Remove the securable object's privilege from the policy. <br>
   * Because Ranger uses unique metadata to location Ranger policy and manages all the privileges,
   * So A Ranger policy maybe contains multiple Gravitino privilege objects. <br>
   * Remove Ranger policy item condition is: <br>
   * 1. This Ranger policy item's accesses are equal the Gravitino securable object's privilege.
   * <br>
   * 2. This Ranger policy item's users and groups are empty. <br>
   * If policy didn't have any policy item, then delete this policy: <br>
   */
  private boolean doRemoveSecurableObject(
      String roleName, RoleChange.RemoveSecurableObject change) {
    RangerPolicy policy = findManagedPolicy(change.getSecurableObject());

    if (policy != null) {
      policy
          .getPolicyItems()
          .forEach(
              policyItem -> {
                boolean match =
                    policyItem.getAccesses().stream()
                        .allMatch(
                            // Find the policy item that match access and role
                            access -> {
                              // Use Gravitino privilege to search the Ranger policy item's access
                              boolean matchPrivilege =
                                  change.getSecurableObject().privileges().stream()
                                      .filter(Objects::nonNull)
                                      .flatMap(
                                          privilege ->
                                              translatePrivilege(privilege.name()).stream())
                                      .filter(Objects::nonNull)
                                      .anyMatch(privilege -> privilege.equals(access.getType()));
                              return matchPrivilege;
                            });
                if (match) {
                  policyItem.getRoles().removeIf(roleName::equals);
                }
              });

      // If the policy does have any role and user and group, then remove it.
      policy
          .getPolicyItems()
          .removeIf(
              policyItem ->
                  policyItem.getRoles().isEmpty()
                      && policyItem.getUsers().isEmpty()
                      && policyItem.getGroups().isEmpty());

      try {
        if (policy.getPolicyItems().size() == 0) {
          rangerClient.deletePolicy(policy.getId());
        } else {
          rangerClient.updatePolicy(policy.getId(), policy);
        }
      } catch (RangerServiceException e) {
        LOG.error("Failed to remove the policy item from the Ranger policy {}!", policy);
        throw new RuntimeException(e);
      }
    } else {
      LOG.warn(
          "Cannot find the Ranger policy({}) for the Gravitino securable object({})!",
          roleName,
          change.getSecurableObject().fullName());
      // Don't throw exception or return false, because need support immutable operation.
    }
    return true;
  }

  /**
   * 1. Find the policy base securable object. <br>
   * 2. If the policy exists, then user new securable object's privilege to update. <br>
   * 3. If the policy does not exist, return false. <br>
   */
  private boolean doUpdateSecurableObject(
      String roleName, RoleChange.UpdateSecurableObject change) {
    RangerPolicy policy = findManagedPolicy(change.getSecurableObject());

    if (policy != null) {
      removePolicyItem(policy, roleName, change.getSecurableObject());
      addPolicyItem(policy, roleName, change.getNewSecurableObject());
      try {
        if (policy.getId() == null) {
          rangerClient.createPolicy(policy);
        } else {
          rangerClient.updatePolicy(policy.getId(), policy);
        }
      } catch (RangerServiceException e) {
        throw new RuntimeException(e);
      }
    } else {
      LOG.warn(
          "Cannot find the policy({}) for the securable object({})!",
          roleName,
          change.getSecurableObject().fullName());
      return false;
    }
    return true;
  }

  /**
   * Add policy item access items base the securable object's privileges. <br>
   * We didn't clean the policy items because one Ranger policy maybe contains multiple Gravitino
   * securable objects. <br>
   */
  private void addPolicyItem(
      RangerPolicy policy, String roleName, SecurableObject securableObject) {
    // First check the privilege if support in the Ranger Hive
    checkSecurableObject(securableObject);

    // Add the policy items by the securable object's privileges
    securableObject
        .privileges()
        .forEach(
            gravitinoPrivilege -> {
              // Translate the Gravitino privilege to map Ranger privilege
              translatePrivilege(gravitinoPrivilege.name())
                  .forEach(
                      mappedPrivilege -> {
                        // Find the policy item that matches Gravitino privilege
                        List<RangerPolicy.RangerPolicyItem> matchPolicyItems =
                            policy.getPolicyItems().stream()
                                .filter(
                                    policyItem -> {
                                      return policyItem.getAccesses().stream()
                                          .anyMatch(
                                              access -> access.getType().equals(mappedPrivilege));
                                    })
                                .collect(Collectors.toList());

                        if (matchPolicyItems.size() == 0) {
                          // If the policy item does not exist, then create a new policy item
                          RangerPolicy.RangerPolicyItem policyItem =
                              new RangerPolicy.RangerPolicyItem();
                          RangerPolicy.RangerPolicyItemAccess access =
                              new RangerPolicy.RangerPolicyItemAccess();
                          access.setType(mappedPrivilege);
                          policyItem.getAccesses().add(access);
                          policyItem.getRoles().add(roleName);
                          if (Privilege.Condition.ALLOW == gravitinoPrivilege.condition()) {
                            policy.getPolicyItems().add(policyItem);
                          } else {
                            policy.getDenyPolicyItems().add(policyItem);
                          }
                        } else {
                          // If the policy item exists, then add the role to the policy item
                          matchPolicyItems.stream()
                              .forEach(
                                  policyItem -> {
                                    // If the role is not in the policy item, then add it
                                    if (!policyItem.getRoles().contains(roleName)) {
                                      policyItem.getRoles().add(roleName);
                                    }
                                  });
                        }
                      });
            });
  }

  /**
   * Remove policy item base the securable object's privileges and role name. <br>
   * We didn't directly clean the policy items, because one Ranger policy maybe contains multiple
   * Gravitino privilege objects. <br>
   */
  private void removePolicyItem(
      RangerPolicy policy, String roleName, SecurableObject securableObject) {
    // First check the privilege if support in the Ranger Hive
    checkSecurableObject(securableObject);

    // Delete the policy role base the securable object's privileges
    policy.getPolicyItems().stream()
        .forEach(
            policyItem -> {
              policyItem
                  .getAccesses()
                  .forEach(
                      access -> {
                        boolean matchPrivilege =
                            securableObject.privileges().stream()
                                .filter(Objects::nonNull)
                                .flatMap(privilege -> translatePrivilege(privilege.name()).stream())
                                .filter(Objects::nonNull)
                                .anyMatch(
                                    privilege -> {
                                      return access.getType().equals(privilege);
                                    });
                        if (matchPrivilege
                            && !policyItem.getUsers().isEmpty()
                            && !policyItem.getGroups().isEmpty()) {
                          // Not ownership policy item, then remove the role
                          policyItem.getRoles().removeIf(roleName::equals);
                        }
                      });
            });

    // Delete the policy items if the roles are empty and not ownership policy item
    policy
        .getPolicyItems()
        .removeIf(
            policyItem -> {
              return policyItem.getRoles().isEmpty()
                  && policyItem.getUsers().isEmpty()
                  && policyItem.getGroups().isEmpty();
            });
  }

  private boolean checkSecurableObject(SecurableObject securableObject) {
    securableObject
        .privileges()
        .forEach(
            privilege -> {
              check(
                  checkPrivilege(privilege.name()),
                  "This privilege %s is not support in the Ranger hive authorization",
                  privilege.name());
            });
    return true;
  }

  @Override
  public void close() throws IOException {}
}
