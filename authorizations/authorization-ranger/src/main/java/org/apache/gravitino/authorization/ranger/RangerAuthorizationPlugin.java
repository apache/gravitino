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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.Group;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.RoleChange;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.authorization.ranger.reference.VXGroup;
import org.apache.gravitino.authorization.ranger.reference.VXGroupList;
import org.apache.gravitino.authorization.ranger.reference.VXUser;
import org.apache.gravitino.authorization.ranger.reference.VXUserList;
import org.apache.gravitino.connector.AuthorizationPropertiesMeta;
import org.apache.gravitino.connector.authorization.AuthorizationPlugin;
import org.apache.gravitino.exceptions.AuthorizationPluginException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.ranger.RangerServiceException;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.util.GrantRevokeRoleRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ranger authorization operations plugin abstract class. <br>
 * 1. For Ranger limit, The same metadata object only has a unique Ranger policy, So a Ranger policy
 * maybe contains multiple Gravitino securable objects. <br>
 * 2. For easy management, each Ranger privilege will create one RangerPolicyItemAccess in the
 * policy. <br>
 * 3. Ranger also have Role concept, and support adds multiple users or groups in the Ranger Role,
 * So we can use the Ranger Role to implement the Gravitino Role. <br>
 * 4. The Ranger policy also supports multiple users and groups, But we only use a user or group to
 * implement Gravitino Owner concept. <br>
 */
public abstract class RangerAuthorizationPlugin
    implements AuthorizationPlugin, RangerPrivilegesMappingProvider, RangerMetadataObjectRule {
  private static final Logger LOG = LoggerFactory.getLogger(RangerAuthorizationPlugin.class);

  protected final String rangerServiceName;
  protected final RangerClientExtension rangerClient;
  private final RangerHelper rangerHelper;
  @VisibleForTesting public final String rangerAdminName;

  protected RangerAuthorizationPlugin(Map<String, String> config) {
    String rangerUrl = config.get(AuthorizationPropertiesMeta.RANGER_ADMIN_URL);
    String authType = config.get(AuthorizationPropertiesMeta.RANGER_AUTH_TYPE);
    rangerAdminName = config.get(AuthorizationPropertiesMeta.RANGER_USERNAME);
    // Apache Ranger Password should be minimum 8 characters with min one alphabet and one numeric.
    String password = config.get(AuthorizationPropertiesMeta.RANGER_PASSWORD);
    rangerServiceName = config.get(AuthorizationPropertiesMeta.RANGER_SERVICE_NAME);
    Preconditions.checkArgument(rangerUrl != null, "Ranger admin URL is required");
    Preconditions.checkArgument(authType != null, "Ranger auth type is required");
    Preconditions.checkArgument(rangerAdminName != null, "Ranger username is required");
    Preconditions.checkArgument(password != null, "Ranger password is required");
    Preconditions.checkArgument(rangerServiceName != null, "Ranger service name is required");
    rangerClient = new RangerClientExtension(rangerUrl, authType, rangerAdminName, password);

    rangerHelper =
        new RangerHelper(
            rangerClient,
            rangerAdminName,
            rangerServiceName,
            ownerMappingRule(),
            policyResourceDefinesRule());
  }

  /**
   * Create a new role in the Ranger. <br>
   * 1. Create a policy for metadata object. <br>
   * 2. Save role name in the Policy items. <br>
   */
  @Override
  public Boolean onRoleCreated(Role role) throws RuntimeException {
    if (!validAuthorizationOperation(role.securableObjects())) {
      return false;
    }

    rangerHelper.createRangerRoleIfNotExists(role.name(), false);
    return onRoleUpdated(
        role,
        role.securableObjects().stream()
            .map(securableObject -> RoleChange.addSecurableObject(role.name(), securableObject))
            .toArray(RoleChange[]::new));
  }

  @Override
  public Boolean onRoleAcquired(Role role) throws RuntimeException {
    if (!validAuthorizationOperation(role.securableObjects())) {
      return false;
    }
    return rangerHelper.checkRangerRole(role.name());
  }

  /** Remove the role name from the Ranger policy item, and delete this Role in the Ranger. <br> */
  @Override
  public Boolean onRoleDeleted(Role role) throws RuntimeException {
    if (!validAuthorizationOperation(role.securableObjects())) {
      return false;
    }
    // First, remove the role in the Ranger policy
    onRoleUpdated(
        role,
        role.securableObjects().stream()
            .map(securableObject -> RoleChange.removeSecurableObject(role.name(), securableObject))
            .toArray(RoleChange[]::new));
    // Lastly, delete the role in the Ranger
    try {
      rangerClient.deleteRole(role.name(), rangerAdminName, rangerServiceName);
    } catch (RangerServiceException e) {
      // Ignore exception to support idempotent operation
      LOG.warn("Ranger delete role: {} failed!", role, e);
    }
    return Boolean.TRUE;
  }

  @Override
  public Boolean onRoleUpdated(Role role, RoleChange... changes) throws RuntimeException {
    for (RoleChange change : changes) {
      if (change instanceof RoleChange.AddSecurableObject) {
        SecurableObject securableObject =
            ((RoleChange.AddSecurableObject) change).getSecurableObject();
        if (!validAuthorizationOperation(Arrays.asList(securableObject))) {
          return false;
        }

        List<RangerSecurableObject> rangerSecurableObjects = translatePrivilege(securableObject);
        rangerSecurableObjects.stream()
            .forEach(
                rangerSecurableObject -> {
                  if (!doAddSecurableObject(role.name(), rangerSecurableObject)) {
                    throw new RuntimeException(
                        "Failed to add the securable object to the Ranger policy!");
                  }
                });
      } else if (change instanceof RoleChange.RemoveSecurableObject) {
        SecurableObject securableObject =
            ((RoleChange.RemoveSecurableObject) change).getSecurableObject();
        if (!validAuthorizationOperation(Arrays.asList(securableObject))) {
          return false;
        }

        List<RangerSecurableObject> rangerSecurableObjects = translatePrivilege(securableObject);
        rangerSecurableObjects.stream()
            .forEach(
                rangerSecurableObject -> {
                  if (!doRemoveSecurableObject(role.name(), rangerSecurableObject)) {
                    throw new RuntimeException(
                        "Failed to add the securable object to the Ranger policy!");
                  }
                });
      } else if (change instanceof RoleChange.UpdateSecurableObject) {
        SecurableObject oldSecurableObject =
            ((RoleChange.UpdateSecurableObject) change).getSecurableObject();
        if (!validAuthorizationOperation(Arrays.asList(oldSecurableObject))) {
          return false;
        }
        SecurableObject newSecurableObject =
            ((RoleChange.UpdateSecurableObject) change).getNewSecurableObject();
        if (!validAuthorizationOperation(Arrays.asList(newSecurableObject))) {
          return false;
        }

        Preconditions.checkArgument(
            (oldSecurableObject.fullName().equals(newSecurableObject.fullName())
                && oldSecurableObject.type().equals(newSecurableObject.type())),
            "The old and new securable objects metadata must be equal!");
        List<RangerSecurableObject> rangerOldSecurableObjects =
            translatePrivilege(oldSecurableObject);
        List<RangerSecurableObject> rangerNewSecurableObjects =
            translatePrivilege(newSecurableObject);
        rangerOldSecurableObjects.stream()
            .forEach(
                rangerSecurableObject -> {
                  doRemoveSecurableObject(role.name(), rangerSecurableObject);
                });
        rangerNewSecurableObjects.stream()
            .forEach(
                rangerSecurableObject -> {
                  doAddSecurableObject(role.name(), rangerSecurableObject);
                });
      } else {
        throw new IllegalArgumentException(
            "Unsupported role change type: "
                + (change == null ? "null" : change.getClass().getSimpleName()));
      }
    }

    return Boolean.TRUE;
  }

  /**
   * Set or transfer the ownership of the metadata object. <br>
   * 1. If the metadata object already has an owner, then transfer the ownership from preOwner to
   * newOwner, The function params preOwner and newOwner don't equal null. <br>
   * 2. If the metadata object doesn't have an owner, then set the owner to newOwner, The function
   * params preOwner equal null and newOwner don't equal null. <br>
   *
   * @param metadataObject The metadata object to set the owner.
   * @param preOwner The previous owner of the metadata object.
   * @param newOwner The new owner of the metadata object.
   */
  @Override
  public Boolean onOwnerSet(MetadataObject metadataObject, Owner preOwner, Owner newOwner)
      throws RuntimeException {
    Preconditions.checkArgument(newOwner != null, "The newOwner must be not null");

    // Add the user or group to the Ranger
    String preOwnerUserName = null,
        preOwnerGroupName = null,
        newOwnerUserName = null,
        newOwnerGroupName = null;
    AuditInfo auditInfo =
        AuditInfo.builder()
            .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
            .withCreateTime(Instant.now())
            .build();
    if (preOwner != null) {
      if (preOwner.type() == Owner.Type.USER) {
        preOwnerUserName = newOwner.name();
      } else {
        preOwnerGroupName = newOwner.name();
      }
    }
    if (newOwner.type() == Owner.Type.USER) {
      newOwnerUserName = newOwner.name();
      UserEntity userEntity =
          UserEntity.builder()
              .withId(1L)
              .withName(newOwnerUserName)
              .withRoleNames(Collections.emptyList())
              .withRoleIds(Collections.emptyList())
              .withAuditInfo(auditInfo)
              .build();
      onUserAdded(userEntity);
    } else {
      newOwnerGroupName = newOwner.name();
      GroupEntity groupEntity =
          GroupEntity.builder()
              .withId(1L)
              .withName(newOwnerGroupName)
              .withRoleNames(Collections.emptyList())
              .withRoleIds(Collections.emptyList())
              .withAuditInfo(auditInfo)
              .build();
      onGroupAdded(groupEntity);
    }

    List<RangerSecurableObject> rangerSecurableObjects = translateOwner(metadataObject);
    String ownerRoleName;
    switch (metadataObject.type()) {
      case METALAKE:
      case CATALOG:
        // The metalake and catalog use role to manage the owner
        if (metadataObject.type() == MetadataObject.Type.METALAKE) {
          ownerRoleName = RangerHelper.GRAVITINO_METALAKE_OWNER_ROLE;
        } else {
          ownerRoleName = RangerHelper.GRAVITINO_CATALOG_OWNER_ROLE;
        }
        rangerHelper.createRangerRoleIfNotExists(ownerRoleName, true);
        try {
          if (preOwnerUserName != null || preOwnerGroupName != null) {
            GrantRevokeRoleRequest revokeRoleRequest =
                rangerHelper.createGrantRevokeRoleRequest(
                    ownerRoleName, preOwnerUserName, preOwnerGroupName);
            rangerClient.revokeRole(rangerServiceName, revokeRoleRequest);
          }
          if (newOwnerUserName != null || newOwnerGroupName != null) {
            GrantRevokeRoleRequest grantRoleRequest =
                rangerHelper.createGrantRevokeRoleRequest(
                    ownerRoleName, newOwnerUserName, newOwnerGroupName);
            rangerClient.grantRole(rangerServiceName, grantRoleRequest);
          }
        } catch (RangerServiceException e) {
          // Ignore exception, support idempotent operation
          LOG.warn("Grant owner role: {} failed!", ownerRoleName, e);
        }

        rangerSecurableObjects.stream()
            .forEach(
                rangerSecurableObject -> {
                  RangerPolicy policy = rangerHelper.findManagedPolicy(rangerSecurableObject);
                  try {
                    if (policy == null) {
                      policy =
                          rangerHelper.addOwnerRoleToNewPolicy(
                              rangerSecurableObject, ownerRoleName);
                      rangerClient.createPolicy(policy);
                    } else {
                      rangerHelper.updatePolicyOwnerRole(policy, ownerRoleName);
                      rangerClient.updatePolicy(policy.getId(), policy);
                    }
                  } catch (RangerServiceException e) {
                    throw new RuntimeException(e);
                  }
                });
        break;
      case SCHEMA:
      case TABLE:
        // The schema and table use user/group to manage the owner
        rangerSecurableObjects.stream()
            .forEach(
                rangerSecurableObject -> {
                  RangerPolicy policy = rangerHelper.findManagedPolicy(rangerSecurableObject);
                  try {
                    if (policy == null) {
                      policy = rangerHelper.addOwnerToNewPolicy(rangerSecurableObject, newOwner);
                      rangerClient.createPolicy(policy);
                    } else {
                      rangerHelper.updatePolicyOwner(policy, preOwner, newOwner);
                      rangerClient.updatePolicy(policy.getId(), policy);
                    }
                  } catch (RangerServiceException e) {
                    throw new RuntimeException(e);
                  }
                });
        break;
      default:
        throw new AuthorizationPluginException(
            "The owner privilege is not supported for the securable object: %s",
            metadataObject.type());
    }

    return Boolean.TRUE;
  }

  /**
   * Grant the roles to the user. <br>
   * 1. Create a user in the Ranger if the user does not exist. <br>
   * 2. Create a role in the Ranger if the role does not exist. <br>
   * 3. Add this user to the role. <br>
   *
   * @param roles The roles to grant to the group.
   * @param user The user to grant the roles.
   */
  @Override
  public Boolean onGrantedRolesToUser(List<Role> roles, User user) throws RuntimeException {
    if (roles.stream().anyMatch(role -> !validAuthorizationOperation(role.securableObjects()))) {
      return false;
    }

    // If the user does not exist, then create it.
    onUserAdded(user);

    roles.stream()
        .forEach(
            role -> {
              rangerHelper.createRangerRoleIfNotExists(role.name(), false);
              GrantRevokeRoleRequest grantRevokeRoleRequest =
                  rangerHelper.createGrantRevokeRoleRequest(role.name(), user.name(), null);
              try {
                rangerClient.grantRole(rangerServiceName, grantRevokeRoleRequest);
              } catch (RangerServiceException e) {
                // Ignore exception, support idempotent operation
                LOG.warn("Grant role: {} to user: {} failed!", role, user, e);
              }
            });

    return Boolean.TRUE;
  }

  /**
   * Revoke the roles from the user. <br>
   * 1. Create a user in the Ranger if the user does not exist. <br>
   * 2. Check the role if it exists in the Ranger. <br>
   * 3. Revoke this user from the role. <br>
   *
   * @param roles The roles to revoke the group.
   * @param user The user to revoke the roles.
   */
  @Override
  public Boolean onRevokedRolesFromUser(List<Role> roles, User user) throws RuntimeException {
    if (roles.stream().anyMatch(role -> !validAuthorizationOperation(role.securableObjects()))) {
      return false;
    }
    // If the user does not exist, then create it.
    onUserAdded(user);

    roles.stream()
        .forEach(
            role -> {
              rangerHelper.checkRangerRole(role.name());
              GrantRevokeRoleRequest grantRevokeRoleRequest =
                  rangerHelper.createGrantRevokeRoleRequest(role.name(), user.name(), null);
              try {
                rangerClient.revokeRole(rangerServiceName, grantRevokeRoleRequest);
              } catch (RangerServiceException e) {
                // Ignore exception to support idempotent operation
                LOG.warn("Revoke role: {} from user: {} failed!", role, user, e);
              }
            });

    return Boolean.TRUE;
  }

  /**
   * Grant the roles to the group. <br>
   * 1. Create a group in the Ranger if the group does not exist. <br>
   * 2. Create a role in the Ranger if the role does not exist. <br>
   * 3. Add this group to the role. <br>
   *
   * @param roles The roles to grant to the group.
   * @param group The group to grant the roles.
   */
  @Override
  public Boolean onGrantedRolesToGroup(List<Role> roles, Group group) throws RuntimeException {
    if (roles.stream().anyMatch(role -> !validAuthorizationOperation(role.securableObjects()))) {
      return false;
    }
    // If the group does not exist, then create it.
    onGroupAdded(group);

    roles.stream()
        .forEach(
            role -> {
              rangerHelper.createRangerRoleIfNotExists(role.name(), false);
              GrantRevokeRoleRequest grantRevokeRoleRequest =
                  rangerHelper.createGrantRevokeRoleRequest(role.name(), null, group.name());
              try {
                rangerClient.grantRole(rangerServiceName, grantRevokeRoleRequest);
              } catch (RangerServiceException e) {
                // Ignore exception to support idempotent operation
                LOG.warn("Grant role: {} to group: {} failed!", role, group, e);
              }
            });
    return Boolean.TRUE;
  }

  /**
   * Revoke the roles from the group. <br>
   * 1. Create a group in the Ranger if the group does not exist. <br>
   * 2. Check the role if it exists in the Ranger. <br>
   * 3. Revoke this group from the role. <br>
   *
   * @param roles The roles to revoke the group.
   * @param group The group to revoke the roles.
   */
  @Override
  public Boolean onRevokedRolesFromGroup(List<Role> roles, Group group) throws RuntimeException {
    if (roles.stream().anyMatch(role -> !validAuthorizationOperation(role.securableObjects()))) {
      return false;
    }
    onGroupAdded(group);
    roles.stream()
        .forEach(
            role -> {
              rangerHelper.checkRangerRole(role.name());
              GrantRevokeRoleRequest grantRevokeRoleRequest =
                  rangerHelper.createGrantRevokeRoleRequest(role.name(), null, group.name());
              try {
                rangerClient.revokeRole(rangerServiceName, grantRevokeRoleRequest);
              } catch (RangerServiceException e) {
                // Ignore exception to support idempotent operation
                LOG.warn("Revoke role: {} from group: {} failed!", role, group, e);
              }
            });

    return Boolean.TRUE;
  }

  @Override
  public Boolean onUserAdded(User user) throws RuntimeException {
    VXUserList list = rangerClient.searchUser(ImmutableMap.of("name", user.name()));
    if (list.getListSize() > 0) {
      LOG.warn("The user({}) already exists in the Ranger!", user.name());
      return Boolean.FALSE;
    }

    VXUser rangerUser = VXUser.builder().withName(user.name()).withDescription(user.name()).build();
    return rangerClient.createUser(rangerUser);
  }

  @Override
  public Boolean onUserRemoved(User user) throws RuntimeException {
    VXUserList list = rangerClient.searchUser(ImmutableMap.of("name", user.name()));
    if (list.getListSize() == 0) {
      LOG.warn("The user({}) doesn't exist in the Ranger!", user);
      return Boolean.FALSE;
    }
    rangerClient.deleteUser(list.getList().get(0).getId());
    return Boolean.TRUE;
  }

  @Override
  public Boolean onUserAcquired(User user) throws RuntimeException {
    VXUserList list = rangerClient.searchUser(ImmutableMap.of("name", user.name()));
    if (list.getListSize() == 0) {
      LOG.warn("The user({}) doesn't exist in the Ranger!", user);
      return Boolean.FALSE;
    }
    return Boolean.TRUE;
  }

  @Override
  public Boolean onGroupAdded(Group group) throws RuntimeException {
    return rangerClient.createGroup(
        VXGroup.builder().withName(group.name()).withDescription(group.name()).build());
  }

  @Override
  public Boolean onGroupRemoved(Group group) throws RuntimeException {
    VXGroupList list = rangerClient.searchGroup(ImmutableMap.of("name", group.name()));
    if (list.getListSize() == 0) {
      LOG.warn("The group({}) doesn't exist in the Ranger!", group);
      return Boolean.FALSE;
    }
    return rangerClient.deleteGroup(list.getList().get(0).getId());
  }

  @Override
  public Boolean onGroupAcquired(Group group) {
    VXGroupList vxGroupList = rangerClient.searchGroup(ImmutableMap.of("name", group.name()));
    if (vxGroupList.getListSize() == 0) {
      LOG.warn("The group({}) doesn't exist in the Ranger!", group);
      return Boolean.FALSE;
    }
    return Boolean.TRUE;
  }

  /**
   * Add the securable object's privilege to the Ranger policy. <br>
   * 1. Find the policy base the metadata object. <br>
   * 2. If the policy exists and has the same privilege, because support idempotent operation, so
   * return true. <br>
   * 3. If the policy does not exist, then create a new policy. <br>
   */
  private boolean doAddSecurableObject(String roleName, RangerSecurableObject securableObject) {
    RangerPolicy policy = rangerHelper.findManagedPolicy(securableObject);

    if (policy != null) {
      // Check the policy item's accesses and roles equal the Ranger securable object's privilege
      Set<RangerPrivilege> policyPrivileges =
          policy.getPolicyItems().stream()
              .filter(policyItem -> policyItem.getRoles().contains(roleName))
              .flatMap(policyItem -> policyItem.getAccesses().stream())
              .map(RangerPolicy.RangerPolicyItemAccess::getType)
              .map(RangerPrivileges::valueOf)
              .collect(Collectors.toSet());
      if (policyPrivileges.containsAll(securableObject.privileges())) {
        LOG.info(
            "The privilege({}) already added to Ranger policy({})!",
            policy.getName(),
            securableObject.fullName());
        // If it exists policy with the same privilege, then directly return true, because support
        // idempotent operation.
        return true;
      }
    } else {
      policy = rangerHelper.createPolicyAddResources(securableObject);
    }

    rangerHelper.addPolicyItem(policy, roleName, securableObject);
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
   * 1. Find the policy base the metadata object. <br>
   * 2. If the policy exists and has the same privilege, then remove role name in the policy items.
   * <br>
   * 3. If policy does not contain any policy item, then delete this policy. <br>
   */
  private boolean doRemoveSecurableObject(
      String roleName, RangerSecurableObject rangerSecurableObject) {
    RangerPolicy policy = rangerHelper.findManagedPolicy(rangerSecurableObject);
    if (policy == null) {
      LOG.warn(
          "Cannot find the Ranger policy for the Ranger securable object({})!",
          rangerSecurableObject.fullName());
      // Don't throw exception or return false, because need support immutable operation.
      return true;
    }

    policy
        .getPolicyItems()
        .forEach(
            policyItem -> {
              boolean match =
                  policyItem.getAccesses().stream()
                      .allMatch(
                          // Find the policy item that matches access and role
                          access -> {
                            // Use Gravitino privilege to search the Ranger policy item's access
                            boolean matchPrivilege =
                                rangerSecurableObject.privileges().stream()
                                    .filter(Objects::nonNull)
                                    .anyMatch(privilege -> privilege.equalsTo(access.getType()));
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
      if (policy.getPolicyItems().isEmpty()) {
        rangerClient.deletePolicy(policy.getId());
      } else {
        rangerClient.updatePolicy(policy.getId(), policy);
      }
    } catch (RangerServiceException e) {
      LOG.error("Failed to remove the policy item from the Ranger policy {}!", policy);
      throw new RuntimeException(e);
    }
    return true;
  }

  @Override
  public void close() throws IOException {}

  /** Generate different Ranger securable object */
  public RangerSecurableObject generateRangerSecurableObject(
      List<String> names, RangerMetadataObject.Type type, Set<RangerPrivilege> privileges) {
    validateRangerMetadataObject(names, type);
    RangerMetadataObject metadataObject =
        new RangerMetadataObjects.RangerMetadataObjectImpl(
            RangerMetadataObjects.getParentFullName(names),
            RangerMetadataObjects.getLastName(names),
            type);
    return new RangerSecurableObjects.RangerSecurableObjectImpl(
        metadataObject.parent(), metadataObject.name(), metadataObject.type(), privileges);
  }

  public boolean validAuthorizationOperation(List<SecurableObject> securableObjects) {
    return securableObjects.stream()
        .allMatch(
            securableObject -> {
              AtomicBoolean match = new AtomicBoolean(true);
              securableObject.privileges().stream()
                  .forEach(
                      privilege -> {
                        if (!allowPrivilegesRule().contains(privilege.name())) {
                          LOG.error(
                              "Authorization to ignore privilege({}) on metadata object({})!",
                              privilege.name(),
                              securableObject.fullName());
                          match.set(false);
                          return;
                        }

                        if (!privilege.canBindTo(securableObject.type())) {
                          LOG.error(
                              "The privilege({}) is not supported for the metadata object({})!",
                              privilege.name(),
                              securableObject.fullName());
                          match.set(false);
                        }
                      });
              return match.get();
            });
  }
}
