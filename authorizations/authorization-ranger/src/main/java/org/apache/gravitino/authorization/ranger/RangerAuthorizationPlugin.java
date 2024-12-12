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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.AuthorizationMetadataObject;
import org.apache.gravitino.authorization.AuthorizationPrivilege;
import org.apache.gravitino.authorization.AuthorizationPrivilegesMappingProvider;
import org.apache.gravitino.authorization.AuthorizationSecurableObject;
import org.apache.gravitino.authorization.Group;
import org.apache.gravitino.authorization.MetadataObjectChange;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Privilege;
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
    implements AuthorizationPlugin, AuthorizationPrivilegesMappingProvider {
  private static final Logger LOG = LoggerFactory.getLogger(RangerAuthorizationPlugin.class);

  protected String metalake;
  protected final String rangerServiceName;
  protected final RangerClientExtension rangerClient;
  private final RangerHelper rangerHelper;
  @VisibleForTesting public final String rangerAdminName;

  protected RangerAuthorizationPlugin(String metalake, Map<String, String> config) {
    this.metalake = metalake;
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

  @VisibleForTesting
  public String getMetalake() {
    return metalake;
  }

  /**
   * Set the Ranger policy resource defines rule.
   *
   * @return The policy resource defines rule.
   */
  public abstract List<String> policyResourceDefinesRule();

  /**
   * Create a new policy for metadata object
   *
   * @return The RangerPolicy for metadata object.
   */
  protected abstract RangerPolicy createPolicyAddResources(
      AuthorizationMetadataObject metadataObject);

  protected RangerPolicy addOwnerToNewPolicy(
      AuthorizationMetadataObject metadataObject, Owner newOwner) {
    RangerPolicy policy = createPolicyAddResources(metadataObject);
    ownerMappingRule()
        .forEach(
            ownerPrivilege -> {
              // Each owner's privilege will create one RangerPolicyItemAccess in the policy
              RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem();
              policyItem
                  .getAccesses()
                  .add(new RangerPolicy.RangerPolicyItemAccess(ownerPrivilege.getName()));
              if (newOwner != null) {
                if (newOwner.type() == Owner.Type.USER) {
                  policyItem.getUsers().add(newOwner.name());
                } else {
                  policyItem.getGroups().add(newOwner.name());
                }
                // mark the policy item is created by Gravitino
                policyItem.getRoles().add(RangerHelper.GRAVITINO_OWNER_ROLE);
              }
              policy.getPolicyItems().add(policyItem);
            });
    return policy;
  }

  protected RangerPolicy addOwnerRoleToNewPolicy(
      AuthorizationMetadataObject metadataObject, String ownerRoleName) {
    RangerPolicy policy = createPolicyAddResources(metadataObject);

    ownerMappingRule()
        .forEach(
            ownerPrivilege -> {
              // Each owner's privilege will create one RangerPolicyItemAccess in the policy
              RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem();
              policyItem
                  .getAccesses()
                  .add(new RangerPolicy.RangerPolicyItemAccess(ownerPrivilege.getName()));
              policyItem.getRoles().add(rangerHelper.generateGravitinoRoleName(ownerRoleName));
              policy.getPolicyItems().add(policyItem);
            });
    return policy;
  }

  /**
   * Create a new role in the Ranger. <br>
   * 1. Create a policy for metadata object. <br>
   * 2. Save role name in the Policy items. <br>
   */
  @Override
  public Boolean onRoleCreated(Role role) throws AuthorizationPluginException {
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
  public Boolean onRoleAcquired(Role role) throws AuthorizationPluginException {
    if (!validAuthorizationOperation(role.securableObjects())) {
      return false;
    }
    return rangerHelper.checkRangerRole(role.name());
  }

  /** Remove the role name from the Ranger policy item, and delete this Role in the Ranger. <br> */
  @Override
  public Boolean onRoleDeleted(Role role) throws AuthorizationPluginException {
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
      rangerClient.deleteRole(
          rangerHelper.generateGravitinoRoleName(role.name()), rangerAdminName, rangerServiceName);
    } catch (RangerServiceException e) {
      // Ignore exception to support idempotent operation
      LOG.warn("Ranger delete role: {} failed!", role, e);
    }
    return Boolean.TRUE;
  }

  @Override
  public Boolean onRoleUpdated(Role role, RoleChange... changes)
      throws AuthorizationPluginException {
    for (RoleChange change : changes) {
      if (change instanceof RoleChange.AddSecurableObject) {
        SecurableObject securableObject =
            ((RoleChange.AddSecurableObject) change).getSecurableObject();
        if (!validAuthorizationOperation(Arrays.asList(securableObject))) {
          return false;
        }

        List<AuthorizationSecurableObject> AuthorizationSecurableObjects =
            translatePrivilege(securableObject);
        AuthorizationSecurableObjects.stream()
            .forEach(
                AuthorizationSecurableObject -> {
                  if (!doAddSecurableObject(role.name(), AuthorizationSecurableObject)) {
                    throw new AuthorizationPluginException(
                        "Failed to add the securable object to the Ranger policy!");
                  }
                });
      } else if (change instanceof RoleChange.RemoveSecurableObject) {
        SecurableObject securableObject =
            ((RoleChange.RemoveSecurableObject) change).getSecurableObject();
        if (!validAuthorizationOperation(Arrays.asList(securableObject))) {
          return false;
        }

        List<AuthorizationSecurableObject> AuthorizationSecurableObjects =
            translatePrivilege(securableObject);
        AuthorizationSecurableObjects.stream()
            .forEach(
                AuthorizationSecurableObject -> {
                  if (!doRemoveSecurableObject(role.name(), AuthorizationSecurableObject)) {
                    throw new AuthorizationPluginException(
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
        List<AuthorizationSecurableObject> rangerOldSecurableObjects =
            translatePrivilege(oldSecurableObject);
        List<AuthorizationSecurableObject> rangerNewSecurableObjects =
            translatePrivilege(newSecurableObject);
        rangerOldSecurableObjects.stream()
            .forEach(
                AuthorizationSecurableObject -> {
                  doRemoveSecurableObject(role.name(), AuthorizationSecurableObject);
                });
        rangerNewSecurableObjects.stream()
            .forEach(
                AuthorizationSecurableObject -> {
                  doAddSecurableObject(role.name(), AuthorizationSecurableObject);
                });
      } else {
        throw new IllegalArgumentException(
            "Unsupported role change type: "
                + (change == null ? "null" : change.getClass().getSimpleName()));
      }
    }

    return Boolean.TRUE;
  }

  @Override
  public Boolean onMetadataUpdated(MetadataObjectChange... changes) throws RuntimeException {
    for (MetadataObjectChange change : changes) {
      if (change instanceof MetadataObjectChange.RenameMetadataObject) {
        MetadataObject metadataObject =
            ((MetadataObjectChange.RenameMetadataObject) change).metadataObject();
        MetadataObject newMetadataObject =
            ((MetadataObjectChange.RenameMetadataObject) change).newMetadataObject();
        if (metadataObject.type() == MetadataObject.Type.METALAKE
            && newMetadataObject.type() == MetadataObject.Type.METALAKE) {
          // Modify the metalake name
          this.metalake = newMetadataObject.name();
        }
        AuthorizationMetadataObject oldAuthMetadataObject = translateMetadataObject(metadataObject);
        AuthorizationMetadataObject newAuthMetadataObject =
            translateMetadataObject(newMetadataObject);
        if (oldAuthMetadataObject.equals(newAuthMetadataObject)) {
          LOG.info(
              "The metadata object({}) and new metadata object({}) are equal, so ignore rename!",
              oldAuthMetadataObject.fullName(),
              newAuthMetadataObject.fullName());
          continue;
        }
        doRenameMetadataObject(oldAuthMetadataObject, newAuthMetadataObject);
      } else if (change instanceof MetadataObjectChange.RemoveMetadataObject) {
        MetadataObject metadataObject =
            ((MetadataObjectChange.RemoveMetadataObject) change).metadataObject();
        if (metadataObject.type() != MetadataObject.Type.FILESET) {
          AuthorizationMetadataObject AuthorizationMetadataObject =
              translateMetadataObject(metadataObject);
          doRemoveMetadataObject(AuthorizationMetadataObject);
        }
      } else {
        throw new IllegalArgumentException(
            "Unsupported metadata object change type: "
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
      throws AuthorizationPluginException {
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

    List<AuthorizationSecurableObject> AuthorizationSecurableObjects =
        translateOwner(metadataObject);
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
        rangerHelper.createRangerRoleIfNotExists(RangerHelper.GRAVITINO_OWNER_ROLE, true);
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

        AuthorizationSecurableObjects.stream()
            .forEach(
                AuthorizationSecurableObject -> {
                  RangerPolicy policy =
                      rangerHelper.findManagedPolicy(AuthorizationSecurableObject);
                  try {
                    if (policy == null) {
                      policy = addOwnerRoleToNewPolicy(AuthorizationSecurableObject, ownerRoleName);
                      rangerClient.createPolicy(policy);
                    } else {
                      rangerHelper.updatePolicyOwnerRole(policy, ownerRoleName);
                      rangerClient.updatePolicy(policy.getId(), policy);
                    }
                  } catch (RangerServiceException e) {
                    throw new AuthorizationPluginException(
                        e, "Failed to add the owner to the Ranger!");
                  }
                });
        break;
      case SCHEMA:
      case TABLE:
      case FILESET:
        // The schema and table use user/group to manage the owner
        AuthorizationSecurableObjects.stream()
            .forEach(
                AuthorizationSecurableObject -> {
                  RangerPolicy policy =
                      rangerHelper.findManagedPolicy(AuthorizationSecurableObject);
                  try {
                    if (policy == null) {
                      policy = addOwnerToNewPolicy(AuthorizationSecurableObject, newOwner);
                      rangerClient.createPolicy(policy);
                    } else {
                      rangerHelper.updatePolicyOwner(policy, preOwner, newOwner);
                      rangerClient.updatePolicy(policy.getId(), policy);
                    }
                  } catch (RangerServiceException e) {
                    throw new AuthorizationPluginException(
                        e, "Failed to add the owner to the Ranger!");
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
  public Boolean onGrantedRolesToUser(List<Role> roles, User user)
      throws AuthorizationPluginException {
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
  public Boolean onRevokedRolesFromUser(List<Role> roles, User user)
      throws AuthorizationPluginException {
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
  public Boolean onGrantedRolesToGroup(List<Role> roles, Group group)
      throws AuthorizationPluginException {
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
  public Boolean onRevokedRolesFromGroup(List<Role> roles, Group group)
      throws AuthorizationPluginException {
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
  public Boolean onUserAdded(User user) throws AuthorizationPluginException {
    VXUserList list = rangerClient.searchUser(ImmutableMap.of("name", user.name()));
    if (list.getListSize() > 0) {
      LOG.warn("The user({}) already exists in the Ranger!", user.name());
      return Boolean.FALSE;
    }

    VXUser rangerUser = VXUser.builder().withName(user.name()).withDescription(user.name()).build();
    return rangerClient.createUser(rangerUser);
  }

  @Override
  public Boolean onUserRemoved(User user) throws AuthorizationPluginException {
    VXUserList list = rangerClient.searchUser(ImmutableMap.of("name", user.name()));
    if (list.getListSize() == 0) {
      LOG.warn("The user({}) doesn't exist in the Ranger!", user);
      return Boolean.FALSE;
    }
    rangerClient.deleteUser(list.getList().get(0).getId());
    return Boolean.TRUE;
  }

  @Override
  public Boolean onUserAcquired(User user) throws AuthorizationPluginException {
    VXUserList list = rangerClient.searchUser(ImmutableMap.of("name", user.name()));
    if (list.getListSize() == 0) {
      LOG.warn("The user({}) doesn't exist in the Ranger!", user);
      return Boolean.FALSE;
    }
    return Boolean.TRUE;
  }

  @Override
  public Boolean onGroupAdded(Group group) throws AuthorizationPluginException {
    return rangerClient.createGroup(
        VXGroup.builder().withName(group.name()).withDescription(group.name()).build());
  }

  @Override
  public Boolean onGroupRemoved(Group group) throws AuthorizationPluginException {
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
  private boolean doAddSecurableObject(
      String roleName, AuthorizationSecurableObject securableObject) {
    RangerPolicy policy = rangerHelper.findManagedPolicy(securableObject);
    if (policy != null) {
      // Check the policy item's accesses and roles equal the Ranger securable object's privilege
      List<AuthorizationPrivilege> allowPrivilies =
          securableObject.privileges().stream()
              .filter(privilege -> privilege.condition() == Privilege.Condition.ALLOW)
              .collect(Collectors.toList());
      List<AuthorizationPrivilege> denyPrivilies =
          securableObject.privileges().stream()
              .filter(privilege -> privilege.condition() == Privilege.Condition.DENY)
              .collect(Collectors.toList());

      Set<AuthorizationPrivilege> policyPrivileges =
          policy.getPolicyItems().stream()
              .filter(
                  policyItem ->
                      policyItem
                          .getRoles()
                          .contains(rangerHelper.generateGravitinoRoleName(roleName)))
              .flatMap(policyItem -> policyItem.getAccesses().stream())
              .map(RangerPolicy.RangerPolicyItemAccess::getType)
              .map(RangerPrivileges::valueOf)
              .collect(Collectors.toSet());

      Set<AuthorizationPrivilege> policyDenyPrivileges =
          policy.getDenyPolicyItems().stream()
              .filter(
                  policyItem ->
                      policyItem
                          .getRoles()
                          .contains(rangerHelper.generateGravitinoRoleName(roleName)))
              .flatMap(policyItem -> policyItem.getAccesses().stream())
              .map(RangerPolicy.RangerPolicyItemAccess::getType)
              .map(RangerPrivileges::valueOf)
              .collect(Collectors.toSet());

      if (policyPrivileges.containsAll(allowPrivilies)
          && policyDenyPrivileges.containsAll(denyPrivilies)) {
        LOG.info(
            "The privilege({}) already added to Ranger policy({})!",
            policy.getName(),
            securableObject.fullName());
        // If it exists policy with the same privilege, then directly return true, because support
        // idempotent operation.
        return true;
      }
    } else {
      policy = createPolicyAddResources(securableObject);
    }

    rangerHelper.addPolicyItem(policy, roleName, securableObject);
    try {
      if (policy.getId() == null) {
        rangerClient.createPolicy(policy);
      } else {
        rangerClient.updatePolicy(policy.getId(), policy);
      }
    } catch (RangerServiceException e) {
      throw new AuthorizationPluginException(
          e, "Failed to add the securable object to the Ranger!");
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
      String roleName, AuthorizationSecurableObject AuthorizationSecurableObject) {
    RangerPolicy policy = rangerHelper.findManagedPolicy(AuthorizationSecurableObject);
    if (policy == null) {
      LOG.warn(
          "Cannot find the Ranger policy for the Ranger securable object({})!",
          AuthorizationSecurableObject.fullName());
      // Don't throw exception or return false, because need support immutable operation.
      return true;
    }

    AuthorizationSecurableObject.privileges().stream()
        .forEach(
            rangerPrivilege -> {
              if (rangerPrivilege.condition() == Privilege.Condition.ALLOW) {
                policy
                    .getPolicyItems()
                    .forEach(
                        policyItem -> {
                          removePolicyItemIfEqualRoleName(
                              policyItem, AuthorizationSecurableObject, roleName);
                        });
              } else {
                policy
                    .getDenyPolicyItems()
                    .forEach(
                        policyItem -> {
                          removePolicyItemIfEqualRoleName(
                              policyItem, AuthorizationSecurableObject, roleName);
                        });
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
    policy
        .getDenyPolicyItems()
        .removeIf(
            policyItem ->
                policyItem.getRoles().isEmpty()
                    && policyItem.getUsers().isEmpty()
                    && policyItem.getGroups().isEmpty());

    try {
      rangerClient.updatePolicy(policy.getId(), policy);
    } catch (RangerServiceException e) {
      LOG.error("Failed to remove the policy item from the Ranger policy {}!", policy);
      throw new AuthorizationPluginException(
          e, "Failed to remove the securable object from Ranger!");
    }
    return true;
  }

  private void removePolicyItemIfEqualRoleName(
      RangerPolicy.RangerPolicyItem policyItem,
      AuthorizationSecurableObject AuthorizationSecurableObject,
      String roleName) {
    roleName = rangerHelper.generateGravitinoRoleName(roleName);
    boolean match =
        policyItem.getAccesses().stream()
            .allMatch(
                // Find the policy item that matches access and role
                access -> {
                  // Use Gravitino privilege to search the Ranger policy item's access
                  boolean matchPrivilege =
                      AuthorizationSecurableObject.privileges().stream()
                          .anyMatch(privilege -> privilege.equalsTo(access.getType()));
                  return matchPrivilege;
                });
    if (match) {
      policyItem.getRoles().removeIf(roleName::equals);
    }
  }

  /**
   * IF remove the SCHEMA, need to remove these the relevant policies, `{schema}`, `{schema}.*`,
   * `{schema}.*.*` <br>
   * IF remove the TABLE, need to remove these the relevant policies, `{schema}.*`, `{schema}.*.*`
   * <br>
   * IF remove the COLUMN, Only need to remove `{schema}.*.*` <br>
   */
  private void doRemoveMetadataObject(AuthorizationMetadataObject authMetadataObject) {
    switch (authMetadataObject.metadataObjectType()) {
      case SCHEMA:
        doRemoveSchemaMetadataObject(authMetadataObject);
        break;
      case TABLE:
        doRemoveTableMetadataObject(authMetadataObject);
        break;
      case COLUMN:
        removePolicyByMetadataObject(authMetadataObject.names());
        break;
      case FILESET:
        // can not get fileset path in this case, do nothing
        break;
      default:
        throw new IllegalArgumentException(
            "Unsupported metadata object type: " + authMetadataObject.type());
    }
  }

  /**
   * Remove the SCHEMA, Need to remove these the relevant policies, `{schema}`, `{schema}.*`,
   * `{schema}.*.*` permissions.
   */
  private void doRemoveSchemaMetadataObject(AuthorizationMetadataObject authMetadataObject) {
    Preconditions.checkArgument(
        authMetadataObject.type() == RangerHadoopSQLMetadataObject.Type.SCHEMA,
        "The metadata object type must be SCHEMA");
    Preconditions.checkArgument(
        authMetadataObject.names().size() == 1, "The metadata object names must be 1");
    if (RangerHelper.RESOURCE_ALL.equals(authMetadataObject.name())) {
      // Delete metalake or catalog policies in this Ranger service
      try {
        List<RangerPolicy> policies = rangerClient.getPoliciesInService(rangerServiceName);
        policies.stream()
            .filter(rangerHelper::hasGravitinoManagedPolicyItem)
            .forEach(rangerHelper::removeAllGravitinoManagedPolicyItem);
      } catch (RangerServiceException e) {
        throw new RuntimeException(e);
      }
    } else {
      List<List<String>> loop =
          ImmutableList.of(
              ImmutableList.of(authMetadataObject.name())
              /** SCHEMA permission */
              ,
              ImmutableList.of(authMetadataObject.name(), RangerHelper.RESOURCE_ALL)
              /** TABLE permission */
              ,
              ImmutableList.of(
                  authMetadataObject.name(), RangerHelper.RESOURCE_ALL, RangerHelper.RESOURCE_ALL)
              /** COLUMN permission */
              );
      for (List<String> resNames : loop) {
        removePolicyByMetadataObject(resNames);
      }
    }
  }

  /**
   * Remove the TABLE, Need to remove these the relevant policies, `*.{table}`, `*.{table}.{column}`
   * permissions.
   */
  private void doRemoveTableMetadataObject(
      AuthorizationMetadataObject AuthorizationMetadataObject) {
    List<List<String>> loop =
        ImmutableList.of(
            AuthorizationMetadataObject.names()
            /** TABLE permission */
            ,
            Stream.concat(
                    AuthorizationMetadataObject.names().stream(),
                    Stream.of(RangerHelper.RESOURCE_ALL))
                .collect(Collectors.toList())
            /** COLUMN permission */
            );
    for (List<String> resNames : loop) {
      removePolicyByMetadataObject(resNames);
    }
  }

  /**
   * IF rename the SCHEMA, Need to rename these the relevant policies, `{schema}`, `{schema}.*`,
   * `{schema}.*.*` <br>
   * IF rename the TABLE, Need to rename these the relevant policies, `{schema}.*`, `{schema}.*.*`
   * <br>
   * IF rename the COLUMN, Only need to rename `{schema}.*.*` <br>
   */
  private void doRenameMetadataObject(
      AuthorizationMetadataObject AuthorizationMetadataObject,
      AuthorizationMetadataObject newAuthMetadataObject) {
    switch (newAuthMetadataObject.metadataObjectType()) {
      case SCHEMA:
        doRenameSchemaMetadataObject(AuthorizationMetadataObject, newAuthMetadataObject);
        break;
      case TABLE:
        doRenameTableMetadataObject(AuthorizationMetadataObject, newAuthMetadataObject);
        break;
      case COLUMN:
        doRenameColumnMetadataObject(AuthorizationMetadataObject, newAuthMetadataObject);
        break;
      case FILESET:
        // do nothing when fileset is renamed
        break;
      default:
        throw new IllegalArgumentException(
            "Unsupported metadata object type: " + AuthorizationMetadataObject.type());
    }
  }

  /**
   * Rename the SCHEMA, Need to rename these the relevant policies, `{schema}`, `{schema}.*`,
   * `{schema}.*.*` <br>
   */
  private void doRenameSchemaMetadataObject(
      AuthorizationMetadataObject AuthorizationMetadataObject,
      AuthorizationMetadataObject newAuthorizationMetadataObject) {
    List<String> oldMetadataNames = new ArrayList<>();
    List<String> newMetadataNames = new ArrayList<>();
    List<Map<String, String>> loop =
        ImmutableList.of(
            ImmutableMap.of(
                AuthorizationMetadataObject.names().get(0),
                newAuthorizationMetadataObject.names().get(0)),
            ImmutableMap.of(RangerHelper.RESOURCE_ALL, RangerHelper.RESOURCE_ALL),
            ImmutableMap.of(RangerHelper.RESOURCE_ALL, RangerHelper.RESOURCE_ALL));
    for (Map<String, String> mapName : loop) {
      oldMetadataNames.add(mapName.keySet().stream().findFirst().get());
      newMetadataNames.add(mapName.values().stream().findFirst().get());
      updatePolicyByMetadataObject(MetadataObject.Type.SCHEMA, oldMetadataNames, newMetadataNames);
    }
  }

  /**
   * Rename the TABLE, Need to rename these the relevant policies, `*.{table}`, `*.{table}.{column}`
   * <br>
   */
  private void doRenameTableMetadataObject(
      AuthorizationMetadataObject AuthorizationMetadataObject,
      AuthorizationMetadataObject newAuthorizationMetadataObject) {
    List<String> oldMetadataNames = new ArrayList<>();
    List<String> newMetadataNames = new ArrayList<>();
    List<Map<String, MetadataObject.Type>> loop =
        ImmutableList.of(
            ImmutableMap.of(AuthorizationMetadataObject.names().get(0), MetadataObject.Type.SCHEMA),
            ImmutableMap.of(AuthorizationMetadataObject.names().get(1), MetadataObject.Type.TABLE),
            ImmutableMap.of(RangerHelper.RESOURCE_ALL, MetadataObject.Type.COLUMN));
    for (Map<String, MetadataObject.Type> nameAndType : loop) {
      oldMetadataNames.add(nameAndType.keySet().stream().findFirst().get());
      if (nameAndType.containsValue(MetadataObject.Type.SCHEMA)) {
        newMetadataNames.add(newAuthorizationMetadataObject.names().get(0));
        // Skip update the schema name operation
        continue;
      } else if (nameAndType.containsValue(MetadataObject.Type.TABLE)) {
        newMetadataNames.add(newAuthorizationMetadataObject.names().get(1));
      } else if (nameAndType.containsValue(MetadataObject.Type.COLUMN)) {
        newMetadataNames.add(RangerHelper.RESOURCE_ALL);
      }
      updatePolicyByMetadataObject(MetadataObject.Type.TABLE, oldMetadataNames, newMetadataNames);
    }
  }

  /** rename the COLUMN, Only need to rename `*.*.{column}` <br> */
  private void doRenameColumnMetadataObject(
      AuthorizationMetadataObject AuthorizationMetadataObject,
      AuthorizationMetadataObject newAuthorizationMetadataObject) {
    List<String> oldMetadataNames = new ArrayList<>();
    List<String> newMetadataNames = new ArrayList<>();
    List<Map<String, MetadataObject.Type>> loop =
        ImmutableList.of(
            ImmutableMap.of(AuthorizationMetadataObject.names().get(0), MetadataObject.Type.SCHEMA),
            ImmutableMap.of(AuthorizationMetadataObject.names().get(1), MetadataObject.Type.TABLE),
            ImmutableMap.of(
                AuthorizationMetadataObject.names().get(2), MetadataObject.Type.COLUMN));
    for (Map<String, MetadataObject.Type> nameAndType : loop) {
      oldMetadataNames.add(nameAndType.keySet().stream().findFirst().get());
      if (nameAndType.containsValue(MetadataObject.Type.SCHEMA)) {
        newMetadataNames.add(newAuthorizationMetadataObject.names().get(0));
        // Skip update the schema name operation
        continue;
      } else if (nameAndType.containsValue(MetadataObject.Type.TABLE)) {
        newMetadataNames.add(newAuthorizationMetadataObject.names().get(1));
        // Skip update the table name operation
        continue;
      } else if (nameAndType.containsValue(MetadataObject.Type.COLUMN)) {
        newMetadataNames.add(newAuthorizationMetadataObject.names().get(2));
      }
      updatePolicyByMetadataObject(MetadataObject.Type.COLUMN, oldMetadataNames, newMetadataNames);
    }
  }

  /**
   * Remove the policy by the metadata object names. <br>
   *
   * @param metadataNames The metadata object names.
   */
  private void removePolicyByMetadataObject(List<String> metadataNames) {
    List<RangerPolicy> policies = rangerHelper.wildcardSearchPolies(metadataNames);
    Map<String, String> preciseFilters = new HashMap<>();
    for (int i = 0; i < metadataNames.size(); i++) {
      preciseFilters.put(rangerHelper.policyResourceDefines.get(i), metadataNames.get(i));
    }
    policies =
        policies.stream()
            .filter(
                policy ->
                    policy.getResources().entrySet().stream()
                        .allMatch(
                            entry ->
                                preciseFilters.containsKey(entry.getKey())
                                    && entry.getValue().getValues().size() == 1
                                    && entry
                                        .getValue()
                                        .getValues()
                                        .contains(preciseFilters.get(entry.getKey()))))
            .collect(Collectors.toList());
    policies.forEach(rangerHelper::removeAllGravitinoManagedPolicyItem);
  }

  private void updatePolicyByMetadataObject(
      MetadataObject.Type operationType,
      List<String> oldMetadataNames,
      List<String> newMetadataNames) {
    List<RangerPolicy> oldPolicies = rangerHelper.wildcardSearchPolies(oldMetadataNames);
    List<RangerPolicy> existNewPolicies = rangerHelper.wildcardSearchPolies(newMetadataNames);
    if (oldPolicies.isEmpty()) {
      LOG.warn("Cannot find the Ranger policy for the metadata object({})!", oldMetadataNames);
    }
    if (!existNewPolicies.isEmpty()) {
      LOG.warn("The Ranger policy for the metadata object({}) already exists!", newMetadataNames);
    }
    Map<MetadataObject.Type, Integer> operationTypeIndex =
        ImmutableMap.of(
            MetadataObject.Type.SCHEMA, 0,
            MetadataObject.Type.TABLE, 1,
            MetadataObject.Type.COLUMN, 2);
    oldPolicies.stream()
        .forEach(
            policy -> {
              try {
                String policyName = policy.getName();
                int index = operationTypeIndex.get(operationType);

                // Update the policy name is following Gravitino's spec
                if (policy
                    .getName()
                    .equals(AuthorizationSecurableObject.DOT_JOINER.join(oldMetadataNames))) {
                  List<String> policyNames =
                      Lists.newArrayList(
                          AuthorizationSecurableObject.DOT_SPLITTER.splitToList(policyName));
                  Preconditions.checkArgument(
                      policyNames.size() >= oldMetadataNames.size(),
                      String.format("The policy name(%s) is invalid!", policyName));
                  if (policyNames.get(index).equals(RangerHelper.RESOURCE_ALL)) {
                    // Doesn't need to rename the policy `*`
                    return;
                  }
                  policyNames.set(index, newMetadataNames.get(index));
                  policy.setName(AuthorizationSecurableObject.DOT_JOINER.join(policyNames));
                }
                // Update the policy resource name to new name
                policy
                    .getResources()
                    .put(
                        rangerHelper.policyResourceDefines.get(index),
                        new RangerPolicy.RangerPolicyResource(newMetadataNames.get(index)));

                boolean alreadyExist =
                    existNewPolicies.stream()
                        .anyMatch(
                            existNewPolicy ->
                                existNewPolicy.getName().equals(policy.getName())
                                    || existNewPolicy.getResources().equals(policy.getResources()));
                if (alreadyExist) {
                  LOG.warn(
                      "The Ranger policy for the metadata object({}) already exists!",
                      newMetadataNames);
                  return;
                }

                // Update the policy
                rangerClient.updatePolicy(policy.getId(), policy);
              } catch (RangerServiceException e) {
                LOG.error("Failed to rename the policy {}!", policy);
                throw new RuntimeException(e);
              }
            });
  }

  @Override
  public void close() throws IOException {}

  /** Generate authorization securable object */
  public abstract AuthorizationSecurableObject generateAuthorizationSecurableObject(
      List<String> names,
      AuthorizationMetadataObject.Type type,
      Set<AuthorizationPrivilege> privileges);

  public boolean validAuthorizationOperation(List<SecurableObject> securableObjects) {
    return securableObjects.stream()
        .allMatch(
            securableObject -> {
              AtomicBoolean match = new AtomicBoolean(true);
              securableObject.privileges().stream()
                  .forEach(
                      privilege -> {
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
