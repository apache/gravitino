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
import com.sun.jersey.api.client.ClientResponse;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.commons.lang3.reflect.FieldUtils;
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
import org.apache.gravitino.authorization.common.ErrorMessages;
import org.apache.gravitino.authorization.common.RangerAuthorizationProperties;
import org.apache.gravitino.authorization.ranger.reference.VXGroup;
import org.apache.gravitino.authorization.ranger.reference.VXGroupList;
import org.apache.gravitino.authorization.ranger.reference.VXUser;
import org.apache.gravitino.authorization.ranger.reference.VXUserList;
import org.apache.gravitino.connector.authorization.AuthorizationPlugin;
import org.apache.gravitino.exceptions.AuthorizationPluginException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.ranger.RangerServiceException;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerService;
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
  protected static final String HDFS_SERVICE_TYPE = "hdfs";
  protected static final String HADOOP_SQL_SERVICE_TYPE = "hive";

  protected String metalake;
  protected final String rangerServiceName;
  protected RangerClientExtension rangerClient;
  protected RangerHelper rangerHelper;
  @VisibleForTesting public final String rangerAdminName;

  protected RangerAuthorizationPlugin(String metalake, Map<String, String> config) {
    this.metalake = metalake;
    RangerAuthorizationProperties rangerAuthorizationProperties =
        new RangerAuthorizationProperties(config);
    rangerAuthorizationProperties.validate();
    String rangerUrl = config.get(RangerAuthorizationProperties.RANGER_ADMIN_URL);

    String authType = config.get(RangerAuthorizationProperties.RANGER_AUTH_TYPE);

    rangerAdminName = config.get(RangerAuthorizationProperties.RANGER_USERNAME);

    // Apache Ranger Password should be minimum 8 characters with min one alphabet and one numeric.
    String password = config.get(RangerAuthorizationProperties.RANGER_PASSWORD);

    rangerServiceName = config.get(RangerAuthorizationProperties.RANGER_SERVICE_NAME);
    rangerClient = new RangerClientExtension(rangerUrl, authType, rangerAdminName, password);

    createRangerServiceIfNecessary(config, rangerServiceName);

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

  @VisibleForTesting
  public RangerHelper getRangerHelper() {
    return rangerHelper;
  }

  @VisibleForTesting
  public void setRangerHelper(RangerHelper rangerHelper) {
    this.rangerHelper = rangerHelper;
  }

  @VisibleForTesting
  public RangerClientExtension getRangerClient() {
    return rangerClient;
  }

  @VisibleForTesting
  public void setRangerClient(RangerClientExtension rangerClient) {
    this.rangerClient = rangerClient;
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
   * @param metadataObject The authorization metadata object to create the policy for.
   * @return The RangerPolicy for metadata object.
   */
  protected abstract RangerPolicy createPolicyAddResources(
      AuthorizationMetadataObject metadataObject);

  /**
   * Wildcard search the Ranger policies in the different Ranger service.
   *
   * @param authzMetadataObject The authorization metadata object used to perform wildcard search.
   * @return A list of Ranger policies matching the wildcard conditions.
   */
  protected abstract List<RangerPolicy> wildcardSearchPolicies(
      AuthorizationMetadataObject authzMetadataObject);

  /**
   * Find the managed policy for the ranger securable object.
   *
   * @param authzMetadataObject The ranger securable object to find the managed policy.
   * @return The managed policy for the metadata object.
   */
  public abstract RangerPolicy findManagedPolicy(AuthorizationMetadataObject authzMetadataObject)
      throws AuthorizationPluginException;

  protected abstract void updatePolicyByMetadataObject(
      MetadataObject.Type operationType,
      AuthorizationMetadataObject oldAuthzMetaobject,
      AuthorizationMetadataObject newAuthzMetaobject);

  /**
   * Because Ranger doesn't support the precise search, Ranger will return the policy meets the
   * wildcard(*,?) conditions, If you use `db.table` condition to search policy, the Ranger will
   * match `db1.table1`, `db1.table2`, `db*.table*`, So we need to manually precisely filter this
   * research results.
   *
   * @param authzMetadataObject The authorization metadata object used as the base for searching
   *     policies.
   * @param preciseFilters A map of resource keys and their expected exact values for filtering the
   *     policies.
   * @return The matched Ranger policy if found, or {@code null} if none matches.
   * @throws AuthorizationPluginException If multiple policies are found or a validation fails.
   */
  protected RangerPolicy preciseFindPolicy(
      AuthorizationMetadataObject authzMetadataObject, Map<String, String> preciseFilters)
      throws AuthorizationPluginException {
    List<RangerPolicy> policies = wildcardSearchPolicies(authzMetadataObject);
    if (!policies.isEmpty()) {
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
    }
    // Only return the policies that are managed by Gravitino.
    if (policies.size() > 1) {
      throw new AuthorizationPluginException("Each metadata object can have at most one policy.");
    }

    if (policies.isEmpty()) {
      return null;
    }

    RangerPolicy policy = policies.get(0);
    // Delegating Gravitino management policies cannot contain duplicate privilege
    policy.getPolicyItems().forEach(RangerHelper::checkPolicyItemAccess);
    policy.getDenyPolicyItems().forEach(RangerHelper::checkPolicyItemAccess);
    policy.getRowFilterPolicyItems().forEach(RangerHelper::checkPolicyItemAccess);
    policy.getDataMaskPolicyItems().forEach(RangerHelper::checkPolicyItemAccess);

    return policy;
  }

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
      if (rangerHelper.getRangerRole(role.name()) == null) {
        // Ignore exception to support idempotent operation
        LOG.info("Ranger delete role: {} failed!", role, e);
      } else {
        throw new AuthorizationPluginException(
            "Fail to delete role %s exception: %s", role, e.getMessage());
      }
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
          return Boolean.FALSE;
        }

        List<AuthorizationSecurableObject> authzSecurableObjects =
            translatePrivilege(securableObject);
        authzSecurableObjects.forEach(
            authzSecurableObject -> {
              if (!doAddSecurableObject(role.name(), authzSecurableObject)) {
                throw new AuthorizationPluginException(
                    "Failed to add the securable object to the Ranger policy!");
              }
            });
      } else if (change instanceof RoleChange.RemoveSecurableObject) {
        SecurableObject securableObject =
            ((RoleChange.RemoveSecurableObject) change).getSecurableObject();
        if (!validAuthorizationOperation(Arrays.asList(securableObject))) {
          return Boolean.FALSE;
        }

        List<AuthorizationSecurableObject> authzSecurableObjects =
            translatePrivilege(securableObject);
        authzSecurableObjects.stream()
            .forEach(
                authzSecurableObject -> {
                  if (!removeSecurableObject(role.name(), authzSecurableObject)) {
                    throw new AuthorizationPluginException(
                        "Failed to add the securable object to the Ranger policy!");
                  }
                });
      } else if (change instanceof RoleChange.UpdateSecurableObject) {
        SecurableObject oldSecurableObject =
            ((RoleChange.UpdateSecurableObject) change).getSecurableObject();
        if (!validAuthorizationOperation(Arrays.asList(oldSecurableObject))) {
          return Boolean.FALSE;
        }
        SecurableObject newSecurableObject =
            ((RoleChange.UpdateSecurableObject) change).getNewSecurableObject();
        if (!validAuthorizationOperation(Arrays.asList(newSecurableObject))) {
          return Boolean.FALSE;
        }

        Preconditions.checkArgument(
            (oldSecurableObject.fullName().equals(newSecurableObject.fullName())
                && oldSecurableObject.type().equals(newSecurableObject.type())),
            "The old and new securable objects metadata must be equal!");
        List<AuthorizationSecurableObject> rangerOldSecurableObjects =
            translatePrivilege(oldSecurableObject);
        List<AuthorizationSecurableObject> rangerNewSecurableObjects =
            translatePrivilege(newSecurableObject);
        rangerOldSecurableObjects.forEach(
            AuthorizationSecurableObject -> {
              removeSecurableObject(role.name(), AuthorizationSecurableObject);
            });
        rangerNewSecurableObjects.forEach(
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
        Preconditions.checkArgument(
            metadataObject.type() == newMetadataObject.type(),
            "The old and new metadata object type must be equal!");
        if (metadataObject.type() == MetadataObject.Type.METALAKE) {
          // Rename the metalake name
          this.metalake = newMetadataObject.name();
          // Did not need to update the Ranger policy
          continue;
        } else if (metadataObject.type() == MetadataObject.Type.CATALOG) {
          // Did not need to update the Ranger policy
          continue;
        }
        List<AuthorizationMetadataObject> oldAuthzMetadataObjects =
            translateMetadataObject(metadataObject);
        List<AuthorizationMetadataObject> newAuthzMetadataObjects =
            translateMetadataObject(newMetadataObject);
        Preconditions.checkArgument(
            oldAuthzMetadataObjects.size() == newAuthzMetadataObjects.size(),
            "The old and new metadata objects size must be equal!");
        for (int i = 0; i < oldAuthzMetadataObjects.size(); i++) {
          AuthorizationMetadataObject oldAuthMetadataObject = oldAuthzMetadataObjects.get(i);
          AuthorizationMetadataObject newAuthzMetadataObject = newAuthzMetadataObjects.get(i);
          if (oldAuthMetadataObject.equals(newAuthzMetadataObject)) {
            LOG.info(
                "The metadata object({}) and new metadata object({}) are equal, so ignore rename!",
                oldAuthMetadataObject.fullName(),
                newAuthzMetadataObject.fullName());
            continue;
          }
          renameMetadataObject(oldAuthMetadataObject, newAuthzMetadataObject);
        }
      } else if (change instanceof MetadataObjectChange.RemoveMetadataObject) {
        MetadataObject metadataObject =
            ((MetadataObjectChange.RemoveMetadataObject) change).metadataObject();
        List<AuthorizationMetadataObject> authzMetadataObjects =
            translateMetadataObject(metadataObject);
        authzMetadataObjects.stream().forEach(this::removeMetadataObject);
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
        preOwnerUserName = preOwner.name();
      } else {
        preOwnerGroupName = preOwner.name();
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

    List<AuthorizationSecurableObject> rangerSecurableObjects = translateOwner(metadataObject);
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

        rangerSecurableObjects.forEach(
            rangerSecurableObject -> {
              RangerPolicy policy = findManagedPolicy(rangerSecurableObject);
              try {
                if (policy == null) {
                  policy = addOwnerRoleToNewPolicy(rangerSecurableObject, ownerRoleName);
                  rangerClient.createPolicy(policy);
                } else {
                  rangerHelper.updatePolicyOwnerRole(policy, ownerRoleName);
                  rangerClient.updatePolicy(policy.getId(), policy);
                }
              } catch (RangerServiceException e) {
                throw new AuthorizationPluginException(e, "Failed to add the owner to the Ranger!");
              }
            });
        break;
      case SCHEMA:
      case TABLE:
      case FILESET:
        // The schema and table use user/group to manage the owner
        rangerSecurableObjects.stream()
            .forEach(
                AuthorizationSecurableObject -> {
                  RangerPolicy policy = findManagedPolicy(AuthorizationSecurableObject);
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
            ErrorMessages.OWNER_PRIVILEGE_NOT_SUPPORTED, metadataObject.type());
    }

    return Boolean.TRUE;
  }

  /**
   * Grant the roles to the user. <br>
   * 1. Create a user in the Ranger if the user does not exist. <br>
   * 2. Create a role in the Ranger if the role does not exist. <br>
   * 3. Add this user to the role. <br>
   *
   * @param roles The roles to grant to the user.
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
                throw new AuthorizationPluginException(
                    "Fail to grant role %s to user %s, exception: %s",
                    role.name(), user.name(), e.getMessage());
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
                throw new AuthorizationPluginException(
                    "Fail to revoke role %s from user %s, exception: %s",
                    role.name(), user.name(), e.getMessage());
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
                throw new AuthorizationPluginException(
                    "Fail to grant role: %s to group %s, exception: %s.",
                    role, group, e.getMessage());
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
                throw new AuthorizationPluginException(
                    "Fail to revoke role %s from group %s, exception: %s",
                    role.name(), group.name(), e.getMessage());
              }
            });

    return Boolean.TRUE;
  }

  @Override
  public Boolean onUserAdded(User user) throws AuthorizationPluginException {
    return getUserId(user.name())
        .map(
            id -> {
              LOG.warn("The user({}) already exists in the Ranger!", user.name());
              return Boolean.FALSE;
            })
        .orElseGet(
            () -> {
              VXUser rangerUser =
                  VXUser.builder().withName(user.name()).withDescription(user.name()).build();
              return rangerClient.createUser(rangerUser);
            });
  }

  @Override
  public Boolean onUserRemoved(User user) throws AuthorizationPluginException {
    return getUserId(user.name())
        .map(id -> rangerClient.deleteUser(id))
        .orElseGet(
            () -> {
              LOG.warn("The user({}) doesn't exist in the Ranger!", user.name());
              return Boolean.FALSE;
            });
  }

  @Override
  public Boolean onUserAcquired(User user) throws AuthorizationPluginException {
    return getUserId(user.name())
        .map(id -> Boolean.TRUE)
        .orElseGet(
            () -> {
              LOG.warn("The user({}) doesn't exist in the Ranger!", user);
              return Boolean.FALSE;
            });
  }

  @Override
  public Boolean onGroupAdded(Group group) throws AuthorizationPluginException {
    return rangerClient.createGroup(
        VXGroup.builder().withName(group.name()).withDescription(group.name()).build());
  }

  @Override
  public Boolean onGroupRemoved(Group group) throws AuthorizationPluginException {
    Optional<Long> groupId = getGroupId(group.name());
    return groupId
        .map(id -> rangerClient.deleteGroup(id))
        .orElseGet(
            () -> {
              LOG.warn("The group({}) doesn't exist in the Ranger!", group.name());
              return Boolean.FALSE;
            });
  }

  @Override
  public Boolean onGroupAcquired(Group group) {
    if (!getGroupId(group.name()).isPresent()) {
      LOG.warn("The group({}) doesn't exist in the Ranger!", group);
      return Boolean.FALSE;
    }
    return Boolean.TRUE;
  }

  private void createRangerServiceIfNecessary(Map<String, String> config, String serviceName) {
    try {
      rangerClient.getService(serviceName);
    } catch (RangerServiceException rse) {
      if (Boolean.parseBoolean(
              config.get(RangerAuthorizationProperties.RANGER_SERVICE_CREATE_IF_ABSENT))
          && ClientResponse.Status.NOT_FOUND.equals(rse.getStatus())) {
        try {
          RangerService rangerService = new RangerService();
          rangerService.setType(getServiceType());
          rangerService.setName(serviceName);
          rangerService.setConfigs(getServiceConfigs(config));
          rangerClient.createService(rangerService);
          // We should remove some default policies, they will cause users to get more policies
          // than they should do.
          List<RangerPolicy> policies = rangerClient.getPoliciesInService(serviceName);
          for (RangerPolicy policy : policies) {
            rangerClient.deletePolicy(policy.getId());
          }
        } catch (RangerServiceException crse) {
          throw new AuthorizationPluginException(
              "Fail to create ranger service %s, exception: %s", serviceName, crse.getMessage());
        }
      } else {
        throw new AuthorizationPluginException(
            "Fail to get ranger service name %s, exception: %s", serviceName, rse.getMessage());
      }
    }
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
    RangerPolicy policy = findManagedPolicy(securableObject);
    if (policy != null) {
      // Check the policy item's accesses and roles equal the Ranger securable object's privilege
      List<AuthorizationPrivilege> allowPrivileges =
          securableObject.privileges().stream()
              .filter(privilege -> privilege.condition() == Privilege.Condition.ALLOW)
              .collect(Collectors.toList());
      List<AuthorizationPrivilege> denyPrivileges =
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

      if (policyPrivileges.containsAll(allowPrivileges)
          && policyDenyPrivileges.containsAll(denyPrivileges)) {
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
  private boolean removeSecurableObject(
      String roleName, AuthorizationSecurableObject authzSecurableObject) {
    RangerPolicy policy = findManagedPolicy(authzSecurableObject);
    if (policy == null) {
      LOG.warn(
          "Cannot find the Ranger policy for the Ranger securable object({})!",
          authzSecurableObject.fullName());
      // Don't throw exception or return false, because need support immutable operation.
      return true;
    }

    authzSecurableObject.privileges().stream()
        .forEach(
            rangerPrivilege -> {
              if (rangerPrivilege.condition() == Privilege.Condition.ALLOW) {
                policy
                    .getPolicyItems()
                    .forEach(
                        policyItem -> {
                          removePolicyItemIfEqualRoleName(
                              policyItem, authzSecurableObject, roleName);
                        });
              } else {
                policy
                    .getDenyPolicyItems()
                    .forEach(
                        policyItem -> {
                          removePolicyItemIfEqualRoleName(
                              policyItem, authzSecurableObject, roleName);
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
      if (policy.getPolicyItems().isEmpty() && policy.getDenyPolicyItems().isEmpty()) {
        rangerClient.deletePolicy(policy.getId());
      } else {
        rangerClient.updatePolicy(policy.getId(), policy);
      }
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
   * IF rename the SCHEMA, Need to rename these the relevant policies, `{schema}`, `{schema}.*`,
   * `{schema}.*.*` <br>
   * IF rename the TABLE, Need to rename these the relevant policies, `{schema}.*`, `{schema}.*.*`
   * <br>
   * IF rename the COLUMN, Only need to rename `{schema}.*.*` <br>
   *
   * @param authzMetadataObject The original metadata object to be renamed.
   * @param newAuthzMetadataObject The new metadata object after renaming.
   */
  protected abstract void renameMetadataObject(
      AuthorizationMetadataObject authzMetadataObject,
      AuthorizationMetadataObject newAuthzMetadataObject);

  protected abstract void removeMetadataObject(AuthorizationMetadataObject authzMetadataObject);

  /**
   * Remove the policy by the metadata object names. <br>
   *
   * @param authzMetadataObject The authorization metadata object.
   */
  protected void removePolicyByMetadataObject(AuthorizationMetadataObject authzMetadataObject) {
    RangerPolicy policy = findManagedPolicy(authzMetadataObject);
    if (policy != null) {
      rangerHelper.removeAllGravitinoManagedPolicyItem(policy);
    }
  }

  protected String getConfValue(Map<String, String> conf, String key, String defaultValue) {
    if (conf.containsKey(key)) {
      return conf.get(key);
    }
    return defaultValue;
  }

  protected abstract String getServiceType();

  protected abstract Map<String, String> getServiceConfigs(Map<String, String> config);

  protected int getPrefixLength() {
    // We should consider `.`. We need to add 1
    return RangerAuthorizationProperties.RANGER_PREFIX.length() + 1;
  }

  @Override
  public void close() throws IOException {}

  /**
   * Generate authorization securable object.
   *
   * @param object The authorization metadata object to base the securable object on.
   * @param privileges The set of privileges associated with the securable object.
   * @return The generated authorization securable object.
   */
  public abstract AuthorizationSecurableObject generateAuthorizationSecurableObject(
      AuthorizationMetadataObject object, Set<AuthorizationPrivilege> privileges);

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

  private Optional<Long> getUserId(String name) {
    VXUserList list = rangerClient.searchUser(ImmutableMap.of("name", name));
    if (list.getListSize() > 0) {
      for (VXUser vxUser : list.getList()) {
        if (vxUser.getName().equals(name)) {
          return Optional.of(vxUser.getId());
        }
      }
    }
    return Optional.empty();
  }

  private Optional<Long> getGroupId(String name) {
    VXGroupList vxGroupList = rangerClient.searchGroup(ImmutableMap.of("name", name));
    try {
      for (VXGroup group : vxGroupList.getList()) {
        String value = (String) FieldUtils.readField(group, "name", true);
        if (name.equals(value)) {
          return Optional.of(group.getId());
        }
      }
    } catch (Exception e) {
      throw new AuthorizationPluginException("Fail to get the field name of class VXGroup");
    }
    return Optional.empty();
  }
}
