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
package org.apache.gravitino.authorization;

import static org.apache.gravitino.authorization.AuthorizationUtils.GROUP_DOES_NOT_EXIST_MSG;
import static org.apache.gravitino.authorization.AuthorizationUtils.ROLE_DOES_NOT_EXIST_MSG;
import static org.apache.gravitino.authorization.AuthorizationUtils.USER_DOES_NOT_EXIST_MSG;
import static org.apache.gravitino.authorization.AuthorizationUtils.filterSecurableObjects;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.exceptions.IllegalRoleException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchGroupException;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.exceptions.NoSuchRoleException;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.apache.gravitino.lock.LockType;
import org.apache.gravitino.lock.TreeLockUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.utils.PrincipalUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PermissionManager is used for managing the logic the granting and revoking roles. Role is used
 * for manging permissions.
 */
class PermissionManager {
  private static final Logger LOG = LoggerFactory.getLogger(PermissionManager.class);

  private final EntityStore store;
  private final RoleManager roleManager;

  PermissionManager(EntityStore store, RoleManager roleManager) {
    this.store = store;
    this.roleManager = roleManager;
  }

  User grantRolesToUser(String metalake, List<String> roles, String user) {
    try {
      List<RoleEntity> roleEntitiesToGrant = Lists.newArrayList();
      for (String role : roles) {
        TreeLockUtils.doWithTreeLock(
            AuthorizationUtils.ofRole(metalake, role),
            LockType.READ,
            () -> roleEntitiesToGrant.add(roleManager.getRole(metalake, role)));
      }

      User updatedUser =
          store.update(
              AuthorizationUtils.ofUser(metalake, user),
              UserEntity.class,
              Entity.EntityType.USER,
              userEntity -> {
                checkObservedRoles(userEntity.roleNames(), userEntity.roleIds());
                List<String> roleNames = mutableCopy(userEntity.roleNames());
                List<Long> roleIds = mutableCopy(userEntity.roleIds());

                for (RoleEntity roleEntityToGrant : roleEntitiesToGrant) {
                  if (roleIds.contains(roleEntityToGrant.id())) {
                    LOG.warn(
                        "Failed to grant, role {} already exists in the user {} of metalake {}",
                        roleEntityToGrant.name(),
                        user,
                        metalake);
                  } else {
                    roleNames.add(roleEntityToGrant.name());
                    roleIds.add(roleEntityToGrant.id());
                  }
                }

                AuditInfo auditInfo =
                    AuditInfo.builder()
                        .withCreator(userEntity.auditInfo().creator())
                        .withCreateTime(userEntity.auditInfo().createTime())
                        .withLastModifier(PrincipalUtils.getCurrentPrincipal().getName())
                        .withLastModifiedTime(Instant.now())
                        .build();

                return UserEntity.builder()
                    .withNamespace(userEntity.namespace())
                    .withId(userEntity.id())
                    .withName(userEntity.name())
                    .withRoleNames(roleNames)
                    .withRoleIds(roleIds)
                    .withAuditInfo(auditInfo)
                    .build();
              });

      List<SecurableObject> securableObjects = Lists.newArrayList();

      for (Role grantedRole : roleEntitiesToGrant) {
        securableObjects.addAll(grantedRole.securableObjects());
      }

      AuthorizationUtils.callAuthorizationPluginForSecurableObjects(
          metalake,
          securableObjects,
          (authorizationPlugin, catalogName) ->
              authorizationPlugin.onGrantedRolesToUser(
                  roleEntitiesToGrant.stream()
                      .map(roleEntity -> filterSecurableObjects(roleEntity, metalake, catalogName))
                      .collect(Collectors.toList()),
                  updatedUser));

      return updatedUser;
    } catch (NoSuchEntityException nse) {
      LOG.warn("Failed to grant, user {} does not exist in the metalake {}", user, metalake, nse);
      throw new NoSuchUserException(USER_DOES_NOT_EXIST_MSG, user, metalake);
    } catch (NoSuchRoleException nsr) {
      throw new IllegalRoleException(nsr);
    } catch (IOException ioe) {
      LOG.error(
          "Failed to grant role {} to user {} in the metalake {} due to storage issues",
          StringUtils.join(roles, ","),
          user,
          metalake,
          ioe);
      throw new RuntimeException(ioe);
    }
  }

  Group grantRolesToGroup(String metalake, List<String> roles, String group) {
    try {
      List<RoleEntity> roleEntitiesToGrant = Lists.newArrayList();
      for (String role : roles) {
        TreeLockUtils.doWithTreeLock(
            AuthorizationUtils.ofRole(metalake, role),
            LockType.READ,
            () -> roleEntitiesToGrant.add(roleManager.getRole(metalake, role)));
      }

      Group updatedGroup =
          store.update(
              AuthorizationUtils.ofGroup(metalake, group),
              GroupEntity.class,
              Entity.EntityType.GROUP,
              groupEntity -> {
                checkObservedRoles(groupEntity.roleNames(), groupEntity.roleIds());
                List<String> roleNames = mutableCopy(groupEntity.roleNames());
                List<Long> roleIds = mutableCopy(groupEntity.roleIds());

                for (RoleEntity roleEntityToGrant : roleEntitiesToGrant) {
                  if (roleIds.contains(roleEntityToGrant.id())) {
                    LOG.warn(
                        "Failed to grant, role {} already exists in the group {} of metalake {}",
                        roleEntityToGrant.name(),
                        group,
                        metalake);
                  } else {
                    roleNames.add(roleEntityToGrant.name());
                    roleIds.add(roleEntityToGrant.id());
                  }
                }

                AuditInfo auditInfo =
                    AuditInfo.builder()
                        .withCreator(groupEntity.auditInfo().creator())
                        .withCreateTime(groupEntity.auditInfo().createTime())
                        .withLastModifier(PrincipalUtils.getCurrentPrincipal().getName())
                        .withLastModifiedTime(Instant.now())
                        .build();

                return GroupEntity.builder()
                    .withId(groupEntity.id())
                    .withNamespace(groupEntity.namespace())
                    .withName(groupEntity.name())
                    .withRoleNames(roleNames)
                    .withRoleIds(roleIds)
                    .withAuditInfo(auditInfo)
                    .build();
              });

      List<SecurableObject> securableObjects = Lists.newArrayList();

      for (Role grantedRole : roleEntitiesToGrant) {
        securableObjects.addAll(grantedRole.securableObjects());
      }

      AuthorizationUtils.callAuthorizationPluginForSecurableObjects(
          metalake,
          securableObjects,
          (authorizationPlugin, catalogName) ->
              authorizationPlugin.onGrantedRolesToGroup(
                  roleEntitiesToGrant.stream()
                      .map(roleEntity -> filterSecurableObjects(roleEntity, metalake, catalogName))
                      .collect(Collectors.toList()),
                  updatedGroup));

      return updatedGroup;
    } catch (NoSuchEntityException nse) {
      LOG.warn("Failed to grant, group {} does not exist in the metalake {}", group, metalake, nse);
      throw new NoSuchGroupException(GROUP_DOES_NOT_EXIST_MSG, group, metalake);
    } catch (NoSuchRoleException nsr) {
      throw new IllegalRoleException(nsr);
    } catch (IOException ioe) {
      LOG.error(
          "Failed to grant role {} to group {} in the metalake {} due to storage issues",
          StringUtils.join(roles, ","),
          group,
          metalake,
          ioe);
      throw new RuntimeException(ioe);
    }
  }

  Group revokeRolesFromGroup(String metalake, List<String> roles, String group) {
    try {
      List<RoleEntity> roleEntitiesToRevoke = Lists.newArrayList();
      for (String role : roles) {
        TreeLockUtils.doWithTreeLock(
            AuthorizationUtils.ofRole(metalake, role),
            LockType.READ,
            () -> roleEntitiesToRevoke.add(roleManager.getRole(metalake, role)));
      }

      Group updatedGroup =
          store.update(
              AuthorizationUtils.ofGroup(metalake, group),
              GroupEntity.class,
              Entity.EntityType.GROUP,
              groupEntity -> {
                checkObservedRoles(groupEntity.roleNames(), groupEntity.roleIds());
                List<String> roleNames = mutableCopy(groupEntity.roleNames());
                List<Long> roleIds = mutableCopy(groupEntity.roleIds());

                for (RoleEntity roleEntityToRevoke : roleEntitiesToRevoke) {
                  int index = roleIds.indexOf(roleEntityToRevoke.id());
                  if (index >= 0) {
                    roleNames.remove(index);
                    roleIds.remove(index);
                  } else {
                    LOG.warn(
                        "Failed to revoke, role {} does not exist in the group {} of metalake {}",
                        roleEntityToRevoke.name(),
                        group,
                        metalake);
                  }
                }

                AuditInfo auditInfo =
                    AuditInfo.builder()
                        .withCreator(groupEntity.auditInfo().creator())
                        .withCreateTime(groupEntity.auditInfo().createTime())
                        .withLastModifier(PrincipalUtils.getCurrentPrincipal().getName())
                        .withLastModifiedTime(Instant.now())
                        .build();

                return GroupEntity.builder()
                    .withNamespace(groupEntity.namespace())
                    .withId(groupEntity.id())
                    .withName(groupEntity.name())
                    .withRoleNames(roleNames)
                    .withRoleIds(roleIds)
                    .withAuditInfo(auditInfo)
                    .build();
              });

      List<SecurableObject> securableObjects = Lists.newArrayList();
      for (Role grantedRole : roleEntitiesToRevoke) {
        securableObjects.addAll(grantedRole.securableObjects());
      }

      AuthorizationUtils.callAuthorizationPluginForSecurableObjects(
          metalake,
          securableObjects,
          (authorizationPlugin, catalogName) ->
              authorizationPlugin.onRevokedRolesFromGroup(
                  roleEntitiesToRevoke.stream()
                      .map(roleEntity -> filterSecurableObjects(roleEntity, metalake, catalogName))
                      .collect(Collectors.toList()),
                  updatedGroup));

      return updatedGroup;

    } catch (NoSuchEntityException nse) {
      LOG.warn(
          "Failed to revoke, group {} does not exist in the metalake {}", group, metalake, nse);
      throw new NoSuchGroupException(GROUP_DOES_NOT_EXIST_MSG, group, metalake);
    } catch (NoSuchRoleException nsr) {
      throw new IllegalRoleException(nsr);
    } catch (IOException ioe) {
      LOG.error(
          "Failed to revoke role {} from  group {} in the metalake {} due to storage issues",
          StringUtils.join(roles, ","),
          group,
          metalake,
          ioe);
      throw new RuntimeException(ioe);
    }
  }

  User revokeRolesFromUser(String metalake, List<String> roles, String user) {
    try {
      List<RoleEntity> roleEntitiesToRevoke = Lists.newArrayList();
      for (String role : roles) {
        TreeLockUtils.doWithTreeLock(
            AuthorizationUtils.ofRole(metalake, role),
            LockType.READ,
            () -> roleEntitiesToRevoke.add(roleManager.getRole(metalake, role)));
      }

      User updatedUser =
          store.update(
              AuthorizationUtils.ofUser(metalake, user),
              UserEntity.class,
              Entity.EntityType.USER,
              userEntity -> {
                checkObservedRoles(userEntity.roleNames(), userEntity.roleIds());
                List<String> roleNames = mutableCopy(userEntity.roleNames());
                List<Long> roleIds = mutableCopy(userEntity.roleIds());

                for (RoleEntity roleEntityToRevoke : roleEntitiesToRevoke) {
                  int index = roleIds.indexOf(roleEntityToRevoke.id());
                  if (index >= 0) {
                    roleNames.remove(index);
                    roleIds.remove(index);
                  } else {
                    LOG.warn(
                        "Failed to revoke, role {} doesn't exist in the user {} of metalake {}",
                        roleEntityToRevoke.name(),
                        user,
                        metalake);
                  }
                }

                AuditInfo auditInfo =
                    AuditInfo.builder()
                        .withCreator(userEntity.auditInfo().creator())
                        .withCreateTime(userEntity.auditInfo().createTime())
                        .withLastModifier(PrincipalUtils.getCurrentPrincipal().getName())
                        .withLastModifiedTime(Instant.now())
                        .build();
                return UserEntity.builder()
                    .withId(userEntity.id())
                    .withNamespace(userEntity.namespace())
                    .withName(userEntity.name())
                    .withRoleNames(roleNames)
                    .withRoleIds(roleIds)
                    .withAuditInfo(auditInfo)
                    .build();
              });

      List<SecurableObject> securableObjects = Lists.newArrayList();
      for (Role grantedRole : roleEntitiesToRevoke) {
        securableObjects.addAll(grantedRole.securableObjects());
      }

      AuthorizationUtils.callAuthorizationPluginForSecurableObjects(
          metalake,
          securableObjects,
          (authorizationPlugin, catalogName) ->
              authorizationPlugin.onRevokedRolesFromUser(
                  roleEntitiesToRevoke.stream()
                      .map(roleEntity -> filterSecurableObjects(roleEntity, metalake, catalogName))
                      .collect(Collectors.toList()),
                  updatedUser));

      return updatedUser;
    } catch (NoSuchEntityException nse) {
      LOG.warn("Failed to revoke, user {} does not exist in the metalake {}", user, metalake, nse);
      throw new NoSuchUserException(USER_DOES_NOT_EXIST_MSG, user, metalake);
    } catch (NoSuchRoleException nsr) {
      throw new IllegalRoleException(nsr);
    } catch (IOException ioe) {
      LOG.error(
          "Failed to revoke role {} from  user {} in the metalake {} due to storage issues",
          StringUtils.join(roles, ","),
          user,
          metalake,
          ioe);
      throw new RuntimeException(ioe);
    }
  }

  Role grantPrivilegesToRole(
      String metalake, String role, MetadataObject object, Set<Privilege> privileges) {
    try {
      AuthorizationPluginCallbackWrapper authorizationPluginCallbackWrapper =
          new AuthorizationPluginCallbackWrapper();

      Role updatedRole =
          store.update(
              AuthorizationUtils.ofRole(metalake, role),
              RoleEntity.class,
              Entity.EntityType.ROLE,
              roleEntity -> {
                List<SecurableObject> grantedSecurableObjects =
                    generateNewSecurableObjects(
                        roleEntity.securableObjects(),
                        object,
                        targetObject -> {
                          if (targetObject == null) {
                            return createNewSecurableObject(
                                metalake,
                                role,
                                object,
                                privileges,
                                roleEntity,
                                authorizationPluginCallbackWrapper);
                          } else {
                            return updateGrantedSecurableObject(
                                metalake,
                                role,
                                object,
                                privileges,
                                roleEntity,
                                targetObject,
                                authorizationPluginCallbackWrapper);
                          }
                        });

                AuditInfo auditInfo =
                    AuditInfo.builder()
                        .withCreator(roleEntity.auditInfo().creator())
                        .withCreateTime(roleEntity.auditInfo().createTime())
                        .withLastModifier(PrincipalUtils.getCurrentPrincipal().getName())
                        .withLastModifiedTime(Instant.now())
                        .build();

                return RoleEntity.builder()
                    .withId(roleEntity.id())
                    .withName(roleEntity.name())
                    .withNamespace(roleEntity.namespace())
                    .withProperties(roleEntity.properties())
                    .withAuditInfo(auditInfo)
                    .withSecurableObjects(grantedSecurableObjects)
                    .build();
              });

      // Execute the authorization plugin callback
      authorizationPluginCallbackWrapper.execute();
      return updatedRole;
    } catch (NoSuchEntityException nse) {
      if (nse.getMessage() != null
          && !nse.getMessage().contains(Entity.EntityType.ROLE.name().toLowerCase())) {
        LOG.error(
            "Failed to grant privilege to role {}, metadata object does not exist", role, nse);
        throw new NoSuchMetadataObjectException(
            nse, "Metadata object does not exist: %s", nse.getMessage());
      }
      LOG.error("Failed to grant, role {} does not exist in the metalake {}", role, metalake, nse);
      throw new NoSuchRoleException(nse, ROLE_DOES_NOT_EXIST_MSG, role, metalake);
    } catch (IOException ioe) {
      LOG.error("Grant privileges to {} failed due to storage issues", role, ioe);
      throw new RuntimeException(ioe);
    }
  }

  private static SecurableObject updateGrantedSecurableObject(
      String metalake,
      String role,
      MetadataObject object,
      Set<Privilege> privileges,
      RoleEntity roleEntity,
      SecurableObject targetObject,
      AuthorizationPluginCallbackWrapper authorizationPluginCallbackWrapper) {
    // Removed duplicated privileges by set
    Set<Privilege> updatePrivileges = Sets.newHashSet();
    updatePrivileges.addAll(targetObject.privileges());
    // If old object contains all the privileges to grant, the object doesn't
    // need to update.
    if (updatePrivileges.containsAll(privileges)) {
      return targetObject;
    } else {
      updatePrivileges.addAll(privileges);
      AuthorizationUtils.checkDuplicatedNamePrivilege(updatePrivileges);

      SecurableObject newSecurableObject =
          SecurableObjects.parse(
              targetObject.fullName(), targetObject.type(), Lists.newArrayList(updatePrivileges));

      // We set authorization callback here, we won't execute this callback in this place.
      // We will execute the callback after we execute the SQL transaction.
      authorizationPluginCallbackWrapper.setCallback(
          () ->
              AuthorizationUtils.callAuthorizationPluginForMetadataObject(
                  metalake,
                  object,
                  authorizationPlugin -> {
                    authorizationPlugin.onRoleUpdated(
                        roleEntity,
                        RoleChange.updateSecurableObject(role, targetObject, newSecurableObject));
                  }));

      return newSecurableObject;
    }
  }

  Role revokePrivilegesFromRole(
      String metalake, String role, MetadataObject object, Set<Privilege> privileges) {
    try {
      AuthorizationPluginCallbackWrapper authorizationCallbackWrapper =
          new AuthorizationPluginCallbackWrapper();

      RoleEntity updatedRole =
          store.update(
              AuthorizationUtils.ofRole(metalake, role),
              RoleEntity.class,
              Entity.EntityType.ROLE,
              roleEntity -> {
                List<SecurableObject> revokedSecurableObjects =
                    generateNewSecurableObjects(
                        roleEntity.securableObjects(),
                        object,
                        targetObject -> {
                          // If the securable object doesn't exist, we do nothing except for
                          // logging.
                          if (targetObject == null) {
                            LOG.warn(
                                "Securable object {} type {} doesn't exist in the role {}",
                                object.fullName(),
                                object.type(),
                                role);
                            return null;
                          } else {
                            // If the securable object exists, we remove the privileges of the
                            // securable object and will remove duplicated privileges at the same
                            // time.
                            return updateRevokedSecurableObject(
                                metalake,
                                role,
                                object,
                                privileges,
                                roleEntity,
                                targetObject,
                                authorizationCallbackWrapper);
                          }
                        });

                AuditInfo auditInfo =
                    AuditInfo.builder()
                        .withCreator(roleEntity.auditInfo().creator())
                        .withCreateTime(roleEntity.auditInfo().createTime())
                        .withLastModifier(PrincipalUtils.getCurrentPrincipal().getName())
                        .withLastModifiedTime(Instant.now())
                        .build();

                return RoleEntity.builder()
                    .withId(roleEntity.id())
                    .withName(roleEntity.name())
                    .withNamespace(roleEntity.namespace())
                    .withProperties(roleEntity.properties())
                    .withAuditInfo(auditInfo)
                    .withSecurableObjects(revokedSecurableObjects)
                    .build();
              });

      authorizationCallbackWrapper.execute();

      return updatedRole;
    } catch (NoSuchEntityException nse) {
      if (nse.getMessage() != null
          && !nse.getMessage().contains(Entity.EntityType.ROLE.name().toLowerCase())) {
        LOG.error(
            "Failed to revoke privilege from role {}, metadata object does not exist", role, nse);
        throw new NoSuchMetadataObjectException(
            nse, "Metadata object does not exist: %s", nse.getMessage());
      }
      LOG.error("Failed to revoke, role {} does not exist in the metalake {}", role, metalake, nse);
      throw new NoSuchRoleException(nse, ROLE_DOES_NOT_EXIST_MSG, role, metalake);
    } catch (IOException ioe) {
      LOG.error("Revoke privileges from {} failed due to storage issues", role, ioe);
      throw new RuntimeException(ioe);
    }
  }

  Role overridePrivilegesInRole(
      String metalake, String role, List<SecurableObject> securableObjectsToOverride) {
    try {
      AuthorizationPluginCallbackWrapper authorizationCallbackWrapper =
          new AuthorizationPluginCallbackWrapper();
      Role updatedRole =
          store.update(
              AuthorizationUtils.ofRole(metalake, role),
              RoleEntity.class,
              Entity.EntityType.ROLE,
              roleEntity -> {
                List<SecurableObject> currentSecurableObjects =
                    Lists.newArrayList(roleEntity.securableObjects());

                // This is used for recording the original state of the role before update.
                // We use this to find which objects existed before update
                Map<MetadataObject, SecurableObject> originObjectMap = Maps.newHashMap();
                for (SecurableObject securableObject : currentSecurableObjects) {
                  originObjectMap.put(
                      MetadataObjects.parse(securableObject.fullName(), securableObject.type()),
                      securableObject);
                }

                // This is used for recording the updated state of the role after update.
                Map<MetadataObject, SecurableObject> updatedObjectMap = Maps.newHashMap();
                for (SecurableObject securableObject : securableObjectsToOverride) {
                  updatedObjectMap.put(
                      MetadataObjects.parse(securableObject.fullName(), securableObject.type()),
                      securableObject);
                }

                // These sets will be used for tracking which objects are created, updated or
                // deleted.
                Set<MetadataObject> authzPluginCreatedObjects =
                    Sets.newHashSet(updatedObjectMap.keySet());
                authzPluginCreatedObjects.removeAll(originObjectMap.keySet());

                Set<MetadataObject> authzPluginUpdateObjects =
                    Sets.newHashSet(updatedObjectMap.keySet());
                authzPluginUpdateObjects.retainAll(originObjectMap.keySet());

                Set<MetadataObject> authzPluginDeletedObjects =
                    Sets.newHashSet(originObjectMap.keySet());
                authzPluginDeletedObjects.removeAll(updatedObjectMap.keySet());

                // We set authorization callback here, we won't execute this callback in this
                // place. We will execute the callback after we execute the SQL transaction.
                authorizationCallbackWrapper.setCallback(
                    () -> {
                      authzPluginCreatedObjects.forEach(
                          object -> {
                            AuthorizationUtils.callAuthorizationPluginForMetadataObject(
                                metalake,
                                object,
                                authorizationPlugin -> {
                                  authorizationPlugin.onRoleUpdated(
                                      roleEntity,
                                      RoleChange.addSecurableObject(
                                          role, updatedObjectMap.get(object)));
                                });
                          });
                      authzPluginUpdateObjects.forEach(
                          object -> {
                            SecurableObject existingObject = originObjectMap.get(object);
                            SecurableObject newSecurableObject = updatedObjectMap.get(object);
                            // If the updated role is the same as the existing one, we don't
                            // need to call the authorization plugin.
                            if (existingObject != null
                                && newSecurableObject != null
                                && !existingObject
                                    .privileges()
                                    .equals(newSecurableObject.privileges())) {
                              AuthorizationUtils.callAuthorizationPluginForMetadataObject(
                                  metalake,
                                  object,
                                  authorizationPlugin -> {
                                    authorizationPlugin.onRoleUpdated(
                                        roleEntity,
                                        RoleChange.updateSecurableObject(
                                            role, existingObject, newSecurableObject));
                                  });
                            }
                          });
                      authzPluginDeletedObjects.forEach(
                          object -> {
                            AuthorizationUtils.callAuthorizationPluginForMetadataObject(
                                metalake,
                                object,
                                authorizationPlugin -> {
                                  authorizationPlugin.onRoleUpdated(
                                      roleEntity,
                                      RoleChange.removeSecurableObject(
                                          role, originObjectMap.get(object)));
                                });
                          });
                    });

                AuditInfo auditInfo =
                    AuditInfo.builder()
                        .withCreator(roleEntity.auditInfo().creator())
                        .withCreateTime(roleEntity.auditInfo().createTime())
                        .withLastModifier(PrincipalUtils.getCurrentPrincipal().getName())
                        .withLastModifiedTime(Instant.now())
                        .build();

                return RoleEntity.builder()
                    .withId(roleEntity.id())
                    .withName(roleEntity.name())
                    .withNamespace(roleEntity.namespace())
                    .withProperties(roleEntity.properties())
                    .withAuditInfo(auditInfo)
                    .withSecurableObjects(Lists.newArrayList(updatedObjectMap.values()))
                    .build();
              });
      authorizationCallbackWrapper.execute();

      return updatedRole;
    } catch (IOException ioe) {
      LOG.error(
          "Updating role {} in the metalake {} failed due to storage issues", role, metalake, ioe);
      throw new RuntimeException(ioe);
    } catch (NoSuchEntityException nse) {
      if (nse.getMessage() != null
          && !nse.getMessage().contains(Entity.EntityType.ROLE.name().toLowerCase())) {
        LOG.error(
            "Failed to override privileges in role {}, metadata object does not exist", role, nse);
        throw new NoSuchMetadataObjectException(
            nse, "Metadata object does not exist: %s", nse.getMessage());
      }
      LOG.error(
          "Failed to override, role {} does not exist in the metalake {}", role, metalake, nse);
      throw new NoSuchRoleException(nse, ROLE_DOES_NOT_EXIST_MSG, role, metalake);
    }
  }

  private static SecurableObject createNewSecurableObject(
      String metalake,
      String role,
      MetadataObject object,
      Set<Privilege> privileges,
      RoleEntity roleEntity,
      AuthorizationPluginCallbackWrapper authorizationPluginCallbackWrapper) {
    AuthorizationUtils.checkDuplicatedNamePrivilege(privileges);

    // Add a new securable object if there doesn't exist the object in the role
    SecurableObject securableObject =
        SecurableObjects.parse(object.fullName(), object.type(), Lists.newArrayList(privileges));

    // We set authorization callback here, we won't execute this callback in this place.
    // We will execute the callback after we execute the SQL transaction.
    authorizationPluginCallbackWrapper.setCallback(
        () ->
            AuthorizationUtils.callAuthorizationPluginForMetadataObject(
                metalake,
                object,
                authorizationPlugin -> {
                  authorizationPlugin.onRoleUpdated(
                      roleEntity, RoleChange.addSecurableObject(role, securableObject));
                }));

    return securableObject;
  }

  @SuppressWarnings("deprecation")
  private static SecurableObject updateRevokedSecurableObject(
      String metalake,
      String role,
      MetadataObject object,
      Set<Privilege> privileges,
      RoleEntity roleEntity,
      SecurableObject targetObject,
      AuthorizationPluginCallbackWrapper authorizationCallbackWrapper) {
    // Use set to deduplicate the privileges
    Set<Privilege> updatePrivileges = Sets.newHashSet();
    updatePrivileges.addAll(targetObject.privileges());
    // Remove the privileges that are being revoked from the current privilege set
    privileges.forEach(updatePrivileges::remove);

    // Handle backward compatibility for model privilege revocation
    // When revoking privileges, we need to handle both old and new privilege names to ensure
    // complete removal regardless of which name was used when granting the privilege.
    for (Privilege privilege : privileges) {
      // Check if this is a deprecated privilege and remove its new equivalent
      Privilege.Name newPrivilegeName =
          AuthorizationUtils.DEPRECATED_PRIVILEGE_MAP.get(privilege.name());
      if (newPrivilegeName != null) {
        // This is a deprecated privilege, remove its new equivalent
        updatePrivileges.remove(
            AuthorizationUtils.replaceLegacyPrivilege(privilege.name(), privilege.condition()));
      }

      // Check if this privilege has a deprecated equivalent and remove it
      Privilege.Name deprecatedPrivilegeName =
          AuthorizationUtils.DEPRECATED_PRIVILEGE_MAP.inverse().get(privilege.name());
      if (deprecatedPrivilegeName != null) {
        // This privilege has a deprecated equivalent, remove it
        updatePrivileges.remove(
            AuthorizationUtils.getLegacyPrivilege(privilege.name(), privilege.condition()));
      }
    }

    // If the object still contains privilege, we should update the object
    // with new privileges
    if (!updatePrivileges.isEmpty()) {
      SecurableObject newSecurableObject =
          SecurableObjects.parse(
              targetObject.fullName(), targetObject.type(), Lists.newArrayList(updatePrivileges));

      // We set authorization callback here, we won't execute this callback in this place.
      // We will execute the callback after we execute the SQL transaction.
      authorizationCallbackWrapper.setCallback(
          () ->
              AuthorizationUtils.callAuthorizationPluginForMetadataObject(
                  metalake,
                  object,
                  authorizationPlugin -> {
                    authorizationPlugin.onRoleUpdated(
                        roleEntity,
                        RoleChange.updateSecurableObject(role, targetObject, newSecurableObject));
                  }));

      return newSecurableObject;
    } else {
      // If the object doesn't contain any privilege, we remove this object.
      // We set authorization callback here, we won't execute this callback in this place.
      // We will execute the callback after we execute the SQL transaction.
      authorizationCallbackWrapper.setCallback(
          () ->
              AuthorizationUtils.callAuthorizationPluginForMetadataObject(
                  metalake,
                  object,
                  authorizationPlugin -> {
                    authorizationPlugin.onRoleUpdated(
                        roleEntity, RoleChange.removeSecurableObject(role, targetObject));
                  }));
      // If we return null, the newly generated objects won't contain this object, the storage will
      // delete this object.
      return null;
    }
  }

  // This method will generate all the securable objects after granting or revoking privileges
  private List<SecurableObject> generateNewSecurableObjects(
      List<SecurableObject> securableObjects,
      MetadataObject targetObject,
      Function<SecurableObject, SecurableObject> objectUpdater) {

    // Find a matched securable object to update privileges
    List<SecurableObject> updateSecurableObjects = Lists.newArrayList(securableObjects);
    SecurableObject matchedSecurableObject =
        securableObjects.stream().filter(targetObject::equals).findFirst().orElse(null);
    if (matchedSecurableObject != null) {
      updateSecurableObjects.remove(matchedSecurableObject);
    }

    // Apply the updates for the matched object
    SecurableObject newSecurableObject = objectUpdater.apply(matchedSecurableObject);
    if (newSecurableObject != null) {
      updateSecurableObjects.add(newSecurableObject);
    }
    return updateSecurableObjects;
  }

  private static class AuthorizationPluginCallbackWrapper {
    private Runnable callback;

    public void setCallback(Runnable callback) {
      this.callback = callback;
    }

    public void execute() {
      if (callback != null) {
        callback.run();
      }
    }
  }

  // The principal handed to the updater already carries its memberships as a (name, ID) pair per
  // role, read from the membership join that UserMetaService#updateUser and
  // GroupMetaService#updateGroup run inside the update transaction. Resolving those names again
  // here would cost one query per existing role inside that transaction, and it would resolve them
  // by name, which is what lets a deleted-and-recreated role take an observed membership over. The
  // observed IDs are carried forward untouched instead, so a replacement never inherits a grant and
  // the transaction issues no extra reads. Only the pairing itself still needs checking, because
  // role IDs are an optional entity field and a name alone cannot prove membership identity.
  private static void checkObservedRoles(
      @Nullable List<String> roleNames, @Nullable List<Long> roleIds) {
    if (roleNames == null || roleNames.isEmpty()) {
      return;
    }
    if (roleIds == null || roleNames.size() != roleIds.size()) {
      throw new IllegalRoleException("Existing role names and IDs must be paired");
    }
  }

  private static <T> List<T> mutableCopy(@Nullable List<T> values) {
    return values == null ? Lists.newArrayList() : Lists.newArrayList(values);
  }
}
