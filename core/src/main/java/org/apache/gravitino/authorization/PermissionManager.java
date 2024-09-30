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

import com.google.common.collect.Lists;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchGroupException;
import org.apache.gravitino.exceptions.NoSuchRoleException;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.utils.PrincipalUtils;
import org.glassfish.jersey.internal.guava.Sets;
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
        roleEntitiesToGrant.add(roleManager.getRole(metalake, role));
      }

      User updatedUser =
          store.update(
              AuthorizationUtils.ofUser(metalake, user),
              UserEntity.class,
              Entity.EntityType.USER,
              userEntity -> {
                List<RoleEntity> roleEntities = Lists.newArrayList();
                if (userEntity.roleNames() != null) {
                  for (String role : userEntity.roleNames()) {
                    roleEntities.add(roleManager.getRole(metalake, role));
                  }
                }
                List<String> roleNames = Lists.newArrayList(toRoleNames(roleEntities));
                List<Long> roleIds = Lists.newArrayList(toRoleIds(roleEntities));

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

      Set<String> catalogs = Sets.newHashSet();
      for (Role grantedRole : roleEntitiesToGrant) {
        AuthorizationUtils.callAuthorizationPluginForSecurableObjects(
            metalake,
            grantedRole.securableObjects(),
            catalogs,
            authorizationPlugin ->
                authorizationPlugin.onGrantedRolesToUser(
                    Lists.newArrayList(roleEntitiesToGrant), updatedUser));
      }

      return updatedUser;
    } catch (NoSuchEntityException nse) {
      LOG.warn("Failed to grant, user {} does not exist in the metalake {}", user, metalake, nse);
      throw new NoSuchUserException(USER_DOES_NOT_EXIST_MSG, user, metalake);
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
        roleEntitiesToGrant.add(roleManager.getRole(metalake, role));
      }

      Group updatedGroup =
          store.update(
              AuthorizationUtils.ofGroup(metalake, group),
              GroupEntity.class,
              Entity.EntityType.GROUP,
              groupEntity -> {
                List<RoleEntity> roleEntities = Lists.newArrayList();
                if (groupEntity.roleNames() != null) {
                  for (String role : groupEntity.roleNames()) {
                    roleEntities.add(roleManager.getRole(metalake, role));
                  }
                }
                List<String> roleNames = Lists.newArrayList(toRoleNames(roleEntities));
                List<Long> roleIds = Lists.newArrayList(toRoleIds(roleEntities));

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

      Set<String> catalogs = Sets.newHashSet();
      for (Role grantedRole : roleEntitiesToGrant) {
        AuthorizationUtils.callAuthorizationPluginForSecurableObjects(
            metalake,
            grantedRole.securableObjects(),
            catalogs,
            authorizationPlugin ->
                authorizationPlugin.onGrantedRolesToGroup(
                    Lists.newArrayList(roleEntitiesToGrant), updatedGroup));
      }

      return updatedGroup;
    } catch (NoSuchEntityException nse) {
      LOG.warn("Failed to grant, group {} does not exist in the metalake {}", group, metalake, nse);
      throw new NoSuchGroupException(GROUP_DOES_NOT_EXIST_MSG, group, metalake);
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
        roleEntitiesToRevoke.add(roleManager.getRole(metalake, role));
      }

      Group updatedGroup =
          store.update(
              AuthorizationUtils.ofGroup(metalake, group),
              GroupEntity.class,
              Entity.EntityType.GROUP,
              groupEntity -> {
                List<RoleEntity> roleEntities = Lists.newArrayList();
                if (groupEntity.roleNames() != null) {
                  for (String role : groupEntity.roleNames()) {
                    roleEntities.add(roleManager.getRole(metalake, role));
                  }
                }
                List<String> roleNames = Lists.newArrayList(toRoleNames(roleEntities));
                List<Long> roleIds = Lists.newArrayList(toRoleIds(roleEntities));

                for (RoleEntity roleEntityToRevoke : roleEntitiesToRevoke) {
                  roleNames.remove(roleEntityToRevoke.name());
                  boolean removed = roleIds.remove(roleEntityToRevoke.id());
                  if (!removed) {
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

      Set<String> catalogs = Sets.newHashSet();
      for (Role grantedRole : roleEntitiesToRevoke) {
        AuthorizationUtils.callAuthorizationPluginForSecurableObjects(
            metalake,
            grantedRole.securableObjects(),
            catalogs,
            authorizationPlugin ->
                authorizationPlugin.onRevokedRolesFromGroup(
                    Lists.newArrayList(roleEntitiesToRevoke), updatedGroup));
      }

      return updatedGroup;

    } catch (NoSuchEntityException nse) {
      LOG.warn(
          "Failed to revoke, group {} does not exist in the metalake {}", group, metalake, nse);
      throw new NoSuchGroupException(GROUP_DOES_NOT_EXIST_MSG, group, metalake);
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
        roleEntitiesToRevoke.add(roleManager.getRole(metalake, role));
      }

      User updatedUser =
          store.update(
              AuthorizationUtils.ofUser(metalake, user),
              UserEntity.class,
              Entity.EntityType.USER,
              userEntity -> {
                List<RoleEntity> roleEntities = Lists.newArrayList();
                if (userEntity.roleNames() != null) {
                  for (String role : userEntity.roleNames()) {
                    roleEntities.add(roleManager.getRole(metalake, role));
                  }
                }

                List<String> roleNames = Lists.newArrayList(toRoleNames(roleEntities));
                List<Long> roleIds = Lists.newArrayList(toRoleIds(roleEntities));

                for (RoleEntity roleEntityToRevoke : roleEntitiesToRevoke) {
                  roleNames.remove(roleEntityToRevoke.name());
                  boolean removed = roleIds.remove(roleEntityToRevoke.id());
                  if (!removed) {
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

      Set<String> catalogs = Sets.newHashSet();
      for (Role grantedRole : roleEntitiesToRevoke) {
        AuthorizationUtils.callAuthorizationPluginForSecurableObjects(
            metalake,
            grantedRole.securableObjects(),
            catalogs,
            authorizationPlugin ->
                authorizationPlugin.onRevokedRolesFromUser(
                    Lists.newArrayList(roleEntitiesToRevoke), updatedUser));
      }

      return updatedUser;
    } catch (NoSuchEntityException nse) {
      LOG.warn("Failed to revoke, user {} does not exist in the metalake {}", user, metalake, nse);
      throw new NoSuchUserException(USER_DOES_NOT_EXIST_MSG, user, metalake);
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
      String metalake, String role, MetadataObject object, List<Privilege> privileges) {
    try {
      AuthorizationPluginCallbackWrapper authorizationPluginCallbackWrapper =
          new AuthorizationPluginCallbackWrapper();
      Role updatedRole =
          store.update(
              AuthorizationUtils.ofRole(metalake, role),
              RoleEntity.class,
              Entity.EntityType.ROLE,
              roleEntity -> {
                List<SecurableObject> updateSecurableObjects =
                    updateSecurableObjects(
                        roleEntity.securableObjects(),
                        object,
                        oldObject -> {
                          if (oldObject == null) {
                            // Add a new securable object if there not exists the object in the role
                            SecurableObject securableObject =
                                SecurableObjects.parse(
                                    object.fullName(),
                                    object.type(),
                                    Lists.newArrayList(privileges));

                            authorizationPluginCallbackWrapper.setCallBack(
                                () ->
                                    AuthorizationUtils.callAuthorizationPluginForMetadataObject(
                                        metalake,
                                        object,
                                        authorizationPlugin -> {
                                          authorizationPlugin.onRoleUpdated(
                                              roleEntity,
                                              RoleChange.addSecurableObject(role, securableObject));
                                        }));

                            return securableObject;
                          } else {
                            // Removed duplicated privileges by set
                            Set<Privilege> updatePrivileges = Sets.newHashSet();
                            updatePrivileges.addAll(oldObject.privileges());
                            // If old object contains all the privileges to grant, the object don't
                            // need to change.
                            if (updatePrivileges.containsAll(privileges)) {
                              return oldObject;
                            } else {
                              updatePrivileges.addAll(privileges);
                              SecurableObject newSecurableObject =
                                  SecurableObjects.parse(
                                      oldObject.fullName(),
                                      oldObject.type(),
                                      Lists.newArrayList(updatePrivileges));

                              authorizationPluginCallbackWrapper.setCallBack(
                                  () ->
                                      AuthorizationUtils.callAuthorizationPluginForMetadataObject(
                                          metalake,
                                          object,
                                          authorizationPlugin -> {
                                            authorizationPlugin.onRoleUpdated(
                                                roleEntity,
                                                RoleChange.updateSecurableObject(
                                                    role, oldObject, newSecurableObject));
                                          }));

                              return newSecurableObject;
                            }
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
                    .withSecurableObjects(updateSecurableObjects)
                    .build();
              });

      authorizationPluginCallbackWrapper.execute();
      return updatedRole;
    } catch (NoSuchEntityException nse) {
      LOG.error("Failed to grant, role {} does not exist in the metalake {}", role, metalake, nse);
      throw new NoSuchRoleException(ROLE_DOES_NOT_EXIST_MSG, role, metalake);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  Role revokePrivilegesFromRole(
      String metalake, String role, MetadataObject object, List<Privilege> privileges) {
    try {
      AuthorizationPluginCallbackWrapper authorizationCallbackWrapper =
          new AuthorizationPluginCallbackWrapper();

      RoleEntity updatedRole =
          store.update(
              AuthorizationUtils.ofRole(metalake, role),
              RoleEntity.class,
              Entity.EntityType.ROLE,
              roleEntity -> {
                List<SecurableObject> updateSecurableObjects =
                    updateSecurableObjects(
                        roleEntity.securableObjects(),
                        object,
                        oldObject -> {
                          // If securable object doesn't exist, we do nothing except for logging.
                          if (oldObject == null) {
                            LOG.warn(
                                "Securable object {} type {} doesn't exist in the role {}",
                                object.fullName(),
                                object.type(),
                                role);
                            return null;
                          } else {
                            // If securable object exists, we remove the privileges of the securable
                            // object.
                            // Remove duplicated privileges
                            Set<Privilege> updatePrivileges = Sets.newHashSet();
                            updatePrivileges.addAll(oldObject.privileges());
                            privileges.forEach(updatePrivileges::remove);

                            // If object still contains privilege, we should update the object with
                            // new privileges
                            if (!updatePrivileges.isEmpty()) {
                              SecurableObject newSecurableObject =
                                  SecurableObjects.parse(
                                      oldObject.fullName(),
                                      oldObject.type(),
                                      Lists.newArrayList(updatePrivileges));
                              authorizationCallbackWrapper.setCallBack(
                                  () ->
                                      AuthorizationUtils.callAuthorizationPluginForMetadataObject(
                                          metalake,
                                          object,
                                          authorizationPlugin -> {
                                            authorizationPlugin.onRoleUpdated(
                                                roleEntity,
                                                RoleChange.updateSecurableObject(
                                                    role, oldObject, newSecurableObject));
                                          }));

                              return newSecurableObject;
                            } else {
                              // If the object doesn't contain any privilege, we remove this object.
                              authorizationCallbackWrapper.setCallBack(
                                  () ->
                                      AuthorizationUtils.callAuthorizationPluginForMetadataObject(
                                          metalake,
                                          object,
                                          authorizationPlugin -> {
                                            authorizationPlugin.onRoleUpdated(
                                                roleEntity,
                                                RoleChange.removeSecurableObject(role, oldObject));
                                          }));

                              return null;
                            }
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
                    .withSecurableObjects(updateSecurableObjects)
                    .build();
              });

      authorizationCallbackWrapper.execute();

      return updatedRole;
    } catch (NoSuchEntityException nse) {
      LOG.error("Failed to revoke, role {} does not exist in the metalake {}", role, metalake, nse);
      throw new NoSuchRoleException(ROLE_DOES_NOT_EXIST_MSG, role, metalake);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  private List<SecurableObject> updateSecurableObjects(
      List<SecurableObject> securableObjects,
      MetadataObject targetObject,
      Function<SecurableObject, SecurableObject> objectUpdater) {
    boolean isExist = false;
    List<SecurableObject> updateSecurableObjects = Lists.newArrayList();
    for (SecurableObject securableObject : securableObjects) {
      // If a securable object is matching the target object, we apply the updates
      // to this securable object.
      if (!isExist && targetObject.equals(securableObject)) {
        isExist = true;
        SecurableObject newSecurableObject = objectUpdater.apply(securableObject);
        if (newSecurableObject != null) {
          updateSecurableObjects.add(newSecurableObject);
        }
      } else {
        updateSecurableObjects.add(securableObject);
      }
    }

    // If there not exists a securable object matching the target object.
    if (!isExist) {
      SecurableObject newSecurableObject = objectUpdater.apply(null);
      if (newSecurableObject != null) {
        updateSecurableObjects.add(newSecurableObject);
      }
    }
    return updateSecurableObjects;
  }

  private static class AuthorizationPluginCallbackWrapper {
    private Runnable callback;

    public void setCallBack(Runnable callback) {
      this.callback = callback;
    }

    public void execute() {
      if (callback != null) {
        callback.run();
      }
    }
  }

  private List<Long> toRoleIds(List<RoleEntity> roleEntities) {
    return roleEntities.stream().map(RoleEntity::id).collect(Collectors.toList());
  }

  private List<String> toRoleNames(List<RoleEntity> roleEntities) {
    return roleEntities.stream().map(RoleEntity::name).collect(Collectors.toList());
  }
}
