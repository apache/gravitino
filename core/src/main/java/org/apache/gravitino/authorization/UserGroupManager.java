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

import com.google.common.collect.Lists;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.connector.authorization.AuthorizationPlugin;
import org.apache.gravitino.exceptions.AuthorizationPluginException;
import org.apache.gravitino.exceptions.GroupAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchGroupException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.apache.gravitino.exceptions.UserAlreadyExistsException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.metalake.MetalakeManager;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.relational.service.GroupMetaService;
import org.apache.gravitino.storage.relational.service.UserMetaService;
import org.apache.gravitino.utils.PrincipalUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * UserGroupManager is used for adding, removing and getting users or groups from one metalake.
 * Metalake is like a concept of the organization. `AddUser` or `AddGroup` means that a user or
 * group enters an organization.
 */
class UserGroupManager {

  private static final Logger LOG = LoggerFactory.getLogger(UserGroupManager.class);
  private static final String METALAKE_DOES_NOT_EXIST_MSG = "Metalake %s does not exist";

  protected final EntityStore store;
  protected final IdGenerator idGenerator;

  protected UserGroupManager(EntityStore store, IdGenerator idGenerator) {
    this.store = store;
    this.idGenerator = idGenerator;
  }

  User addUser(String metalake, String name) throws UserAlreadyExistsException {
    MetalakeManager.checkMetalake(NameIdentifier.of(metalake), store);
    try {
      UserEntity userEntity =
          UserEntity.builder()
              .withId(idGenerator.nextId())
              .withName(name)
              .withNamespace(AuthorizationUtils.ofUserNamespace(metalake))
              .withRoleNames(Lists.newArrayList())
              .withAuditInfo(
                  AuditInfo.builder()
                      .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                      .withCreateTime(Instant.now())
                      .build())
              .build();
      store.put(userEntity, false /* overwritten */);
      return userEntity;
    } catch (EntityAlreadyExistsException e) {
      LOG.warn("User {} in the metalake {} already exists", name, metalake, e);
      throw new UserAlreadyExistsException(
          "User %s in the metalake %s already exists", name, metalake);
    } catch (IOException ioe) {
      LOG.error(
          "Adding user {} failed in the metalake {} due to storage issues", name, metalake, ioe);
      throw new RuntimeException(ioe);
    }
  }

  boolean removeUser(String metalake, String user) {
    MetalakeManager.checkMetalake(NameIdentifier.of(metalake), store);
    NameIdentifier userIdent = AuthorizationUtils.ofUser(metalake, user);
    try {
      // Read the user's roles before the delete, which also removes the user's role relations.
      UserEntity userEntity = getEntityOrNull(userIdent, Entity.EntityType.USER, UserEntity.class);
      List<RoleEntity> roles =
          userEntity == null
              ? Collections.emptyList()
              : loadRoles(metalake, userEntity.roleNames());

      boolean deleted = store.delete(userIdent, Entity.EntityType.USER);

      // Only after a successful delete, revoke the roles from the user in the authorization
      // plugins, the same way PermissionManager.revokeRolesFromUser does. Otherwise the plugins,
      // e.g. Ranger, keep the user as a member of its roles. onUserRemoved is not called: it drops
      // the user in the underlying system, and addUser does not call onUserAdded either.
      if (deleted && !roles.isEmpty()) {
        revokeRolesInAuthorizationPlugins(
            metalake,
            "User " + user,
            roles,
            (authorizationPlugin, catalogRoles) ->
                authorizationPlugin.onRevokedRolesFromUser(catalogRoles, userEntity));
      }
      return deleted;
    } catch (IOException ioe) {
      LOG.error(
          "Removing user {} in the metalake {} failed due to storage issues", user, metalake, ioe);
      throw new RuntimeException(ioe);
    }
  }

  User getUser(String metalake, String user) throws NoSuchUserException {
    try {
      return store.get(
          AuthorizationUtils.ofUser(metalake, user), Entity.EntityType.USER, UserEntity.class);
    } catch (NoSuchEntityException e) {
      LOG.warn("User {} does not exist in the metalake {}", user, metalake, e);
      throw new NoSuchUserException(AuthorizationUtils.USER_DOES_NOT_EXIST_MSG, user, metalake);
    } catch (IOException ioe) {
      LOG.error("Getting user {} failed due to storage issues", user, ioe);
      throw new RuntimeException(ioe);
    }
  }

  String[] listUserNames(String metalake) {

    return Arrays.stream(listUsersInternal(metalake, false /* allFields */))
        .map(User::name)
        .toArray(String[]::new);
  }

  User[] listUsers(String metalake) {
    return listUsersInternal(metalake, true /* allFields */);
  }

  long countUsers(String metalake) {
    MetalakeManager.checkMetalake(NameIdentifier.of(metalake), store);
    return UserMetaService.getInstance().countUsersByMetalake(metalake);
  }

  PagedResult<User> listUsers(String metalake, int offset, int limit) {
    MetalakeManager.checkMetalake(NameIdentifier.of(metalake), store);
    PagedResult<UserEntity> result =
        UserMetaService.getInstance().listUsersByMetalakePaginated(metalake, offset, limit);
    return new PagedResult<>(
        result.totalCount(), Arrays.asList(result.items().toArray(new User[0])));
  }

  Group addGroup(String metalake, String group) throws GroupAlreadyExistsException {
    MetalakeManager.checkMetalake(NameIdentifier.of(metalake), store);
    try {
      GroupEntity groupEntity =
          GroupEntity.builder()
              .withId(idGenerator.nextId())
              .withName(group)
              .withNamespace(AuthorizationUtils.ofGroupNamespace(metalake))
              .withRoleNames(Collections.emptyList())
              .withAuditInfo(
                  AuditInfo.builder()
                      .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                      .withCreateTime(Instant.now())
                      .build())
              .build();
      store.put(groupEntity, false /* overwritten */);
      return groupEntity;
    } catch (EntityAlreadyExistsException e) {
      LOG.warn("Group {} in the metalake {} already exists", group, metalake, e);
      throw new GroupAlreadyExistsException(
          "Group %s in the metalake %s already exists", group, metalake);
    } catch (IOException ioe) {
      LOG.error(
          "Adding group {} failed in the metalake {} due to storage issues", group, metalake, ioe);
      throw new RuntimeException(ioe);
    }
  }

  boolean removeGroup(String metalake, String group) {
    MetalakeManager.checkMetalake(NameIdentifier.of(metalake), store);
    NameIdentifier groupIdent = AuthorizationUtils.ofGroup(metalake, group);
    try {
      // Read the group's roles before the delete, which also removes the group's role relations.
      GroupEntity groupEntity =
          getEntityOrNull(groupIdent, Entity.EntityType.GROUP, GroupEntity.class);
      List<RoleEntity> roles =
          groupEntity == null
              ? Collections.emptyList()
              : loadRoles(metalake, groupEntity.roleNames());

      boolean deleted = store.delete(groupIdent, Entity.EntityType.GROUP);

      // Only after a successful delete, revoke the roles from the group in the authorization
      // plugins, the same way PermissionManager.revokeRolesFromGroup does. onGroupRemoved is not
      // called for the same reason as onUserRemoved in removeUser.
      if (deleted && !roles.isEmpty()) {
        revokeRolesInAuthorizationPlugins(
            metalake,
            "Group " + group,
            roles,
            (authorizationPlugin, catalogRoles) ->
                authorizationPlugin.onRevokedRolesFromGroup(catalogRoles, groupEntity));
      }
      return deleted;
    } catch (IOException ioe) {
      LOG.error(
          "Removing group {} in the metalake {} failed due to storage issues",
          group,
          metalake,
          ioe);
      throw new RuntimeException(ioe);
    }
  }

  Group getGroup(String metalake, String group) {
    try {
      return store.get(
          AuthorizationUtils.ofGroup(metalake, group), Entity.EntityType.GROUP, GroupEntity.class);
    } catch (NoSuchEntityException e) {
      LOG.warn("Group {} does not exist in the metalake {}", group, metalake, e);
      throw new NoSuchGroupException(AuthorizationUtils.GROUP_DOES_NOT_EXIST_MSG, group, metalake);
    } catch (IOException ioe) {
      LOG.error("Getting group {} failed due to storage issues", group, ioe);
      throw new RuntimeException(ioe);
    }
  }

  Group[] listGroups(String metalake) {
    return listGroupInternal(metalake, true);
  }

  long countGroups(String metalake) {
    MetalakeManager.checkMetalake(NameIdentifier.of(metalake), store);
    return GroupMetaService.getInstance().countGroupsByMetalake(metalake);
  }

  PagedResult<Group> listGroups(String metalake, int offset, int limit) {
    MetalakeManager.checkMetalake(NameIdentifier.of(metalake), store);
    PagedResult<GroupEntity> result =
        GroupMetaService.getInstance().listGroupsByMetalakePaginated(metalake, offset, limit);
    return new PagedResult<>(
        result.totalCount(), Arrays.asList(result.items().toArray(new Group[0])));
  }

  String[] listGroupNames(String metalake) {
    return Arrays.stream(listGroupInternal(metalake, false))
        .map(Group::name)
        .toArray(String[]::new);
  }

  private User[] listUsersInternal(String metalake, boolean allFields) {
    try {
      Namespace namespace = AuthorizationUtils.ofUserNamespace(metalake);
      return store
          .list(namespace, UserEntity.class, Entity.EntityType.USER, allFields)
          .toArray(new User[0]);
    } catch (NoSuchEntityException e) {
      LOG.error("Metalake {} does not exist", metalake, e);
      throw new NoSuchMetalakeException(METALAKE_DOES_NOT_EXIST_MSG, metalake);
    } catch (IOException ioe) {
      LOG.error("Listing user under metalake {} failed due to storage issues", metalake, ioe);
      throw new RuntimeException(ioe);
    }
  }

  private Group[] listGroupInternal(String metalake, boolean allFields) {
    try {
      Namespace namespace = AuthorizationUtils.ofGroupNamespace(metalake);
      return store
          .list(namespace, GroupEntity.class, EntityType.GROUP, allFields)
          .toArray(new Group[0]);
    } catch (NoSuchEntityException e) {
      LOG.error("Metalake {} does not exist", metalake, e);
      throw new NoSuchMetalakeException(METALAKE_DOES_NOT_EXIST_MSG, metalake);
    } catch (IOException ioe) {
      LOG.error("Listing group under metalake {} failed due to storage issues", metalake, ioe);
      throw new RuntimeException(ioe);
    }
  }

  @Nullable
  private <E extends Entity & HasIdentifier> E getEntityOrNull(
      NameIdentifier ident, Entity.EntityType type, Class<E> clazz) throws IOException {
    try {
      return store.get(ident, type, clazz);
    } catch (NoSuchEntityException e) {
      return null;
    }
  }

  private List<RoleEntity> loadRoles(String metalake, @Nullable List<String> roleNames)
      throws IOException {
    List<RoleEntity> roles = Lists.newArrayList();
    if (roleNames == null) {
      return roles;
    }

    for (String roleName : roleNames) {
      RoleEntity role =
          getEntityOrNull(
              AuthorizationUtils.ofRole(metalake, roleName),
              Entity.EntityType.ROLE,
              RoleEntity.class);
      if (role == null) {
        LOG.warn("Role {} in the metalake {} no longer exists, skip it", roleName, metalake);
        continue;
      }
      roles.add(role);
    }
    return roles;
  }

  /**
   * Revokes the roles of a removed user or group in the authorization plugin of every catalog the
   * roles touch. The principal is already deleted, so a retry would not reach the plugins again: a
   * failing catalog is logged and does not stop the remaining catalogs, and the failures are
   * reported together at the end.
   */
  private static void revokeRolesInAuthorizationPlugins(
      String metalake,
      String principal,
      List<RoleEntity> roles,
      BiConsumer<AuthorizationPlugin, List<Role>> revoke) {
    List<String> roleNames = roles.stream().map(RoleEntity::name).collect(Collectors.toList());
    List<SecurableObject> securableObjects = Lists.newArrayList();
    for (RoleEntity role : roles) {
      securableObjects.addAll(role.securableObjects());
    }

    Set<NameIdentifier> catalogs;
    try {
      catalogs = AuthorizationUtils.getSecurableObjectCatalogs(metalake, securableObjects);
    } catch (RuntimeException e) {
      LOG.error(
          "{} was removed from the metalake {}, but the catalogs of its roles {} could not be "
              + "listed, so the roles were not revoked in any authorization plugin",
          principal,
          metalake,
          roleNames,
          e);
      throw new AuthorizationPluginException(
          e,
          "%s was removed from the metalake %s, but revoking its roles in the authorization "
              + "plugins failed: %s",
          principal,
          metalake,
          e.getMessage());
    }

    List<String> failedCatalogs = Lists.newArrayList();
    List<RuntimeException> failures = Lists.newArrayList();
    for (NameIdentifier catalog : catalogs) {
      try {
        AuthorizationUtils.callAuthorizationPluginForCatalog(
            catalog,
            (authorizationPlugin, catalogName) ->
                revoke.accept(
                    authorizationPlugin,
                    roles.stream()
                        .map(
                            role ->
                                AuthorizationUtils.filterSecurableObjects(
                                    role, metalake, catalogName))
                        .collect(Collectors.toList())));
      } catch (RuntimeException e) {
        LOG.error(
            "{} was removed from the metalake {}, but revoking its roles {} in the authorization "
                + "plugin of the catalog {} failed. The plugin may keep it as a member of these "
                + "roles.",
            principal,
            metalake,
            roleNames,
            catalog.name(),
            e);
        failedCatalogs.add(catalog.name());
        failures.add(e);
      }
    }

    if (!failures.isEmpty()) {
      AuthorizationPluginException aggregated =
          new AuthorizationPluginException(
              failures.get(0),
              "%s was removed from the metalake %s, but revoking its roles in the authorization "
                  + "plugins of the catalogs %s failed",
              principal,
              metalake,
              failedCatalogs);
      for (int i = 1; i < failures.size(); i++) {
        aggregated.addSuppressed(failures.get(i));
      }
      throw aggregated;
    }
  }
}
