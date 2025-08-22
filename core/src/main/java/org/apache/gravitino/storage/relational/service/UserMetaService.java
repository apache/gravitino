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
package org.apache.gravitino.storage.relational.service;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.storage.relational.mapper.OwnerMetaMapper;
import org.apache.gravitino.storage.relational.mapper.UserMetaMapper;
import org.apache.gravitino.storage.relational.mapper.UserRoleRelMapper;
import org.apache.gravitino.storage.relational.po.ExtendedUserPO;
import org.apache.gravitino.storage.relational.po.RolePO;
import org.apache.gravitino.storage.relational.po.UserPO;
import org.apache.gravitino.storage.relational.po.UserRoleRelPO;
import org.apache.gravitino.storage.relational.utils.ExceptionUtils;
import org.apache.gravitino.storage.relational.utils.POConverters;
import org.apache.gravitino.storage.relational.utils.SessionUtils;

/** The service class for user metadata. It provides the basic database operations for user. */
public class UserMetaService {
  private static final UserMetaService INSTANCE = new UserMetaService();

  public static UserMetaService getInstance() {
    return INSTANCE;
  }

  private UserMetaService() {}

  private UserPO getUserPOByMetalakeIdAndName(Long metalakeId, String userName) {
    UserPO userPO =
        SessionUtils.getWithoutCommit(
            UserMetaMapper.class,
            mapper -> mapper.selectUserMetaByMetalakeIdAndName(metalakeId, userName));

    if (userPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.USER.name().toLowerCase(),
          userName);
    }
    return userPO;
  }

  public Long getUserIdByMetalakeIdAndName(Long metalakeId, String userName) {
    Long userId =
        SessionUtils.getWithoutCommit(
            UserMetaMapper.class,
            mapper -> mapper.selectUserIdByMetalakeIdAndName(metalakeId, userName));

    if (userId == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.USER.name().toLowerCase(),
          userName);
    }
    return userId;
  }

  public UserEntity getUserByIdentifier(NameIdentifier identifier) {
    AuthorizationUtils.checkUser(identifier);

    Long metalakeId =
        MetalakeMetaService.getInstance().getMetalakeIdByName(identifier.namespace().level(0));
    UserPO userPO = getUserPOByMetalakeIdAndName(metalakeId, identifier.name());
    List<RolePO> rolePOs = RoleMetaService.getInstance().listRolesByUserId(userPO.getUserId());

    return POConverters.fromUserPO(userPO, rolePOs, identifier.namespace());
  }

  public List<UserEntity> listUsersByRoleIdent(NameIdentifier roleIdent) {
    RoleEntity roleEntity = RoleMetaService.getInstance().getRoleByIdentifier(roleIdent);
    List<UserPO> userPOs =
        SessionUtils.getWithoutCommit(
            UserMetaMapper.class, mapper -> mapper.listUsersByRoleId(roleEntity.id()));
    return userPOs.stream()
        .map(
            po ->
                POConverters.fromUserPO(
                    po,
                    Collections.emptyList(),
                    AuthorizationUtils.ofUserNamespace(roleIdent.namespace().level(0))))
        .collect(Collectors.toList());
  }

  public void insertUser(UserEntity userEntity, boolean overwritten) throws IOException {
    try {
      AuthorizationUtils.checkUser(userEntity.nameIdentifier());

      Long metalakeId =
          MetalakeMetaService.getInstance().getMetalakeIdByName(userEntity.namespace().level(0));
      UserPO.Builder builder = UserPO.builder().withMetalakeId(metalakeId);
      UserPO userPO = POConverters.initializeUserPOWithVersion(userEntity, builder);

      List<Long> roleIds = Optional.ofNullable(userEntity.roleIds()).orElse(Lists.newArrayList());
      List<UserRoleRelPO> userRoleRelPOs =
          POConverters.initializeUserRoleRelsPOWithVersion(userEntity, roleIds);

      SessionUtils.doMultipleWithCommit(
          () ->
              SessionUtils.doWithoutCommit(
                  UserMetaMapper.class,
                  mapper -> {
                    if (overwritten) {
                      mapper.insertUserMetaOnDuplicateKeyUpdate(userPO);
                    } else {
                      mapper.insertUserMeta(userPO);
                    }
                  }),
          () -> {
            SessionUtils.doWithoutCommit(
                UserRoleRelMapper.class,
                mapper -> {
                  if (overwritten) {
                    mapper.softDeleteUserRoleRelByUserId(userEntity.id());
                  }
                  if (!userRoleRelPOs.isEmpty()) {
                    mapper.batchInsertUserRoleRel(userRoleRelPOs);
                  }
                });
          });
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.USER, userEntity.nameIdentifier().toString());
      throw re;
    }
  }

  public boolean deleteUser(NameIdentifier identifier) {
    AuthorizationUtils.checkUser(identifier);

    Long metalakeId =
        MetalakeMetaService.getInstance().getMetalakeIdByName(identifier.namespace().level(0));
    Long userId = getUserIdByMetalakeIdAndName(metalakeId, identifier.name());

    SessionUtils.doMultipleWithCommit(
        () ->
            SessionUtils.doWithoutCommit(
                UserMetaMapper.class, mapper -> mapper.softDeleteUserMetaByUserId(userId)),
        () ->
            SessionUtils.doWithoutCommit(
                UserRoleRelMapper.class, mapper -> mapper.softDeleteUserRoleRelByUserId(userId)),
        () ->
            SessionUtils.doWithoutCommit(
                OwnerMetaMapper.class,
                mapper ->
                    mapper.softDeleteOwnerRelByOwnerIdAndType(
                        userId, Entity.EntityType.USER.name())));
    return true;
  }

  public <E extends Entity & HasIdentifier> UserEntity updateUser(
      NameIdentifier identifier, Function<E, E> updater) throws IOException {
    AuthorizationUtils.checkUser(identifier);

    Long metalakeId =
        MetalakeMetaService.getInstance().getMetalakeIdByName(identifier.namespace().level(0));
    UserPO oldUserPO = getUserPOByMetalakeIdAndName(metalakeId, identifier.name());
    List<RolePO> rolePOs = RoleMetaService.getInstance().listRolesByUserId(oldUserPO.getUserId());
    UserEntity oldUserEntity = POConverters.fromUserPO(oldUserPO, rolePOs, identifier.namespace());

    UserEntity newEntity = (UserEntity) updater.apply((E) oldUserEntity);
    Preconditions.checkArgument(
        Objects.equals(oldUserEntity.id(), newEntity.id()),
        "The updated user entity id: %s should be same with the user entity id before: %s",
        newEntity.id(),
        oldUserEntity.id());

    Set<Long> oldRoleIds =
        oldUserEntity.roleIds() == null
            ? Sets.newHashSet()
            : Sets.newHashSet(oldUserEntity.roleIds());
    Set<Long> newRoleIds =
        newEntity.roleIds() == null ? Sets.newHashSet() : Sets.newHashSet(newEntity.roleIds());

    Set<Long> insertRoleIds = Sets.difference(newRoleIds, oldRoleIds);
    Set<Long> deleteRoleIds = Sets.difference(oldRoleIds, newRoleIds);

    if (insertRoleIds.isEmpty() && deleteRoleIds.isEmpty()) {
      return newEntity;
    }

    try {
      SessionUtils.doMultipleWithCommit(
          () ->
              SessionUtils.doWithoutCommit(
                  UserMetaMapper.class,
                  mapper ->
                      mapper.updateUserMeta(
                          POConverters.updateUserPOWithVersion(oldUserPO, newEntity), oldUserPO)),
          () -> {
            if (insertRoleIds.isEmpty()) {
              return;
            }
            SessionUtils.doWithoutCommit(
                UserRoleRelMapper.class,
                mapper ->
                    mapper.batchInsertUserRoleRel(
                        POConverters.initializeUserRoleRelsPOWithVersion(
                            newEntity, Lists.newArrayList(insertRoleIds))));
          },
          () -> {
            if (deleteRoleIds.isEmpty()) {
              return;
            }
            SessionUtils.doWithoutCommit(
                UserRoleRelMapper.class,
                mapper ->
                    mapper.softDeleteUserRoleRelByUserAndRoles(
                        newEntity.id(), Lists.newArrayList(deleteRoleIds)));
          });
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.USER, newEntity.nameIdentifier().toString());
      throw re;
    }
    return newEntity;
  }

  public List<UserEntity> listUsersByNamespace(Namespace namespace, boolean allFields) {
    AuthorizationUtils.checkUserNamespace(namespace);
    String metalakeName = namespace.level(0);

    if (allFields) {
      Long metalakeId = MetalakeMetaService.getInstance().getMetalakeIdByName(metalakeName);
      List<ExtendedUserPO> userPOs =
          SessionUtils.getWithoutCommit(
              UserMetaMapper.class, mapper -> mapper.listExtendedUserPOsByMetalakeId(metalakeId));
      return userPOs.stream()
          .map(
              po ->
                  POConverters.fromExtendedUserPO(
                      po, AuthorizationUtils.ofUserNamespace(metalakeName)))
          .collect(Collectors.toList());
    } else {
      List<UserPO> userPOs =
          SessionUtils.getWithoutCommit(
              UserMetaMapper.class, mapper -> mapper.listUserPOsByMetalake(metalakeName));
      return userPOs.stream()
          .map(
              po ->
                  POConverters.fromUserPO(
                      po,
                      Collections.emptyList(),
                      AuthorizationUtils.ofUserNamespace(metalakeName)))
          .collect(Collectors.toList());
    }
  }

  public int deleteUserMetasByLegacyTimeline(long legacyTimeline, int limit) {
    int[] userDeletedCount = new int[] {0};
    int[] userRoleRelDeletedCount = new int[] {0};

    SessionUtils.doMultipleWithCommit(
        () ->
            userDeletedCount[0] =
                SessionUtils.getWithoutCommit(
                    UserMetaMapper.class,
                    mapper -> mapper.deleteUserMetasByLegacyTimeline(legacyTimeline, limit)),
        () ->
            userRoleRelDeletedCount[0] =
                SessionUtils.getWithoutCommit(
                    UserRoleRelMapper.class,
                    mapper ->
                        mapper.deleteUserRoleRelMetasByLegacyTimeline(legacyTimeline, limit)));

    return userDeletedCount[0] + userRoleRelDeletedCount[0];
  }
}
