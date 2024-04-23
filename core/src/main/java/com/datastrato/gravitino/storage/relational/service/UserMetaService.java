/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.storage.relational.service;

import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.HasIdentifier;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.meta.UserEntity;
import com.datastrato.gravitino.storage.relational.mapper.UserMetaMapper;
import com.datastrato.gravitino.storage.relational.mapper.UserRoleRelMapper;
import com.datastrato.gravitino.storage.relational.po.RolePO;
import com.datastrato.gravitino.storage.relational.po.UserPO;
import com.datastrato.gravitino.storage.relational.po.UserRoleRelPO;
import com.datastrato.gravitino.storage.relational.utils.ExceptionUtils;
import com.datastrato.gravitino.storage.relational.utils.POConverters;
import com.datastrato.gravitino.storage.relational.utils.SessionUtils;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

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

  private Long getUserIdByMetalakeIdAndName(Long metalakeId, String userName) {
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
    Preconditions.checkArgument(
        identifier != null
            && !identifier.namespace().isEmpty()
            && identifier.namespace().levels().length == 3,
        "The identifier should not be null and should have three level.");
    Long metalakeId =
        MetalakeMetaService.getInstance().getMetalakeIdByName(identifier.namespace().level(0));
    UserPO userPO = getUserPOByMetalakeIdAndName(metalakeId, identifier.name());
    List<RolePO> rolePOs = RoleMetaService.getInstance().listRolesByUserId(userPO.getUserId());

    return POConverters.fromUserPO(userPO, rolePOs, identifier.namespace());
  }

  public void insertUser(UserEntity userEntity, boolean overwritten) {
    try {
      Preconditions.checkArgument(
          userEntity.namespace() != null
              && !userEntity.namespace().isEmpty()
              && userEntity.namespace().levels().length == 3,
          "The identifier should not be null and should have three level.");

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
            if (userRoleRelPOs.isEmpty()) {
              return;
            }
            SessionUtils.doWithoutCommit(
                UserRoleRelMapper.class,
                mapper -> {
                  if (overwritten) {
                    mapper.batchInsertUserRoleRelOnDuplicateKeyUpdate(userRoleRelPOs);
                  } else {
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
    Preconditions.checkArgument(
        identifier != null
            && !identifier.namespace().isEmpty()
            && identifier.namespace().levels().length == 3,
        "The identifier should not be null and should have three level.");
    Long metalakeId =
        MetalakeMetaService.getInstance().getMetalakeIdByName(identifier.namespace().level(0));
    Long userId = getUserIdByMetalakeIdAndName(metalakeId, identifier.name());

    SessionUtils.doMultipleWithCommit(
        () ->
            SessionUtils.doWithoutCommit(
                UserMetaMapper.class, mapper -> mapper.softDeleteUserMetaByUserId(userId)),
        () ->
            SessionUtils.doWithoutCommit(
                UserRoleRelMapper.class, mapper -> mapper.softDeleteUserRoleRelByUserId(userId)));
    return true;
  }

  public <E extends Entity & HasIdentifier> UserEntity updateUser(
      NameIdentifier identifier, Function<E, E> updater) {
    Preconditions.checkArgument(
        identifier != null
            && !identifier.namespace().isEmpty()
            && identifier.namespace().levels().length == 3,
        "The identifier should not be null and should have three level.");

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
}
