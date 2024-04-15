/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.storage.relational.service;

import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.meta.UserEntity;
import com.datastrato.gravitino.storage.relational.mapper.UserMetaMapper;
import com.datastrato.gravitino.storage.relational.mapper.UserRoleRelMapper;
import com.datastrato.gravitino.storage.relational.po.UserPO;
import com.datastrato.gravitino.storage.relational.po.UserRoleRelPO;
import com.datastrato.gravitino.storage.relational.utils.ExceptionUtils;
import com.datastrato.gravitino.storage.relational.utils.POConverters;
import com.datastrato.gravitino.storage.relational.utils.SessionUtils;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.stream.Collectors;

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
            mapper -> mapper.selectUserIdBySchemaIdAndName(metalakeId, userName));

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
    List<String> roleNames = RoleMetaService.getInstance().listRoleNameByUserId(userPO.getUserId());

    return POConverters.fromUserPO(userPO, roleNames, identifier.namespace());
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

      List<Long> roleIds =
          userEntity.roles().stream()
              .map(
                  role ->
                      RoleMetaService.getInstance().getRoleIdByMetalakeIdAndName(metalakeId, role))
              .collect(Collectors.toList());
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

  public boolean deleteUser(NameIdentifier ident) {
    Preconditions.checkArgument(
        ident != null && !ident.namespace().isEmpty() && ident.namespace().levels().length == 3,
        "The identifier should not be null and should have three level.");
    Long metalakeId =
        MetalakeMetaService.getInstance().getMetalakeIdByName(ident.namespace().level(0));
    Long userId = getUserIdByMetalakeIdAndName(metalakeId, ident.name());

    SessionUtils.doMultipleWithCommit(
        () ->
            SessionUtils.doWithoutCommit(
                UserMetaMapper.class, mapper -> mapper.softDeleteUserMetaByUserId(userId)),
        () ->
            SessionUtils.doWithoutCommit(
                UserRoleRelMapper.class, mapper -> mapper.softDeleteUserRoleRelByUserId(userId)));
    return true;
  }
}
