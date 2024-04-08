/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.proto;

import com.datastrato.gravitino.meta.UserEntity;
import java.util.Collection;

public class UserEntitySerDe implements ProtoSerDe<UserEntity, User> {

  @Override
  public User serialize(UserEntity userEntity) {
    User.Builder builder =
        User.newBuilder()
            .setId(userEntity.id())
            .setName(userEntity.name())
            .setAuditInfo(new AuditInfoSerDe().serialize(userEntity.auditInfo()));

    if (isCollectionNotEmpty(userEntity.roles())) {
      builder.addAllRoleNames(userEntity.roles());
    }

    if (isCollectionNotEmpty(userEntity.roleIds())) {
      builder.addAllRoleIds(userEntity.roleIds());
    }

    return builder.build();
  }

  @Override
  public UserEntity deserialize(User user) {
    UserEntity.Builder builder =
        UserEntity.builder()
            .withId(user.getId())
            .withName(user.getName())
            .withAuditInfo(new AuditInfoSerDe().deserialize(user.getAuditInfo()));

    if (user.getRoleNamesCount() > 0) {
      builder.withRoleNames(user.getRoleNamesList());
    }

    if (user.getRoleIdsCount() > 0) {
      builder.withRoleIds(user.getRoleIdsList());
    }

    return builder.build();
  }

  private boolean isCollectionNotEmpty(Collection collection) {
    return collection != null && !collection.isEmpty();
  }
}
