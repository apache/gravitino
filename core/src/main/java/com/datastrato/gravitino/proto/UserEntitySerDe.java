/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.proto;

import com.datastrato.gravitino.meta.UserEntity;

public class UserEntitySerDe implements ProtoSerDe<UserEntity, User> {

  @Override
  public User serialize(UserEntity userEntity) {
    User.Builder builder =
        User.newBuilder()
            .setId(userEntity.id())
            .setName(userEntity.name())
            .setAuditInfo(new AuditInfoSerDe().serialize(userEntity.auditInfo()));
    if (userEntity.properties() != null && !userEntity.properties().isEmpty()) {
      builder.putAllProperties(userEntity.properties());
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
    if (user.getPropertiesCount() > 0) {
      builder.withProperties(user.getPropertiesMap());
    }
    return builder.build();
  }
}
