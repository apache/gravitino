/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.proto;

import com.datastrato.gravitino.meta.MetalakeUser;

public class UserEntitySerDe implements ProtoSerDe<MetalakeUser, com.datastrato.gravitino.proto.User> {

  @Override
  public com.datastrato.gravitino.proto.User serialize(MetalakeUser metalakeUser) {
    com.datastrato.gravitino.proto.User.Builder builder =
        com.datastrato.gravitino.proto.User.newBuilder()
            .setId(metalakeUser.id())
            .setName(metalakeUser.name())
            .setAuditInfo(new AuditInfoSerDe().serialize(metalakeUser.auditInfo()));
    if (metalakeUser.properties() != null && !metalakeUser.properties().isEmpty()) {
      builder.putAllProperties(metalakeUser.properties());
    }
    return builder.build();
  }

  @Override
  public MetalakeUser deserialize(com.datastrato.gravitino.proto.User user) {
    MetalakeUser.Builder builder =
        MetalakeUser.builder()
            .withId(user.getId())
            .withName(user.getName())
            .withAuditInfo(new AuditInfoSerDe().deserialize(user.getAuditInfo()));
    if (user.getPropertiesCount() > 0) {
      builder.withProperties(user.getPropertiesMap());
    }
    return builder.build();
  }
}
