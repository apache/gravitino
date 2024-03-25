/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.proto;

import com.datastrato.gravitino.meta.ManagedUser;
import java.util.Collection;

public class ManagedUserSerDe implements ProtoSerDe<ManagedUser, User> {

  @Override
  public User serialize(ManagedUser managedUser) {
    User.Builder builder =
        User.newBuilder()
            .setId(managedUser.id())
            .setName(managedUser.name())
            .setAuditInfo(new AuditInfoSerDe().serialize(managedUser.auditInfo()));

    if (isCollectionNotEmpty(managedUser.groups())) {
      builder.addAllGroups(managedUser.groups());
    }

    if (isCollectionNotEmpty(managedUser.roles())) {
      builder.addAllRoles(managedUser.roles());
    }

    return builder.build();
  }

  @Override
  public ManagedUser deserialize(User user) {
    ManagedUser.Builder builder =
        ManagedUser.builder()
            .withId(user.getId())
            .withName(user.getName())
            .withAuditInfo(new AuditInfoSerDe().deserialize(user.getAuditInfo()));

    if (user.getRolesCount() > 0) {
      builder.withRoles(user.getRolesList());
    }

    if (user.getGroupsCount() > 0) {
      builder.withGroups(user.getGroupsList());
    }

    return builder.build();
  }

  private boolean isCollectionNotEmpty(Collection collection) {
    return collection != null && !collection.isEmpty();
  }
}
