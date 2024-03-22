/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.proto;

import com.datastrato.gravitino.meta.ManagedUser;
import java.util.Collection;
import java.util.Map;

public class ManagedUserSerDe implements ProtoSerDe<ManagedUser, User> {

  @Override
  public User serialize(ManagedUser managedUser) {
    User.Builder builder =
        User.newBuilder()
            .setId(managedUser.id())
            .setName(managedUser.name())
            .setFirstName(managedUser.firstName())
            .setLastName(managedUser.lastName())
            .setDisplayName(managedUser.displayName())
            .setActive(managedUser.active())
            .setEmailAddress(managedUser.emailAddress())
            .setAuditInfo(new AuditInfoSerDe().serialize(managedUser.auditInfo()));

    if (managedUser.comment() != null) {
      builder.setComment(managedUser.comment());
    }

    if (managedUser.defaultRole() != null) {
      builder.setDefaultRole(managedUser.defaultRole());
    }

    if (isMapNotEmpty(managedUser.properties())) {
      builder.putAllProperties(managedUser.properties());
    }

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
            .withFirstName(user.getFirstName())
            .withLastName(user.getLastName())
            .withDisplayName(user.getDisplayName())
            .withActive(user.getActive())
            .withEmailAddress(user.getEmailAddress())
            .withAuditInfo(new AuditInfoSerDe().deserialize(user.getAuditInfo()));

    if (user.getPropertiesCount() > 0) {
      builder.withProperties(user.getPropertiesMap());
    }

    if (user.getRolesCount() > 0) {
      builder.withRoles(user.getRolesList());
    }

    if (user.getGroupsCount() > 0) {
      builder.withGroups(user.getGroupsList());
    }

    if (user.hasDefaultRole()) {
      builder.withDefaultRole(user.getDefaultRole());
    }

    if (user.hasComment()) {
      builder.withComment(user.getComment());
    }
    return builder.build();
  }

  private boolean isMapNotEmpty(Map map) {
    return map != null && !map.isEmpty();
  }

  private boolean isCollectionNotEmpty(Collection collection) {
    return collection != null && !collection.isEmpty();
  }
}
