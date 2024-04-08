/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.proto;

import com.datastrato.gravitino.meta.GroupEntity;
import java.util.Collection;

public class GroupEntitySerDe implements ProtoSerDe<GroupEntity, Group> {

  @Override
  public Group serialize(GroupEntity groupEntity) {
    Group.Builder builder =
        Group.newBuilder()
            .setId(groupEntity.id())
            .setName(groupEntity.name())
            .setAuditInfo(new AuditInfoSerDe().serialize(groupEntity.auditInfo()));

    if (isCollectionNotEmpty(groupEntity.roles())) {
      builder.addAllRoleNames(groupEntity.roles());
    }

    if (isCollectionNotEmpty(groupEntity.roleIds())) {
      builder.addAllRoleIds(groupEntity.roleIds());
    }

    return builder.build();
  }

  @Override
  public GroupEntity deserialize(Group group) {
    GroupEntity.Builder builder =
        GroupEntity.builder()
            .withId(group.getId())
            .withName(group.getName())
            .withAuditInfo(new AuditInfoSerDe().deserialize(group.getAuditInfo()));

    if (group.getRoleNamesCount() > 0) {
      builder.withRoleNames(group.getRoleNamesList());
    }

    if (group.getRoleIdsCount() > 0) {
      builder.withRoleIds(group.getRoleIdsList());
    }

    return builder.build();
  }

  private boolean isCollectionNotEmpty(Collection collection) {
    return collection != null && !collection.isEmpty();
  }
}
