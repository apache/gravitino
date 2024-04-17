/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.proto;

import com.datastrato.gravitino.Namespace;
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
      builder.addAllRoles(groupEntity.roles());
    }

    return builder.build();
  }

  @Override
  public GroupEntity deserialize(Group group, Namespace namespace) {
    GroupEntity.Builder builder =
        GroupEntity.builder()
            .withId(group.getId())
            .withName(group.getName())
            .withNamespace(namespace)
            .withAuditInfo(new AuditInfoSerDe().deserialize(group.getAuditInfo(), namespace));

    if (group.getRolesCount() > 0) {
      builder.withRoles(group.getRolesList());
    }

    return builder.build();
  }

  private boolean isCollectionNotEmpty(Collection collection) {
    return collection != null && !collection.isEmpty();
  }
}
