/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.proto;

import com.datastrato.gravitino.meta.MetalakeGroup;

public class GroupEntitySerDe
    implements ProtoSerDe<MetalakeGroup, com.datastrato.gravitino.proto.Group> {

  /**
   * Serializes the provided entity into its corresponding Protocol Buffer message representation.
   *
   * @param metalakeGroup The entity to be serialized.
   * @return The Protocol Buffer message representing the serialized entity.
   */
  @Override
  public Group serialize(MetalakeGroup metalakeGroup) {
    Group.Builder builder =
        Group.newBuilder()
            .addAllUser(metalakeGroup.users())
            .setId(metalakeGroup.id())
            .setName(metalakeGroup.name())
            .setAuditInfo(new AuditInfoSerDe().serialize(metalakeGroup.auditInfo()));

    if (metalakeGroup.properties() != null && !metalakeGroup.properties().isEmpty()) {
      builder.putAllProperties(metalakeGroup.properties());
    }

    return builder.build();
  }

  /**
   * Deserializes the provided Protocol Buffer message into its corresponding entity representation.
   *
   * @param p The Protocol Buffer message to be deserialized.
   * @return The entity representing the deserialized Protocol Buffer message.
   */
  @Override
  public MetalakeGroup deserialize(Group group) {
    MetalakeGroup.Builder builder =
        MetalakeGroup.builder()
            .withId(group.getId())
            .withName(group.getName())
            .withUsers(group.getUserList())
            .withAuditInfo(new AuditInfoSerDe().deserialize(group.getAuditInfo()));
    if (group.getPropertiesCount() > 0) {
      builder.withProperties(group.getPropertiesMap());
    }
    return builder.build();
  }
}
