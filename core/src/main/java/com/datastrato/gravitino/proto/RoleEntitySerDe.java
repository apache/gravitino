/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.proto;

import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.authorization.Privileges;
import com.datastrato.gravitino.meta.RoleEntity;
import java.util.stream.Collectors;

public class RoleEntitySerDe implements ProtoSerDe<RoleEntity, Role> {

  /**
   * Serializes the provided entity into its corresponding Protocol Buffer message representation.
   *
   * @param roleEntity The entity to be serialized.
   * @return The Protocol Buffer message representing the serialized entity.
   */
  @Override
  public Role serialize(RoleEntity roleEntity) {
    Role.Builder builder =
        Role.newBuilder()
            .setId(roleEntity.id())
            .setName(roleEntity.name())
            .setAuditInfo(new AuditInfoSerDe().serialize(roleEntity.auditInfo()))
            .addAllPrivileges(
                roleEntity.privileges().stream()
                    .map(privilege -> privilege.name().toString())
                    .collect(Collectors.toList()))
            .setPrivilegeEntityIdentifier(roleEntity.privilegeEntityIdentifier().toString())
            .setPrivilegeEntityType(roleEntity.privilegeEntityType());

    if (roleEntity.properties() != null && !roleEntity.properties().isEmpty()) {
      builder.putAllProperties(roleEntity.properties());
    }

    return builder.build();
  }

  /**
   * Deserializes the provided Protocol Buffer message into its corresponding entity representation.
   *
   * @param role The Protocol Buffer message to be deserialized.
   * @return The entity representing the deserialized Protocol Buffer message.
   */
  @Override
  public RoleEntity deserialize(Role role) {
    RoleEntity.Builder builder =
        RoleEntity.builder()
            .withId(role.getId())
            .withName(role.getName())
            .withPrivileges(
                role.getPrivilegesList().stream()
                    .map(Privileges::fromString)
                    .collect(Collectors.toList()))
            .withPrivilegeEntityIdentifier(
                NameIdentifier.parse(role.getPrivilegeEntityIdentifier()))
            .withPrivilegeEntityType(Entity.EntityType.valueOf(role.getPrivilegeEntityType()))
            .withAuditInfo(new AuditInfoSerDe().deserialize(role.getAuditInfo()));

    if (!role.getPropertiesMap().isEmpty()) {
      builder.withProperties(role.getPropertiesMap());
    }

    return builder.build();
  }
}
