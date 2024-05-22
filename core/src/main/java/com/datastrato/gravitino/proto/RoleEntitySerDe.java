/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.proto;

import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.authorization.Privilege;
import com.datastrato.gravitino.authorization.Privileges;
import com.datastrato.gravitino.authorization.SecurableObject;
import com.datastrato.gravitino.authorization.SecurableObjects;
import com.datastrato.gravitino.meta.RoleEntity;
import com.google.common.collect.Lists;
import java.util.List;
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
                roleEntity.securableObjects().get(0).privileges().stream()
                    .map(privilege -> privilege.name().toString())
                    .collect(Collectors.toList()))
            .addAllPrivilegeConditions(
                roleEntity.securableObjects().get(0).privileges().stream()
                    .map(privilege -> privilege.condition().toString())
                    .collect(Collectors.toList()))
            .setSecurableObjectFullName(roleEntity.securableObjects().get(0).fullName())
            .setSecurableObjectType(roleEntity.securableObjects().get(0).type().name());

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
  public RoleEntity deserialize(Role role, Namespace namespace) {
    List<Privilege> privileges = Lists.newArrayList();

    if (!role.getPrivilegesList().isEmpty()) {

      for (int index = 0; index < role.getPrivilegeConditionsCount(); index++) {
        if (Privilege.Condition.ALLOW.name().equals(role.getPrivilegeConditions(index))) {
          privileges.add(Privileges.allow(role.getPrivileges(index)));
        } else {
          privileges.add(Privileges.deny(role.getPrivileges(index)));
        }
      }
    }

    SecurableObject securableObject =
        SecurableObjects.parse(
            role.getSecurableObjectFullName(),
            SecurableObject.Type.valueOf(role.getSecurableObjectType()),
            privileges);

    RoleEntity.Builder builder =
        RoleEntity.builder()
            .withId(role.getId())
            .withName(role.getName())
            .withNamespace(namespace)
            .withSecurableObject(securableObject)
            .withAuditInfo(new AuditInfoSerDe().deserialize(role.getAuditInfo(), namespace));

    if (!role.getPropertiesMap().isEmpty()) {
      builder.withProperties(role.getPropertiesMap());
    }

    return builder.build();
  }
}
