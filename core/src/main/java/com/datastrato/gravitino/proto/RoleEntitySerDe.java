/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.proto;

import com.datastrato.gravitino.authorization.Privileges;
import com.datastrato.gravitino.authorization.Resource;
import com.datastrato.gravitino.authorization.Resources;
import com.datastrato.gravitino.meta.RoleEntity;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

public class RoleEntitySerDe implements ProtoSerDe<RoleEntity, Role> {

  private static final Splitter DOT = Splitter.on('.');

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
            .setResource(roleEntity.resource().toString());

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
            .withResource(parseResource(role.getResource()))
            .withAuditInfo(new AuditInfoSerDe().deserialize(role.getAuditInfo()));

    if (!role.getPropertiesMap().isEmpty()) {
      builder.withProperties(role.getPropertiesMap());
    }

    return builder.build();
  }

  public static Resource parseResource(String resourceIdentifier) {
    if ("*".equals(resourceIdentifier)) {
      return Resources.ofAllCatalogs();
    }

    if (StringUtils.isBlank(resourceIdentifier)) {
      throw new IllegalArgumentException("resource identifier can't be blank");
    }

    Iterable<String> parts = DOT.split(resourceIdentifier);
    return Resources.of(Iterables.toArray(parts, String.class));
  }
}
