/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
            .setAuditInfo(new AuditInfoSerDe().serialize(roleEntity.auditInfo()));

    for (SecurableObject securableObject : roleEntity.securableObjects()) {
      builder.addSecurableObjects(
          com.datastrato.gravitino.proto.SecurableObject.newBuilder()
              .setFullName(securableObject.fullName())
              .setType(securableObject.type().name())
              .addAllPrivilegeConditions(
                  securableObject.privileges().stream()
                      .map(Privilege::condition)
                      .map(Privilege.Condition::name)
                      .collect(Collectors.toList()))
              .addAllPrivilegeNames(
                  securableObject.privileges().stream()
                      .map(Privilege::name)
                      .map(Privilege.Name::name)
                      .collect(Collectors.toList()))
              .build());
    }

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
    List<SecurableObject> securableObjects = Lists.newArrayList();

    for (int index = 0; index < role.getSecurableObjectsCount(); index++) {
      List<Privilege> privileges = Lists.newArrayList();
      com.datastrato.gravitino.proto.SecurableObject object = role.getSecurableObjects(index);
      for (int privIndex = 0; privIndex < object.getPrivilegeConditionsCount(); privIndex++) {
        if (Privilege.Condition.ALLOW.name().equals(object.getPrivilegeConditions(privIndex))) {
          privileges.add(Privileges.allow(object.getPrivilegeNames(privIndex)));
        } else {
          privileges.add(Privileges.deny(object.getPrivilegeNames(privIndex)));
        }
      }

      SecurableObject securableObject =
          SecurableObjects.parse(
              object.getFullName(), SecurableObject.Type.valueOf(object.getType()), privileges);

      securableObjects.add(securableObject);
    }

    RoleEntity.Builder builder =
        RoleEntity.builder()
            .withId(role.getId())
            .withName(role.getName())
            .withNamespace(namespace)
            .withSecurableObjects(securableObjects)
            .withAuditInfo(new AuditInfoSerDe().deserialize(role.getAuditInfo(), namespace));

    if (!role.getPropertiesMap().isEmpty()) {
      builder.withProperties(role.getPropertiesMap());
    }

    return builder.build();
  }
}
