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
package org.apache.gravitino.proto;

import java.util.Collection;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.meta.UserEntity;

public class UserEntitySerDe implements ProtoSerDe<UserEntity, User> {

  @Override
  public User serialize(UserEntity userEntity) {
    User.Builder builder =
        User.newBuilder()
            .setId(userEntity.id())
            .setName(userEntity.name())
            .setAuditInfo(new AuditInfoSerDe().serialize(userEntity.auditInfo()));

    if (isCollectionNotEmpty(userEntity.roles())) {
      builder.addAllRoleNames(userEntity.roles());
    }

    if (isCollectionNotEmpty(userEntity.roleIds())) {
      builder.addAllRoleIds(userEntity.roleIds());
    }

    return builder.build();
  }

  @Override
  public UserEntity deserialize(User user, Namespace namespace) {
    UserEntity.Builder builder =
        UserEntity.builder()
            .withId(user.getId())
            .withName(user.getName())
            .withNamespace(namespace)
            .withAuditInfo(new AuditInfoSerDe().deserialize(user.getAuditInfo(), namespace));

    if (user.getRoleNamesCount() > 0) {
      builder.withRoleNames(user.getRoleNamesList());
    }

    if (user.getRoleIdsCount() > 0) {
      builder.withRoleIds(user.getRoleIdsList());
    }

    return builder.build();
  }

  private boolean isCollectionNotEmpty(Collection collection) {
    return collection != null && !collection.isEmpty();
  }
}
