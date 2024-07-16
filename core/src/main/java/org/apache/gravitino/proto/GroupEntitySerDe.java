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
import org.apache.gravitino.meta.GroupEntity;

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
  public GroupEntity deserialize(Group group, Namespace namespace) {
    GroupEntity.Builder builder =
        GroupEntity.builder()
            .withId(group.getId())
            .withName(group.getName())
            .withNamespace(namespace)
            .withAuditInfo(new AuditInfoSerDe().deserialize(group.getAuditInfo(), namespace));

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
