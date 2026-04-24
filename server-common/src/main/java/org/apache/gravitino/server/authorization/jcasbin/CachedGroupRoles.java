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
package org.apache.gravitino.server.authorization.jcasbin;

import java.util.List;

/**
 * Cached snapshot of a group's role assignments. The {@code updatedAt} timestamp corresponds to the
 * {@code group_meta.updated_at} column and is used as a version sentinel: if the DB value is newer,
 * the cached role list is stale and must be reloaded.
 */
public class CachedGroupRoles {

  private final long groupId;
  private final long updatedAt;
  private final List<Long> roleIds;

  public CachedGroupRoles(long groupId, long updatedAt, List<Long> roleIds) {
    this.groupId = groupId;
    this.updatedAt = updatedAt;
    this.roleIds = roleIds;
  }

  public long getGroupId() {
    return groupId;
  }

  public long getUpdatedAt() {
    return updatedAt;
  }

  public List<Long> getRoleIds() {
    return roleIds;
  }
}
