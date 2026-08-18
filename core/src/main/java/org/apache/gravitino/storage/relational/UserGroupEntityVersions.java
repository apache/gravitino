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
package org.apache.gravitino.storage.relational;

import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.storage.relational.mapper.GroupMetaMapper;
import org.apache.gravitino.storage.relational.mapper.UserMetaMapper;
import org.apache.gravitino.storage.relational.po.auth.GroupUpdatedAt;
import org.apache.gravitino.storage.relational.po.auth.UserUpdatedAt;
import org.apache.gravitino.storage.relational.utils.SessionUtils;

/**
 * JCasbin-style version checks for name-keyed USER/GROUP {@link
 * org.apache.gravitino.cache.EntityCache} entries.
 *
 * <p>{@code touchUserUpdatedAt} / {@code touchGroupUpdatedAt} already advance {@code
 * *_meta.updated_at} on every mutating write. A cache hit is served only when the cached id and
 * sentinel still match the database; otherwise the caller reloads.
 */
final class UserGroupEntityVersions {

  private UserGroupEntityVersions() {}

  /**
   * Returns whether {@code type} is version-validated on name-keyed {@code get}.
   *
   * @param type entity type
   * @return {@code true} for USER and GROUP
   */
  static boolean isVersionValidatedType(Entity.EntityType type) {
    return type == Entity.EntityType.USER || type == Entity.EntityType.GROUP;
  }

  /**
   * Returns whether {@code cached} is still current relative to {@code *_meta.updated_at}.
   *
   * <p>A missing row (deleted) or a recycled name with a new id is treated as stale so the caller
   * reloads from the store.
   *
   * @param ident name identifier used as the cache key
   * @param type USER or GROUP
   * @param cached the cache entry
   * @return {@code true} if the cached snapshot may be returned
   */
  static boolean isFresh(NameIdentifier ident, Entity.EntityType type, Entity cached) {
    String metalake = ident.namespace().level(0);
    String name = ident.name();
    if (type == Entity.EntityType.USER) {
      if (!(cached instanceof UserEntity)) {
        return false;
      }
      UserEntity user = (UserEntity) cached;
      UserUpdatedAt info =
          SessionUtils.getWithoutCommit(
              UserMetaMapper.class, mapper -> mapper.getUserUpdatedAt(metalake, name));
      return info != null
          && info.getUserId() == user.id()
          && user.updatedAt() >= info.getUpdatedAt();
    }
    if (type == Entity.EntityType.GROUP) {
      if (!(cached instanceof GroupEntity)) {
        return false;
      }
      GroupEntity group = (GroupEntity) cached;
      GroupUpdatedAt info =
          SessionUtils.getWithoutCommit(
              GroupMetaMapper.class, mapper -> mapper.getGroupUpdatedAt(metalake, name));
      return info != null
          && info.getGroupId() == group.id()
          && group.updatedAt() >= info.getUpdatedAt();
    }
    return true;
  }
}
