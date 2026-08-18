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
package org.apache.gravitino.storage.relational.utils;

import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.storage.relational.EntityChangeLogNameIdentifierCodec;
import org.apache.gravitino.storage.relational.mapper.EntityChangeLogMapper;
import org.apache.gravitino.storage.relational.po.cache.OperateType;

/**
 * Helpers for appending rows to {@code entity_change_log}. Callers must invoke {@link #insert}
 * inside an existing {@link SessionUtils#doMultipleWithCommit} so the log row commits with the
 * metadata write.
 */
public final class EntityChangeLogs {

  private EntityChangeLogs() {}

  /**
   * Inserts one change-log row for {@code ident}. {@code full_name} is encoded so peer caches can
   * invalidate the same key they used on {@code get}.
   *
   * @param metalakeName metalake the entity belongs to
   * @param entityType entity type stored in the log
   * @param ident name identifier currently cached by peers
   * @param operateType ALTER or DROP
   */
  public static void insert(
      String metalakeName,
      Entity.EntityType entityType,
      NameIdentifier ident,
      OperateType operateType) {
    SessionUtils.doWithoutCommit(
        EntityChangeLogMapper.class,
        mapper ->
            mapper.insertEntityChange(
                metalakeName,
                entityType.name(),
                EntityChangeLogNameIdentifierCodec.encode(ident),
                operateType));
  }
}
