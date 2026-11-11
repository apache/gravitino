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
import org.apache.gravitino.storage.relational.mapper.EntityChangeLogMapper;
import org.apache.gravitino.storage.relational.po.cache.OperateType;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;

/** Appends {@code entity_change_log} records that peer nodes replay to invalidate their caches. */
public final class EntityChangeLogWriter {

  private EntityChangeLogWriter() {}

  /**
   * Appends a change record in the caller's transaction, so it commits or rolls back with the
   * change it describes.
   *
   * @param ident the name that peers may have cached: the pre-mutation name for an alter and the
   *     current name for a drop
   * @param entityType the entity type
   * @param operateType the change operation
   */
  public static void append(
      NameIdentifier ident, Entity.EntityType entityType, OperateType operateType) {
    String metalake = NameIdentifierUtil.getMetalake(ident);
    String fullName = EntityChangeLogNameIdentifierCodec.encode(ident);
    SessionUtils.doWithoutCommit(
        EntityChangeLogMapper.class,
        mapper -> mapper.insertEntityChange(metalake, entityType.name(), fullName, operateType));
    EntityChangeLogDiagnostics.logAppended(metalake, entityType.name(), operateType, fullName);
  }
}
