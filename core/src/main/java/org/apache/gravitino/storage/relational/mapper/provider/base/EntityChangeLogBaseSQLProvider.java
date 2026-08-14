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
package org.apache.gravitino.storage.relational.mapper.provider.base;

import static org.apache.gravitino.storage.relational.mapper.EntityChangeLogMapper.ENTITY_CHANGE_LOG_PRUNE_BATCH_SIZE;
import static org.apache.gravitino.storage.relational.mapper.EntityChangeLogMapper.ENTITY_CHANGE_LOG_TABLE_NAME;

import org.apache.gravitino.storage.relational.mapper.provider.DatabaseTimeSQL;
import org.apache.gravitino.storage.relational.po.cache.OperateType;
import org.apache.ibatis.annotations.Param;

public class EntityChangeLogBaseSQLProvider {

  /**
   * DB-side expression for "now" in milliseconds; PostgreSQL overrides both statements that use it
   * in its own provider.
   *
   * <p>Insertion and expiration both use this expression, so retention is measured entirely with
   * the database clock and is immune to clock skew between Gravitino nodes. Round-trip behaviour is
   * verified by {@code TestEntityChangeLogMapper#testEntityChangeLogInsertAndSelect}, which asserts
   * the persisted value is within 1 s of the JVM clock.
   */
  private static final String CURRENT_TIME_MILLIS_SQL = DatabaseTimeSQL.MYSQL;

  /**
   * Cursor-advance contract for the entity change poller: {@code id} is monotonic and unique, so
   * callers only need to remember the last consumed id.
   *
   * <p>This table is a short-lived broadcast log for local cache invalidation, not a queue. In a
   * multi-node deployment every server instance has its own local cache and should independently
   * consume the same change rows. A new instance may initialize its cursor from {@link
   * #selectMaxChangeId()} because its cache starts empty and it does not need historical
   * invalidations. Re-consuming a row on an existing instance is acceptable: entity DROP/ALTER
   * handling only invalidates cache keys, and invalidation is idempotent.
   */
  public String selectEntityChanges(
      @Param("lastConsumedId") long lastConsumedId, @Param("maxRows") int maxRows) {
    return "SELECT id, metalake_name as metalakeName, entity_type as entityType,"
        + " entity_full_name as fullName, operate_type as operateType, created_at as createdAt"
        + " FROM "
        + ENTITY_CHANGE_LOG_TABLE_NAME
        + " WHERE id > #{lastConsumedId} ORDER BY id LIMIT #{maxRows}";
  }

  public String selectMaxChangeId() {
    return "SELECT COALESCE(MAX(id), 0) FROM " + ENTITY_CHANGE_LOG_TABLE_NAME;
  }

  /** Inserts a change record, stamping {@code created_at} with {@link #CURRENT_TIME_MILLIS_SQL}. */
  public String insertEntityChange(
      @Param("metalakeName") String metalakeName,
      @Param("entityType") String entityType,
      @Param("fullName") String fullName,
      @Param("operateType") OperateType operateType) {
    return "INSERT INTO "
        + ENTITY_CHANGE_LOG_TABLE_NAME
        + " (metalake_name, entity_type, entity_full_name, operate_type, created_at)"
        + " VALUES (#{metalakeName}, #{entityType}, #{fullName}, #{operateType},"
        + CURRENT_TIME_MILLIS_SQL
        + ")";
  }

  public String pruneOldEntityChanges(@Param("retentionMs") long retentionMs) {
    // Keep the retention window conservative. A running server can be delayed by long GC pauses,
    // network isolation, or scheduler stalls; pruning too aggressively can let that server miss an
    // invalidation while its local cache is still warm.
    //
    // No ORDER BY here, unlike the PostgreSQL provider: H2's DELETE grammar accepts LIMIT but not
    // ORDER BY. Every matched row is expired anyway, so the deletion order does not matter; the
    // cleaner randomizes its start time to keep HA nodes from deleting the same rows at once.
    return "DELETE FROM "
        + ENTITY_CHANGE_LOG_TABLE_NAME
        + " WHERE created_at < "
        + CURRENT_TIME_MILLIS_SQL
        + " - #{retentionMs} LIMIT "
        + ENTITY_CHANGE_LOG_PRUNE_BATCH_SIZE;
  }
}
