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

import static org.apache.gravitino.storage.relational.mapper.EntityChangeLogMapper.ENTITY_CHANGE_LOG_TABLE_NAME;

import org.apache.gravitino.storage.relational.po.cache.OperateType;
import org.apache.ibatis.annotations.Param;

public class EntityChangeLogBaseSQLProvider {

  /**
   * Cursor-advance contract for the entity change poller: {@code id} is monotonic and unique, so
   * callers only need to remember the last consumed id.
   */
  public String selectEntityChanges(@Param("lastId") long lastId, @Param("maxRows") int maxRows) {
    return "SELECT id, metalake_name as metalakeName, entity_type as entityType,"
        + " entity_full_name as fullName, operate_type as operateType, created_at as createdAt"
        + " FROM "
        + ENTITY_CHANGE_LOG_TABLE_NAME
        + " WHERE id > #{lastId} ORDER BY id LIMIT #{maxRows}";
  }

  public String selectLatestChangeId() {
    return "SELECT COALESCE(MAX(id), 0) FROM " + ENTITY_CHANGE_LOG_TABLE_NAME;
  }

  /**
   * The {@code (UNIX_TIMESTAMP() * 1000.0) + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000}
   * expression is the established codebase convention for DB-generated millisecond timestamps,
   * shared with 27+ other base providers (TableMetaBaseSQLProvider, FilesetVersionBaseSQLProvider,
   * etc.). It works on MySQL natively and on H2 in {@code MODE=MYSQL}; PostgreSQL overrides this
   * method in its own provider. Round-trip behaviour is verified by {@code
   * TestEntityChangeLogMapper#testEntityChangeLogInsertAndSelect}, which asserts the persisted
   * value is within 1 s of the JVM clock.
   */
  public String insertEntityChange(
      @Param("metalakeName") String metalakeName,
      @Param("entityType") String entityType,
      @Param("fullName") String fullName,
      @Param("operateType") OperateType operateType) {
    return "INSERT INTO "
        + ENTITY_CHANGE_LOG_TABLE_NAME
        + " (metalake_name, entity_type, entity_full_name, operate_type, created_at)"
        + " VALUES (#{metalakeName}, #{entityType}, #{fullName}, #{operateType},"
        + " (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000)";
  }

  public String pruneOldEntityChanges(@Param("before") long before) {
    return "DELETE FROM "
        + ENTITY_CHANGE_LOG_TABLE_NAME
        + " WHERE created_at < #{before} LIMIT 1000";
  }
}
