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
package org.apache.gravitino.storage.relational.mapper.provider.postgresql;

import static org.apache.gravitino.storage.relational.mapper.EntityChangeLogMapper.ENTITY_CHANGE_LOG_TABLE_NAME;

import org.apache.gravitino.storage.relational.mapper.provider.base.EntityChangeLogBaseSQLProvider;
import org.apache.gravitino.storage.relational.po.cache.OperateType;
import org.apache.ibatis.annotations.Param;

public class EntityChangeLogPostgreSQLProvider extends EntityChangeLogBaseSQLProvider {

  @Override
  public String insertEntityChange(
      @Param("metalakeName") String metalakeName,
      @Param("entityType") String entityType,
      @Param("fullName") String fullName,
      @Param("operateType") OperateType operateType) {
    return "INSERT INTO "
        + ENTITY_CHANGE_LOG_TABLE_NAME
        + " (metalake_name, entity_type, entity_full_name, operate_type, created_at)"
        + " VALUES (#{metalakeName}, #{entityType}, #{fullName}, #{operateType},"
        + " CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT))";
  }

  @Override
  public String pruneOldEntityChanges(@Param("before") long before) {
    return "DELETE FROM "
        + ENTITY_CHANGE_LOG_TABLE_NAME
        + " WHERE id IN (SELECT id FROM "
        + ENTITY_CHANGE_LOG_TABLE_NAME
        + " WHERE created_at < #{before} ORDER BY created_at LIMIT 1000)";
  }
}
