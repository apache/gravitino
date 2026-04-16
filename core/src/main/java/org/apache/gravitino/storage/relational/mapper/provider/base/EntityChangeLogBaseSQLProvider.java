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

import org.apache.ibatis.annotations.Param;

public class EntityChangeLogBaseSQLProvider {

  public String selectEntityChanges(
      @Param("createdAtAfter") long createdAtAfter, @Param("maxRows") int maxRows) {
    return "SELECT metalake_name as metalakeName, entity_type as entityType,"
        + " full_name as fullName, operate_type as operateType, created_at as createdAt"
        + " FROM "
        + ENTITY_CHANGE_LOG_TABLE_NAME
        + " WHERE created_at > #{createdAtAfter} ORDER BY created_at LIMIT #{maxRows}";
  }

  public String insertEntityChange(
      @Param("metalakeName") String metalakeName,
      @Param("entityType") String entityType,
      @Param("fullName") String fullName,
      @Param("operateType") String operateType,
      @Param("createdAt") long createdAt) {
    return "INSERT INTO "
        + ENTITY_CHANGE_LOG_TABLE_NAME
        + " (metalake_name, entity_type, full_name, operate_type, created_at)"
        + " VALUES (#{metalakeName}, #{entityType}, #{fullName}, #{operateType}, #{createdAt})";
  }

  public String pruneOldEntityChanges(@Param("before") long before) {
    return "DELETE FROM "
        + ENTITY_CHANGE_LOG_TABLE_NAME
        + " WHERE created_at < #{before} LIMIT 1000";
  }
}
