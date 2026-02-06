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

import static org.apache.gravitino.storage.relational.mapper.CacheInvalidationVersionMapper.TABLE_NAME;

import org.apache.ibatis.annotations.Param;

/** Base SQL provider for cache invalidation version. */
public class CacheInvalidationVersionBaseSQLProvider {

  public String selectVersion(@Param("id") Long id) {
    return "SELECT version FROM " + TABLE_NAME + " WHERE id = #{id}";
  }

  public String insertVersion(
      @Param("id") Long id, @Param("version") Long version, @Param("updatedAt") Long updatedAt) {
    return "INSERT INTO "
        + TABLE_NAME
        + " (id, version, updated_at) VALUES (#{id}, #{version}, #{updatedAt})";
  }

  public String incrementVersion(@Param("id") Long id, @Param("updatedAt") Long updatedAt) {
    return "UPDATE "
        + TABLE_NAME
        + " SET version = version + 1, updated_at = #{updatedAt} WHERE id = #{id}";
  }
}
