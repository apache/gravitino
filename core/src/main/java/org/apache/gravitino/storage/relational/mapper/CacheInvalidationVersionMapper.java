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

package org.apache.gravitino.storage.relational.mapper;

import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

/** Mapper for cache invalidation version table. */
public interface CacheInvalidationVersionMapper {
  String TABLE_NAME = "cache_invalidation_version";

  @SelectProvider(type = CacheInvalidationVersionSQLProviderFactory.class, method = "selectVersion")
  Long selectVersion(@Param("id") Long id);

  @InsertProvider(type = CacheInvalidationVersionSQLProviderFactory.class, method = "insertVersion")
  void insertVersion(
      @Param("id") Long id, @Param("version") Long version, @Param("updatedAt") Long updatedAt);

  @UpdateProvider(
      type = CacheInvalidationVersionSQLProviderFactory.class,
      method = "incrementVersion")
  int incrementVersion(@Param("id") Long id, @Param("updatedAt") Long updatedAt);
}
