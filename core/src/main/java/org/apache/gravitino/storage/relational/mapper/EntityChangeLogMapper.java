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

import java.util.List;
import org.apache.gravitino.storage.relational.po.auth.EntityChangeRecord;
import org.apache.gravitino.storage.relational.po.auth.OperateType;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;

/**
 * A MyBatis Mapper for entity_change_log table operations.
 *
 * <p>This append-only log tracks structural changes to entities (create, alter, drop) and is used
 * by the entity change poller to drive targeted invalidation of the metadataIdCache on HA peer
 * nodes.
 */
public interface EntityChangeLogMapper {

  String ENTITY_CHANGE_LOG_TABLE_NAME = "entity_change_log";

  @SelectProvider(type = EntityChangeLogSQLProviderFactory.class, method = "selectEntityChanges")
  List<EntityChangeRecord> selectChanges(
      @Param("createdAtAfter") long createdAtAfter, @Param("maxRows") int maxRows);

  @InsertProvider(type = EntityChangeLogSQLProviderFactory.class, method = "insertEntityChange")
  void insertChange(
      @Param("metalakeName") String metalakeName,
      @Param("entityType") String entityType,
      @Param("fullName") String fullName,
      @Param("operateType") OperateType operateType,
      @Param("createdAt") long createdAt);

  @DeleteProvider(type = EntityChangeLogSQLProviderFactory.class, method = "pruneOldEntityChanges")
  void pruneOldEntries(@Param("before") long before);
}
