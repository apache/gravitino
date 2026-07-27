/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.storage.relational.mapper;

import javax.annotation.Nullable;
import org.apache.gravitino.storage.relational.po.EntityDeletionAuditPO;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;

/** Append-only MyBatis mapper for metadata deletion lifecycle audit events. */
public interface EntityDeletionAuditMapper {

  /** Append-only deletion-audit table name. */
  String TABLE_NAME = "entity_deletion_audit";

  /**
   * Appends one lifecycle audit event.
   *
   * @param audit audit event to append
   */
  @InsertProvider(type = EntityDeletionAuditSQLProvider.class, method = "insertAudit")
  void insertAudit(@Param("audit") EntityDeletionAuditPO audit);

  /**
   * Selects one exact audit event.
   *
   * @param auditId opaque audit identifier
   * @return persisted event, or {@code null} when absent
   */
  @Nullable
  @SelectProvider(type = EntityDeletionAuditSQLProvider.class, method = "selectAudit")
  EntityDeletionAuditPO selectAudit(@Param("auditId") String auditId);
}
