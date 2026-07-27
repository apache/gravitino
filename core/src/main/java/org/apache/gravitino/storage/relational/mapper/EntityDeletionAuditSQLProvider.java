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

import static org.apache.gravitino.storage.relational.mapper.EntityDeletionAuditMapper.TABLE_NAME;

import org.apache.gravitino.storage.relational.po.EntityDeletionAuditPO;
import org.apache.ibatis.annotations.Param;

/** Portable SQL provider for append-only deletion lifecycle audit events. */
public class EntityDeletionAuditSQLProvider {

  private static final String SELECT_COLUMNS =
      "audit_id AS auditId, deletion_id AS deletionId, entity_type AS entityType,"
          + " entity_id AS entityId, event_type AS eventType,"
          + " action_revision AS actionRevision, prior_state AS priorState,"
          + " new_state AS newState, prior_cleanup_status AS priorCleanupStatus,"
          + " new_cleanup_status AS newCleanupStatus, purge_job_id AS purgeJobId,"
          + " lease_epoch AS leaseEpoch, actor, request_id AS requestId,"
          + " correlation_id AS correlationId, reason_code AS reasonCode, reason,"
          + " created_at AS createdAt";

  /**
   * Builds the append statement for one audit event.
   *
   * @param audit audit event to append
   * @return parameterized insert SQL
   */
  public static String insertAudit(@Param("audit") EntityDeletionAuditPO audit) {
    return "INSERT INTO "
        + TABLE_NAME
        + " (audit_id, deletion_id, entity_type, entity_id, event_type, action_revision,"
        + " prior_state, new_state, prior_cleanup_status, new_cleanup_status, purge_job_id,"
        + " lease_epoch, actor, request_id, correlation_id, reason_code, reason, created_at)"
        + " VALUES (#{audit.auditId}, #{audit.deletionId}, #{audit.entityType},"
        + " #{audit.entityId}, #{audit.eventType}, #{audit.actionRevision},"
        + " #{audit.priorState}, #{audit.newState}, #{audit.priorCleanupStatus},"
        + " #{audit.newCleanupStatus}, #{audit.purgeJobId}, #{audit.leaseEpoch},"
        + " #{audit.actor}, #{audit.requestId}, #{audit.correlationId},"
        + " #{audit.reasonCode}, #{audit.reason}, #{audit.createdAt})";
  }

  /**
   * Builds an exact audit-event lookup.
   *
   * @param auditId opaque audit identifier
   * @return parameterized select SQL
   */
  public static String selectAudit(@Param("auditId") String auditId) {
    return "SELECT " + SELECT_COLUMNS + " FROM " + TABLE_NAME + " WHERE audit_id = #{auditId}";
  }
}
