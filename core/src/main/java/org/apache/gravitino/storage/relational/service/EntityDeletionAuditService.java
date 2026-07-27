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
package org.apache.gravitino.storage.relational.service;

import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.gravitino.storage.relational.mapper.EntityDeletionAuditMapper;
import org.apache.gravitino.storage.relational.po.EntityDeletionAuditPO;
import org.apache.gravitino.storage.relational.utils.SessionUtils;

/** Append-only storage service for metadata deletion lifecycle audit events. */
public class EntityDeletionAuditService {

  private static final EntityDeletionAuditService INSTANCE = new EntityDeletionAuditService();

  /**
   * Returns the singleton deletion-audit storage service.
   *
   * @return deletion-audit storage service
   */
  public static EntityDeletionAuditService getInstance() {
    return INSTANCE;
  }

  private EntityDeletionAuditService() {}

  /**
   * Appends one lifecycle audit event.
   *
   * <p>The session utility is nesting-aware, so this method commits when called alone and joins an
   * existing outer {@link SessionUtils#doMultipleWithCommit(Runnable...)} transaction when one is
   * active. No update or delete operation is exposed for this append-only record.
   *
   * @param audit audit event to append
   */
  public void insert(EntityDeletionAuditPO audit) {
    Objects.requireNonNull(audit, "audit must not be null");
    SessionUtils.doWithCommit(EntityDeletionAuditMapper.class, mapper -> mapper.insertAudit(audit));
  }

  /**
   * Loads one exact audit event.
   *
   * @param auditId opaque audit identifier
   * @return audit event, or {@code null} when absent
   */
  @Nullable
  public EntityDeletionAuditPO get(String auditId) {
    Objects.requireNonNull(auditId, "auditId must not be null");
    return SessionUtils.doWithCommitAndFetchResult(
        EntityDeletionAuditMapper.class, mapper -> mapper.selectAudit(auditId));
  }
}
