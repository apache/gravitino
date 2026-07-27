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
package org.apache.gravitino.storage.relational.po;

import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/** Persistent append-only audit event for one metadata deletion generation. */
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EntityDeletionAuditPO {
  private String auditId;
  private String deletionId;
  private String entityType;
  private Long entityId;
  private String eventType;
  @Nullable private Long actionRevision;
  @Nullable private String priorState;
  @Nullable private String newState;
  @Nullable private String priorCleanupStatus;
  @Nullable private String newCleanupStatus;
  @Nullable private String purgeJobId;
  @Nullable private Long leaseEpoch;
  private String actor;
  @Nullable private String requestId;
  private String correlationId;
  @Nullable private String reasonCode;
  @Nullable private String reason;
  private Long createdAt;
}
