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

/** Persistent lifecycle record for one immutable metadata deletion generation. */
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EntityDeletionPO {
  private String deletionId;
  private String entityType;
  private Long entityId;
  private Long entityVersion;
  private Long metalakeId;
  private Long catalogId;
  private Long parentId;
  private String namespaceSnapshot;
  private String entityNameSnapshot;
  @Nullable private String activeNameKey;
  private String state;
  private Long revision;
  private Long deletedAt;
  @Nullable private Long retentionExpiresAt;
  private String deletedBy;
  private Boolean purgeRequested;
  private String purgeJobType;
  @Nullable private String purgeJobId;
  @Nullable private String cleanupStatus;
  private Integer cleanupAttemptCount;
  @Nullable private String cleanupLastError;
  @Nullable private String acceptedRestoreEtag;
  @Nullable private String requestId;
  @Nullable private String correlationId;
  @Nullable private Long restoredAt;
  @Nullable private Long purgedAt;
  private Long updatedAt;
}
