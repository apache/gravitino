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
package org.apache.gravitino.storage.relational.po;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import java.time.Instant;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.dto.semantic.SemanticModelRevisionDTO;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.semantic.SemanticModel.Status;

/** The persistent object for a complete Semantic Model version snapshot. */
@EqualsAndHashCode
@Getter
@ToString
@Accessors(fluent = true)
public class SemanticModelVersionInfoPO {

  /** Database publication state, including the retention distinction between published states. */
  public enum PublicationStatus {
    /** A revision that has not been published. */
    DRAFT,
    /** A revision made active at save time. */
    PUBLISHED_ON_SAVE,
    /** A revision made active by a separate publication operation. */
    PUBLISHED_EXPLICITLY
  }

  private Long id;
  private Long metalakeId;
  private Long catalogId;
  private Long schemaId;
  private Long semanticModelId;
  private Integer version;
  private PublicationStatus publicationStatus;
  private String publishedBy;
  private Long publishedAt;
  private String semanticModelComment;
  private String datasets;
  @Nullable private String relationships;
  @Nullable private String metrics;
  @Nullable private String aiContext;
  @Nullable private String customExtensions;
  private String properties;
  private String auditInfo;
  private Long deletedAt;

  /** Creates an empty persistent object for MyBatis. */
  public SemanticModelVersionInfoPO() {}

  @lombok.Builder(setterPrefix = "with", toBuilder = true)
  private SemanticModelVersionInfoPO(
      Long id,
      Long metalakeId,
      Long catalogId,
      Long schemaId,
      Long semanticModelId,
      Integer version,
      PublicationStatus publicationStatus,
      String publishedBy,
      Long publishedAt,
      String semanticModelComment,
      String datasets,
      @Nullable String relationships,
      @Nullable String metrics,
      @Nullable String aiContext,
      @Nullable String customExtensions,
      String properties,
      String auditInfo,
      Long deletedAt) {
    Preconditions.checkArgument(metalakeId != null, "Metalake id is required");
    Preconditions.checkArgument(catalogId != null, "Catalog id is required");
    Preconditions.checkArgument(schemaId != null, "Schema id is required");
    Preconditions.checkArgument(semanticModelId != null, "Semantic Model id is required");
    Preconditions.checkArgument(version != null, "Semantic Model version is required");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(datasets), "Semantic Model datasets cannot be empty");
    Preconditions.checkArgument(StringUtils.isNotBlank(auditInfo), "Audit info cannot be empty");
    Preconditions.checkArgument(deletedAt != null, "Deleted at is required");

    this.id = id;
    this.metalakeId = metalakeId;
    this.catalogId = catalogId;
    this.schemaId = schemaId;
    this.semanticModelId = semanticModelId;
    this.version = version;
    this.publicationStatus = publicationStatus;
    this.publishedBy = publishedBy;
    this.publishedAt = publishedAt;
    this.semanticModelComment = semanticModelComment;
    this.datasets = datasets;
    this.relationships = relationships;
    this.metrics = metrics;
    this.aiContext = aiContext;
    this.customExtensions = customExtensions;
    this.properties = properties;
    this.auditInfo = auditInfo;
    this.deletedAt = deletedAt;
  }

  /** Returns the public metadata for this retained revision, without its definition. */
  public SemanticModelRevisionDTO revisionInfo() {
    boolean draft = publicationStatus == PublicationStatus.DRAFT;
    return new SemanticModelRevisionDTO(
        version,
        draft ? Status.DRAFT : Status.PUBLISHED,
        draft ? null : publishedBy,
        draft ? null : Instant.ofEpochMilli(publishedAt),
        DTOConverters.toDTO(writerAudit()));
  }

  /** Returns the audit information recorded when this snapshot was saved. */
  public AuditInfo writerAudit() {
    try {
      return JsonUtils.anyFieldMapper().readValue(auditInfo, AuditInfo.class);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to deserialize Semantic Model revision audit", e);
    }
  }
}
