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

import static org.apache.gravitino.storage.relational.utils.POConverters.DEFAULT_DELETED_AT;
import static org.apache.gravitino.storage.relational.utils.POConverters.INIT_VERSION;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.AuditInfo;

/**
 * The persistent object of a row in {@code domain_rel}, the edge from a sub-domain to its parent
 * domain. The parent domain name is not a column of the row: reads that join the live parent fill
 * it.
 */
@Getter
@Accessors(fluent = true)
@EqualsAndHashCode
@ToString
public class DomainRelPO {

  private Long metalakeId;
  private Long parentId;
  private String parentName;
  private Long childId;
  private String auditInfo;
  private Long currentVersion;
  private Long lastVersion;
  private Long deletedAt;

  /** Creates an empty domain relation persistent object, used by MyBatis to map result rows. */
  public DomainRelPO() {}

  @lombok.Builder(setterPrefix = "with")
  private DomainRelPO(
      Long metalakeId,
      Long parentId,
      Long childId,
      String auditInfo,
      Long currentVersion,
      Long lastVersion,
      Long deletedAt) {
    Preconditions.checkArgument(metalakeId != null, "metalakeId cannot be null");
    Preconditions.checkArgument(parentId != null, "parentId cannot be null");
    Preconditions.checkArgument(childId != null, "childId cannot be null");
    Preconditions.checkArgument(
        !parentId.equals(childId), "A domain cannot be its own parent: %s", childId);
    Preconditions.checkArgument(StringUtils.isNotBlank(auditInfo), "auditInfo cannot be blank");
    Preconditions.checkArgument(currentVersion != null, "currentVersion cannot be null");
    Preconditions.checkArgument(lastVersion != null, "lastVersion cannot be null");
    Preconditions.checkArgument(deletedAt != null, "deletedAt cannot be null");

    this.metalakeId = metalakeId;
    this.parentId = parentId;
    this.childId = childId;
    this.auditInfo = auditInfo;
    this.currentVersion = currentVersion;
    this.lastVersion = lastVersion;
    this.deletedAt = deletedAt;
  }

  /**
   * Creates the first version of the persistent object of a new parent edge.
   *
   * @param metalakeId The ID of the metalake of both domains.
   * @param parentId The ID of the parent domain.
   * @param childId The ID of the sub-domain.
   * @param auditInfo The audit info of the edge.
   * @return The domain relation persistent object.
   */
  public static DomainRelPO initializeDomainRelPO(
      Long metalakeId, Long parentId, Long childId, AuditInfo auditInfo) {
    return builder()
        .withMetalakeId(metalakeId)
        .withParentId(parentId)
        .withChildId(childId)
        .withAuditInfo(serializeAuditInfo(auditInfo))
        .withCurrentVersion(INIT_VERSION)
        .withLastVersion(INIT_VERSION)
        .withDeletedAt(DEFAULT_DELETED_AT)
        .build();
  }

  /**
   * Creates the next version of the persistent object of an edge that is re-pointed to another
   * parent. The edge keeps its creator and creation time and takes the last modifier and the last
   * modified time from {@code modifiedAuditInfo}.
   *
   * @param oldDomainRelPO The edge observed by the caller.
   * @param newParentId The ID of the new parent domain.
   * @param modifiedAuditInfo The audit info of the change.
   * @return The domain relation persistent object with the next version.
   */
  public static DomainRelPO updateDomainRelPO(
      DomainRelPO oldDomainRelPO, Long newParentId, AuditInfo modifiedAuditInfo) {
    AuditInfo oldAuditInfo = deserializeAuditInfo(oldDomainRelPO.auditInfo());
    AuditInfo newAuditInfo =
        AuditInfo.builder()
            .withCreator(oldAuditInfo.creator())
            .withCreateTime(oldAuditInfo.createTime())
            .withLastModifier(modifiedAuditInfo.lastModifier())
            .withLastModifiedTime(modifiedAuditInfo.lastModifiedTime())
            .build();
    Long nextVersion = oldDomainRelPO.currentVersion() + 1;
    return builder()
        .withMetalakeId(oldDomainRelPO.metalakeId())
        .withParentId(newParentId)
        .withChildId(oldDomainRelPO.childId())
        .withAuditInfo(serializeAuditInfo(newAuditInfo))
        .withCurrentVersion(nextVersion)
        .withLastVersion(nextVersion)
        .withDeletedAt(DEFAULT_DELETED_AT)
        .build();
  }

  private static String serializeAuditInfo(AuditInfo auditInfo) {
    try {
      return JsonUtils.anyFieldMapper().writeValueAsString(auditInfo);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize domain relation audit info", e);
    }
  }

  private static AuditInfo deserializeAuditInfo(String auditInfo) {
    try {
      return JsonUtils.anyFieldMapper().readValue(auditInfo, AuditInfo.class);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to deserialize domain relation audit info", e);
    }
  }
}
