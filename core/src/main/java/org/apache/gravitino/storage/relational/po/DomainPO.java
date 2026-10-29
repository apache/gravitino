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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.DomainEntity;

/**
 * The persistent object of a row in {@code domain_meta}. The backing tag name is not a column of
 * the row: reads that join the live tag fill it, and it is null when the domain has no backing tag
 * or the tag is deleted.
 *
 * <p>A tag bound without a value has a NULL {@code backing_tag_value}, while {@code
 * tag_relation_meta.tag_value} stores the empty string for an assignment without a value. A lookup
 * from tag assignments to the domains they back must map the empty string to NULL instead of
 * comparing the two columns directly.
 */
@Getter
@Accessors(fluent = true)
@EqualsAndHashCode
@ToString
public class DomainPO {

  private Long domainId;
  private String domainName;
  private Long metalakeId;
  private String comment;
  private String status;
  private Long backingTagId;
  private String backingTagName;
  private String backingTagValue;
  private String properties;
  private String auditInfo;
  private Long currentVersion;
  private Long lastVersion;
  private Long deletedAt;

  /** Creates an empty domain persistent object, used by MyBatis to map result rows. */
  public DomainPO() {}

  @lombok.Builder(setterPrefix = "with")
  private DomainPO(
      Long domainId,
      String domainName,
      Long metalakeId,
      String comment,
      String status,
      Long backingTagId,
      String backingTagValue,
      String properties,
      String auditInfo,
      Long currentVersion,
      Long lastVersion,
      Long deletedAt) {
    Preconditions.checkArgument(domainId != null, "domainId cannot be null");
    Preconditions.checkArgument(StringUtils.isNotBlank(domainName), "domainName cannot be blank");
    Preconditions.checkArgument(metalakeId != null, "metalakeId cannot be null");
    Preconditions.checkArgument(StringUtils.isNotBlank(status), "status cannot be blank");
    Preconditions.checkArgument(
        backingTagId != null || backingTagValue == null,
        "backingTagValue cannot be set without backingTagId");
    Preconditions.checkArgument(StringUtils.isNotBlank(auditInfo), "auditInfo cannot be blank");
    Preconditions.checkArgument(currentVersion != null, "currentVersion cannot be null");
    Preconditions.checkArgument(lastVersion != null, "lastVersion cannot be null");
    Preconditions.checkArgument(deletedAt != null, "deletedAt cannot be null");

    this.domainId = domainId;
    this.domainName = domainName;
    this.metalakeId = metalakeId;
    this.comment = comment;
    this.status = status;
    this.backingTagId = backingTagId;
    this.backingTagValue = backingTagValue;
    this.properties = properties;
    this.auditInfo = auditInfo;
    this.currentVersion = currentVersion;
    this.lastVersion = lastVersion;
    this.deletedAt = deletedAt;
  }

  /**
   * Creates the first version of the persistent object of a new domain.
   *
   * @param domainEntity The domain entity.
   * @param metalakeId The ID of the metalake of the domain.
   * @return The domain persistent object.
   */
  public static DomainPO initializeDomainPO(DomainEntity domainEntity, Long metalakeId) {
    return toDomainPO(domainEntity, domainEntity.id(), metalakeId, INIT_VERSION);
  }

  /**
   * Creates the next version of the persistent object of a domain from its updated entity.
   *
   * @param oldDomainPO The domain persistent object observed by the caller.
   * @param newDomainEntity The updated domain entity.
   * @return The domain persistent object with the next version.
   */
  public static DomainPO updateDomainPO(DomainPO oldDomainPO, DomainEntity newDomainEntity) {
    Long nextVersion = oldDomainPO.currentVersion() + 1;
    return toDomainPO(
        newDomainEntity, oldDomainPO.domainId(), oldDomainPO.metalakeId(), nextVersion);
  }

  /**
   * Converts a domain persistent object to a top-level domain entity. The parent is not stored in
   * the domain row, so it is not set. The backing tag name is set only when the read joined the
   * live tag.
   *
   * @param domainPO The domain persistent object.
   * @param namespace The namespace of the domain.
   * @return The domain entity.
   */
  public static DomainEntity fromDomainPO(DomainPO domainPO, Namespace namespace) {
    return fromDomainPO(domainPO, namespace, null);
  }

  /**
   * Converts a domain persistent object to a domain entity with the given parent. The parent is not
   * stored in the domain row; it is read from the sub-domain relation.
   *
   * @param domainPO The domain persistent object.
   * @param namespace The namespace of the domain.
   * @param parentName The name of the parent domain, or null if the domain is top-level.
   * @return The domain entity.
   */
  @SuppressWarnings("unchecked")
  public static DomainEntity fromDomainPO(
      DomainPO domainPO, Namespace namespace, @Nullable String parentName) {
    try {
      return DomainEntity.builder()
          .withId(domainPO.domainId())
          .withName(domainPO.domainName())
          .withNamespace(namespace)
          .withComment(domainPO.comment())
          .withStatus(Domain.Status.valueOf(domainPO.status()))
          .withParent(parentName)
          .withBackingTagId(domainPO.backingTagId())
          .withBackingTagName(domainPO.backingTagName())
          .withBackingTagValue(domainPO.backingTagValue())
          .withProperties(JsonUtils.anyFieldMapper().readValue(domainPO.properties(), Map.class))
          .withAuditInfo(
              JsonUtils.anyFieldMapper().readValue(domainPO.auditInfo(), AuditInfo.class))
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to deserialize domain PO", e);
    }
  }

  /**
   * Converts domain persistent objects to domain entities with their parents.
   *
   * @param domainPOs The domain persistent objects.
   * @param namespace The namespace of the domains.
   * @param parentNames The name of the parent of each sub-domain, keyed by domain ID. Domains that
   *     are not in the map are top-level.
   * @return The domain entities.
   */
  public static List<DomainEntity> fromDomainPOs(
      List<DomainPO> domainPOs, Namespace namespace, Map<Long, String> parentNames) {
    return domainPOs.stream()
        .map(po -> fromDomainPO(po, namespace, parentNames.get(po.domainId())))
        .collect(Collectors.toList());
  }

  // Returns the built PO rather than the lombok builder: javadoc runs before lombok, so a
  // signature that names the generated builder class fails the javadoc task.
  private static DomainPO toDomainPO(
      DomainEntity domainEntity, Long domainId, Long metalakeId, Long version) {
    try {
      return builder()
          .withDomainId(domainId)
          .withMetalakeId(metalakeId)
          .withDomainName(domainEntity.name())
          .withComment(domainEntity.comment())
          .withStatus(domainEntity.status().name())
          .withBackingTagId(domainEntity.backingTagId())
          .withBackingTagValue(domainEntity.backingTagValue())
          .withProperties(JsonUtils.anyFieldMapper().writeValueAsString(domainEntity.properties()))
          .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(domainEntity.auditInfo()))
          .withCurrentVersion(version)
          .withLastVersion(version)
          .withDeletedAt(DEFAULT_DELETED_AT)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize domain entity", e);
    }
  }
}
