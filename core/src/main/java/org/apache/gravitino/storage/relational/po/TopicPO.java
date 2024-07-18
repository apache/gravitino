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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import lombok.Getter;

@Getter
public class TopicPO {
  private Long topicId;
  private String topicName;
  private Long metalakeId;
  private Long catalogId;
  private Long schemaId;
  private String comment;
  private String properties;
  private String auditInfo;
  private Long currentVersion;
  private Long lastVersion;
  private Long deletedAt;

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TopicPO)) {
      return false;
    }
    TopicPO topicPO = (TopicPO) o;
    return Objects.equal(topicId, topicPO.topicId)
        && Objects.equal(topicName, topicPO.topicName)
        && Objects.equal(metalakeId, topicPO.metalakeId)
        && Objects.equal(catalogId, topicPO.catalogId)
        && Objects.equal(schemaId, topicPO.schemaId)
        && Objects.equal(comment, topicPO.comment)
        && Objects.equal(properties, topicPO.properties)
        && Objects.equal(auditInfo, topicPO.auditInfo)
        && Objects.equal(currentVersion, topicPO.currentVersion)
        && Objects.equal(lastVersion, topicPO.lastVersion)
        && Objects.equal(deletedAt, topicPO.deletedAt);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        topicId,
        topicName,
        metalakeId,
        catalogId,
        schemaId,
        comment,
        properties,
        auditInfo,
        currentVersion,
        lastVersion,
        deletedAt);
  }

  public static class Builder {
    private final TopicPO topicPO;

    private Builder() {
      topicPO = new TopicPO();
    }

    public Builder withTopicId(Long topicId) {
      topicPO.topicId = topicId;
      return this;
    }

    public Builder withTopicName(String topicName) {
      topicPO.topicName = topicName;
      return this;
    }

    public Builder withMetalakeId(Long metalakeId) {
      topicPO.metalakeId = metalakeId;
      return this;
    }

    public Builder withCatalogId(Long catalogId) {
      topicPO.catalogId = catalogId;
      return this;
    }

    public Builder withSchemaId(Long schemaId) {
      topicPO.schemaId = schemaId;
      return this;
    }

    public Builder withAuditInfo(String auditInfo) {
      topicPO.auditInfo = auditInfo;
      return this;
    }

    public Builder withCurrentVersion(Long currentVersion) {
      topicPO.currentVersion = currentVersion;
      return this;
    }

    public Builder withLastVersion(Long lastVersion) {
      topicPO.lastVersion = lastVersion;
      return this;
    }

    public Builder withDeletedAt(Long deletedAt) {
      topicPO.deletedAt = deletedAt;
      return this;
    }

    public Builder withComment(String comment) {
      topicPO.comment = comment;
      return this;
    }

    public Builder withProperties(String properties) {
      topicPO.properties = properties;
      return this;
    }

    private void validate() {
      Preconditions.checkArgument(topicPO.topicId != null, "topicId cannot be null");
      Preconditions.checkArgument(topicPO.topicName != null, "topicName cannot be null");
      Preconditions.checkArgument(topicPO.metalakeId != null, "metalakeId cannot be null");
      Preconditions.checkArgument(topicPO.catalogId != null, "catalogId cannot be null");
      Preconditions.checkArgument(topicPO.schemaId != null, "schemaId cannot be null");
      Preconditions.checkArgument(topicPO.auditInfo != null, "auditInfo cannot be null");
      Preconditions.checkArgument(topicPO.currentVersion != null, "currentVersion cannot be null");
      Preconditions.checkArgument(topicPO.lastVersion != null, "lastVersion cannot be null");
      Preconditions.checkArgument(topicPO.deletedAt != null, "deletedAt cannot be null");
    }

    public TopicPO build() {
      validate();
      return topicPO;
    }
  }
}
