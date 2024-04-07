/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.storage.relational.po;

import com.google.common.base.Preconditions;
import java.util.Objects;
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
    return Objects.equals(topicId, topicPO.topicId)
        && Objects.equals(topicName, topicPO.topicName)
        && Objects.equals(metalakeId, topicPO.metalakeId)
        && Objects.equals(catalogId, topicPO.catalogId)
        && Objects.equals(schemaId, topicPO.schemaId)
        && Objects.equals(comment, topicPO.comment)
        && Objects.equals(properties, topicPO.properties)
        && Objects.equals(auditInfo, topicPO.auditInfo)
        && Objects.equals(currentVersion, topicPO.currentVersion)
        && Objects.equals(lastVersion, topicPO.lastVersion)
        && Objects.equals(deletedAt, topicPO.deletedAt);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
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
