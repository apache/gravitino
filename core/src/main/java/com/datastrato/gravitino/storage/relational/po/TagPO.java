/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.relational.po;

import com.google.common.base.Preconditions;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

@Getter
public class TagPO {
  private Long tagId;
  private String tagName;
  private Long metalakeId;
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
    if (!(o instanceof TagPO)) {
      return false;
    }
    TagPO tagPO = (TagPO) o;
    return java.util.Objects.equals(tagId, tagPO.tagId)
        && java.util.Objects.equals(tagName, tagPO.tagName)
        && java.util.Objects.equals(metalakeId, tagPO.metalakeId)
        && java.util.Objects.equals(comment, tagPO.comment)
        && java.util.Objects.equals(properties, tagPO.properties)
        && java.util.Objects.equals(auditInfo, tagPO.auditInfo)
        && java.util.Objects.equals(currentVersion, tagPO.currentVersion)
        && java.util.Objects.equals(lastVersion, tagPO.lastVersion)
        && java.util.Objects.equals(deletedAt, tagPO.deletedAt);
  }

  @Override
  public int hashCode() {
    return java.util.Objects.hash(
        tagId,
        tagName,
        metalakeId,
        comment,
        properties,
        auditInfo,
        currentVersion,
        lastVersion,
        deletedAt);
  }

  public static class Builder {
    private final TagPO tagPO;

    private Builder() {
      tagPO = new TagPO();
    }

    public Builder withTagId(Long tagId) {
      tagPO.tagId = tagId;
      return this;
    }

    public Builder withTagName(String tagName) {
      tagPO.tagName = tagName;
      return this;
    }

    public Builder withMetalakeId(Long metalakeId) {
      tagPO.metalakeId = metalakeId;
      return this;
    }

    public Builder withComment(String comment) {
      tagPO.comment = comment;
      return this;
    }

    public Builder withProperties(String properties) {
      tagPO.properties = properties;
      return this;
    }

    public Builder withAuditInfo(String auditInfo) {
      tagPO.auditInfo = auditInfo;
      return this;
    }

    public Builder withCurrentVersion(Long currentVersion) {
      tagPO.currentVersion = currentVersion;
      return this;
    }

    public Builder withLastVersion(Long lastVersion) {
      tagPO.lastVersion = lastVersion;
      return this;
    }

    public Builder withDeletedAt(Long deletedAt) {
      tagPO.deletedAt = deletedAt;
      return this;
    }

    public TagPO build() {
      validate();
      return tagPO;
    }

    private void validate() {
      Preconditions.checkArgument(tagPO.tagId != null, "tagId cannot be null");
      Preconditions.checkArgument(StringUtils.isNotBlank(tagPO.tagName), "tagName cannot be empty");
      Preconditions.checkArgument(tagPO.metalakeId != null, "metalakeId cannot be null");
      Preconditions.checkArgument(tagPO.auditInfo != null, "auditInfo cannot be null");
      Preconditions.checkArgument(tagPO.currentVersion != null, "currentVersion cannot be null");
      Preconditions.checkArgument(tagPO.lastVersion != null, "lastVersion cannot be null");
      Preconditions.checkArgument(tagPO.deletedAt != null, "deletedAt cannot be null");
    }
  }
}
