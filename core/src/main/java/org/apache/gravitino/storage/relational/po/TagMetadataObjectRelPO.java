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
import org.apache.commons.lang3.StringUtils;

@Getter
public class TagMetadataObjectRelPO {
  private Long tagId;
  private Long metadataObjectId;
  private String metadataObjectType;
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
    if (!(o instanceof TagMetadataObjectRelPO)) {
      return false;
    }
    TagMetadataObjectRelPO tagRelPO = (TagMetadataObjectRelPO) o;
    return Objects.equal(tagId, tagRelPO.tagId)
        && Objects.equal(metadataObjectId, tagRelPO.metadataObjectId)
        && Objects.equal(metadataObjectType, tagRelPO.metadataObjectType)
        && Objects.equal(auditInfo, tagRelPO.auditInfo)
        && Objects.equal(currentVersion, tagRelPO.currentVersion)
        && Objects.equal(lastVersion, tagRelPO.lastVersion)
        && Objects.equal(deletedAt, tagRelPO.deletedAt);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        tagId,
        metadataObjectId,
        metadataObjectType,
        auditInfo,
        currentVersion,
        lastVersion,
        deletedAt);
  }

  public static class Builder {
    private final TagMetadataObjectRelPO tagRelPO;

    private Builder() {
      tagRelPO = new TagMetadataObjectRelPO();
    }

    public Builder withTagId(Long tagId) {
      tagRelPO.tagId = tagId;
      return this;
    }

    public Builder withMetadataObjectId(Long metadataObjectId) {
      tagRelPO.metadataObjectId = metadataObjectId;
      return this;
    }

    public Builder withMetadataObjectType(String metadataObjectType) {
      tagRelPO.metadataObjectType = metadataObjectType;
      return this;
    }

    public Builder withAuditInfo(String auditInfo) {
      tagRelPO.auditInfo = auditInfo;
      return this;
    }

    public Builder withCurrentVersion(Long currentVersion) {
      tagRelPO.currentVersion = currentVersion;
      return this;
    }

    public Builder withLastVersion(Long lastVersion) {
      tagRelPO.lastVersion = lastVersion;
      return this;
    }

    public Builder withDeletedAt(Long deletedAt) {
      tagRelPO.deletedAt = deletedAt;
      return this;
    }

    private void validate() {
      Preconditions.checkArgument(tagRelPO.tagId != null, "Tag id is required");
      Preconditions.checkArgument(
          tagRelPO.metadataObjectId != null, "Metadata object id is required");
      Preconditions.checkArgument(
          StringUtils.isNotBlank(tagRelPO.metadataObjectType),
          "Metadata object type should not be empty");
      Preconditions.checkArgument(tagRelPO.auditInfo != null, "Audit info is required");
      Preconditions.checkArgument(tagRelPO.currentVersion != null, "Current version is required");
      Preconditions.checkArgument(tagRelPO.lastVersion != null, "Last version is required");
      Preconditions.checkArgument(tagRelPO.deletedAt != null, "Deleted at is required");
    }

    public TagMetadataObjectRelPO build() {
      validate();
      return tagRelPO;
    }
  }
}
