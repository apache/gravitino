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
    return Objects.equal(tagId, tagPO.tagId)
        && Objects.equal(tagName, tagPO.tagName)
        && Objects.equal(metalakeId, tagPO.metalakeId)
        && Objects.equal(comment, tagPO.comment)
        && Objects.equal(properties, tagPO.properties)
        && Objects.equal(auditInfo, tagPO.auditInfo)
        && Objects.equal(currentVersion, tagPO.currentVersion)
        && Objects.equal(lastVersion, tagPO.lastVersion)
        && Objects.equal(deletedAt, tagPO.deletedAt);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
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
