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

import com.google.common.base.Preconditions;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

/** This class is the persistent object of owner relation. */
@Getter
public class OwnerRelPO {

  Long metalakeId;
  Long ownerId;
  String ownerType;
  Long metadataObjectId;
  String metadataObjectType;
  private String auditInfo;
  private Long currentVersion;
  private Long lastVersion;
  private Long deletedAt;

  private OwnerRelPO() {}

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final OwnerRelPO ownerRelPO;

    private Builder() {
      this.ownerRelPO = new OwnerRelPO();
    }

    public Builder withMetalakeId(Long metalakeId) {
      ownerRelPO.metalakeId = metalakeId;
      return this;
    }

    public Builder withOwnerId(Long ownerId) {
      ownerRelPO.ownerId = ownerId;
      return this;
    }

    public Builder withOwnerType(String ownerType) {
      ownerRelPO.ownerType = ownerType;
      return this;
    }

    public Builder withMetadataObjectId(Long metadataObjectId) {
      ownerRelPO.metadataObjectId = metadataObjectId;
      return this;
    }

    public Builder withMetadataObjectType(String metadataObjectType) {
      ownerRelPO.metadataObjectType = metadataObjectType;
      return this;
    }

    public Builder withAuditIfo(String auditIfo) {
      ownerRelPO.auditInfo = auditIfo;
      return this;
    }

    public Builder withCurrentVersion(Long currentVersion) {
      ownerRelPO.currentVersion = currentVersion;
      return this;
    }

    public Builder withLastVersion(Long lastVersion) {
      ownerRelPO.lastVersion = lastVersion;
      return this;
    }

    public Builder withDeleteAt(Long deleteAt) {
      ownerRelPO.deletedAt = deleteAt;
      return this;
    }

    public OwnerRelPO build() {
      validate();
      return ownerRelPO;
    }

    private void validate() {
      Preconditions.checkArgument(ownerRelPO.ownerId != null, "Owner id is required");
      Preconditions.checkArgument(
          StringUtils.isNotBlank(ownerRelPO.ownerType), "Owner type is required");
      Preconditions.checkArgument(
          ownerRelPO.metadataObjectId != null, "Metadata object id is required");
      Preconditions.checkArgument(
          ownerRelPO.metadataObjectType != null, "Metadata object type is required");
      Preconditions.checkArgument(ownerRelPO.auditInfo != null, "Audit info is required");
      Preconditions.checkArgument(ownerRelPO.currentVersion != null, "Current version is required");
      Preconditions.checkArgument(ownerRelPO.lastVersion != null, "Last version is required");
      Preconditions.checkArgument(ownerRelPO.deletedAt != null, "Deleted at is required");
    }
  }
}
