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
public class PolicyMetadataObjectRelPO {
  private Long policyId;
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
    if (!(o instanceof PolicyMetadataObjectRelPO)) {
      return false;
    }
    PolicyMetadataObjectRelPO policyRelPO = (PolicyMetadataObjectRelPO) o;
    return Objects.equal(policyId, policyRelPO.policyId)
        && Objects.equal(metadataObjectId, policyRelPO.metadataObjectId)
        && Objects.equal(metadataObjectType, policyRelPO.metadataObjectType)
        && Objects.equal(auditInfo, policyRelPO.auditInfo)
        && Objects.equal(currentVersion, policyRelPO.currentVersion)
        && Objects.equal(lastVersion, policyRelPO.lastVersion)
        && Objects.equal(deletedAt, policyRelPO.deletedAt);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        policyId,
        metadataObjectId,
        metadataObjectType,
        auditInfo,
        currentVersion,
        lastVersion,
        deletedAt);
  }

  public static class Builder {
    private final PolicyMetadataObjectRelPO policyRelPO;

    private Builder() {
      policyRelPO = new PolicyMetadataObjectRelPO();
    }

    public Builder withPolicyId(Long policyId) {
      policyRelPO.policyId = policyId;
      return this;
    }

    public Builder withMetadataObjectId(Long metadataObjectId) {
      policyRelPO.metadataObjectId = metadataObjectId;
      return this;
    }

    public Builder withMetadataObjectType(String metadataObjectType) {
      policyRelPO.metadataObjectType = metadataObjectType;
      return this;
    }

    public Builder withAuditInfo(String auditInfo) {
      policyRelPO.auditInfo = auditInfo;
      return this;
    }

    public Builder withCurrentVersion(Long currentVersion) {
      policyRelPO.currentVersion = currentVersion;
      return this;
    }

    public Builder withLastVersion(Long lastVersion) {
      policyRelPO.lastVersion = lastVersion;
      return this;
    }

    public Builder withDeletedAt(Long deletedAt) {
      policyRelPO.deletedAt = deletedAt;
      return this;
    }

    private void validate() {
      Preconditions.checkArgument(policyRelPO.policyId != null, "Policy id is required");
      Preconditions.checkArgument(
          policyRelPO.metadataObjectId != null, "Metadata object id is required");
      Preconditions.checkArgument(
          StringUtils.isNotBlank(policyRelPO.metadataObjectType),
          "Metadata object type should not be empty");
      Preconditions.checkArgument(policyRelPO.auditInfo != null, "Audit info is required");
      Preconditions.checkArgument(
          policyRelPO.currentVersion != null, "Current version is required");
      Preconditions.checkArgument(policyRelPO.lastVersion != null, "Last version is required");
      Preconditions.checkArgument(policyRelPO.deletedAt != null, "Deleted at is required");
    }

    public PolicyMetadataObjectRelPO build() {
      validate();
      return policyRelPO;
    }
  }
}
