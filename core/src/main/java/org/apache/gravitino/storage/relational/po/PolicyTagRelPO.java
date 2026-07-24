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

/** Persistent object for a policy-tag relation row. */
@Getter
public class PolicyTagRelPO {
  private Long policyId;
  private Long tagId;
  private String auditInfo;
  private Long currentVersion;
  private Long lastVersion;
  private Long deletedAt;

  /** Creates a new builder for {@link PolicyTagRelPO}. */
  public static Builder builder() {
    return new Builder();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PolicyTagRelPO)) {
      return false;
    }
    PolicyTagRelPO policyTagRelPO = (PolicyTagRelPO) o;
    return Objects.equal(policyId, policyTagRelPO.policyId)
        && Objects.equal(tagId, policyTagRelPO.tagId)
        && Objects.equal(auditInfo, policyTagRelPO.auditInfo)
        && Objects.equal(currentVersion, policyTagRelPO.currentVersion)
        && Objects.equal(lastVersion, policyTagRelPO.lastVersion)
        && Objects.equal(deletedAt, policyTagRelPO.deletedAt);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(policyId, tagId, auditInfo, currentVersion, lastVersion, deletedAt);
  }

  /** Builder for {@link PolicyTagRelPO}. */
  public static class Builder {
    private final PolicyTagRelPO policyTagRelPO;

    private Builder() {
      policyTagRelPO = new PolicyTagRelPO();
    }

    public Builder withPolicyId(Long policyId) {
      policyTagRelPO.policyId = policyId;
      return this;
    }

    public Builder withTagId(Long tagId) {
      policyTagRelPO.tagId = tagId;
      return this;
    }

    public Builder withAuditInfo(String auditInfo) {
      policyTagRelPO.auditInfo = auditInfo;
      return this;
    }

    public Builder withCurrentVersion(Long currentVersion) {
      policyTagRelPO.currentVersion = currentVersion;
      return this;
    }

    public Builder withLastVersion(Long lastVersion) {
      policyTagRelPO.lastVersion = lastVersion;
      return this;
    }

    public Builder withDeletedAt(Long deletedAt) {
      policyTagRelPO.deletedAt = deletedAt;
      return this;
    }

    private void validate() {
      Preconditions.checkArgument(policyTagRelPO.policyId != null, "Policy id is required");
      Preconditions.checkArgument(policyTagRelPO.tagId != null, "Tag id is required");
      Preconditions.checkArgument(policyTagRelPO.auditInfo != null, "Audit info is required");
      Preconditions.checkArgument(
          policyTagRelPO.currentVersion != null, "Current version is required");
      Preconditions.checkArgument(policyTagRelPO.lastVersion != null, "Last version is required");
      Preconditions.checkArgument(policyTagRelPO.deletedAt != null, "Deleted at is required");
    }

    public PolicyTagRelPO build() {
      validate();
      return policyTagRelPO;
    }
  }
}
