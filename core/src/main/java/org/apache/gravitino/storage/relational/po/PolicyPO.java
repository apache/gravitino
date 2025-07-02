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
public class PolicyPO {
  private Long policyId;
  private String policyName;
  private String policyType;
  private Long metalakeId;
  private boolean inheritable;
  private boolean exclusive;
  private String supportedObjectTypes;
  private String auditInfo;
  private Long currentVersion;
  private Long lastVersion;
  private Long deletedAt;
  private PolicyVersionPO policyVersionPO;

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PolicyPO)) {
      return false;
    }
    PolicyPO policyPO = (PolicyPO) o;
    return Objects.equal(getPolicyId(), policyPO.policyId)
        && Objects.equal(getPolicyName(), policyPO.policyName)
        && Objects.equal(getPolicyType(), policyPO.policyType)
        && Objects.equal(getMetalakeId(), policyPO.metalakeId)
        && Objects.equal(isInheritable(), policyPO.inheritable)
        && Objects.equal(isExclusive(), policyPO.exclusive)
        && Objects.equal(getSupportedObjectTypes(), policyPO.supportedObjectTypes)
        && Objects.equal(getAuditInfo(), policyPO.auditInfo)
        && Objects.equal(getCurrentVersion(), policyPO.currentVersion)
        && Objects.equal(getLastVersion(), policyPO.lastVersion)
        && Objects.equal(getPolicyVersionPO(), policyPO.policyVersionPO)
        && Objects.equal(getDeletedAt(), policyPO.deletedAt);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        getPolicyId(),
        getPolicyName(),
        getPolicyType(),
        getMetalakeId(),
        isInheritable(),
        isExclusive(),
        getSupportedObjectTypes(),
        getAuditInfo(),
        getCurrentVersion(),
        getLastVersion(),
        getPolicyVersionPO(),
        getDeletedAt());
  }

  public static class Builder {
    private Long policyId;
    private String policyName;
    private String policyType;
    private Long metalakeId;
    private boolean inheritable;
    private boolean exclusive;
    private String supportedObjectTypes;
    private String auditInfo;
    private Long currentVersion;
    private Long lastVersion;
    private Long deletedAt;
    private PolicyVersionPO policyVersionPO;

    public Builder withPolicyId(Long policyId) {
      this.policyId = policyId;
      return this;
    }

    public Builder withPolicyName(String policyName) {
      this.policyName = policyName;
      return this;
    }

    public Builder withMetalakeId(Long metalakeId) {
      this.metalakeId = metalakeId;
      return this;
    }

    public Builder withPolicyType(String policyType) {
      this.policyType = policyType;
      return this;
    }

    public Builder withInheritable(boolean inheritable) {
      this.inheritable = inheritable;
      return this;
    }

    public Builder withExclusive(boolean exclusive) {
      this.exclusive = exclusive;
      return this;
    }

    public Builder withSupportedObjectTypes(String supportedObjectTypes) {
      this.supportedObjectTypes = supportedObjectTypes;
      return this;
    }

    public Builder withAuditInfo(String auditInfo) {
      this.auditInfo = auditInfo;
      return this;
    }

    public Builder withCurrentVersion(Long currentVersion) {
      this.currentVersion = currentVersion;
      return this;
    }

    public Builder withLastVersion(Long lastVersion) {
      this.lastVersion = lastVersion;
      return this;
    }

    public Builder withDeletedAt(Long deletedAt) {
      this.deletedAt = deletedAt;
      return this;
    }

    public Builder withPolicyVersionPO(PolicyVersionPO policyVersionPO) {
      this.policyVersionPO = policyVersionPO;
      return this;
    }

    public Long getMetalakeId() {
      Preconditions.checkArgument(metalakeId != null, "Metalake id is required");
      return metalakeId;
    }

    public PolicyPO build() {
      validate();
      PolicyPO policyPO = new PolicyPO();
      policyPO.policyId = policyId;
      policyPO.policyName = policyName;
      policyPO.metalakeId = metalakeId;
      policyPO.policyType = policyType;
      policyPO.inheritable = inheritable;
      policyPO.exclusive = exclusive;
      policyPO.supportedObjectTypes = supportedObjectTypes;
      policyPO.auditInfo = auditInfo;
      policyPO.currentVersion = currentVersion;
      policyPO.lastVersion = lastVersion;
      policyPO.deletedAt = deletedAt;
      policyPO.policyVersionPO = policyVersionPO;
      return policyPO;
    }

    private void validate() {
      Preconditions.checkArgument(policyId != null, "Policy id is required");
      Preconditions.checkArgument(policyName != null, "Policy name is required");
      Preconditions.checkArgument(metalakeId != null, "Metalake id is required");
      Preconditions.checkArgument(policyType != null, "Policy type is required");
      Preconditions.checkArgument(
          supportedObjectTypes != null, "Supported object types is required");
      Preconditions.checkArgument(currentVersion != null, "Current version is required");
      Preconditions.checkArgument(lastVersion != null, "Last version is required");
      Preconditions.checkArgument(deletedAt != null, "Deleted at is required");
      Preconditions.checkArgument(auditInfo != null, "Audit info is required");
      Preconditions.checkArgument(policyVersionPO != null, "Policy version is required");
    }
  }
}
