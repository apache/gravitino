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

/** Persistent object for a policy-to-role reference row derived from policy content. */
@Getter
public class PolicyRoleRelPO {
  private Long id;
  private Long policyId;
  private String policyName;
  private String roleName;
  private Long currentVersion;
  private Long lastVersion;
  private Long deletedAt;

  /**
   * @return A builder for a policy-to-role reference persistent object.
   */
  public static Builder builder() {
    return new Builder();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PolicyRoleRelPO)) {
      return false;
    }
    PolicyRoleRelPO that = (PolicyRoleRelPO) o;
    return Objects.equal(id, that.id)
        && Objects.equal(policyId, that.policyId)
        && Objects.equal(policyName, that.policyName)
        && Objects.equal(roleName, that.roleName)
        && Objects.equal(currentVersion, that.currentVersion)
        && Objects.equal(lastVersion, that.lastVersion)
        && Objects.equal(deletedAt, that.deletedAt);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        id, policyId, policyName, roleName, currentVersion, lastVersion, deletedAt);
  }

  /** Builder for {@link PolicyRoleRelPO}. */
  public static class Builder {
    private final PolicyRoleRelPO relation;

    private Builder() {
      relation = new PolicyRoleRelPO();
    }

    /** Sets the relation row ID. */
    public Builder withId(Long id) {
      relation.id = id;
      return this;
    }

    /** Sets the policy ID. */
    public Builder withPolicyId(Long policyId) {
      relation.policyId = policyId;
      return this;
    }

    /** Sets the referenced role name. */
    public Builder withRoleName(String roleName) {
      relation.roleName = roleName;
      return this;
    }

    /** Sets the current version. */
    public Builder withCurrentVersion(Long currentVersion) {
      relation.currentVersion = currentVersion;
      return this;
    }

    /** Sets the last version. */
    public Builder withLastVersion(Long lastVersion) {
      relation.lastVersion = lastVersion;
      return this;
    }

    /** Sets the deletion timestamp. */
    public Builder withDeletedAt(Long deletedAt) {
      relation.deletedAt = deletedAt;
      return this;
    }

    /**
     * Builds the persistent object.
     *
     * @return The persistent object.
     */
    public PolicyRoleRelPO build() {
      Preconditions.checkArgument(relation.policyId != null, "Policy id is required");
      Preconditions.checkArgument(relation.roleName != null, "Role name is required");
      Preconditions.checkArgument(relation.currentVersion != null, "Current version is required");
      Preconditions.checkArgument(relation.lastVersion != null, "Last version is required");
      Preconditions.checkArgument(relation.deletedAt != null, "Deleted at is required");
      return relation;
    }
  }
}
