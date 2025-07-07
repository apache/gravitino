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
public class PolicyVersionPO {
  private Long id;
  private Long metalakeId;
  private Long policyId;
  private Long version;
  private String policyComment;
  private boolean enabled;
  private String content;
  private Long deletedAt;

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PolicyVersionPO)) {
      return false;
    }
    PolicyVersionPO that = (PolicyVersionPO) o;
    return Objects.equal(id, that.id)
        && Objects.equal(metalakeId, that.metalakeId)
        && Objects.equal(policyId, that.policyId)
        && Objects.equal(version, that.version)
        && Objects.equal(policyComment, that.policyComment)
        && Objects.equal(enabled, that.enabled)
        && Objects.equal(content, that.content)
        && Objects.equal(deletedAt, that.deletedAt);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        id, metalakeId, policyId, version, policyComment, enabled, content, deletedAt);
  }

  public static class Builder {
    private Long id;
    private Long metalakeId;
    private Long policyId;
    private Long version;
    private String policyComment;
    private boolean enabled;
    private String content;
    private Long deletedAt;

    public Builder withId(Long id) {
      this.id = id;
      return this;
    }

    public Builder withMetalakeId(Long metalakeId) {
      this.metalakeId = metalakeId;
      return this;
    }

    public Builder withPolicyId(Long policyId) {
      this.policyId = policyId;
      return this;
    }

    public Builder withVersion(Long version) {
      this.version = version;
      return this;
    }

    public Builder withPolicyComment(String policyComment) {
      this.policyComment = policyComment;
      return this;
    }

    public Builder withEnabled(boolean enabled) {
      this.enabled = enabled;
      return this;
    }

    public Builder withContent(String content) {
      this.content = content;
      return this;
    }

    public Builder withDeletedAt(Long deletedAt) {
      this.deletedAt = deletedAt;
      return this;
    }

    public PolicyVersionPO build() {
      validate();
      PolicyVersionPO policyVersionPO = new PolicyVersionPO();
      policyVersionPO.id = this.id;
      policyVersionPO.metalakeId = this.metalakeId;
      policyVersionPO.policyId = this.policyId;
      policyVersionPO.version = this.version;
      policyVersionPO.policyComment = this.policyComment;
      policyVersionPO.enabled = this.enabled;
      policyVersionPO.content = this.content;
      policyVersionPO.deletedAt = this.deletedAt;
      return policyVersionPO;
    }

    private void validate() {
      Preconditions.checkArgument(metalakeId != null, "metalakeId is required");
      Preconditions.checkArgument(policyId != null, "policyId is required");
      Preconditions.checkArgument(version != null, "version is required");
      Preconditions.checkArgument(content != null, "content is required");
      Preconditions.checkArgument(deletedAt != null, "deletedAt is required");
    }
  }
}
