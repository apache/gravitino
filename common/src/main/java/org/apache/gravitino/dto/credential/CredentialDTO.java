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
package org.apache.gravitino.dto.credential;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import java.util.Map;
import org.apache.gravitino.credential.Credential;

/** Represents a credential Data Transfer Object (DTO). */
public class CredentialDTO implements Credential {

  @JsonProperty("credentialType")
  private String credentialType;

  @JsonProperty("expireTimeInMs")
  private long expireTimeInMs;

  @JsonProperty("credentialInfo")
  private Map<String, String> credentialInfo;

  private CredentialDTO() {}

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CredentialDTO)) {
      return false;
    }

    CredentialDTO credentialDTO = (CredentialDTO) o;
    return Objects.equal(credentialType, credentialDTO.credentialType)
        && Objects.equal(expireTimeInMs, credentialDTO.expireTimeInMs)
        && Objects.equal(credentialInfo, credentialDTO.credentialInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(credentialType, expireTimeInMs, credentialInfo);
  }

  @Override
  public String credentialType() {
    return credentialType;
  }

  @Override
  public long expireTimeInMs() {
    return expireTimeInMs;
  }

  @Override
  public Map<String, String> credentialInfo() {
    return credentialInfo;
  }

  @Override
  public void initialize(Map<String, String> credentialInfo, long expireTimeInMs) {
    throw new UnsupportedOperationException("CredentialDTO doesn't support initWithCredentialInfo");
  }

  /**
   * @return a new builder for constructing a Credential DTO.
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder class for constructing CredentialDTO instances. */
  public static class Builder {
    private final CredentialDTO credentialDTO;

    private Builder() {
      credentialDTO = new CredentialDTO();
    }

    /**
     * Sets the credential type.
     *
     * @param credentialType The specific credential type.
     * @return The builder instance.
     */
    public Builder withCredentialType(String credentialType) {
      credentialDTO.credentialType = credentialType;
      return this;
    }

    /**
     * Sets the credential expire time.
     *
     * @param expireTimeInMs The credential expire time.
     * @return The builder instance.
     */
    public Builder withExpireTimeInMs(long expireTimeInMs) {
      credentialDTO.expireTimeInMs = expireTimeInMs;
      return this;
    }

    /**
     * Sets the credential information.
     *
     * @param credentialInfo The specific credential information map.
     * @return The builder instance.
     */
    public Builder withCredentialInfo(Map<String, String> credentialInfo) {
      credentialDTO.credentialInfo = credentialInfo;
      return this;
    }

    /**
     * @return The constructed credential DTO.
     */
    public CredentialDTO build() {
      return credentialDTO;
    }
  }
}
