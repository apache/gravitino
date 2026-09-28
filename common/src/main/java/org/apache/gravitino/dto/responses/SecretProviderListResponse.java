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
package org.apache.gravitino.dto.responses;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.dto.secret.SecretProviderDTO;

/** Represents a response for a list of secrets providers. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class SecretProviderListResponse extends BaseResponse {

  @JsonProperty("providers")
  private final SecretProviderDTO[] providers;

  /**
   * Constructor for SecretProviderListResponse.
   *
   * @param providers The array of secrets-provider DTOs.
   */
  public SecretProviderListResponse(SecretProviderDTO[] providers) {
    super(0);
    this.providers = providers;
  }

  /** Default constructor for Jackson deserialization. */
  public SecretProviderListResponse() {
    super();
    this.providers = null;
  }

  /**
   * Validates the response data.
   *
   * @throws IllegalArgumentException if providers is null or contains a blank name or type.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();

    Preconditions.checkArgument(providers != null, "providers must not be null");
    Arrays.stream(providers)
        .forEach(
            provider -> {
              Preconditions.checkArgument(
                  StringUtils.isNotBlank(provider.getName()), "provider 'name' must not be blank");
              Preconditions.checkArgument(
                  StringUtils.isNotBlank(provider.getType()), "provider 'type' must not be blank");
            });
  }
}
