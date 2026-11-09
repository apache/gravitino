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
import org.apache.gravitino.dto.domain.DomainDTO;

/** A response containing a list of domains. */
public class DomainListResponse extends BaseResponse {

  @JsonProperty("domains")
  private final DomainDTO[] domains;

  /** Creates an empty response for Jackson deserialization. */
  public DomainListResponse() {
    super();
    domains = null;
  }

  /**
   * Creates a successful list response.
   *
   * @param domains The returned domains.
   */
  public DomainListResponse(DomainDTO[] domains) {
    super(0);
    this.domains = domains.clone();
  }

  /**
   * @return The returned domains.
   */
  public DomainDTO[] getDomains() {
    return domains.clone();
  }

  /** Validates that the list and its entries are present. */
  @Override
  public void validate() {
    super.validate();
    Preconditions.checkArgument(domains != null, "domains must not be null");
    Arrays.stream(domains)
        .forEach(
            item -> Preconditions.checkArgument(item != null, "domains must not contain null"));
  }
}
