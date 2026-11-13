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
import lombok.Getter;
import org.apache.gravitino.dto.domain.DomainDTO;

/** A successful response containing a domain snapshot. */
@Getter
public class DomainResponse extends BaseResponse {

  @JsonProperty("domain")
  private final DomainDTO domain;

  /** Creates a response for Jackson deserialization. */
  public DomainResponse() {
    super();
    domain = null;
  }

  /**
   * Creates a successful domain response.
   *
   * @param domain The domain snapshot.
   */
  public DomainResponse(DomainDTO domain) {
    super(0);
    this.domain = domain;
  }

  /** Validates that the domain is present. */
  @Override
  public void validate() {
    super.validate();
    Preconditions.checkArgument(domain != null, "domain must not be null");
  }
}
