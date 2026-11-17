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
package org.apache.gravitino.dto.requests;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.Getter;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.rest.RESTRequest;

/** A request to set a domain's status. */
@Getter
public class DomainSetRequest implements RESTRequest {

  @JsonProperty("status")
  private final Domain.Status status;

  /** Creates a request for Jackson deserialization. */
  public DomainSetRequest() {
    this(null);
  }

  /**
   * Creates a set request.
   *
   * @param status The new status.
   */
  public DomainSetRequest(Domain.Status status) {
    this.status = status;
  }

  /** Validates the requested value. */
  @Override
  public void validate() {
    Preconditions.checkArgument(status != null, "status must not be null");
  }
}
