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
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.gravitino.rest.RESTRequest;

/** Represents a request to set a Metalake in use. */
@Getter
@EqualsAndHashCode
@ToString
public class MetalakeSetRequest implements RESTRequest {

  @JsonProperty("inUse")
  @Getter(AccessLevel.NONE)
  private final Boolean inUse;

  /** Default constructor for MetalakeSetRequest. */
  public MetalakeSetRequest() {
    this.inUse = null;
  }

  /**
   * Constructor for MetalakeSetRequest.
   *
   * @param inUse The in use status to set.
   */
  public MetalakeSetRequest(boolean inUse) {
    this.inUse = inUse;
  }

  /**
   * Validates that the requested state was explicitly provided.
   *
   * @throws IllegalArgumentException If the request is invalid.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(inUse != null, "inUse field is required and must not be null");
  }

  /**
   * Returns the explicitly requested state.
   *
   * @return whether the object should be in use
   * @throws IllegalArgumentException if the state was not provided
   */
  public boolean isInUse() {
    validate();
    return inUse;
  }
}
