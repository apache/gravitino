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
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.gravitino.rest.RESTRequest;

/** Represents a request to set a catalog in use. */
@Getter
@EqualsAndHashCode
@ToString
public class CatalogSetRequest implements RESTRequest {

  /**
   * Creates a CatalogSetRequest to set a catalog in use or not in use.
   *
   * @param inUse The in use status to set.
   * @return The CatalogSetRequest.
   */
  public static CatalogSetRequest setInUse(Boolean inUse) {
    return new CatalogSetRequest(inUse, null);
  }

  /**
   * Creates a CatalogSetRequest to set a catalog read-only or not.
   *
   * @param readOnly The read-only status to set.
   * @return The CatalogSetRequest.
   */
  public static CatalogSetRequest setReadOnly(Boolean readOnly) {
    return new CatalogSetRequest(null, readOnly);
  }

  @JsonProperty("inUse")
  private final Boolean inUse;

  @JsonProperty("readOnly")
  private final Boolean readOnly;

  /** Default constructor for CatalogSetRequest. */
  public CatalogSetRequest() {
    this(null, null);
  }

  /**
   * Constructor for CatalogSetRequest.
   *
   * @param inUse The in use status to set.
   * @param readOnly The read-only status to set.
   */
  private CatalogSetRequest(Boolean inUse, Boolean readOnly) {
    this.inUse = inUse;
    this.readOnly = readOnly;
  }

  /**
   * Validates the request.
   *
   * @throws IllegalArgumentException If the request is invalid.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(
        inUse != null || readOnly != null, "Either inUse or readOnly must be set.");
    Preconditions.checkArgument(
        inUse == null || readOnly == null, "Only one of inUse or readOnly can be set.");
  }
}
