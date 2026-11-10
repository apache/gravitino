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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.json.JsonUtils.NameIdentifierDeserializer;
import org.apache.gravitino.json.JsonUtils.NameIdentifierSerializer;

/** Represents a response containing a list of catalogs. */
@EqualsAndHashCode(callSuper = true)
@ToString
public class EntityListResponse extends BaseResponse implements PaginatedResponse {

  @JsonSerialize(contentUsing = NameIdentifierSerializer.class)
  @JsonDeserialize(contentUsing = NameIdentifierDeserializer.class)
  @JsonProperty("identifiers")
  private final NameIdentifier[] idents;

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonProperty("nextPageToken")
  private final String nextPageToken;

  /**
   * Constructor for EntityListResponse.
   *
   * @param idents The array of entity identifiers.
   */
  public EntityListResponse(NameIdentifier[] idents) {
    this(idents, null);
  }

  /**
   * Constructor for EntityListResponse holding one page of identifiers.
   *
   * @param idents The array of entity identifiers in the page.
   * @param nextPageToken The token for the next page, or null when no further entry remains.
   */
  public EntityListResponse(NameIdentifier[] idents, String nextPageToken) {
    super(0);
    this.idents = idents;
    this.nextPageToken = nextPageToken;
  }

  /** Default constructor for EntityListResponse. (Used for Jackson deserialization.) */
  public EntityListResponse() {
    super();
    this.idents = null;
    this.nextPageToken = null;
  }

  /**
   * Returns the array of entity identifiers.
   *
   * @return The array of identifiers.
   */
  public NameIdentifier[] identifiers() {
    return idents;
  }

  @Override
  public String nextPageToken() {
    return nextPageToken;
  }

  /**
   * Validates the response data.
   *
   * @throws IllegalArgumentException if catalog identifiers are not set.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();
    Preconditions.checkArgument(idents != null, "identifiers must not be null");
  }
}
