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
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.rest.RESTRequest;

/** Represents a request to create a Metalake. */
@Getter
@EqualsAndHashCode
@ToString
public class MetalakeCreateRequest implements RESTRequest {

  @JsonProperty("name")
  private final String name;

  @Nullable
  @JsonProperty("comment")
  private final String comment;

  @Nullable
  @JsonProperty("properties")
  private final Map<String, String> properties;

  /** Default constructor for MetalakeCreateRequest. (Used for Jackson deserialization.) */
  public MetalakeCreateRequest() {
    this(null, null, null);
  }

  /**
   * Constructor for MetalakeCreateRequest.
   *
   * @param name The name of the Metalake.
   * @param comment The comment for the Metalake.
   * @param properties The properties for the Metalake.
   */
  public MetalakeCreateRequest(String name, String comment, Map<String, String> properties) {
    super();

    this.name = Optional.ofNullable(name).map(String::trim).orElse(null);
    this.comment = Optional.ofNullable(comment).map(String::trim).orElse(null);
    this.properties = properties;
  }

  /**
   * Validates the fields of the request.
   *
   * @throws IllegalArgumentException if the name is not set.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(name), "\"name\" field is required and cannot be empty");
  }
}
