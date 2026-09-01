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
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * Represents a response reporting the endpoint of the Gravitino Iceberg REST server, if it is
 * running and serves the requested metalake.
 */
@Getter
@EqualsAndHashCode(callSuper = true)
@ToString
public class IcebergRESTServiceResponse extends BaseResponse {

  @Nullable
  @JsonProperty("uri")
  private final String uri;

  /**
   * Constructor for IcebergRESTServiceResponse.
   *
   * @param uri the Iceberg REST server endpoint, or {@code null} when it is not running or does not
   *     serve the requested metalake
   */
  public IcebergRESTServiceResponse(@Nullable String uri) {
    super(0);
    this.uri = uri;
  }

  /** Default constructor for IcebergRESTServiceResponse. (Used for Jackson deserialization.) */
  public IcebergRESTServiceResponse() {
    super();
    this.uri = null;
  }
}
