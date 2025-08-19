/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.dto.responses;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

/** Response for the model version uri. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class ModelVersionUriResponse extends BaseResponse {
  @JsonProperty("uri")
  private final String uri;

  /** Default constructor for ModelVersionUriResponse. */
  public ModelVersionUriResponse() {
    super(0);
    this.uri = null;
  }

  /**
   * Constructor for ModelVersionUriResponse.
   *
   * @param uri the uri of the model version
   */
  public ModelVersionUriResponse(String uri) {
    super(0);
    this.uri = uri;
  }

  /**
   * Validates the response.
   *
   * @throws IllegalArgumentException if the response is invalid.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();
    Preconditions.checkArgument(StringUtils.isNotBlank(uri), "uri must not be blank");
  }
}
