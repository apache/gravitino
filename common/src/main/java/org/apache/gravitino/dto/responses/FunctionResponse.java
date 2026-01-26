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
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.dto.function.FunctionDTO;

/** Response for function operations. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class FunctionResponse extends BaseResponse {

  @JsonProperty("function")
  private final FunctionDTO function;

  /** Constructor for FunctionResponse. */
  public FunctionResponse() {
    super(0);
    this.function = null;
  }

  /**
   * Constructor for FunctionResponse.
   *
   * @param function the function DTO object.
   */
  public FunctionResponse(FunctionDTO function) {
    super(0);
    this.function = function;
  }

  /**
   * Validates the response.
   *
   * @throws IllegalArgumentException if the response is invalid.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();
    Preconditions.checkArgument(function != null, "function must not be null");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(function.name()), "function 'name' must not be null and empty");
    Preconditions.checkArgument(
        function.functionType() != null, "function 'functionType' must not be null");
    Preconditions.checkArgument(
        function.definitions() != null, "function 'definitions' must not be null");
  }
}
