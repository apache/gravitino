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
package org.apache.gravitino.dto.function;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.function.FunctionParam;
import org.apache.gravitino.function.FunctionSignature;
import org.apache.gravitino.rest.RESTRequest;

/** DTO for {@link FunctionSignature}. */
@Getter
@EqualsAndHashCode
@ToString
public class FunctionSignatureDTO implements RESTRequest {

  @JsonProperty("name")
  private String name;

  @JsonProperty("functionParams")
  private FunctionParamDTO[] functionParams;

  private FunctionSignatureDTO() {}

  /**
   * Creates a function signature DTO.
   *
   * @param name Function name.
   * @param functionParams Function parameters.
   */
  public FunctionSignatureDTO(String name, FunctionParamDTO[] functionParams) {
    this.name = name;
    this.functionParams = functionParams;
  }

  /**
   * Converts this DTO to a {@link FunctionSignature}.
   *
   * @return The converted signature.
   */
  public FunctionSignature toFunctionSignature() {
    FunctionParam[] params =
        functionParams == null
            ? new FunctionParam[0]
            : Arrays.stream(functionParams)
                .map(FunctionParam.class::cast)
                .toArray(FunctionParam[]::new);
    return FunctionSignature.of(name, params);
  }

  /** {@inheritDoc} */
  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(StringUtils.isNotBlank(name), "\"name\" field is required");
    if (functionParams != null) {
      Arrays.stream(functionParams).forEach(FunctionParamDTO::validate);
    }
  }
}
