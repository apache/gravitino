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
import java.util.Arrays;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.gravitino.function.FunctionDefinition;
import org.apache.gravitino.function.FunctionDefinitions;
import org.apache.gravitino.function.FunctionImpl;
import org.apache.gravitino.function.FunctionParam;

/** DTO for function definition. */
@Getter
@EqualsAndHashCode
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder(setterPrefix = "with")
public class FunctionDefinitionDTO implements FunctionDefinition {

  @JsonProperty("parameters")
  private FunctionParamDTO[] parameters;

  @JsonProperty("impls")
  private FunctionImplDTO[] impls;

  @Override
  public FunctionParam[] parameters() {
    if (parameters == null) {
      return new FunctionParam[0];
    }
    return parameters;
  }

  @Override
  public FunctionImpl[] impls() {
    if (impls == null) {
      return new FunctionImpl[0];
    }
    return Arrays.stream(impls).map(FunctionImplDTO::toFunctionImpl).toArray(FunctionImpl[]::new);
  }

  /**
   * Convert this DTO to a {@link FunctionDefinition} instance.
   *
   * @return The function definition.
   */
  public FunctionDefinition toFunctionDefinition() {
    FunctionParam[] params =
        parameters == null
            ? new FunctionParam[0]
            : Arrays.stream(parameters)
                .map(FunctionParamDTO::toFunctionParam)
                .toArray(FunctionParam[]::new);
    FunctionImpl[] implArr =
        impls == null
            ? new FunctionImpl[0]
            : Arrays.stream(impls)
                .map(FunctionImplDTO::toFunctionImpl)
                .toArray(FunctionImpl[]::new);
    return FunctionDefinitions.of(params, implArr);
  }

  /**
   * Create a {@link FunctionDefinitionDTO} from a {@link FunctionDefinition} instance.
   *
   * @param definition The function definition.
   * @return The function definition DTO.
   */
  public static FunctionDefinitionDTO fromFunctionDefinition(FunctionDefinition definition) {
    FunctionParamDTO[] paramDTOs =
        definition.parameters() == null
            ? new FunctionParamDTO[0]
            : Arrays.stream(definition.parameters())
                .map(
                    param ->
                        param instanceof FunctionParamDTO
                            ? (FunctionParamDTO) param
                            : FunctionParamDTO.fromFunctionParam(param))
                .toArray(FunctionParamDTO[]::new);
    FunctionImplDTO[] implDTOs =
        definition.impls() == null
            ? new FunctionImplDTO[0]
            : Arrays.stream(definition.impls())
                .map(FunctionImplDTO::fromFunctionImpl)
                .toArray(FunctionImplDTO[]::new);
    return new FunctionDefinitionDTO(paramDTOs, implDTOs);
  }

  @Override
  public String toString() {
    return "FunctionDefinitionDTO{"
        + "parameters="
        + Arrays.toString(parameters)
        + ", impls="
        + Arrays.toString(impls)
        + '}';
  }
}
