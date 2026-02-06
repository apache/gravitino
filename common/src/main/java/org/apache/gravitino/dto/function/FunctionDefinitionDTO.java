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
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Arrays;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.gravitino.function.FunctionColumn;
import org.apache.gravitino.function.FunctionDefinition;
import org.apache.gravitino.function.FunctionDefinitions;
import org.apache.gravitino.function.FunctionImpl;
import org.apache.gravitino.function.FunctionParam;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.rel.types.Type;

/** DTO for function definition. */
@Getter
@EqualsAndHashCode
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder(setterPrefix = "with")
public class FunctionDefinitionDTO implements FunctionDefinition {

  @JsonProperty("parameters")
  private FunctionParamDTO[] parameters;

  @Nullable
  @JsonProperty("returnType")
  @JsonSerialize(using = JsonUtils.TypeSerializer.class)
  @JsonDeserialize(using = JsonUtils.TypeDeserializer.class)
  private Type returnType;

  @JsonProperty("returnColumns")
  private FunctionColumnDTO[] returnColumns;

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
  public Type returnType() {
    return returnType;
  }

  @Override
  public FunctionColumn[] returnColumns() {
    if (returnColumns == null) {
      return FunctionDefinition.EMPTY_COLUMNS;
    }
    return Arrays.stream(returnColumns)
        .map(FunctionColumnDTO::toFunctionColumn)
        .toArray(FunctionColumn[]::new);
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
  @SuppressWarnings("deprecation")
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

    if (returnType != null) {
      return FunctionDefinitions.of(params, returnType, implArr);
    } else if (returnColumns != null && returnColumns.length > 0) {
      FunctionColumn[] cols =
          Arrays.stream(returnColumns)
              .map(FunctionColumnDTO::toFunctionColumn)
              .toArray(FunctionColumn[]::new);
      return FunctionDefinitions.of(params, cols, implArr);
    } else {
      // Fallback for backward compatibility
      return FunctionDefinitions.of(params, implArr);
    }
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

    FunctionColumnDTO[] columnDTOs = null;
    if (definition.returnColumns() != null && definition.returnColumns().length > 0) {
      columnDTOs =
          Arrays.stream(definition.returnColumns())
              .map(FunctionColumnDTO::fromFunctionColumn)
              .toArray(FunctionColumnDTO[]::new);
    }

    FunctionImplDTO[] implDTOs =
        definition.impls() == null
            ? new FunctionImplDTO[0]
            : Arrays.stream(definition.impls())
                .map(FunctionImplDTO::fromFunctionImpl)
                .toArray(FunctionImplDTO[]::new);

    return FunctionDefinitionDTO.builder()
        .withParameters(paramDTOs)
        .withReturnType(definition.returnType())
        .withReturnColumns(columnDTOs)
        .withImpls(implDTOs)
        .build();
  }

  @Override
  public String toString() {
    return "FunctionDefinitionDTO{"
        + "parameters="
        + Arrays.toString(parameters)
        + ", returnType="
        + returnType
        + ", returnColumns="
        + Arrays.toString(returnColumns)
        + ", impls="
        + Arrays.toString(impls)
        + '}';
  }
}
