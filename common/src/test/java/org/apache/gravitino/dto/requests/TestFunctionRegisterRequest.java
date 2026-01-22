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

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.gravitino.dto.function.FunctionDefinitionDTO;
import org.apache.gravitino.dto.function.FunctionParamDTO;
import org.apache.gravitino.dto.function.SQLImplDTO;
import org.apache.gravitino.function.FunctionImpl;
import org.apache.gravitino.function.FunctionType;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestFunctionRegisterRequest {

  @Test
  public void testFunctionRegisterRequestSerDe() throws JsonProcessingException {
    FunctionParamDTO param =
        FunctionParamDTO.builder().withName("x").withDataType(Types.IntegerType.get()).build();
    SQLImplDTO impl =
        new SQLImplDTO(FunctionImpl.RuntimeType.SPARK.name(), null, null, "SELECT x + 1");
    FunctionDefinitionDTO definition =
        FunctionDefinitionDTO.builder()
            .withParameters(new FunctionParamDTO[] {param})
            .withImpls(new SQLImplDTO[] {impl})
            .build();

    FunctionRegisterRequest request =
        FunctionRegisterRequest.builder()
            .withName("test_func")
            .withFunctionType(FunctionType.SCALAR)
            .withDeterministic(true)
            .withComment("test function")
            .withReturnType(Types.IntegerType.get())
            .withDefinitions(new FunctionDefinitionDTO[] {definition})
            .build();

    String json = JsonUtils.objectMapper().writeValueAsString(request);
    FunctionRegisterRequest deserialized =
        JsonUtils.objectMapper().readValue(json, FunctionRegisterRequest.class);

    Assertions.assertEquals(request, deserialized);
  }

  @Test
  public void testValidateScalarFunction() {
    FunctionDefinitionDTO definition =
        FunctionDefinitionDTO.builder().withParameters(new FunctionParamDTO[0]).build();

    FunctionRegisterRequest validRequest =
        FunctionRegisterRequest.builder()
            .withName("scalar_func")
            .withFunctionType(FunctionType.SCALAR)
            .withDeterministic(true)
            .withReturnType(Types.IntegerType.get())
            .withDefinitions(new FunctionDefinitionDTO[] {definition})
            .build();

    Assertions.assertDoesNotThrow(validRequest::validate);

    // Missing returnType for SCALAR function
    FunctionRegisterRequest invalidRequest =
        FunctionRegisterRequest.builder()
            .withName("scalar_func")
            .withFunctionType(FunctionType.SCALAR)
            .withDeterministic(true)
            .withDefinitions(new FunctionDefinitionDTO[] {definition})
            .build();

    Assertions.assertThrows(IllegalArgumentException.class, invalidRequest::validate);
  }

  @Test
  public void testValidateAggregateFunction() {
    FunctionDefinitionDTO definition =
        FunctionDefinitionDTO.builder().withParameters(new FunctionParamDTO[0]).build();

    FunctionRegisterRequest validRequest =
        FunctionRegisterRequest.builder()
            .withName("agg_func")
            .withFunctionType(FunctionType.AGGREGATE)
            .withDeterministic(true)
            .withReturnType(Types.IntegerType.get())
            .withDefinitions(new FunctionDefinitionDTO[] {definition})
            .build();

    Assertions.assertDoesNotThrow(validRequest::validate);

    // Missing returnType for AGGREGATE function
    FunctionRegisterRequest invalidRequest =
        FunctionRegisterRequest.builder()
            .withName("agg_func")
            .withFunctionType(FunctionType.AGGREGATE)
            .withDeterministic(true)
            .withDefinitions(new FunctionDefinitionDTO[] {definition})
            .build();

    Assertions.assertThrows(IllegalArgumentException.class, invalidRequest::validate);
  }

  @Test
  public void testValidateTableFunction() {
    FunctionDefinitionDTO definition =
        FunctionDefinitionDTO.builder().withParameters(new FunctionParamDTO[0]).build();

    org.apache.gravitino.dto.function.FunctionColumnDTO col =
        org.apache.gravitino.dto.function.FunctionColumnDTO.builder()
            .withName("id")
            .withDataType(Types.IntegerType.get())
            .build();

    FunctionRegisterRequest validRequest =
        FunctionRegisterRequest.builder()
            .withName("table_func")
            .withFunctionType(FunctionType.TABLE)
            .withDeterministic(false)
            .withReturnColumns(new org.apache.gravitino.dto.function.FunctionColumnDTO[] {col})
            .withDefinitions(new FunctionDefinitionDTO[] {definition})
            .build();

    Assertions.assertDoesNotThrow(validRequest::validate);

    // Missing returnColumns for TABLE function
    FunctionRegisterRequest invalidRequest =
        FunctionRegisterRequest.builder()
            .withName("table_func")
            .withFunctionType(FunctionType.TABLE)
            .withDeterministic(false)
            .withDefinitions(new FunctionDefinitionDTO[] {definition})
            .build();

    Assertions.assertThrows(IllegalArgumentException.class, invalidRequest::validate);
  }

  @Test
  public void testValidateMissingName() {
    FunctionDefinitionDTO definition =
        FunctionDefinitionDTO.builder().withParameters(new FunctionParamDTO[0]).build();

    FunctionRegisterRequest invalidRequest =
        FunctionRegisterRequest.builder()
            .withFunctionType(FunctionType.SCALAR)
            .withDeterministic(true)
            .withReturnType(Types.IntegerType.get())
            .withDefinitions(new FunctionDefinitionDTO[] {definition})
            .build();

    Assertions.assertThrows(IllegalArgumentException.class, invalidRequest::validate);
  }

  @Test
  public void testValidateMissingDefinitions() {
    FunctionRegisterRequest invalidRequest =
        FunctionRegisterRequest.builder()
            .withName("test_func")
            .withFunctionType(FunctionType.SCALAR)
            .withDeterministic(true)
            .withReturnType(Types.IntegerType.get())
            .build();

    Assertions.assertThrows(IllegalArgumentException.class, invalidRequest::validate);
  }
}
