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
import org.apache.gravitino.dto.function.FunctionColumnDTO;
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
            .withReturnType(Types.IntegerType.get())
            .withImpls(new SQLImplDTO[] {impl})
            .build();

    FunctionRegisterRequest request =
        FunctionRegisterRequest.builder()
            .withName("test_func")
            .withFunctionType(FunctionType.SCALAR)
            .withDeterministic(true)
            .withComment("test function")
            .withDefinitions(new FunctionDefinitionDTO[] {definition})
            .build();

    String json = JsonUtils.objectMapper().writeValueAsString(request);
    FunctionRegisterRequest deserialized =
        JsonUtils.objectMapper().readValue(json, FunctionRegisterRequest.class);

    Assertions.assertEquals(request, deserialized);
  }

  @Test
  public void testValidateScalarFunction() {
    // Definition with returnType for SCALAR function
    FunctionDefinitionDTO validDefinition =
        FunctionDefinitionDTO.builder()
            .withParameters(new FunctionParamDTO[0])
            .withReturnType(Types.IntegerType.get())
            .build();

    FunctionRegisterRequest validRequest =
        FunctionRegisterRequest.builder()
            .withName("scalar_func")
            .withFunctionType(FunctionType.SCALAR)
            .withDeterministic(true)
            .withDefinitions(new FunctionDefinitionDTO[] {validDefinition})
            .build();

    Assertions.assertDoesNotThrow(validRequest::validate);

    // Definition without returnType for SCALAR function should fail
    FunctionDefinitionDTO invalidDefinition =
        FunctionDefinitionDTO.builder().withParameters(new FunctionParamDTO[0]).build();

    FunctionRegisterRequest invalidRequest =
        FunctionRegisterRequest.builder()
            .withName("scalar_func")
            .withFunctionType(FunctionType.SCALAR)
            .withDeterministic(true)
            .withDefinitions(new FunctionDefinitionDTO[] {invalidDefinition})
            .build();

    Assertions.assertThrows(IllegalArgumentException.class, invalidRequest::validate);
  }

  @Test
  public void testValidateAggregateFunction() {
    // Definition with returnType for AGGREGATE function
    FunctionDefinitionDTO validDefinition =
        FunctionDefinitionDTO.builder()
            .withParameters(new FunctionParamDTO[0])
            .withReturnType(Types.IntegerType.get())
            .build();

    FunctionRegisterRequest validRequest =
        FunctionRegisterRequest.builder()
            .withName("agg_func")
            .withFunctionType(FunctionType.AGGREGATE)
            .withDeterministic(true)
            .withDefinitions(new FunctionDefinitionDTO[] {validDefinition})
            .build();

    Assertions.assertDoesNotThrow(validRequest::validate);

    // Definition without returnType for AGGREGATE function should fail
    FunctionDefinitionDTO invalidDefinition =
        FunctionDefinitionDTO.builder().withParameters(new FunctionParamDTO[0]).build();

    FunctionRegisterRequest invalidRequest =
        FunctionRegisterRequest.builder()
            .withName("agg_func")
            .withFunctionType(FunctionType.AGGREGATE)
            .withDeterministic(true)
            .withDefinitions(new FunctionDefinitionDTO[] {invalidDefinition})
            .build();

    Assertions.assertThrows(IllegalArgumentException.class, invalidRequest::validate);
  }

  @Test
  public void testValidateTableFunction() {
    FunctionColumnDTO col =
        FunctionColumnDTO.builder().withName("id").withDataType(Types.IntegerType.get()).build();

    // Definition with returnColumns for TABLE function
    FunctionDefinitionDTO validDefinition =
        FunctionDefinitionDTO.builder()
            .withParameters(new FunctionParamDTO[0])
            .withReturnColumns(new FunctionColumnDTO[] {col})
            .build();

    FunctionRegisterRequest validRequest =
        FunctionRegisterRequest.builder()
            .withName("table_func")
            .withFunctionType(FunctionType.TABLE)
            .withDeterministic(false)
            .withDefinitions(new FunctionDefinitionDTO[] {validDefinition})
            .build();

    Assertions.assertDoesNotThrow(validRequest::validate);

    // Definition without returnColumns for TABLE function should fail
    FunctionDefinitionDTO invalidDefinition =
        FunctionDefinitionDTO.builder().withParameters(new FunctionParamDTO[0]).build();

    FunctionRegisterRequest invalidRequest =
        FunctionRegisterRequest.builder()
            .withName("table_func")
            .withFunctionType(FunctionType.TABLE)
            .withDeterministic(false)
            .withDefinitions(new FunctionDefinitionDTO[] {invalidDefinition})
            .build();

    Assertions.assertThrows(IllegalArgumentException.class, invalidRequest::validate);
  }

  @Test
  public void testValidateMissingName() {
    FunctionDefinitionDTO definition =
        FunctionDefinitionDTO.builder()
            .withParameters(new FunctionParamDTO[0])
            .withReturnType(Types.IntegerType.get())
            .build();

    FunctionRegisterRequest invalidRequest =
        FunctionRegisterRequest.builder()
            .withFunctionType(FunctionType.SCALAR)
            .withDeterministic(true)
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
            .build();

    Assertions.assertThrows(IllegalArgumentException.class, invalidRequest::validate);
  }

  @Test
  public void testValidateMultipleDefinitionsWithDifferentReturnTypes() {
    // Test that multiple definitions can have different return types
    FunctionParamDTO intParam =
        FunctionParamDTO.builder().withName("x").withDataType(Types.IntegerType.get()).build();
    FunctionParamDTO floatParam =
        FunctionParamDTO.builder().withName("x").withDataType(Types.FloatType.get()).build();

    FunctionDefinitionDTO intDef =
        FunctionDefinitionDTO.builder()
            .withParameters(new FunctionParamDTO[] {intParam})
            .withReturnType(Types.IntegerType.get())
            .build();

    FunctionDefinitionDTO floatDef =
        FunctionDefinitionDTO.builder()
            .withParameters(new FunctionParamDTO[] {floatParam})
            .withReturnType(Types.FloatType.get())
            .build();

    FunctionRegisterRequest request =
        FunctionRegisterRequest.builder()
            .withName("overloaded_func")
            .withFunctionType(FunctionType.SCALAR)
            .withDeterministic(true)
            .withDefinitions(new FunctionDefinitionDTO[] {intDef, floatDef})
            .build();

    // This should succeed - different definitions can have different return types
    Assertions.assertDoesNotThrow(request::validate);
  }
}
