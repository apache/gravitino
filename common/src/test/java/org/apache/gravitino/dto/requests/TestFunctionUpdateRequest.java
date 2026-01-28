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
import org.apache.gravitino.dto.function.FunctionImplDTO;
import org.apache.gravitino.dto.function.FunctionParamDTO;
import org.apache.gravitino.dto.function.SQLImplDTO;
import org.apache.gravitino.function.FunctionChange;
import org.apache.gravitino.function.FunctionImpl;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestFunctionUpdateRequest {

  @Test
  public void testUpdateCommentRequestSerDe() throws JsonProcessingException {
    FunctionUpdateRequest.UpdateCommentRequest request =
        new FunctionUpdateRequest.UpdateCommentRequest("new comment");

    String json = JsonUtils.objectMapper().writeValueAsString(request);
    FunctionUpdateRequest deserialized =
        JsonUtils.objectMapper().readValue(json, FunctionUpdateRequest.class);

    Assertions.assertInstanceOf(FunctionUpdateRequest.UpdateCommentRequest.class, deserialized);
    Assertions.assertEquals(request, deserialized);
  }

  @Test
  public void testUpdateCommentRequestValidate() {
    // null comment is allowed (to clear comment)
    FunctionUpdateRequest.UpdateCommentRequest request =
        new FunctionUpdateRequest.UpdateCommentRequest(null);
    Assertions.assertDoesNotThrow(request::validate);

    // non-null comment is also valid
    FunctionUpdateRequest.UpdateCommentRequest request2 =
        new FunctionUpdateRequest.UpdateCommentRequest("new comment");
    Assertions.assertDoesNotThrow(request2::validate);
  }

  @Test
  public void testUpdateCommentRequestFunctionChange() {
    FunctionUpdateRequest.UpdateCommentRequest request =
        new FunctionUpdateRequest.UpdateCommentRequest("new comment");
    FunctionChange change = request.functionChange();
    Assertions.assertNotNull(change);
  }

  @Test
  public void testAddDefinitionRequestSerDe() throws JsonProcessingException {
    FunctionParamDTO param =
        FunctionParamDTO.builder().withName("x").withDataType(Types.IntegerType.get()).build();
    SQLImplDTO impl = new SQLImplDTO(FunctionImpl.RuntimeType.SPARK.name(), null, null, "SELECT x");
    FunctionDefinitionDTO definition =
        FunctionDefinitionDTO.builder()
            .withParameters(new FunctionParamDTO[] {param})
            .withImpls(new FunctionImplDTO[] {impl})
            .build();

    FunctionUpdateRequest.AddDefinitionRequest request =
        new FunctionUpdateRequest.AddDefinitionRequest(definition);

    String json = JsonUtils.objectMapper().writeValueAsString(request);
    FunctionUpdateRequest deserialized =
        JsonUtils.objectMapper().readValue(json, FunctionUpdateRequest.class);

    Assertions.assertInstanceOf(FunctionUpdateRequest.AddDefinitionRequest.class, deserialized);
    Assertions.assertEquals(request, deserialized);
  }

  @Test
  public void testAddDefinitionRequestValidate() {
    // Valid request
    FunctionDefinitionDTO definition =
        FunctionDefinitionDTO.builder().withParameters(new FunctionParamDTO[0]).build();
    FunctionUpdateRequest.AddDefinitionRequest validRequest =
        new FunctionUpdateRequest.AddDefinitionRequest(definition);
    Assertions.assertDoesNotThrow(validRequest::validate);

    // Invalid request - null definition
    FunctionUpdateRequest.AddDefinitionRequest invalidRequest =
        new FunctionUpdateRequest.AddDefinitionRequest(null);
    Assertions.assertThrows(IllegalArgumentException.class, invalidRequest::validate);
  }

  @Test
  public void testRemoveDefinitionRequestSerDe() throws JsonProcessingException {
    FunctionParamDTO param =
        FunctionParamDTO.builder().withName("x").withDataType(Types.IntegerType.get()).build();

    FunctionUpdateRequest.RemoveDefinitionRequest request =
        new FunctionUpdateRequest.RemoveDefinitionRequest(new FunctionParamDTO[] {param});

    String json = JsonUtils.objectMapper().writeValueAsString(request);
    FunctionUpdateRequest deserialized =
        JsonUtils.objectMapper().readValue(json, FunctionUpdateRequest.class);

    Assertions.assertInstanceOf(FunctionUpdateRequest.RemoveDefinitionRequest.class, deserialized);
    Assertions.assertEquals(request, deserialized);
  }

  @Test
  public void testRemoveDefinitionRequestValidate() {
    // Valid request
    FunctionUpdateRequest.RemoveDefinitionRequest validRequest =
        new FunctionUpdateRequest.RemoveDefinitionRequest(new FunctionParamDTO[0]);
    Assertions.assertDoesNotThrow(validRequest::validate);

    // Invalid request - null parameters
    FunctionUpdateRequest.RemoveDefinitionRequest invalidRequest =
        new FunctionUpdateRequest.RemoveDefinitionRequest(null);
    Assertions.assertThrows(IllegalArgumentException.class, invalidRequest::validate);
  }

  @Test
  public void testAddImplRequestSerDe() throws JsonProcessingException {
    FunctionParamDTO param =
        FunctionParamDTO.builder().withName("x").withDataType(Types.IntegerType.get()).build();
    SQLImplDTO impl = new SQLImplDTO(FunctionImpl.RuntimeType.SPARK.name(), null, null, "SELECT x");

    FunctionUpdateRequest.AddImplRequest request =
        new FunctionUpdateRequest.AddImplRequest(new FunctionParamDTO[] {param}, impl);

    String json = JsonUtils.objectMapper().writeValueAsString(request);
    FunctionUpdateRequest deserialized =
        JsonUtils.objectMapper().readValue(json, FunctionUpdateRequest.class);

    Assertions.assertInstanceOf(FunctionUpdateRequest.AddImplRequest.class, deserialized);
    Assertions.assertEquals(request, deserialized);
  }

  @Test
  public void testAddImplRequestValidate() {
    SQLImplDTO impl = new SQLImplDTO(FunctionImpl.RuntimeType.SPARK.name(), null, null, "SELECT 1");

    // Valid request
    FunctionUpdateRequest.AddImplRequest validRequest =
        new FunctionUpdateRequest.AddImplRequest(new FunctionParamDTO[0], impl);
    Assertions.assertDoesNotThrow(validRequest::validate);

    // Invalid request - null parameters
    FunctionUpdateRequest.AddImplRequest invalidRequest1 =
        new FunctionUpdateRequest.AddImplRequest(null, impl);
    Assertions.assertThrows(IllegalArgumentException.class, invalidRequest1::validate);

    // Invalid request - null implementation
    FunctionUpdateRequest.AddImplRequest invalidRequest2 =
        new FunctionUpdateRequest.AddImplRequest(new FunctionParamDTO[0], null);
    Assertions.assertThrows(IllegalArgumentException.class, invalidRequest2::validate);
  }

  @Test
  public void testUpdateImplRequestSerDe() throws JsonProcessingException {
    FunctionParamDTO param =
        FunctionParamDTO.builder().withName("x").withDataType(Types.IntegerType.get()).build();
    SQLImplDTO impl = new SQLImplDTO(FunctionImpl.RuntimeType.SPARK.name(), null, null, "SELECT x");

    FunctionUpdateRequest.UpdateImplRequest request =
        new FunctionUpdateRequest.UpdateImplRequest(
            new FunctionParamDTO[] {param}, FunctionImpl.RuntimeType.SPARK.name(), impl);

    String json = JsonUtils.objectMapper().writeValueAsString(request);
    FunctionUpdateRequest deserialized =
        JsonUtils.objectMapper().readValue(json, FunctionUpdateRequest.class);

    Assertions.assertInstanceOf(FunctionUpdateRequest.UpdateImplRequest.class, deserialized);
    Assertions.assertEquals(request, deserialized);
  }

  @Test
  public void testUpdateImplRequestValidate() {
    SQLImplDTO impl = new SQLImplDTO(FunctionImpl.RuntimeType.SPARK.name(), null, null, "SELECT 1");

    // Valid request
    FunctionUpdateRequest.UpdateImplRequest validRequest =
        new FunctionUpdateRequest.UpdateImplRequest(
            new FunctionParamDTO[0], FunctionImpl.RuntimeType.SPARK.name(), impl);
    Assertions.assertDoesNotThrow(validRequest::validate);

    // Invalid request - null parameters
    FunctionUpdateRequest.UpdateImplRequest invalidRequest1 =
        new FunctionUpdateRequest.UpdateImplRequest(
            null, FunctionImpl.RuntimeType.SPARK.name(), impl);
    Assertions.assertThrows(IllegalArgumentException.class, invalidRequest1::validate);

    // Invalid request - null runtime
    FunctionUpdateRequest.UpdateImplRequest invalidRequest2 =
        new FunctionUpdateRequest.UpdateImplRequest(new FunctionParamDTO[0], null, impl);
    Assertions.assertThrows(IllegalArgumentException.class, invalidRequest2::validate);

    // Invalid request - null implementation
    FunctionUpdateRequest.UpdateImplRequest invalidRequest3 =
        new FunctionUpdateRequest.UpdateImplRequest(
            new FunctionParamDTO[0], FunctionImpl.RuntimeType.SPARK.name(), null);
    Assertions.assertThrows(IllegalArgumentException.class, invalidRequest3::validate);
  }

  @Test
  public void testRemoveImplRequestSerDe() throws JsonProcessingException {
    FunctionParamDTO param =
        FunctionParamDTO.builder().withName("x").withDataType(Types.IntegerType.get()).build();

    FunctionUpdateRequest.RemoveImplRequest request =
        new FunctionUpdateRequest.RemoveImplRequest(
            new FunctionParamDTO[] {param}, FunctionImpl.RuntimeType.SPARK.name());

    String json = JsonUtils.objectMapper().writeValueAsString(request);
    FunctionUpdateRequest deserialized =
        JsonUtils.objectMapper().readValue(json, FunctionUpdateRequest.class);

    Assertions.assertInstanceOf(FunctionUpdateRequest.RemoveImplRequest.class, deserialized);
    Assertions.assertEquals(request, deserialized);
  }

  @Test
  public void testRemoveImplRequestValidate() {
    // Valid request
    FunctionUpdateRequest.RemoveImplRequest validRequest =
        new FunctionUpdateRequest.RemoveImplRequest(
            new FunctionParamDTO[0], FunctionImpl.RuntimeType.SPARK.name());
    Assertions.assertDoesNotThrow(validRequest::validate);

    // Invalid request - null parameters
    FunctionUpdateRequest.RemoveImplRequest invalidRequest1 =
        new FunctionUpdateRequest.RemoveImplRequest(null, FunctionImpl.RuntimeType.SPARK.name());
    Assertions.assertThrows(IllegalArgumentException.class, invalidRequest1::validate);

    // Invalid request - null runtime
    FunctionUpdateRequest.RemoveImplRequest invalidRequest2 =
        new FunctionUpdateRequest.RemoveImplRequest(new FunctionParamDTO[0], null);
    Assertions.assertThrows(IllegalArgumentException.class, invalidRequest2::validate);
  }
}
