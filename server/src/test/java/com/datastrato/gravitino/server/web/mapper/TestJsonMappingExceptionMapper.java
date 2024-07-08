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
package com.datastrato.gravitino.server.web.mapper;

import com.datastrato.gravitino.dto.responses.ErrorResponse;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonMappingException;
import javax.ws.rs.core.Response;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestJsonMappingExceptionMapper {
  private final JsonMappingExceptionMapper jsonMappingExceptionMapper =
      new JsonMappingExceptionMapper();

  @Test
  public void testJsonMappingExceptionMapper() {
    JsonParser mockParser = Mockito.mock(JsonParser.class);
    Response response =
        jsonMappingExceptionMapper.toResponse(JsonMappingException.from(mockParser, ""));
    ErrorResponse errorResponse = ErrorResponse.illegalArguments("Malformed json request");
    ErrorResponse entity = (ErrorResponse) response.getEntity();
    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    Assertions.assertEquals(errorResponse.getCode(), entity.getCode());
    Assertions.assertEquals(errorResponse.getMessage(), entity.getMessage());
    Assertions.assertEquals(errorResponse.getType(), entity.getType());
  }
}
