/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.mapper;

import com.datastrato.gravitino.dto.responses.ErrorResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import javax.ws.rs.core.Response;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestJsonProcessingExceptionMapper {
  private final JsonProcessingExceptionMapper jsonProcessingExceptionMapper =
      new JsonProcessingExceptionMapper();

  @Test
  public void testJsonProcessingExceptionMapper() {
    Response response =
        jsonProcessingExceptionMapper.toResponse(new JsonProcessingException("") {});
    ErrorResponse errorResponse =
        ErrorResponse.internalError("Unexpected error occurs when json processing.");
    ErrorResponse entity = (ErrorResponse) response.getEntity();
    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
    Assertions.assertEquals(errorResponse.getCode(), entity.getCode());
    Assertions.assertEquals(errorResponse.getMessage(), entity.getMessage());
    Assertions.assertEquals(errorResponse.getType(), entity.getType());
  }
}
