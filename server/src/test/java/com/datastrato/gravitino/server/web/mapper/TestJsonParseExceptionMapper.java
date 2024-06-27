/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.mapper;

import com.datastrato.gravitino.dto.responses.ErrorResponse;
import com.fasterxml.jackson.core.JsonParseException;
import javax.ws.rs.core.Response;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestJsonParseExceptionMapper {
  private final JsonParseExceptionMapper jsonParseExceptionMapper = new JsonParseExceptionMapper();

  @Test
  public void testJsonParseExceptionMapper() {
    Response response = jsonParseExceptionMapper.toResponse(new JsonParseException(""));
    ErrorResponse errorResponse = ErrorResponse.illegalArguments("Malformed json request");
    ErrorResponse entity = (ErrorResponse) response.getEntity();
    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    Assertions.assertEquals(errorResponse.getCode(), entity.getCode());
    Assertions.assertEquals(errorResponse.getMessage(), entity.getMessage());
    Assertions.assertEquals(errorResponse.getType(), entity.getType());
  }
}
