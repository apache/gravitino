/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.mapper;

import com.datastrato.gravitino.server.web.Utils;
import com.fasterxml.jackson.core.JsonProcessingException;
import javax.annotation.Priority;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

/**
 * JsonProcessingExceptionMapper is used for returning consistent format of response when a general
 * Json issue occurs.
 */
@Priority(1)
public class JsonProcessingExceptionMapper implements ExceptionMapper<JsonProcessingException> {
  @Override
  public Response toResponse(JsonProcessingException e) {
    String errorMsg = "Unexpected error occurs when json processing.";
    return Utils.internalError(errorMsg, e);
  }
}
